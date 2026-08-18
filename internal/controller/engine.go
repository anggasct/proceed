package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/executor"
	"proceed/internal/store"
)

type RunInput struct {
	GraphVersionID string
}

func (c *Controller) Run(ctx context.Context, input RunInput) (string, error) {
	if err := c.acquireLease(ctx, time.Now()); err != nil {
		return "", err
	}
	run, err := c.store.CreateRun(ctx, input.GraphVersionID)
	if err != nil {
		return "", err
	}
	return run.ID, nil
}

type runnableNode struct {
	NodeKey   string
	NodeType  string
	Config    string
	AttemptNo int64

	MaxAttempts int64
	BackoffMs   int64
}

func (c *Controller) eligibleNodes(ctx context.Context, runID, graphVersionID string) ([]runnableNode, error) {
	defRows, err := c.store.DB().QueryContext(ctx, `
SELECT gn.node_key, gn.type, gn.config, COALESCE(rn.attempt_count, 0) + 1, rn.status
FROM graph_node gn
LEFT JOIN run_node rn ON rn.node_key = gn.node_key AND rn.run_id = ?
WHERE gn.graph_version_id = ?
ORDER BY gn.node_key`, runID, graphVersionID)
	if err != nil {
		return nil, err
	}
	defer defRows.Close()

	type defNode struct {
		runnableNode
		runStatus sql.NullString
	}
	defs := map[string]defNode{}
	var order []string
	for defRows.Next() {
		var d defNode
		if err := defRows.Scan(&d.NodeKey, &d.NodeType, &d.Config, &d.AttemptNo, &d.runStatus); err != nil {
			return nil, err
		}
		defs[d.NodeKey] = d
		order = append(order, d.NodeKey)
	}
	if err := defRows.Err(); err != nil {
		return nil, err
	}

	edgeRows, err := c.store.DB().QueryContext(ctx, `
SELECT ge.id, ge.to_node_key, ge.from_node_key, ge.type FROM graph_edge ge
WHERE ge.graph_version_id = ?`, graphVersionID)
	if err != nil {
		return nil, err
	}
	type edge struct {
		id, to, from, typ string
	}
	var edges []edge
	for edgeRows.Next() {
		var e edge
		if err := edgeRows.Scan(&e.id, &e.to, &e.from, &e.typ); err != nil {
			edgeRows.Close()
			return nil, err
		}
		edges = append(edges, e)
	}
	edgeRows.Close()
	if err := edgeRows.Err(); err != nil {
		return nil, err
	}

	traversed := map[string]bool{}
	travRows, err := c.store.DB().QueryContext(ctx,
		"SELECT edge_id FROM run_edge WHERE run_id = ?", runID)
	if err != nil {
		return nil, err
	}
	for travRows.Next() {
		var id string
		if err := travRows.Scan(&id); err != nil {
			travRows.Close()
			return nil, err
		}
		traversed[id] = true
	}
	travRows.Close()
	if err := travRows.Err(); err != nil {
		return nil, err
	}

	done := func(key string) bool {
		d, ok := defs[key]
		return ok && d.runStatus.Valid && (d.runStatus.String == "succeeded" || d.runStatus.String == "skipped")
	}

	var out []runnableNode
	for _, key := range order {
		d := defs[key]
		if d.runStatus.Valid {
			if d.runStatus.String != "eligible" {
				continue
			}
			out = append(out, d.runnableNode)
			continue
		}
		ready := true
		for _, e := range edges {
			if e.to != key {
				continue
			}
			switch e.typ {
			case "depends_on", "produces", "consumes":
				if !done(e.from) {
					ready = false
				}
			case "routes_to":
				if !traversed[e.id] {
					ready = false
				}
			}
			if !ready {
				break
			}
		}
		if ready {
			out = append(out, d.runnableNode)
		}
	}
	return out, nil
}

func (c *Controller) parseNodeConfig(config string) (map[string]any, executor.Kind, executor.Contract, error) {
	var cfg map[string]any
	if err := json.Unmarshal([]byte(config), &cfg); err != nil {
		return nil, "", "", fmt.Errorf("node config: %w", err)
	}
	ex, kind, contract, err := c.resolveExecutor(cfg)
	if err != nil {
		return nil, "", "", err
	}
	_ = ex
	return cfg, kind, contract, nil
}

func (c *Controller) resolveExecutor(cfg map[string]any) (executor.Executor, executor.Kind, executor.Contract, error) {
	rawExec, ok := cfg["executor"].(map[string]any)
	if !ok {
		return nil, "", "", fmt.Errorf("node has no executor config")
	}
	kindStr, _ := rawExec["kind"].(string)
	kind := executor.Kind(kindStr)
	ex, ok := c.pool[kind]
	if !ok {
		return nil, "", "", fmt.Errorf("no executor registered for kind %q", kindStr)
	}
	contractStr, _ := cfg["contract"].(string)
	contract := executor.Contract(contractStr)
	switch contract {
	case executor.Pure, executor.Idempotent, executor.Reconcilable, executor.NonReplayable:
	default:
		return nil, "", "", fmt.Errorf("node contract %q is not a known side-effect contract", contractStr)
	}
	return ex, kind, contract, nil
}

func retryPolicy(cfg map[string]any) (maxAttempts, backoffMs int64) {
	maxAttempts = 1
	if r, ok := cfg["retry"].(map[string]any); ok {
		if v, ok := r["max_attempts"].(float64); ok && int64(v) >= 1 {
			maxAttempts = int64(v)
		}
		if v, ok := r["backoff_ms"].(float64); ok && int64(v) >= 0 {
			backoffMs = int64(v)
		}
	}
	return maxAttempts, backoffMs
}

func (c *Controller) claimNonExecutable(ctx context.Context, runID, nodeKey string) error {
	return c.appendEvent(ctx, runID, "node_started", map[string]any{
		"node_key":   nodeKey,
		"attempt_no": 1,
	})
}

func (c *Controller) executeNode(ctx context.Context, runID, graphVersionID, digest string, n runnableNode) error {
	if n.NodeType == "router" || n.NodeType == "gate" {
		if err := c.claimNonExecutable(ctx, runID, n.NodeKey); err != nil {
			return err
		}
		if n.NodeType == "gate" {
			return c.waitingNode(ctx, runID, n.NodeKey)
		}
		return c.commitNodeSuccess(ctx, runID, graphVersionID, digest, n, &executor.Result{}, "")
	}

	var cfg map[string]any
	if err := json.Unmarshal([]byte(n.Config), &cfg); err != nil {
		return fmt.Errorf("node %s config: %w", n.NodeKey, err)
	}
	ex, kind, contract, err := c.resolveExecutor(cfg)
	if err != nil {
		return c.failNode(ctx, runID, n.NodeKey, n.AttemptNo, err)
	}
	maxAttempts, backoffMs := retryPolicy(cfg)
	n.MaxAttempts, n.BackoffMs = maxAttempts, backoffMs

	opKey := OperationKey(runID, digest, n.NodeKey, n.AttemptNo)
	leaseToken := ulid.Make().String()
	leaseExpiry := time.Now().Add(c.cfg.LeaseTTL).UnixMilli()

	claimed, execCtx, cancelExec, err := c.beginClaimedAttempt(ctx, runID, n, kind, contract, opKey, leaseToken, leaseExpiry)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	c.trackInflight(runID, n.NodeKey, cancelExec)
	defer func() {
		c.untrackInflight(runID, n.NodeKey)
		cancelExec()
	}()

	cancelChan := make(chan struct{})
	req := &executor.Request{
		RunID:            runID,
		GraphVersionID:   graphVersionID,
		DefinitionDigest: digest,
		NodeKey:          n.NodeKey,
		AttemptNo:        n.AttemptNo,
		OperationKey:     opKey,
		Config:           cfg,
		Cancellation:     cancelChan,
	}
	if v, ok := cfg["timeout_ms"].(float64); ok && int64(v) > 0 {
		req.TimeoutMs = int64(v)
	}

	var execErr error
	var result *executor.Result
	done := make(chan struct{})
	go func() {
		defer close(done)
		result, execErr = ex.Execute(execCtx, req)
	}()

	timedOut := false
	var timer <-chan time.Time
	if req.TimeoutMs > 0 {
		t := time.NewTimer(time.Duration(req.TimeoutMs) * time.Millisecond)
		defer t.Stop()
		timer = t.C
	}
watch:
	for {
		select {
		case <-done:
			break watch
		case <-timer:
			timedOut = true
			cancelExec()
			close(cancelChan)
			<-done
			break watch
		case <-ctx.Done():
			cancelExec()
			close(cancelChan)
			<-done
			return errLeaseLost
		}
	}

	if timedOut {
		execErr = executor.ErrTimeout
	}
	if execErr != nil {
		if errors.Is(execErr, executor.ErrUncertain) {
			return c.uncertainNode(ctx, runID, n.NodeKey, n.AttemptNo, execErr)
		}
		if errors.Is(execErr, executor.ErrCancelled) {
			return c.cancelledNode(ctx, runID, n.NodeKey, n.AttemptNo)
		}
		return c.recordAttemptFailure(ctx, runID, graphVersionID, digest, n, execErr)
	}
	if result == nil {
		result = &executor.Result{}
	}

	if err := c.leaseValid(ctx); err != nil {
		return c.uncertainNode(ctx, runID, n.NodeKey, n.AttemptNo, errLeaseLost)
	}

	if c.nodeCancelled(ctx, runID, n.NodeKey) {
		return c.cancelledNode(ctx, runID, n.NodeKey, n.AttemptNo)
	}

	if err := c.commitNodeSuccess(ctx, runID, graphVersionID, digest, n, result, opKey); err != nil {
		return err
	}
	return c.routeFrom(ctx, runID, graphVersionID, n, result)
}

func (c *Controller) nodeCancelled(ctx context.Context, runID, nodeKey string) bool {
	var count int
	if err := c.store.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM run_node WHERE run_id = ? AND node_key = ? AND status IN ('cancel_requested','cancelled')",
		runID, nodeKey).Scan(&count); err != nil {
		return false
	}
	return count > 0
}

func (c *Controller) beginClaimedAttempt(ctx context.Context, runID string, n runnableNode, kind executor.Kind, contract executor.Contract, opKey, leaseToken string, leaseExpiry int64) (bool, context.Context, context.CancelFunc, error) {
	execCtx, cancelExec := context.WithCancel(ctx)
	nowMs := time.Now().UnixMilli()
	var claimed bool
	err := c.store.WithTx(ctx, func(tx *sql.Tx) error {
		claimed = false
		var current sql.NullString
		err := tx.QueryRowContext(ctx,
			"SELECT status FROM run_node WHERE run_id = ? AND node_key = ?", runID, n.NodeKey).Scan(&current)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		if err == nil && (!current.Valid || (current.String != "eligible" && current.String != "running")) {
			return nil
		}
		var cancelledCount int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM run_node WHERE run_id = ? AND node_key = ? AND status IN ('cancel_requested','cancelled')",
			runID, n.NodeKey).Scan(&cancelledCount); err != nil {
			return err
		}
		if cancelledCount > 0 {
			return nil
		}
		ev := store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "node_started",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			CorrelationID: opKey,
			Payload: payloadJSON(map[string]any{
				"node_key":             n.NodeKey,
				"attempt_no":           n.AttemptNo,
				"executor":             string(kind),
				"side_effect_contract": string(contract),
				"operation_key":        opKey,
				"lease_token":          leaseToken,
				"lease_expires_at":     leaseExpiry,
			}),
		}
		if _, err := c.appendWithin(ctx, tx, &ev); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	if err != nil {
		cancelExec()
		return false, nil, nil, err
	}
	return claimed, execCtx, cancelExec, nil
}
