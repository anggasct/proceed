package controller

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"proceed/internal/executor"
	"proceed/internal/store"
)

func (c *Controller) Recover(ctx context.Context, runID string) error {
	run, err := c.loadRun(ctx, runID)
	if err != nil {
		return err
	}
	rows, err := c.store.DB().QueryContext(ctx, `
SELECT rn.node_key, rn.attempt_count + 1, gn.config
FROM run_node rn
JOIN graph_node gn ON gn.node_key = rn.node_key AND gn.graph_version_id = ?
WHERE rn.run_id = ? AND rn.status IN ('uncertain', 'leased', 'running')`,
		run.graphVersionID, runID)
	if err != nil {
		return err
	}
	type uncertain struct {
		key     string
		attempt int64
		config  string
	}
	var list []uncertain
	for rows.Next() {
		var u uncertain
		if err := rows.Scan(&u.key, &u.attempt, &u.config); err != nil {
			rows.Close()
			return err
		}
		list = append(list, u)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, u := range list {
		if err := c.recoverNode(ctx, runID, run.graphVersionID, run.digest, u.key, u.attempt, u.config); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) recoverNode(ctx context.Context, runID, graphVersionID, digest, nodeKey string, nextAttempt int64, config string) error {
	_, contract, err := c.contractFor(config)
	if err != nil {
		return err
	}
	switch contract {
	case executor.Pure, executor.Idempotent:
		_, err := c.store.DB().ExecContext(ctx,
			"UPDATE run_node SET status = 'eligible' WHERE run_id = ? AND node_key = ?", runID, nodeKey)
		return err
	case executor.Reconcilable:
		return c.reconcileNode(ctx, runID, graphVersionID, digest, nodeKey, nextAttempt, config)
	default:
		_, err := c.store.DB().ExecContext(ctx, `
UPDATE run_node SET status = 'waiting' WHERE run_id = ? AND node_key = ? AND status = 'uncertain'`,
			runID, nodeKey)
		return err
	}
}

func (c *Controller) reconcileNode(ctx context.Context, runID, graphVersionID, digest, nodeKey string, nextAttempt int64, config string) error {
	kind, _, err := c.contractFor(config)
	if err != nil {
		return err
	}
	ex, ok := c.pool[kind]
	if !ok {
		return c.escalateNode(ctx, runID, nodeKey, "no executor for reconcile")
	}
	recon, ok := ex.(executor.Reconciler)
	if !ok {
		return c.escalateNode(ctx, runID, nodeKey, "executor does not support reconcile")
	}
	_, err = c.store.DB().ExecContext(ctx,
		"UPDATE run_node SET status = 'reconciling' WHERE run_id = ? AND node_key = ?", runID, nodeKey)
	if err != nil {
		return err
	}
	req := &executor.Request{
		RunID:            runID,
		GraphVersionID:   graphVersionID,
		DefinitionDigest: digest,
		NodeKey:          nodeKey,
		AttemptNo:        nextAttempt,
		OperationKey:     OperationKey(runID, digest, nodeKey, nextAttempt-1),
	}
	state, rerr := recon.Reconcile(ctx, req)
	if rerr != nil || state == executor.EffectUnknown {
		return c.escalateNode(ctx, runID, nodeKey, "reconcile returned unknown")
	}
	if state == executor.EffectConfirmed {
		return c.completeFromReconcile(ctx, runID, graphVersionID, nodeKey, nextAttempt-1)
	}
	_, err = c.store.DB().ExecContext(ctx,
		"UPDATE run_node SET status = 'eligible' WHERE run_id = ? AND node_key = ?", runID, nodeKey)
	return err
}

func (c *Controller) completeFromReconcile(ctx context.Context, runID, graphVersionID, nodeKey string, attemptNo int64) error {
	nowMs := time.Now().UnixMilli()
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		ev := store.Event{
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "node_finished",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload: payloadJSON(map[string]any{
				"node_key":   nodeKey,
				"attempt_no": attemptNo,
				"reconciled": true,
			}),
		}
		if _, err := c.appendWithin(ctx, tx, &ev); err != nil {
			return err
		}
		return markEligibleAfter(ctx, tx, runID, graphVersionID, nowMs)
	})
}

func (c *Controller) escalateNode(ctx context.Context, runID, nodeKey, reason string) error {
	_, err := c.store.DB().ExecContext(ctx,
		"UPDATE run_node SET status = 'waiting' WHERE run_id = ? AND node_key = ?", runID, nodeKey)
	return err
}

func (c *Controller) contractFor(config string) (executor.Kind, executor.Contract, error) {
	_, kind, contract, err := c.parseNodeConfig(config)
	return kind, contract, err
}

var _ = errors.Is
var _ = sql.ErrNoRows
