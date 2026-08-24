package controller

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"proceed/internal/executor"
)

func (c *Controller) contractFor(config string) (executor.Kind, executor.Contract, error) {
	_, kind, contract, err := c.parseNodeConfig(config)
	return kind, contract, err
}

func (c *Controller) Recover(ctx context.Context, runID string) error {
	nowMs := time.Now().UnixMilli()
	rows, err := c.store.DB().QueryContext(ctx, `
SELECT rn.node_key, rn.status, gn.config,
       (SELECT na.lease_expires_at FROM node_attempt na
         WHERE na.run_node_id = rn.id ORDER BY na.attempt_no DESC LIMIT 1)
FROM run_node rn
JOIN graph_node gn ON gn.node_key = rn.node_key AND gn.graph_version_id = (
  SELECT graph_version_id FROM graph_run WHERE id = ?
)
	WHERE rn.run_id = ? AND rn.status IN ('uncertain', 'leased', 'running', 'reconciling', 'cancel_requested')`, runID, runID)
	if err != nil {
		return err
	}
	type candidate struct {
		key    string
		status string
		config string
		expiry sql.NullInt64
	}
	var list []candidate
	for rows.Next() {
		var u candidate
		if err := rows.Scan(&u.key, &u.status, &u.config, &u.expiry); err != nil {
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
		if u.expiry.Valid && u.expiry.Int64 > nowMs {
			continue
		}
		if err := c.recoverNode(ctx, runID, u.key, u.config, u.status); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) recoverNode(ctx context.Context, runID, nodeKey, config, status string) error {
	_, contract, err := c.contractFor(config)
	if err != nil {
		return err
	}
	if status == "cancel_requested" {
		return c.recoverCancelledNode(ctx, runID, nodeKey, contract)
	}
	switch contract {
	case executor.Pure:
		return c.requeuedNode(ctx, runID, nodeKey, 0)
	case executor.Idempotent:
		return c.resumeIdempotentAttempt(ctx, runID, nodeKey)
	case executor.Reconcilable:
		return c.reconcileNode(ctx, runID, nodeKey)
	default:
		return c.waitingNode(ctx, runID, nodeKey)
	}
}

func (c *Controller) recoverCancelledNode(ctx context.Context, runID, nodeKey string, contract executor.Contract) error {
	_, attemptNo, err := c.latestAttempt(ctx, runID, nodeKey)
	if err != nil {
		return err
	}
	switch contract {
	case executor.Pure:
		return c.cancelledNode(ctx, runID, nodeKey, attemptNo)
	case executor.Reconcilable:
		return c.reconcileCancelledNode(ctx, runID, nodeKey, attemptNo)
	default:
		return c.waitingNode(ctx, runID, nodeKey)
	}
}

func (c *Controller) resumeIdempotentAttempt(ctx context.Context, runID, nodeKey string) error {
	var attemptNo int64
	if err := c.store.DB().QueryRowContext(ctx, `
SELECT na.attempt_no FROM node_attempt na
JOIN run_node rn ON rn.id = na.run_node_id
WHERE rn.run_id = ? AND rn.node_key = ?
ORDER BY na.attempt_no DESC LIMIT 1`, runID, nodeKey).Scan(&attemptNo); err != nil {
		return err
	}
	return c.appendEvent(ctx, runID, "node_requeued", map[string]any{
		"node_key":       nodeKey,
		"attempt_no":     attemptNo,
		"resume_attempt": true,
	})
}

func (c *Controller) reconcileNode(ctx context.Context, runID, nodeKey string) error {
	if err := c.appendEvent(ctx, runID, "node_reconciling", map[string]any{
		"node_key": nodeKey,
	}); err != nil {
		return err
	}
	run, err := c.loadRun(ctx, runID)
	if err != nil {
		return err
	}
	opKey, attemptNo, err := c.latestAttempt(ctx, runID, nodeKey)
	if err != nil {
		return err
	}
	cfg, kind, contract, err := c.parseNodeConfig(nodeConfigForKey(c, ctx, runID, run.graphVersionID, nodeKey))
	if err != nil {
		return err
	}
	ex, ok := c.pool[kind]
	if !ok {
		return c.waitingNode(ctx, runID, nodeKey)
	}
	req := &executor.Request{
		RunID:            runID,
		GraphVersionID:   run.graphVersionID,
		DefinitionDigest: run.digest,
		NodeKey:          nodeKey,
		AttemptNo:        attemptNo,
		OperationKey:     opKey,
		Contract:         contract,
		Config:           cfg,
		Secrets:          c.cfg.Secrets,
	}
	result, state, rerr := reconcileEffect(ctx, ex, req)
	if errors.Is(rerr, executor.ErrNotReconcilable) {
		return c.waitingNode(ctx, runID, nodeKey)
	}
	if rerr != nil || state == executor.EffectUnknown {
		if c.nodeCancelRequested(ctx, runID, nodeKey) {
			return nil
		}
		return c.waitingNode(ctx, runID, nodeKey)
	}
	if state == executor.EffectConfirmed {
		c.recordReconciledEffect(ctx, runID, nodeKey, opKey, attemptNo, executor.EffectConfirmed)
		return c.commitNodeSuccess(ctx, runID, run.graphVersionID, run.digest,
			runnableNode{NodeKey: nodeKey, AttemptNo: attemptNo}, result, opKey)
	}
	c.recordReconciledEffect(ctx, runID, nodeKey, opKey, attemptNo, executor.EffectRejected)
	if c.nodeCancelRequested(ctx, runID, nodeKey) {
		return c.cancelledNode(ctx, runID, nodeKey, attemptNo)
	}
	return c.requeuedNode(ctx, runID, nodeKey, attemptNo)
}

func reconcileEffect(ctx context.Context, ex executor.Executor, req *executor.Request) (*executor.Result, executor.EffectState, error) {
	if recon, ok := ex.(executor.ResultReconciler); ok {
		return recon.ReconcileResult(ctx, req)
	}
	if recon, ok := ex.(executor.Reconciler); ok {
		state, err := recon.Reconcile(ctx, req)
		return nil, state, err
	}
	return nil, executor.EffectUnknown, executor.ErrNotReconcilable
}

func (c *Controller) reconcileCancelledNode(ctx context.Context, runID, nodeKey string, attemptNo int64) error {
	run, err := c.loadRun(ctx, runID)
	if err != nil {
		return err
	}
	opKey, storedAttemptNo, err := c.latestAttempt(ctx, runID, nodeKey)
	if err != nil {
		return err
	}
	if storedAttemptNo > 0 {
		attemptNo = storedAttemptNo
	}
	cfg, kind, contract, err := c.parseNodeConfig(nodeConfigForKey(c, ctx, runID, run.graphVersionID, nodeKey))
	if err != nil {
		return err
	}
	ex, ok := c.pool[kind]
	if !ok {
		return c.waitingNode(ctx, runID, nodeKey)
	}
	_, state, rerr := reconcileEffect(ctx, ex, &executor.Request{
		RunID:            runID,
		GraphVersionID:   run.graphVersionID,
		DefinitionDigest: run.digest,
		NodeKey:          nodeKey,
		AttemptNo:        attemptNo,
		OperationKey:     opKey,
		Contract:         contract,
		Config:           cfg,
		Secrets:          c.cfg.Secrets,
	})
	if errors.Is(rerr, executor.ErrNotReconcilable) {
		return c.waitingNode(ctx, runID, nodeKey)
	}
	if rerr != nil || state == executor.EffectUnknown {
		if c.nodeCancelRequested(ctx, runID, nodeKey) {
			return nil
		}
		return c.waitingNode(ctx, runID, nodeKey)
	}
	if state == executor.EffectConfirmed {
		c.recordReconciledEffect(ctx, runID, nodeKey, opKey, attemptNo, executor.EffectConfirmed)
	}
	return c.cancelledNode(ctx, runID, nodeKey, attemptNo)
}

func (c *Controller) latestAttempt(ctx context.Context, runID, nodeKey string) (string, int64, error) {
	var opKey string
	var attemptNo int64
	err := c.store.DB().QueryRowContext(ctx, `
SELECT na.operation_key, na.attempt_no FROM node_attempt na
JOIN run_node rn ON rn.id = na.run_node_id
WHERE rn.run_id = ? AND rn.node_key = ?
ORDER BY na.attempt_no DESC LIMIT 1`, runID, nodeKey).Scan(&opKey, &attemptNo)
	return opKey, attemptNo, err
}

func nodeConfigForKey(c *Controller, ctx context.Context, runID, graphVersionID, nodeKey string) string {
	var config string
	if err := c.store.DB().QueryRowContext(ctx,
		"SELECT config FROM graph_node WHERE graph_version_id = ? AND node_key = ?",
		graphVersionID, nodeKey).Scan(&config); err != nil {
		return ""
	}
	return config
}
