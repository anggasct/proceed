package controller

import (
	"context"
	"database/sql"
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
WHERE rn.run_id = ? AND rn.status IN ('uncertain', 'leased', 'running')`, runID, runID)
	if err != nil {
		return err
	}
	type candidate struct {
		key    string
		config string
		expiry sql.NullInt64
	}
	var list []candidate
	for rows.Next() {
		var u candidate
		var status string
		if err := rows.Scan(&u.key, &status, &u.config, &u.expiry); err != nil {
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
		if err := c.recoverNode(ctx, runID, u.key, u.config); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) recoverNode(ctx context.Context, runID, nodeKey, config string) error {
	_, contract, err := c.contractFor(config)
	if err != nil {
		return err
	}
	switch contract {
	case executor.Pure, executor.Idempotent:
		return c.requeuedNode(ctx, runID, nodeKey, 0)
	case executor.Reconcilable:
		return c.reconcileNode(ctx, runID, nodeKey)
	default:
		return c.waitingNode(ctx, runID, nodeKey)
	}
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
	kind, _, err := c.contractFor(nodeConfigForKey(c, ctx, runID, run.graphVersionID, nodeKey))
	if err != nil {
		return err
	}
	ex, ok := c.pool[kind]
	if !ok {
		return c.waitingNode(ctx, runID, nodeKey)
	}
	recon, ok := ex.(executor.Reconciler)
	if !ok {
		return c.waitingNode(ctx, runID, nodeKey)
	}
	req := &executor.Request{
		RunID:          runID,
		GraphVersionID: run.graphVersionID,
		NodeKey:        nodeKey,
	}
	state, rerr := recon.Reconcile(ctx, req)
	if rerr != nil || state == executor.EffectUnknown {
		return c.waitingNode(ctx, runID, nodeKey)
	}
	if state == executor.EffectConfirmed {
		return c.appendEvent(ctx, runID, "node_finished", map[string]any{
			"node_key":   nodeKey,
			"reconciled": true,
		})
	}
	return c.requeuedNode(ctx, runID, nodeKey, 0)
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
