package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/executor"
	"proceed/internal/store"
)

func (c *Controller) failNode(ctx context.Context, runID, nodeKey string, attemptNo int64, cause error) error {
	nowMs := time.Now().UnixMilli()
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		ev := store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "node_failed",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload: payloadJSON(map[string]any{
				"node_key":   nodeKey,
				"attempt_no": attemptNo,
				"error":      trimErr(cause),
			}),
		}
		_, err := c.appendWithin(ctx, tx, &ev)
		return err
	})
}

func (c *Controller) uncertainNode(ctx context.Context, runID, nodeKey string, attemptNo int64, cause error) error {
	nowMs := time.Now().UnixMilli()
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		ev := store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "node_uncertain",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload: payloadJSON(map[string]any{
				"node_key":   nodeKey,
				"attempt_no": attemptNo,
				"reason":     trimErr(cause),
			}),
		}
		_, err := c.appendWithin(ctx, tx, &ev)
		return err
	})
}

func (c *Controller) recordAttemptFailure(ctx context.Context, runID, graphVersionID, digest string, n runnableNode, cause error) error {
	if n.MaxAttempts > 0 && n.AttemptNo >= n.MaxAttempts {
		return c.failNode(ctx, runID, n.NodeKey, n.AttemptNo, cause)
	}
	nowMs := time.Now().UnixMilli()
	err := c.store.WithTx(ctx, func(tx *sql.Tx) error {
		var runNodeID string
		if err := tx.QueryRowContext(ctx,
			"SELECT id FROM run_node WHERE run_id = ? AND node_key = ?", runID, n.NodeKey).Scan(&runNodeID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE node_attempt SET status = 'failed', finished_at = ?, result = ?
WHERE run_node_id = ? AND attempt_no = ?`,
			nowMs, payloadJSON(map[string]any{"error": trimErr(cause)}), runNodeID, n.AttemptNo); err != nil {
			return err
		}
		ev := store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "node_attempt_failed",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload: payloadJSON(map[string]any{
				"node_key":    n.NodeKey,
				"attempt_no":  n.AttemptNo,
				"retry_in_ms": n.BackoffMs,
				"error":       trimErr(cause),
			}),
		}
		_, err := c.appendWithin(ctx, tx, &ev)
		return err
	})
	if err != nil {
		return err
	}
	return c.requeueForRetry(ctx, runID, n.NodeKey)
}

func (c *Controller) requeueForRetry(ctx context.Context, runID, nodeKey string) error {
	_, err := c.store.DB().ExecContext(ctx,
		"UPDATE run_node SET status = 'eligible' WHERE run_id = ? AND node_key = ?", runID, nodeKey)
	return err
}

func (c *Controller) executeNonExecutable(ctx context.Context, runID, graphVersionID, digest string, n runnableNode, kind executor.Kind, contract executor.Contract) error {
	if n.NodeType == "gate" {
		_, err := c.store.DB().ExecContext(ctx,
			"UPDATE run_node SET status = 'waiting' WHERE run_id = ? AND node_key = ?", runID, n.NodeKey)
		return err
	}
	return c.commitNodeSuccess(ctx, runID, graphVersionID, digest, n, &executor.Result{}, "")
}

func (c *Controller) commitNodeSuccess(ctx context.Context, runID, graphVersionID, digest string, n runnableNode, result *executor.Result, opKey string) error {
	nowMs := time.Now().UnixMilli()
	err := c.store.WithTx(ctx, func(tx *sql.Tx) error {
		ev := store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "node_finished",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			CorrelationID: opKey,
			Payload: payloadJSON(map[string]any{
				"node_key":   n.NodeKey,
				"attempt_no": n.AttemptNo,
				"result":     result.Output,
				"route":      result.Route,
			}),
		}
		seq, err := c.appendWithin(ctx, tx, &ev)
		if err != nil {
			return err
		}
		return c.routeFrom(ctx, tx, runID, graphVersionID, digest, n, result, seq, nowMs)
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *Controller) routeFrom(ctx context.Context, tx *sql.Tx, runID, graphVersionID, digest string, n runnableNode, result *executor.Result, seq int64, nowMs int64) error {
	rows, err := tx.QueryContext(ctx, `
SELECT id, to_node_key, type, condition, max_traversals
FROM graph_edge WHERE graph_version_id = ? AND from_node_key = ?
ORDER BY id`, graphVersionID, n.NodeKey)
	if err != nil {
		return err
	}
	type edgeRow struct {
		ID      string
		To      string
		Type    string
		Cond    sql.NullString
		MaxTrav sql.NullInt64
	}
	var edges []edgeRow
	for rows.Next() {
		var e edgeRow
		if err := rows.Scan(&e.ID, &e.To, &e.Type, &e.Cond, &e.MaxTrav); err != nil {
			rows.Close()
			return err
		}
		edges = append(edges, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	type target struct {
		edgeID string
		toKey  string
		route  string
	}
	var traversed []target
	for _, e := range edges {
		switch e.Type {
		case "depends_on", "produces", "consumes":
			traversed = append(traversed, target{edgeID: e.ID, toKey: e.To})
		case "routes_to":
			if e.Cond.Valid && e.Cond.String != "" {
				if result.Route == e.Cond.String {
					traversed = append(traversed, target{edgeID: e.ID, toKey: e.To, route: e.Cond.String})
				}
			} else {
				traversed = append(traversed, target{edgeID: e.ID, toKey: e.To})
			}
		}
	}

	for _, t := range traversed {
		ev := store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "edge_traversed",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload: payloadJSON(map[string]any{
				"edge_id":         t.edgeID,
				"route":           t.route,
				"sequence_in_run": seq,
			}),
		}
		if _, err := c.appendWithin(ctx, tx, &ev); err != nil {
			return err
		}
	}
	return markEligibleAfter(ctx, tx, runID, graphVersionID, nowMs)
}

func markEligibleAfter(ctx context.Context, tx *sql.Tx, runID, graphVersionID string, nowMs int64) error {
	_, err := tx.ExecContext(ctx, `
UPDATE run_node SET status = 'eligible'
WHERE run_id = ? AND status = 'pending'
  AND node_key IN (
    SELECT ge.to_node_key FROM graph_edge ge
    WHERE ge.graph_version_id = ?
      AND ge.type IN ('depends_on','produces','consumes')
      AND ge.from_node_key IN (
        SELECT rn.node_key FROM run_node rn
        WHERE rn.run_id = ? AND rn.status IN ('succeeded','skipped')
      )
  )
  AND node_key NOT IN (
    SELECT ge2.to_node_key FROM graph_edge ge2
    WHERE ge2.graph_version_id = ?
      AND ge2.type IN ('depends_on','produces','consumes')
      AND ge2.from_node_key NOT IN (
        SELECT rn2.node_key FROM run_node rn2
        WHERE rn2.run_id = ? AND rn2.status IN ('succeeded','skipped')
      )
  )`, runID, graphVersionID, runID, graphVersionID, runID)
	return err
}

var _ = json.Marshal
