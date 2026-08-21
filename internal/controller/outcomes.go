package controller

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/executor"
	"proceed/internal/store"
)

func (c *Controller) appendEvent(ctx context.Context, runID, typ string, payload map[string]any) error {
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          typ,
			OccurredAt:    time.Now().UnixMilli(),
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload:       payloadJSON(payload),
		})
		return err
	})
}

func (c *Controller) failNode(ctx context.Context, runID, nodeKey string, attemptNo int64, cause error) error {
	return c.appendEvent(ctx, runID, "node_failed", map[string]any{
		"node_key":   nodeKey,
		"attempt_no": attemptNo,
		"error":      trimErr(cause),
	})
}

func (c *Controller) uncertainNode(ctx context.Context, runID, nodeKey string, attemptNo int64, cause error) error {
	return c.appendEvent(ctx, runID, "node_uncertain", map[string]any{
		"node_key":   nodeKey,
		"attempt_no": attemptNo,
		"reason":     trimErr(cause),
	})
}

func (c *Controller) cancelledNode(ctx context.Context, runID, nodeKey string, attemptNo int64) error {
	return c.appendEvent(ctx, runID, "node_cancelled", map[string]any{
		"node_key":   nodeKey,
		"attempt_no": attemptNo,
	})
}

func (c *Controller) requeuedNode(ctx context.Context, runID, nodeKey string, attemptNo int64) error {
	return c.appendNodeTransition(ctx, runID, nodeKey, "node_requeued", map[string]any{
		"node_key":   nodeKey,
		"attempt_no": attemptNo,
	})
}

func (c *Controller) waitingNode(ctx context.Context, runID, nodeKey string) error {
	return c.appendNodeTransition(ctx, runID, nodeKey, "node_waiting", map[string]any{
		"node_key": nodeKey,
	})
}

func (c *Controller) appendNodeTransition(ctx context.Context, runID, nodeKey, typ string, payload map[string]any) error {
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		var status string
		if err := tx.QueryRowContext(ctx,
			"SELECT status FROM run_node WHERE run_id = ? AND node_key = ?", runID, nodeKey).Scan(&status); err != nil {
			return err
		}
		if status == "cancel_requested" || status == "cancelled" {
			return nil
		}
		_, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          typ,
			OccurredAt:    time.Now().UnixMilli(),
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload:       payloadJSON(payload),
		})
		return err
	})
}

func (c *Controller) recordAttemptFailure(ctx context.Context, runID, graphVersionID, digest string, n runnableNode, cause error) error {
	if n.MaxAttempts > 0 && n.AttemptNo >= n.MaxAttempts {
		return c.failNode(ctx, runID, n.NodeKey, n.AttemptNo, cause)
	}
	if err := c.appendEvent(ctx, runID, "node_attempt_failed", map[string]any{
		"node_key":    n.NodeKey,
		"attempt_no":  n.AttemptNo,
		"retry_in_ms": n.BackoffMs,
		"error":       trimErr(cause),
	}); err != nil {
		return err
	}
	return c.requeuedNode(ctx, runID, n.NodeKey, n.AttemptNo)
}

func (c *Controller) commitNodeSuccess(ctx context.Context, runID, graphVersionID, digest string, n runnableNode, result *executor.Result, opKey string) error {
	routeKnown := result != nil
	if result == nil {
		result = &executor.Result{}
	}
	nowMs := time.Now().UnixMilli()
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		var status string
		if err := tx.QueryRowContext(ctx,
			"SELECT status FROM run_node WHERE run_id = ? AND node_key = ?", runID, n.NodeKey).Scan(&status); err != nil {
			return err
		}
		terminalType := "node_finished"
		if status == "cancelled" {
			return nil
		}
		if status == "cancel_requested" {
			terminalType = "node_cancelled"
		} else if status != "running" && status != "reconciling" {
			return nil
		}
		if err := c.appendArtifactEventsTx(ctx, tx, runID, n.NodeKey, opKey, result.Artifacts, nowMs); err != nil {
			return err
		}
		ev := store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          terminalType,
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
		if _, err := c.appendWithin(ctx, tx, &ev); err != nil {
			return err
		}
		if terminalType == "node_cancelled" {
			return nil
		}
		return c.routeFromTx(ctx, tx, runID, graphVersionID, n, result, routeKnown, nowMs)
	})
}

func (c *Controller) persistArtifacts(ctx context.Context, runID, nodeKey, opKey string, artifacts []executor.ArtifactRef) error {
	if len(artifacts) == 0 {
		return nil
	}
	nowMs := time.Now().UnixMilli()
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		return c.appendArtifactEventsTx(ctx, tx, runID, nodeKey, opKey, artifacts, nowMs)
	})
}

func (c *Controller) appendArtifactEventsTx(ctx context.Context, tx *sql.Tx, runID, nodeKey, opKey string, artifacts []executor.ArtifactRef, nowMs int64) error {
	for _, artifact := range artifacts {
		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "artifact_published",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			CorrelationID: opKey,
			Payload: payloadJSON(map[string]any{
				"node_key":     nodeKey,
				"name":         artifact.Name,
				"path":         artifact.Path,
				"content_hash": artifact.ContentHash,
				"media_type":   artifact.MediaType,
				"size_bytes":   artifact.SizeBytes,
				"truncated":    artifact.Truncated,
			}),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) routeFromTx(ctx context.Context, tx *sql.Tx, runID, graphVersionID string, n runnableNode, result *executor.Result, routeKnown bool, nowMs int64) error {
	rows, err := tx.QueryContext(ctx, `
SELECT id, to_node_key, type, condition FROM graph_edge
WHERE graph_version_id = ? AND from_node_key = ? ORDER BY id`, graphVersionID, n.NodeKey)
	if err != nil {
		return err
	}
	type edgeRow struct {
		ID   string
		To   string
		Type string
		Cond sql.NullString
	}
	var edges []edgeRow
	for rows.Next() {
		var e edgeRow
		if err := rows.Scan(&e.ID, &e.To, &e.Type, &e.Cond); err != nil {
			rows.Close()
			return err
		}
		edges = append(edges, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	var skipped []string
	for _, e := range edges {
		switch e.Type {
		case "depends_on", "produces", "consumes":
			if _, err := c.appendWithin(ctx, tx, &store.Event{
				EventID:       ulid.Make().String(),
				RunID:         runID,
				SchemaVersion: "proceed/v1",
				Type:          "edge_traversed",
				OccurredAt:    nowMs,
				ActorType:     "controller",
				ActorID:       c.cfg.OwnerID,
				Payload: payloadJSON(map[string]any{
					"edge_id": e.ID,
				}),
			}); err != nil {
				return err
			}
		case "routes_to":
			if !routeKnown {
				continue
			}
			selected := !e.Cond.Valid || e.Cond.String == "" || result.Route == e.Cond.String
			if selected {
				if _, err := c.appendWithin(ctx, tx, &store.Event{
					EventID:       ulid.Make().String(),
					RunID:         runID,
					SchemaVersion: "proceed/v1",
					Type:          "edge_traversed",
					OccurredAt:    nowMs,
					ActorType:     "controller",
					ActorID:       c.cfg.OwnerID,
					Payload: payloadJSON(map[string]any{
						"edge_id": e.ID,
						"route":   e.Cond.String,
					}),
				}); err != nil {
					return err
				}
			} else if c.onlyIncomingEdge(ctx, tx, graphVersionID, e.To, e.ID) {
				skipped = append(skipped, e.To)
			}
		}
	}
	for _, key := range skipped {
		var exists int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM run_node WHERE run_id = ? AND node_key = ?", runID, key).Scan(&exists); err != nil {
			return err
		}
		if exists > 0 {
			continue
		}
		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "node_skipped",
			OccurredAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload: payloadJSON(map[string]any{
				"node_key": key,
			}),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) onlyIncomingEdge(ctx context.Context, tx *sql.Tx, graphVersionID, nodeKey, edgeID string) bool {
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM graph_edge WHERE graph_version_id = ? AND to_node_key = ? AND id <> ?`,
		graphVersionID, nodeKey, edgeID).Scan(&count); err != nil {
		return false
	}
	return count == 0
}
