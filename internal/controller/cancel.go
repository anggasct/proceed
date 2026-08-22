package controller

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/store"
)

func (c *Controller) CancelRun(ctx context.Context, runID string) error {
	run, err := c.loadRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.status != "running" {
		return nil
	}

	nowMs := time.Now().UnixMilli()
	err = c.store.WithTx(ctx, func(tx *sql.Tx) error {
		var status string
		if err := tx.QueryRowContext(ctx,
			"SELECT status FROM graph_run WHERE id = ?", runID).Scan(&status); err != nil {
			return err
		}
		if status != "running" {
			return nil
		}
		rows, err := tx.QueryContext(ctx, `
SELECT node_key, status,
  CASE WHEN status IN ('running','leased','uncertain','reconciling','cancel_requested') THEN 1 ELSE 0 END
FROM run_node WHERE run_id = ?`, runID)
		if err != nil {
			return err
		}
		type entry struct {
			key      string
			status   string
			inflight bool
			terminal bool
		}
		var entries []entry
		started := map[string]bool{}
		for rows.Next() {
			var e entry
			var status string
			var flag int
			if err := rows.Scan(&e.key, &status, &flag); err != nil {
				rows.Close()
				return err
			}
			e.status = status
			e.inflight = flag == 1
			switch status {
			case "succeeded", "failed", "skipped", "cancelled":
				e.terminal = true
			}
			started[e.key] = true
			if !e.terminal {
				entries = append(entries, e)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		for _, e := range entries {
			if e.status == "cancel_requested" {
				continue
			}
			typ := "node_cancelled"
			if e.inflight {
				typ = "node_cancel_requested"
			}
			if _, err := c.appendWithin(ctx, tx, &store.Event{
				EventID:       ulid.Make().String(),
				RunID:         runID,
				SchemaVersion: "proceed/v1",
				Type:          typ,
				OccurredAt:    nowMs,
				ActorType:     "controller",
				ActorID:       c.cfg.OwnerID,
				Payload:       payloadJSON(map[string]any{"node_key": e.key}),
			}); err != nil {
				return err
			}
		}

		defRows, err := tx.QueryContext(ctx,
			"SELECT node_key FROM graph_node WHERE graph_version_id = ?", run.graphVersionID)
		if err != nil {
			return err
		}
		var notStarted []string
		for defRows.Next() {
			var key string
			if err := defRows.Scan(&key); err != nil {
				defRows.Close()
				return err
			}
			if !started[key] {
				notStarted = append(notStarted, key)
			}
		}
		defRows.Close()
		if err := defRows.Err(); err != nil {
			return err
		}
		for _, key := range notStarted {
			if _, err := c.appendWithin(ctx, tx, &store.Event{
				EventID:       ulid.Make().String(),
				RunID:         runID,
				SchemaVersion: "proceed/v1",
				Type:          "node_cancelled",
				OccurredAt:    nowMs,
				ActorType:     "controller",
				ActorID:       c.cfg.OwnerID,
				Payload:       payloadJSON(map[string]any{"node_key": key}),
			}); err != nil {
				return err
			}
		}

		var active int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM run_node WHERE run_id = ?
  AND status IN ('pending','eligible','leased','running','uncertain','waiting','reconciling','cancel_requested')`,
			runID).Scan(&active); err != nil {
			return err
		}
		if active == 0 {
			if _, err := c.appendWithin(ctx, tx, &store.Event{
				EventID:       ulid.Make().String(),
				RunID:         runID,
				SchemaVersion: "proceed/v1",
				Type:          "run_cancelled",
				OccurredAt:    nowMs,
				ActorType:     "controller",
				ActorID:       c.cfg.OwnerID,
				Payload:       payloadJSON(map[string]any{}),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.cancelInflightRun(runID)
	return nil
}
