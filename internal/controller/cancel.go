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

	c.cancelInflightRun(runID)

	nowMs := time.Now().UnixMilli()
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
SELECT node_key,
  CASE WHEN status IN ('running','leased','uncertain','cancel_requested') THEN 1 ELSE 0 END
FROM run_node WHERE run_id = ?`, runID)
		if err != nil {
			return err
		}
		type entry struct {
			key      string
			inflight bool
		}
		var entries []entry
		for rows.Next() {
			var e entry
			var flag int
			if err := rows.Scan(&e.key, &flag); err != nil {
				rows.Close()
				return err
			}
			e.inflight = flag == 1
			entries = append(entries, e)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		started := map[string]bool{}
		for _, e := range entries {
			started[e.key] = true
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
}
