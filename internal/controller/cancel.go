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
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
SELECT node_key FROM run_node WHERE run_id = ? AND status IN ('pending','eligible','leased','running')`,
			runID)
		if err != nil {
			return err
		}
		var keys []string
		for rows.Next() {
			var k string
			if err := rows.Scan(&k); err != nil {
				rows.Close()
				return err
			}
			keys = append(keys, k)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, k := range keys {
			ev := store.Event{
				EventID:       ulid.Make().String(),
				RunID:         runID,
				SchemaVersion: "proceed/v1",
				Type:          "node_cancel_requested",
				OccurredAt:    nowMs,
				RecordedAt:    nowMs,
				ActorType:     "controller",
				ActorID:       c.cfg.OwnerID,
				Payload: payloadJSON(map[string]any{
					"node_key": k,
				}),
			}
			if _, err := c.appendWithin(ctx, tx, &ev); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE run_node SET status = 'cancelled', finished_at = ?
WHERE run_id = ? AND node_key = ? AND status IN ('pending','eligible','leased','running')`,
				nowMs, runID, k); err != nil {
				return err
			}
		}
		ev := store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "run_cancelled",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload:       payloadJSON(map[string]any{}),
		}
		_, err = c.appendWithin(ctx, tx, &ev)
		return err
	})
}
