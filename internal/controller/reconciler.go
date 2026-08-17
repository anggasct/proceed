package controller

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/store"
)

func (c *Controller) Step(ctx context.Context, runID string) (bool, error) {
	run, err := c.loadRun(ctx, runID)
	if err != nil {
		return false, err
	}
	if run.status != "running" {
		return false, nil
	}
	nodes, err := c.eligibleNodes(ctx, runID, run.graphVersionID)
	if err != nil {
		return false, err
	}
	if len(nodes) == 0 {
		return false, c.tryCompleteRun(ctx, runID, run.graphVersionID)
	}
	for _, n := range nodes {
		if err := c.executeNode(ctx, runID, run.graphVersionID, run.digest, n); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (c *Controller) Drain(ctx context.Context, runID string) error {
	for {
		if err := c.heartbeat(ctx); err != nil {
			return err
		}
		progressed, err := c.Step(ctx, runID)
		if err != nil {
			return err
		}
		if !progressed {
			return nil
		}
		check := c.store.DB().QueryRowContext(ctx,
			"SELECT status FROM graph_run WHERE id = ?", runID)
		var status string
		if err := check.Scan(&status); err != nil {
			return err
		}
		if status != "running" {
			return nil
		}
	}
}

type runInfo struct {
	graphVersionID string
	digest         string
	status         string
}

func (c *Controller) loadRun(ctx context.Context, runID string) (runInfo, error) {
	var r runInfo
	err := c.store.DB().QueryRowContext(ctx,
		"SELECT graph_version_id, definition_digest, status FROM graph_run WHERE id = ?", runID).
		Scan(&r.graphVersionID, &r.digest, &r.status)
	if err == sql.ErrNoRows {
		return r, store.NewCodeError("RUN_NOT_FOUND", "run %s does not exist", runID)
	}
	return r, err
}

func (c *Controller) tryCompleteRun(ctx context.Context, runID, graphVersionID string) error {
	var counts struct {
		pending     int
		eligible    int
		leased      int
		running     int
		uncertain   int
		waiting     int
		cancelReq   int
		reconciling int
		succeeded   int
		failed      int
		cancelled   int
		skipped     int
	}
	row := c.store.DB().QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'eligible' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'leased' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'uncertain' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'waiting' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'cancel_requested' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'reconciling' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END), 0)
FROM run_node WHERE run_id = ?`, runID)
	if err := row.Scan(&counts.pending, &counts.eligible, &counts.leased, &counts.running,
		&counts.uncertain, &counts.waiting, &counts.cancelReq, &counts.reconciling,
		&counts.succeeded, &counts.failed, &counts.cancelled, &counts.skipped); err != nil {
		return err
	}

	var terminalEvent string
	switch {
	case counts.failed > 0:
		terminalEvent = "run_failed"
	case counts.cancelled > 0 || counts.cancelReq > 0:
		terminalEvent = "run_cancelled"
	case counts.uncertain > 0 || counts.waiting > 0 || counts.reconciling > 0 ||
		counts.pending > 0 || counts.eligible > 0 || counts.leased > 0 || counts.running > 0:
		return nil
	default:
		terminalEvent = "run_completed"
	}

	nowMs := time.Now().UnixMilli()
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		ev := store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          terminalEvent,
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload:       payloadJSON(map[string]any{}),
		}
		_, err := c.appendWithin(ctx, tx, &ev)
		return err
	})
}
