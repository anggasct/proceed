package controller

import (
	"context"
	"database/sql"
	"sync"
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
	sem := make(chan struct{}, c.cfg.MaxConcurrent)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for _, n := range nodes {
		n := n
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			err := c.executeNode(ctx, runID, run.graphVersionID, run.digest, n)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return false, firstErr
	}
	return true, nil
}

func (c *Controller) Drain(ctx context.Context, runID string) error {
	if err := c.acquireLease(ctx, time.Now()); err != nil {
		return err
	}
	hbCtx, stopHB := context.WithCancel(context.Background())
	defer stopHB()
	hbErr := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(c.cfg.HeartbeatPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := c.heartbeat(hbCtx); err != nil {
					hbErr <- err
					return
				}
			}
		}
	}()

	settle := func(err error) error {
		stopHB()
		if err != nil {
			c.releaseLease(context.Background())
			return err
		}
		status := runStatusOf(c, context.Background(), runID)
		if status != "running" {
			c.releaseLease(context.Background())
		}
		return nil
	}

	for {
		select {
		case err := <-hbErr:
			return settle(err)
		default:
		}
		if err := c.heartbeat(ctx); err != nil {
			return settle(err)
		}
		progressed, err := c.Step(ctx, runID)
		if err != nil {
			return settle(err)
		}
		if !progressed {
			return settle(nil)
		}
	}
}

func runStatusOf(c *Controller, ctx context.Context, runID string) string {
	var status string
	if err := c.store.DB().QueryRowContext(ctx,
		"SELECT status FROM graph_run WHERE id = ?", runID).Scan(&status); err != nil {
		return ""
	}
	return status
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
	nowMs := time.Now().UnixMilli()
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		var status string
		if err := tx.QueryRowContext(ctx,
			"SELECT status FROM graph_run WHERE id = ?", runID).Scan(&status); err != nil {
			return err
		}
		if status != "running" {
			return nil
		}

		row := tx.QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(CASE WHEN status IN ('pending','eligible','leased','running','uncertain','waiting','reconciling','cancel_requested') THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status IN ('succeeded','skipped') THEN 1 ELSE 0 END), 0),
  (SELECT COUNT(*) FROM graph_node WHERE graph_version_id = ?)
FROM run_node WHERE run_id = ?`, graphVersionID, runID)
		var active, failed, cancelled, done, defTotal int
		if err := row.Scan(&active, &failed, &cancelled, &done, &defTotal); err != nil {
			return err
		}

		var terminalEvent string
		switch {
		case failed > 0:
			terminalEvent = "run_failed"
		case active > 0:
			return nil
		case cancelled > 0:
			terminalEvent = "run_cancelled"
		case done == defTotal && defTotal > 0:
			terminalEvent = "run_completed"
		default:
			return nil
		}

		var alreadyTerminal int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM event WHERE run_id = ? AND type IN ('run_completed','run_failed','run_cancelled')",
			runID).Scan(&alreadyTerminal); err != nil {
			return err
		}
		if alreadyTerminal > 0 {
			return nil
		}

		ev := store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          terminalEvent,
			OccurredAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload:       payloadJSON(map[string]any{}),
		}
		_, err := c.appendWithin(ctx, tx, &ev)
		return err
	})
}
