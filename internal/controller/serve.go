package controller

import (
	"context"
	"time"
)

func (c *Controller) AcquireLease(ctx context.Context) error {
	return c.acquireLease(ctx, time.Now())
}

func (c *Controller) ReleaseLease() {
	c.releaseLease(context.Background())
}

func (c *Controller) Heartbeat() error {
	return c.heartbeat(context.Background())
}

func (c *Controller) RecoverAll(ctx context.Context) error {
	rows, err := c.store.DB().QueryContext(ctx, "SELECT id FROM graph_run WHERE status = 'running'")
	if err != nil {
		return err
	}
	var runIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		runIDs = append(runIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, runID := range runIDs {
		if err := c.Recover(ctx, runID); err != nil {
			return err
		}
	}
	return nil
}
