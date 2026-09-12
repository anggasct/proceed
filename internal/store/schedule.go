package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

type Schedule struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	GraphVersionID   string `json:"graph_version_id"`
	DefinitionDigest string `json:"definition_digest"`
	Cron             string `json:"cron"`
	Enabled          bool   `json:"enabled"`
	NextFireAt       int64  `json:"next_fire_at"`
	LastRunID        string `json:"last_run_id"`
	SkippedCount     int64  `json:"skipped_count"`
	LastSkippedAt    int64  `json:"last_skipped_at"`
	LastSkipWindow   string `json:"last_skip_window"`
	CreatedAt        int64  `json:"created_at"`
}

func scanSchedule(scanner interface{ Scan(...any) error }) (*Schedule, error) {
	var s Schedule
	var enabled int
	var lastRun, skipWindow sql.NullString
	var lastSkipped sql.NullInt64
	err := scanner.Scan(&s.ID, &s.Name, &s.GraphVersionID, &s.DefinitionDigest, &s.Cron,
		&enabled, &s.NextFireAt, &lastRun, &s.SkippedCount, &lastSkipped, &skipWindow, &s.CreatedAt)
	if err != nil {
		return nil, err
	}
	s.Enabled = enabled == 1
	if lastRun.Valid {
		s.LastRunID = lastRun.String
	}
	if lastSkipped.Valid {
		s.LastSkippedAt = lastSkipped.Int64
	}
	if skipWindow.Valid {
		s.LastSkipWindow = skipWindow.String
	}
	return &s, nil
}

const scheduleColumns = `id, name, graph_version_id, definition_digest, cron,
enabled, next_fire_at, last_run_id, skipped_count, last_skipped_at, last_skip_window, created_at`

func (s *Store) AddSchedule(ctx context.Context, name, graphVersionID, cron string, nextFireAt int64) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		var digest string
		err := tx.QueryRowContext(ctx,
			"SELECT definition_digest FROM graph_version WHERE id = ?", graphVersionID).Scan(&digest)
		if err == sql.ErrNoRows {
			return storeErr(CodeGraphInvalid, "graph version %s does not exist", graphVersionID)
		}
		if err != nil {
			return err
		}
		var existing int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM schedule WHERE name = ?", name).Scan(&existing); err != nil {
			return err
		}
		if existing > 0 {
			return storeErr(CodeGraphInvalid, "schedule %q already exists; remove it first", name)
		}
		_, err = tx.ExecContext(ctx, `
INSERT INTO schedule (id, name, graph_version_id, definition_digest, cron, enabled,
                      next_fire_at, skipped_count, created_at)
VALUES (?, ?, ?, ?, ?, 1, ?, 0, ?)`,
			ulid.Make().String(), name, graphVersionID, digest, cron, nextFireAt, time.Now().UnixMilli())
		return err
	})
}

func (s *Store) ListSchedules(ctx context.Context) ([]Schedule, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+scheduleColumns+" FROM schedule ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Schedule{}
	for rows.Next() {
		sched, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *sched)
	}
	return out, rows.Err()
}

func (s *Store) SetScheduleEnabled(ctx context.Context, name string, enabled bool) (bool, error) {
	v := 0
	if enabled {
		v = 1
	}
	res, err := s.db.ExecContext(ctx,
		"UPDATE schedule SET enabled = ? WHERE name = ?", v, name)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) RemoveSchedule(ctx context.Context, name string) (bool, error) {
	res, err := s.db.ExecContext(ctx, "DELETE FROM schedule WHERE name = ?", name)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

type ScheduleFireOutcome struct {
	ScheduleID string
	Name       string
	RunID      string
	Tick       int64
	Skipped    int64
	SkipFrom   int64
	SkipTo     int64
	NextFireAt int64
	Error      string
}

type CronNextFunc func(expr string, after time.Time) (time.Time, bool, error)

func (s *Store) FireDueSchedules(ctx context.Context, nowMs int64, next CronNextFunc) ([]ScheduleFireOutcome, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+scheduleColumns+" FROM schedule WHERE enabled = 1 AND next_fire_at <= ? ORDER BY name", nowMs)
	if err != nil {
		return nil, err
	}
	var due []Schedule
	for rows.Next() {
		sched, err := scanSchedule(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		due = append(due, *sched)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var outcomes []ScheduleFireOutcome
	for i := range due {
		sched := &due[i]
		outcome, err := s.fireOneSchedule(ctx, sched, nowMs, next)
		if err != nil {
			outcomes = append(outcomes, ScheduleFireOutcome{
				ScheduleID: sched.ID,
				Name:       sched.Name,
				Tick:       sched.NextFireAt,
				Error:      err.Error(),
			})
			continue
		}
		if outcome != nil {
			outcomes = append(outcomes, *outcome)
		}
	}
	return outcomes, nil
}

func (s *Store) fireOneSchedule(ctx context.Context, sched *Schedule, nowMs int64, next CronNextFunc) (*ScheduleFireOutcome, error) {
	tickMs := sched.NextFireAt
	tick := time.UnixMilli(tickMs)
	nextAfterTick, ok, err := next(sched.Cron, tick)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, storeErr(CodeGraphInvalid, "schedule %q cron %q never matches", sched.Name, sched.Cron)
	}
	nextMs := nextAfterTick.UnixMilli()
	if nextMs <= nowMs {
		return s.skipScheduleTicks(ctx, sched, tick, nowMs, next)
	}
	outcome := &ScheduleFireOutcome{
		ScheduleID: sched.ID,
		Name:       sched.Name,
		Tick:       tickMs,
		NextFireAt: nextMs,
	}
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := startRunTx(ctx, tx, runStart{
			RunID:            ulid.Make().String(),
			GraphVersionID:   sched.GraphVersionID,
			DefinitionDigest: sched.DefinitionDigest,
			ScheduleID:       sched.ID,
			ScheduleTick:     tickMs,
			IdempotencyKey:   fmt.Sprintf("schedule:%s:%d", sched.ID, tickMs/60000),
		})
		if err != nil {
			return err
		}
		outcome.RunID = run
		_, err = tx.ExecContext(ctx, `
UPDATE schedule SET next_fire_at = ?, last_run_id = ? WHERE id = ? AND next_fire_at = ?`,
			nextMs, run, sched.ID, tickMs)
		return err
	})
	if err != nil {
		return nil, err
	}
	return outcome, nil
}

func (s *Store) skipScheduleTicks(ctx context.Context, sched *Schedule, from time.Time, nowMs int64, next CronNextFunc) (*ScheduleFireOutcome, error) {
	outcome := &ScheduleFireOutcome{ScheduleID: sched.ID, Name: sched.Name, Skipped: 1}
	first := from.UnixMilli()
	last := first
	cur := from
	for {
		n, ok, err := next(sched.Cron, cur)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, storeErr(CodeGraphInvalid, "schedule %q cron %q never matches", sched.Name, sched.Cron)
		}
		cur = n
		if cur.UnixMilli() > nowMs {
			break
		}
		last = cur.UnixMilli()
		outcome.Skipped++
	}
	nextMs := cur.UnixMilli()
	outcome.NextFireAt = nextMs
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var window any
		if outcome.Skipped > 0 {
			encoded, err := json.Marshal(map[string]int64{
				"from_tick": first,
				"to_tick":   last,
				"count":     outcome.Skipped,
			})
			if err != nil {
				return err
			}
			window = string(encoded)
			outcome.SkipFrom = first
			outcome.SkipTo = last
		}
		_, err := tx.ExecContext(ctx, `
UPDATE schedule
SET next_fire_at = ?,
    skipped_count = skipped_count + ?,
    last_skipped_at = CASE WHEN ? > 0 THEN ? ELSE last_skipped_at END,
    last_skip_window = COALESCE(?, last_skip_window)
WHERE id = ? AND next_fire_at = ?`,
			nextMs, outcome.Skipped, outcome.Skipped, nowMs, window, sched.ID, from.UnixMilli())
		return err
	})
	if err != nil {
		return nil, err
	}
	return outcome, nil
}
