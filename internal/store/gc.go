package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

var gcTerminalStatuses = []string{"completed", "failed", "cancelled", "abandoned"}

var gcPerTableOrder = []string{
	"external_wait", "causal_link", "decision", "evaluation", "effect",
	"artifact", "outcome", "metric", "anchor", "approval", "node_attempt",
	"run_edge", "run_node", "run_param", "event", "graph_run",
}

type GCPolicy struct {
	KeepRuns   int
	OlderThan  time.Duration
	OlderThanS string
	DryRun     bool
}

type GCResult struct {
	DeletedRuns int
	Counts      map[string]int
	RecordID    string
	Anomalies   int
}

func (s *Store) GCRuns(ctx context.Context, policy GCPolicy, now time.Time) (GCResult, error) {
	cutoff := now.Add(-policy.OlderThan).UnixMilli()
	keepIDs, err := gcKeepNewestIDs(ctx, s.db, policy.KeepRuns)
	if err != nil {
		return GCResult{}, err
	}
	candidates, anomalies, err := gcCandidates(ctx, s.db, keepIDs, cutoff)
	if err != nil {
		return GCResult{}, err
	}
	if policy.DryRun {
		counts, err := gcCountRows(ctx, s.db, candidates)
		if err != nil {
			return GCResult{}, err
		}
		recordID, err := gcWriteRecord(ctx, s.db, policy, 0, counts, now)
		if err != nil {
			return GCResult{}, err
		}
		return GCResult{DeletedRuns: 0, Counts: counts, RecordID: recordID, Anomalies: anomalies}, nil
	}
	counts, err := gcDeleteRuns(ctx, s, candidates)
	if err != nil {
		return GCResult{}, err
	}
	recordID, err := gcWriteRecord(ctx, s.db, policy, len(candidates), counts, now)
	if err != nil {
		return GCResult{}, err
	}
	if err := gcSweepOrphanFiles(ctx, s.db, s.dataDir); err != nil {
		return GCResult{}, err
	}
	return GCResult{DeletedRuns: len(candidates), Counts: counts, RecordID: recordID, Anomalies: anomalies}, nil
}

func gcKeepNewestIDs(ctx context.Context, q *sql.DB, keep int) (map[string]bool, error) {
	keepIDs := map[string]bool{}
	if keep <= 0 {
		return keepIDs, nil
	}
	rows, err := q.QueryContext(ctx,
		`SELECT id FROM graph_run ORDER BY created_at DESC, id DESC LIMIT ?`, keep)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		keepIDs[id] = true
	}
	return keepIDs, rows.Err()
}

func gcCandidates(ctx context.Context, q *sql.DB, keepIDs map[string]bool, cutoff int64) ([]string, int, error) {
	statusArgs := make([]any, len(gcTerminalStatuses))
	for i, st := range gcTerminalStatuses {
		statusArgs[i] = st
	}
	rows, err := q.QueryContext(ctx,
		`SELECT id, finished_at FROM graph_run
		 WHERE status IN (`+gcPlaceholders(len(gcTerminalStatuses))+`)
		 AND NOT EXISTS (SELECT 1 FROM approval a
		                 WHERE a.run_id = graph_run.id
		                 AND a.id IN (SELECT approval_id FROM policy_change_proposal WHERE approval_id IS NOT NULL))
		 AND NOT EXISTS (SELECT 1 FROM outcome o
		                 WHERE o.run_id = graph_run.id
		                 AND EXISTS (SELECT 1 FROM metric m
		                             JOIN proposal_metric pm ON pm.metric_id = m.id
		                             WHERE m.anchor_id = o.anchor_id))
		 ORDER BY created_at DESC, id DESC`, statusArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var candidates []string
	anomalies := 0
	for rows.Next() {
		var id string
		var finished sql.NullInt64
		if err := rows.Scan(&id, &finished); err != nil {
			return nil, 0, err
		}
		if keepIDs[id] {
			continue
		}
		if !finished.Valid {
			anomalies++
			continue
		}
		if finished.Int64 >= cutoff {
			continue
		}
		candidates = append(candidates, id)
	}
	return candidates, anomalies, rows.Err()
}

func gcPlaceholders(n int) string {
	marks := make([]string, n)
	for i := range marks {
		marks[i] = "?"
	}
	return strings.Join(marks, ",")
}

func gcRunArgs(runIDs []string) []any {
	args := make([]any, len(runIDs))
	for i, id := range runIDs {
		args[i] = id
	}
	return args
}

func gcCountRows(ctx context.Context, q *sql.DB, runIDs []string) (map[string]int, error) {
	counts := map[string]int{}
	for _, table := range gcPerTableOrder {
		counts[table] = 0
	}
	if len(runIDs) == 0 {
		return counts, nil
	}
	args := gcRunArgs(runIDs)
	marks := gcPlaceholders(len(runIDs))
	queries := map[string]string{
		"event":         `SELECT COUNT(*) FROM event WHERE run_id IN (` + marks + `)`,
		"run_node":      `SELECT COUNT(*) FROM run_node WHERE run_id IN (` + marks + `)`,
		"run_edge":      `SELECT COUNT(*) FROM run_edge WHERE run_id IN (` + marks + `)`,
		"node_attempt":  `SELECT COUNT(*) FROM node_attempt WHERE run_node_id IN (SELECT id FROM run_node WHERE run_id IN (` + marks + `))`,
		"run_param":     `SELECT COUNT(*) FROM run_param WHERE run_id IN (` + marks + `)`,
		"approval":      `SELECT COUNT(*) FROM approval WHERE run_id IN (` + marks + `)`,
		"external_wait": `SELECT COUNT(*) FROM external_wait WHERE run_id IN (` + marks + `)`,
		"artifact":      `SELECT COUNT(*) FROM artifact WHERE run_id IN (` + marks + `)`,
		"evaluation":    `SELECT COUNT(*) FROM evaluation WHERE run_id IN (` + marks + `)`,
		"decision":      `SELECT COUNT(*) FROM decision WHERE run_id IN (` + marks + `)`,
		"outcome":       `SELECT COUNT(*) FROM outcome WHERE run_id IN (` + marks + `)`,
		"anchor":        `SELECT COUNT(*) FROM anchor WHERE id IN (SELECT anchor_id FROM outcome WHERE run_id IN (` + marks + `))`,
		"metric":        `SELECT COUNT(*) FROM metric WHERE anchor_id IN (SELECT anchor_id FROM outcome WHERE run_id IN (` + marks + `)) AND id NOT IN (SELECT metric_id FROM proposal_metric)`,
		"effect":        `SELECT COUNT(*) FROM effect WHERE node_attempt_id IN (SELECT na.id FROM node_attempt na JOIN run_node rn ON rn.id = na.run_node_id WHERE rn.run_id IN (` + marks + `))`,
		"causal_link":   `SELECT COUNT(*) FROM causal_link WHERE decision_id IN (SELECT id FROM decision WHERE run_id IN (` + marks + `))`,
		"graph_run":     `SELECT COUNT(*) FROM graph_run WHERE id IN (` + marks + `)`,
	}
	for table, query := range queries {
		var n int
		if err := q.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
			return nil, err
		}
		counts[table] = n
	}
	return counts, nil
}

func gcCandidateAnchors(ctx context.Context, tx *sql.Tx, args []any, marks string) ([]string, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT anchor_id FROM outcome WHERE run_id IN (`+marks+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var anchors []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		anchors = append(anchors, id)
	}
	return anchors, rows.Err()
}

func gcDeleteRuns(ctx context.Context, s *Store, runIDs []string) (map[string]int, error) {
	counts := map[string]int{}
	for _, table := range gcPerTableOrder {
		counts[table] = 0
	}
	if len(runIDs) == 0 {
		return counts, nil
	}
	args := gcRunArgs(runIDs)
	marks := gcPlaceholders(len(runIDs))
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "PRAGMA defer_foreign_keys = 1"); err != nil {
			return err
		}
		anchors, err := gcCandidateAnchors(ctx, tx, args, marks)
		if err != nil {
			return err
		}
		anchorArgs := gcRunArgs(anchors)
		anchorMarks := gcPlaceholders(len(anchors))
		deletes := []struct {
			table string
			query string
			args  []any
		}{
			{"external_wait", `DELETE FROM external_wait WHERE run_id IN (` + marks + `)`, args},
			{"causal_link", `DELETE FROM causal_link WHERE decision_id IN (SELECT id FROM decision WHERE run_id IN (` + marks + `))`, args},
			{"decision", `DELETE FROM decision WHERE run_id IN (` + marks + `)`, args},
			{"evaluation", `DELETE FROM evaluation WHERE run_id IN (` + marks + `)`, args},
			{"effect", `DELETE FROM effect WHERE node_attempt_id IN (SELECT na.id FROM node_attempt na JOIN run_node rn ON rn.id = na.run_node_id WHERE rn.run_id IN (` + marks + `))`, args},
			{"artifact", `DELETE FROM artifact WHERE run_id IN (` + marks + `)`, args},
			{"outcome", `DELETE FROM outcome WHERE run_id IN (` + marks + `)`, args},
			{"metric", `DELETE FROM metric WHERE anchor_id IN (` + anchorMarks + `) AND id NOT IN (SELECT metric_id FROM proposal_metric)`, anchorArgs},
			{"anchor", `DELETE FROM anchor WHERE id IN (` + anchorMarks + `) AND NOT EXISTS (SELECT 1 FROM metric WHERE metric.anchor_id = anchor.id)`, anchorArgs},
			{"approval", `DELETE FROM approval WHERE run_id IN (` + marks + `)`, args},
			{"node_attempt", `DELETE FROM node_attempt WHERE run_node_id IN (SELECT id FROM run_node WHERE run_id IN (` + marks + `))`, args},
			{"run_edge", `DELETE FROM run_edge WHERE run_id IN (` + marks + `)`, args},
			{"run_node", `DELETE FROM run_node WHERE run_id IN (` + marks + `)`, args},
			{"run_param", `DELETE FROM run_param WHERE run_id IN (` + marks + `)`, args},
			{"event", `DELETE FROM event WHERE run_id IN (` + marks + `)`, args},
			{"graph_run", `DELETE FROM graph_run WHERE id IN (` + marks + `)`, args},
		}
		for _, d := range deletes[:len(deletes)-1] {
			if len(d.args) == 0 {
				continue
			}
			res, err := tx.ExecContext(ctx, d.query, d.args...)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			counts[d.table] = int(n)
		}
		graphRun := deletes[len(deletes)-1]
		for _, id := range runIDs {
			if _, err := tx.ExecContext(ctx,
				`UPDATE schedule SET last_run_id = NULL WHERE last_run_id = ?`, id); err != nil {
				return err
			}
		}
		res, err := tx.ExecContext(ctx, graphRun.query, graphRun.args...)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		counts[graphRun.table] = int(n)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return counts, nil
}

func gcWriteRecord(ctx context.Context, q *sql.DB, policy GCPolicy, deleted int, counts map[string]int, now time.Time) (string, error) {
	encoded, err := json.Marshal(counts)
	if err != nil {
		return "", err
	}
	id := ulid.Make().String()
	dry := 0
	if policy.DryRun {
		dry = 1
	}
	_, err = q.ExecContext(ctx, `
INSERT INTO gc_record (id, policy_keep_runs, policy_older_than, dry_run, deleted_runs, per_table_counts, executed_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, policy.KeepRuns, policy.OlderThanS, dry, deleted, string(encoded), now.UnixMilli())
	if err != nil {
		return "", err
	}
	return id, nil
}

func gcSweepOrphanFiles(ctx context.Context, q *sql.DB, dataDir string) error {
	root := filepath.Join(dataDir, "artifacts")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		var n int
		if err := q.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM artifact WHERE content_hash = ?`, e.Name()).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			continue
		}
		if err := os.Remove(filepath.Join(root, e.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
