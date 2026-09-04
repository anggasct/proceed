package store

import (
	"context"
	"database/sql"
)

var validRunListStatuses = map[string]bool{
	"running":   true,
	"completed": true,
	"failed":    true,
	"cancelled": true,
	"abandoned": true,
}

const defaultRunListLimit = 50

const maxRunListLimit = 500

type RunSummary struct {
	RunID      string `json:"run_id"`
	GraphName  string `json:"graph_name"`
	Status     string `json:"status"`
	CreatedAt  int64  `json:"created_at"`
	StartedAt  *int64 `json:"started_at"`
	FinishedAt *int64 `json:"finished_at"`
}

type RunList struct {
	Runs []RunSummary `json:"runs"`
}

func (s *Store) ListRuns(ctx context.Context, status string, limit int) ([]RunSummary, error) {
	if status != "" && !validRunListStatuses[status] {
		return nil, NewCodeError("GRAPH_INVALID", "unknown run status %q", status)
	}
	if limit < 1 || limit > maxRunListLimit {
		return nil, NewCodeError("GRAPH_INVALID", "limit must be 1-500")
	}
	query := `SELECT gr.id, g.name, gr.status, gr.created_at, gr.started_at, gr.finished_at
FROM graph_run gr
JOIN graph_version gv ON gv.id = gr.graph_version_id
JOIN graph g ON g.id = gv.graph_id`
	args := []any{}
	if status != "" {
		query += " WHERE gr.status = ?"
		args = append(args, status)
	}
	query += " ORDER BY gr.created_at DESC, gr.id DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	summaries := []RunSummary{}
	for rows.Next() {
		var summary RunSummary
		var started, finished sql.NullInt64
		if err := rows.Scan(&summary.RunID, &summary.GraphName, &summary.Status,
			&summary.CreatedAt, &started, &finished); err != nil {
			return nil, err
		}
		if started.Valid {
			summary.StartedAt = &started.Int64
		}
		if finished.Valid {
			summary.FinishedAt = &finished.Int64
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return summaries, nil
}
