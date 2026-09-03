package store

import (
	"context"
	"database/sql"
)

type Approval struct {
	ID              string
	RunID           string
	RunNodeID       string
	GraphVersionID  string
	RequestedAction string
	EvidenceRefs    string
	RequiredScope   string
	ExpiresAt       int64
	Decision        sql.NullString
	DecidedBy       sql.NullString
	DecidedAt       sql.NullInt64
	IdempotencyKey  sql.NullString
	CreatedAt       int64
}

const approvalSelectCols = `
id, run_id, run_node_id, graph_version_id, requested_action, evidence_references,
required_scope, expires_at, decision, decided_by, decided_at, decision_idempotency_key, created_at
`

func scanApproval(row interface {
	Scan(dest ...any) error
}) (*Approval, error) {
	var a Approval
	err := row.Scan(
		&a.ID,
		&a.RunID,
		&a.RunNodeID,
		&a.GraphVersionID,
		&a.RequestedAction,
		&a.EvidenceRefs,
		&a.RequiredScope,
		&a.ExpiresAt,
		&a.Decision,
		&a.DecidedBy,
		&a.DecidedAt,
		&a.IdempotencyKey,
		&a.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (s *Store) GetApproval(ctx context.Context, id string) (*Approval, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+approvalSelectCols+` FROM approval WHERE id = ?`, id)
	a, err := scanApproval(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (s *Store) ListExpiredApprovals(ctx context.Context, nowMs int64) ([]Approval, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT `+approvalSelectCols+`
FROM approval
WHERE decision IS NULL AND expires_at <= ?
ORDER BY expires_at`, nowMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []Approval
	for rows.Next() {
		a, err := scanApproval(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, *a)
	}
	return list, rows.Err()
}
