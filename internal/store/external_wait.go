package store

import (
	"context"
	"database/sql"
)

type ExternalWait struct {
	ID                string
	RunID             string
	RunNodeID         string
	GraphVersionID    string
	DefinitionDigest  string
	EventType         string
	CorrelationKey    string
	ExpectedCondition string
	Status            string
	ExpiresAt         sql.NullInt64
	CompletedEventID  sql.NullString
	PayloadDigest     sql.NullString
	CreatedAt         int64
	CompletedAt       sql.NullInt64
}

func scanExternalWait(row interface {
	Scan(dest ...any) error
}) (*ExternalWait, error) {
	var w ExternalWait
	err := row.Scan(
		&w.ID,
		&w.RunID,
		&w.RunNodeID,
		&w.GraphVersionID,
		&w.DefinitionDigest,
		&w.EventType,
		&w.CorrelationKey,
		&w.ExpectedCondition,
		&w.Status,
		&w.ExpiresAt,
		&w.CompletedEventID,
		&w.PayloadDigest,
		&w.CreatedAt,
		&w.CompletedAt,
	)
	if err != nil {
		return nil, err
	}
	return &w, nil
}

const externalWaitSelectCols = `
id, run_id, run_node_id, graph_version_id, definition_digest,
event_type, correlation_key, expected_condition, status,
expires_at, completed_event_id, payload_digest, created_at, completed_at
`

func (s *Store) GetExternalWait(ctx context.Context, id string) (*ExternalWait, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT `+externalWaitSelectCols+`
FROM external_wait WHERE id = ?`, id)
	w, err := scanExternalWait(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *Store) FindPendingExternalWait(ctx context.Context, eventType, correlationKey string) (*ExternalWait, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT `+externalWaitSelectCols+`
FROM external_wait
WHERE event_type = ? AND correlation_key = ? AND status = 'pending'`, eventType, correlationKey)
	w, err := scanExternalWait(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *Store) ListPendingExternalWaits(ctx context.Context, runID string) ([]ExternalWait, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT `+externalWaitSelectCols+`
FROM external_wait
WHERE run_id = ? AND status = 'pending'
ORDER BY created_at`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []ExternalWait
	for rows.Next() {
		w, err := scanExternalWait(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, *w)
	}
	return list, rows.Err()
}

func (s *Store) ListExpiredExternalWaits(ctx context.Context, nowMs int64) ([]ExternalWait, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT `+externalWaitSelectCols+`
FROM external_wait
WHERE status = 'pending' AND expires_at IS NOT NULL AND expires_at <= ?
ORDER BY expires_at`, nowMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []ExternalWait
	for rows.Next() {
		w, err := scanExternalWait(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, *w)
	}
	return list, rows.Err()
}
