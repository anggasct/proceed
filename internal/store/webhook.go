package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type WebhookTrigger struct {
	Name             string `json:"name"`
	GraphVersionID   string `json:"graph_version_id"`
	DefinitionDigest string `json:"definition_digest"`
	CreatedAt        int64  `json:"created_at"`
}

func (s *Store) AddWebhookTrigger(ctx context.Context, name, graphVersionID string) error {
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
			"SELECT COUNT(*) FROM webhook_trigger WHERE name = ?", name).Scan(&existing); err != nil {
			return err
		}
		if existing > 0 {
			return storeErr(CodeGraphInvalid, "trigger %q is already bound; remove it first", name)
		}
		_, err = tx.ExecContext(ctx, `
INSERT INTO webhook_trigger (id, name, graph_version_id, definition_digest, created_at)
VALUES (?, ?, ?, ?, ?)`,
			ulid.Make().String(), name, graphVersionID, digest, time.Now().UnixMilli())
		return err
	})
}

func (s *Store) RemoveWebhookTrigger(ctx context.Context, name string) (bool, error) {
	res, err := s.db.ExecContext(ctx, "DELETE FROM webhook_trigger WHERE name = ?", name)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) WebhookTrigger(ctx context.Context, name string) (*WebhookTrigger, error) {
	var t WebhookTrigger
	err := s.db.QueryRowContext(ctx, `
SELECT name, graph_version_id, definition_digest, created_at
FROM webhook_trigger WHERE name = ?`, name).
		Scan(&t.Name, &t.GraphVersionID, &t.DefinitionDigest, &t.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *Store) ListWebhookTriggers(ctx context.Context) ([]WebhookTrigger, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT name, graph_version_id, definition_digest, created_at
FROM webhook_trigger ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []WebhookTrigger{}
	for rows.Next() {
		var t WebhookTrigger
		if err := rows.Scan(&t.Name, &t.GraphVersionID, &t.DefinitionDigest, &t.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
