package store

import (
	"context"
	"database/sql"
	"time"
)

func (s *Store) IsWebhookDeliverySeen(ctx context.Context, deliveryID string) (bool, error) {
	if deliveryID == "" {
		return false, nil
	}
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM webhook_delivery WHERE delivery_id = ?", deliveryID).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *Store) RecordWebhookDelivery(ctx context.Context, deliveryID string) error {
	if deliveryID == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, "INSERT OR IGNORE INTO webhook_delivery (delivery_id, completed_at) VALUES (?, ?)", deliveryID, time.Now().UnixMilli())
	return err
}

func (s *Store) IsWebhookDeliverySeenTx(ctx context.Context, tx *sql.Tx, deliveryID string) (bool, error) {
	if deliveryID == "" {
		return false, nil
	}
	var count int
	err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM webhook_delivery WHERE delivery_id = ?", deliveryID).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *Store) RecordWebhookDeliveryTx(ctx context.Context, tx *sql.Tx, deliveryID string) error {
	if deliveryID == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO webhook_delivery (delivery_id, completed_at) VALUES (?, ?)", deliveryID, time.Now().UnixMilli())
	return err
}
