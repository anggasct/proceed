package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/executor"
	"proceed/internal/store"
)

type effectPublisher struct {
	c         *Controller
	runID     string
	nodeKey   string
	attemptNo int64
	opKey     string
}

func (c *Controller) newEffectPublisher(runID, nodeKey string, attemptNo int64, opKey string) *effectPublisher {
	return &effectPublisher{c: c, runID: runID, nodeKey: nodeKey, attemptNo: attemptNo, opKey: opKey}
}

func (p *effectPublisher) attemptID(ctx context.Context) (string, error) {
	var attemptID string
	err := p.c.store.DB().QueryRowContext(ctx, `
SELECT na.id FROM node_attempt na
JOIN run_node rn ON rn.id = na.run_node_id
WHERE rn.run_id = ? AND rn.node_key = ? AND na.attempt_no = ?`,
		p.runID, p.nodeKey, p.attemptNo).Scan(&attemptID)
	if err != nil {
		return "", err
	}
	return attemptID, nil
}

func (p *effectPublisher) RecordIntent(ctx context.Context, intent executor.EffectIntent) (string, error) {
	attemptID, err := p.attemptID(ctx)
	if err != nil {
		return "", err
	}
	effectID := ulid.Make().String()
	err = p.c.store.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := p.c.appendWithin(ctx, tx, &store.Event{
			EventID:       effectID,
			RunID:         p.runID,
			SchemaVersion: "proceed/v1",
			Type:          "effect_intent",
			OccurredAt:    time.Now().UnixMilli(),
			ActorType:     "executor",
			ActorID:       p.c.cfg.OwnerID,
			CorrelationID: p.opKey,
			Payload: payloadJSON(map[string]any{
				"node_attempt_id": attemptID,
				"operation_key":   p.opKey,
				"target":          intent.Target,
				"request_digest":  intent.RequestDigest,
			}),
		})
		return err
	})
	if err != nil {
		return "", err
	}
	return effectID, nil
}

func (p *effectPublisher) RecordReceipt(ctx context.Context, receipt executor.EffectReceipt) error {
	if receipt.EffectID == "" {
		return errors.New("effect receipt requires an effect id")
	}
	switch receipt.Status {
	case executor.EffectConfirmed, executor.EffectRejected, executor.EffectUnknown:
	default:
		return errors.New("effect receipt status is not recordable")
	}
	var raw json.RawMessage
	if len(receipt.Receipt) > 0 {
		raw = json.RawMessage(receipt.Receipt)
	}
	return p.c.store.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := p.c.appendWithin(ctx, tx, &store.Event{
			EventID:       ulid.Make().String(),
			RunID:         p.runID,
			SchemaVersion: "proceed/v1",
			Type:          "effect_receipt",
			OccurredAt:    time.Now().UnixMilli(),
			ActorType:     "executor",
			ActorID:       p.c.cfg.OwnerID,
			CorrelationID: p.opKey,
			Payload: payloadJSON(map[string]any{
				"effect_id": receipt.EffectID,
				"status":    string(receipt.Status),
				"receipt":   raw,
			}),
		})
		return err
	})
}

func (c *Controller) recordReconciledEffect(ctx context.Context, runID, nodeKey, opKey string, attemptNo int64, status executor.EffectState) error {
	var effectID string
	err := c.store.DB().QueryRowContext(ctx, `
SELECT e.id FROM effect e
JOIN node_attempt na ON na.id = e.node_attempt_id
JOIN run_node rn ON rn.id = na.run_node_id
WHERE rn.run_id = ? AND rn.node_key = ? AND na.operation_key = ?
ORDER BY e.created_at DESC, e.id DESC LIMIT 1`,
		runID, nodeKey, opKey).Scan(&effectID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("record reconciled effect: %w", err)
	}
	receipt, _ := json.Marshal(map[string]any{"reconciled": true})
	return c.appendEvent(ctx, runID, "effect_receipt", map[string]any{
		"effect_id":          effectID,
		"status":             string(status),
		"receipt":            json.RawMessage(receipt),
		"reconciliation_ref": "reconcile:" + opKey,
	})
}
