package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

var actorTypes = map[string]bool{
	"controller": true,
	"executor":   true,
	"node":       true,
	"human":      true,
}

type Event struct {
	EventID        string
	RunID          string
	Sequence       int64
	SchemaVersion  string
	Type           string
	OccurredAt     int64
	RecordedAt     int64
	ActorType      string
	ActorID        string
	CausationID    string
	CorrelationID  string
	IdempotencyKey string
	PayloadDigest  string
	Payload        string
}

func (e *Event) validate(now int64) error {
	switch {
	case e.RunID == "":
		return storeErr(CodeGraphInvalid, "event run_id is required")
	case e.Sequence <= 0:
		return storeErr(CodeGraphInvalid, "event sequence must be positive, got %d", e.Sequence)
	case e.SchemaVersion == "":
		return storeErr(CodeGraphInvalid, "event schema_version is required")
	case e.Type == "":
		return storeErr(CodeGraphInvalid, "event type is required")
	case e.OccurredAt <= 0:
		return storeErr(CodeGraphInvalid, "event occurred_at is required")
	case e.OccurredAt > now:
		return storeErr(CodeGraphInvalid, "event occurred_at is in the future")
	case !actorTypes[e.ActorType]:
		return storeErr(CodeGraphInvalid, "event actor_type %q is not one of controller, executor, node, human", e.ActorType)
	case e.ActorID == "":
		return storeErr(CodeGraphInvalid, "event actor_id is required")
	case !json.Valid([]byte(e.Payload)):
		return storeErr(CodeGraphInvalid, "event payload must be valid JSON")
	}
	return nil
}

func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func payloadDigest(payload string) string {
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

const eventSchemaVersion = "proceed/v1"

func canonicalJSON(s string) (string, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	if dec.More() {
		return "", errors.New("trailing data after JSON value")
	}
	out, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func (s *Store) Append(ctx context.Context, ev Event) (Event, error) {
	now := time.Now().UnixMilli()
	if ev.EventID == "" {
		ev.EventID = ulid.Make().String()
	}
	if err := ev.validate(now); err != nil {
		return Event{}, err
	}
	canonical, cerr := canonicalJSON(ev.Payload)
	if cerr != nil {
		return Event{}, storeErr(CodeGraphInvalid, "event payload must be a single valid JSON value: %v", cerr)
	}
	ev.Payload = canonical
	computed := payloadDigest(ev.Payload)
	if ev.PayloadDigest != "" && ev.PayloadDigest != computed {
		return Event{}, storeErr(CodeGraphInvalid,
			"payload_digest %s does not match payload (computed %s)", ev.PayloadDigest, computed)
	}
	ev.PayloadDigest = computed
	ev.RecordedAt = now

	err := s.withTx(ctx, func(tx *sql.Tx) error {
		return appendEventTx(ctx, tx, &ev)
	})
	if err != nil {
		return Event{}, err
	}
	return ev, nil
}

// AppendTx canonicalizes, validates, and appends an event inside an existing
// transaction, assigning the next per-run sequence. Idempotency keys are honored.
func (s *Store) AppendTx(ctx context.Context, tx *sql.Tx, ev *Event) error {
	now := time.Now().UnixMilli()
	if ev.EventID == "" {
		ev.EventID = ulid.Make().String()
	}
	var maxSeq int64
	if err := tx.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(sequence), 0) FROM event WHERE run_id = ?", ev.RunID).Scan(&maxSeq); err != nil {
		return err
	}
	ev.Sequence = maxSeq + 1
	if err := ev.validate(now); err != nil {
		return err
	}
	canonical, cerr := canonicalJSON(ev.Payload)
	if cerr != nil {
		return storeErr(CodeGraphInvalid, "event payload must be a single valid JSON value: %v", cerr)
	}
	ev.Payload = canonical
	ev.PayloadDigest = payloadDigest(ev.Payload)
	if ev.RecordedAt == 0 {
		ev.RecordedAt = now
	}
	return appendEventTx(ctx, tx, ev)
}

func appendEventTx(ctx context.Context, tx *sql.Tx, ev *Event) error {
	if ev.IdempotencyKey != "" {
		var existing string
		err := tx.QueryRowContext(ctx,
			"SELECT event_id FROM event WHERE idempotency_key = ?", ev.IdempotencyKey).Scan(&existing)
		if err == nil {
			return storeErr(CodeStoreConflict,
				"idempotency_key %q already recorded as event %s", ev.IdempotencyKey, existing)
		}
		if err != sql.ErrNoRows {
			return err
		}
	}
	var max int64
	if err := tx.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(sequence), 0) FROM event WHERE run_id = ?", ev.RunID).Scan(&max); err != nil {
		return err
	}
	if ev.Sequence <= max {
		if ev.Sequence == max {
			return storeErr(CodeStoreConflict,
				"sequence %d for run %s is a duplicate of the latest event", ev.Sequence, ev.RunID)
		}
		return storeErr(CodeStoreConflict,
			"sequence %d for run %s is not monotonic (latest is %d)", ev.Sequence, ev.RunID, max)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO event (event_id, run_id, sequence, schema_version, type, occurred_at, recorded_at,
                   actor_type, actor_id, causation_id, correlation_id, idempotency_key,
                   payload_digest, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.EventID, ev.RunID, ev.Sequence, ev.SchemaVersion, ev.Type, ev.OccurredAt, ev.RecordedAt,
		ev.ActorType, ev.ActorID, nullable(ev.CausationID), nullable(ev.CorrelationID),
		nullable(ev.IdempotencyKey), ev.PayloadDigest, ev.Payload); err != nil {
		if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
			return storeErr(CodeGraphInvalid,
				"run %s does not exist or causation event %s does not exist", ev.RunID, ev.CausationID)
		}
		return err
	}
	return applyProjections(ctx, tx, ev)
}

func (s *Store) Events(ctx context.Context, runID string) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT event_id, run_id, sequence, schema_version, type, occurred_at, recorded_at,
       actor_type, actor_id, causation_id, correlation_id, idempotency_key,
       payload_digest, payload
FROM event WHERE run_id = ? ORDER BY sequence`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, rows.Err()
}

func (s *Store) MaxSequence(ctx context.Context, runID string) (int64, error) {
	var max int64
	if err := s.db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(sequence), 0) FROM event WHERE run_id = ?", runID).Scan(&max); err != nil {
		return 0, err
	}
	return max, nil
}

func scanEvent(rows *sql.Rows) (Event, error) {
	var ev Event
	var causation, correlation, idempotency sql.NullString
	err := rows.Scan(&ev.EventID, &ev.RunID, &ev.Sequence, &ev.SchemaVersion, &ev.Type,
		&ev.OccurredAt, &ev.RecordedAt, &ev.ActorType, &ev.ActorID,
		&causation, &correlation, &idempotency, &ev.PayloadDigest, &ev.Payload)
	if err != nil {
		return Event{}, err
	}
	ev.CausationID = causation.String
	ev.CorrelationID = correlation.String
	ev.IdempotencyKey = idempotency.String
	return ev, nil
}
