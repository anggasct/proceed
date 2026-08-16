package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

func mustRun(t *testing.T, s *Store, graphVersionID string) string {
	t.Helper()
	var digest string
	if err := s.db.QueryRow("SELECT definition_digest FROM graph_version WHERE id = ?",
		graphVersionID).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	id := ulid.Make().String()
	_, err := s.db.Exec(`INSERT INTO graph_run (id, graph_version_id, definition_digest, status, created_at)
VALUES (?, ?, ?, 'running', ?)`, id, graphVersionID, digest, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func seededRun(t *testing.T, s *Store) string {
	t.Helper()
	src := readFixture(t, "../../internal/compiler/testdata/customer-research.yaml")
	doc := compileFixture(t, src)
	frozen, err := s.FreezeDefinition(context.Background(), "customer-research.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	return mustRun(t, s, frozen.GraphVersionID)
}

func makeEvent(runID string, seq int64, typ string) Event {
	return Event{
		RunID:         runID,
		Sequence:      seq,
		SchemaVersion: "proceed/v1",
		Type:          typ,
		OccurredAt:    time.Now().UnixMilli(),
		ActorType:     "controller",
		ActorID:       "controller-1",
		Payload:       `{"node_key":"research"}`,
	}
}

func TestAppendRejectsDuplicateSequence(t *testing.T) {
	s := openTestStore(t)
	runID := seededRun(t, s)
	ctx := context.Background()

	if _, err := s.Append(ctx, makeEvent(runID, 1, "node_started")); err != nil {
		t.Fatal(err)
	}
	_, err := s.Append(ctx, makeEvent(runID, 1, "node_finished"))
	if !IsCode(err, CodeStoreConflict) {
		t.Fatalf("duplicate sequence error = %v, want STORE_CONFLICT", err)
	}
	_, err = s.Append(ctx, makeEvent(runID, 1, "node_started"))
	if !IsCode(err, CodeStoreConflict) {
		t.Fatalf("exact duplicate error = %v, want STORE_CONFLICT", err)
	}
	if _, err := s.Append(ctx, makeEvent(runID, 3, "node_finished")); err != nil {
		t.Fatal(err)
	}
	_, err = s.Append(ctx, makeEvent(runID, 2, "node_finished"))
	if !IsCode(err, CodeStoreConflict) {
		t.Fatalf("non-monotonic error = %v, want STORE_CONFLICT", err)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM event"); n != 2 {
		t.Errorf("event rows = %d, want 2", n)
	}
}

func TestAppendRejectsDuplicateIdempotencyKey(t *testing.T) {
	s := openTestStore(t)
	runID := seededRun(t, s)
	ctx := context.Background()

	first := makeEvent(runID, 1, "node_started")
	first.IdempotencyKey = "cmd-abc"
	first.Payload = `{"node_key":"research"}`
	stored, err := s.Append(ctx, first)
	if err != nil {
		t.Fatal(err)
	}

	retry := makeEvent(runID, 2, "node_started")
	retry.IdempotencyKey = "cmd-abc"
	retry.Payload = `{"node_key":"other"}`
	_, err = s.Append(ctx, retry)
	if !IsCode(err, CodeStoreConflict) {
		t.Fatalf("duplicate idempotency error = %v, want STORE_CONFLICT", err)
	}

	var payload string
	var seq int64
	var eventID string
	err = s.db.QueryRow("SELECT payload, sequence, event_id FROM event WHERE idempotency_key = ?", "cmd-abc").
		Scan(&payload, &seq, &eventID)
	if err != nil {
		t.Fatal(err)
	}
	if payload != stored.Payload || seq != 1 || eventID != stored.EventID {
		t.Errorf("original row changed: payload=%q seq=%d eventID=%s", payload, seq, eventID)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM event WHERE run_id = ?", runID); n != 1 {
		t.Errorf("event rows = %d, want 1", n)
	}
}

func TestAppendValidatesEnvelope(t *testing.T) {
	s := openTestStore(t)
	runID := seededRun(t, s)
	ctx := context.Background()

	cases := []struct {
		name string
		mut  func(*Event)
	}{
		{"missing run", func(e *Event) { e.RunID = "" }},
		{"zero sequence", func(e *Event) { e.Sequence = 0 }},
		{"missing schema version", func(e *Event) { e.SchemaVersion = "" }},
		{"missing type", func(e *Event) { e.Type = "" }},
		{"zero occurred_at", func(e *Event) { e.OccurredAt = 0 }},
		{"future occurred_at", func(e *Event) { e.OccurredAt = time.Now().Add(time.Hour).UnixMilli() }},
		{"bad actor type", func(e *Event) { e.ActorType = "robot" }},
		{"missing actor id", func(e *Event) { e.ActorID = "" }},
		{"invalid payload", func(e *Event) { e.Payload = `{nope` }},
	}
	for _, tc := range cases {
		ev := makeEvent(runID, 1, "node_started")
		tc.mut(&ev)
		if _, err := s.Append(ctx, ev); !IsCode(err, CodeGraphInvalid) {
			t.Errorf("%s: error = %v, want GRAPH_INVALID", tc.name, err)
		}
	}
	if n := count(t, s, "SELECT COUNT(*) FROM event"); n != 0 {
		t.Errorf("event rows = %d, want 0", n)
	}
}

func TestAppendUnknownRunRejected(t *testing.T) {
	s := openTestStore(t)
	seededRun(t, s)
	_, err := s.Append(context.Background(), makeEvent("01NOPE", 1, "node_started"))
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("unknown run error = %v, want GRAPH_INVALID", err)
	}
}

func TestAppendRejectsMismatchedPayloadDigest(t *testing.T) {
	s := openTestStore(t)
	runID := seededRun(t, s)
	ev := makeEvent(runID, 1, "node_started")
	ev.PayloadDigest = payloadDigest("a different payload")
	_, err := s.Append(context.Background(), ev)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM event"); n != 0 {
		t.Errorf("event rows = %d, want 0", n)
	}

	ev.PayloadDigest = payloadDigest(ev.Payload)
	if _, err := s.Append(context.Background(), ev); err != nil {
		t.Fatalf("matching digest must be accepted: %v", err)
	}
}

func TestAppendComputesPayloadDigest(t *testing.T) {
	s := openTestStore(t)
	runID := seededRun(t, s)
	stored, err := s.Append(context.Background(), makeEvent(runID, 1, "node_started"))
	if err != nil {
		t.Fatal(err)
	}
	if want := payloadDigest(stored.Payload); stored.PayloadDigest != want {
		t.Errorf("payload_digest = %q, want %q", stored.PayloadDigest, want)
	}
}

func TestAppendMonotonicUnderConcurrency(t *testing.T) {
	s := openTestStore(t)
	runID := seededRun(t, s)
	ctx := context.Background()

	const writers = 2
	const perWriter = 50
	var unclassified atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				for {
					next, err := s.MaxSequence(ctx, runID)
					if err != nil {
						t.Errorf("MaxSequence: %v", err)
						return
					}
					ev := makeEvent(runID, next+1, "checkpoint")
					_, err = s.Append(ctx, ev)
					if err == nil {
						break
					}
					if !IsCode(err, CodeStoreConflict) {
						unclassified.Add(1)
						t.Errorf("unclassified append error: %v", err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	if unclassified.Load() != 0 {
		t.Fatalf("%d unclassified errors during concurrent appends", unclassified.Load())
	}
	total := writers * perWriter
	if n := count(t, s, "SELECT COUNT(*) FROM event WHERE run_id = ?", runID); n != total {
		t.Fatalf("event rows = %d, want %d", n, total)
	}
	rows, err := s.db.Query("SELECT DISTINCT sequence FROM event WHERE run_id = ?", runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatal(err)
		}
		if seen[seq] {
			t.Fatalf("duplicate sequence %d after concurrent appends", seq)
		}
		seen[seq] = true
	}
	if len(seen) != total {
		t.Fatalf("distinct sequences = %d, want %d", len(seen), total)
	}
	for seq := int64(1); seq <= int64(total); seq++ {
		if !seen[seq] {
			t.Fatalf("sequence gap at %d", seq)
		}
	}
}

func TestOpenRejectsNewerSchema(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/proceed.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(fmt.Sprintf("PRAGMA user_version = %d", storeSchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	s.Close()

	_, err = Open(path)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("newer schema error = %v, want GRAPH_INVALID", err)
	}
}

func TestOpenUpgradesOlderBaseline(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/proceed.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("PRAGMA user_version = 1"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var v int
	if err := s2.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != storeSchemaVersion {
		t.Errorf("user_version = %d, want %d", v, storeSchemaVersion)
	}
	var n int
	if err := s2.db.QueryRow("SELECT COUNT(*) FROM event").Scan(&n); err != nil {
		t.Fatal(err)
	}
}
