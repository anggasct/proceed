package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func fixtureVersion(t *testing.T, s *Store) (versionID, edgeID, nodeA, nodeB string) {
	t.Helper()
	src := readFixture(t, "../../internal/compiler/testdata/customer-research.yaml")
	doc := compileFixture(t, src)
	frozen, err := s.FreezeDefinition(context.Background(), "customer-research.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT id FROM graph_edge WHERE graph_version_id = ? LIMIT 1`,
		frozen.GraphVersionID).Scan(&edgeID); err != nil {
		t.Fatal(err)
	}
	rows, err := s.db.Query(`SELECT node_key FROM graph_node WHERE graph_version_id = ? ORDER BY node_key`,
		frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	if len(keys) < 2 {
		t.Fatalf("fixture has %d nodes, need 2", len(keys))
	}
	return frozen.GraphVersionID, edgeID, keys[0], keys[1]
}

func app(t *testing.T, s *Store, run Run, seq int64, typ string, payload string) Event {
	t.Helper()
	ev := Event{
		RunID:         run.ID,
		Sequence:      seq,
		SchemaVersion: eventSchemaVersion,
		Type:          typ,
		OccurredAt:    time.Now().UnixMilli(),
		ActorType:     "controller",
		ActorID:       "controller-1",
		Payload:       payload,
	}
	stored, err := s.Append(context.Background(), ev)
	if err != nil {
		t.Fatalf("append %s seq %d: %v", typ, seq, err)
	}
	return stored
}

func appendFixtureStream(t *testing.T, s *Store, run Run, edgeID, nodeA, nodeB string) Event {
	t.Helper()
	app(t, s, run, 2, "node_started", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"executor":"shell","side_effect_contract":"pure","operation_key":"op-1"}`, nodeA))
	app(t, s, run, 3, "edge_traversed", fmt.Sprintf(
		`{"edge_id":%q,"sequence_in_run":1}`, edgeID))
	artifact := app(t, s, run, 4, "artifact_published", fmt.Sprintf(
		`{"node_key":%q,"name":"stdout","path":"artifacts/aa","content_hash":"deadbeef","media_type":"text/plain","size_bytes":42}`,
		nodeA))
	app(t, s, run, 5, "node_finished", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"result":{"ok":true}}`, nodeA))
	app(t, s, run, 6, "node_started", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"executor":"shell","side_effect_contract":"idempotent","operation_key":"op-2"}`, nodeB))
	links := map[string]any{
		"node_key":           nodeB,
		"kind":               "routing",
		"candidate_edges":    []string{edgeID},
		"selected_edge_id":   edgeID,
		"predicate_snapshot": map[string]any{"ready": true},
		"input_references":   []string{artifact.EventID},
		"policy_version":     "policy-1",
		"causal_links": []map[string]any{{
			"target_node_key": nodeA,
			"attribution":     "necessary",
			"source_kind":     "event",
			"source_id":       artifact.EventID,
		}},
	}
	encoded, err := json.Marshal(links)
	if err != nil {
		t.Fatal(err)
	}
	app(t, s, run, 7, "decision_recorded", string(encoded))
	app(t, s, run, 8, "run_completed", `{"detail":"done"}`)
	if n := count(t, s, "SELECT COUNT(*) FROM event WHERE run_id = ?", run.ID); n != 8 {
		t.Fatalf("event rows = %d, want 8", n)
	}
	return artifact
}

func TestCreateRunEmitsRunStarted(t *testing.T) {
	s := openTestStore(t)
	versionID, _, _, _ := fixtureVersion(t, s)
	run, err := s.CreateRun(context.Background(), versionID)
	if err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM event WHERE run_id = ? AND type = 'run_started'", run.ID); n != 1 {
		t.Errorf("run_started events = %d, want 1", n)
	}
	var status string
	var digest string
	if err := s.db.QueryRow("SELECT status, definition_digest FROM graph_run WHERE id = ?", run.ID).
		Scan(&status, &digest); err != nil {
		t.Fatal(err)
	}
	if status != "running" || digest == "" {
		t.Errorf("graph_run = %s/%s", status, digest)
	}
	if run.DefinitionDigest != digest {
		t.Errorf("run digest %q != row digest %q", run.DefinitionDigest, digest)
	}
}

func TestCreateRunRejectsUnknownVersion(t *testing.T) {
	s := openTestStore(t)
	_, err := s.CreateRun(context.Background(), "01NOPE")
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
}

func TestRebuildFromEmptyProjectionReproducesDigest(t *testing.T) {
	s := openTestStore(t)
	versionID, edgeID, nodeA, nodeB := fixtureVersion(t, s)
	run, err := s.CreateRun(context.Background(), versionID)
	if err != nil {
		t.Fatal(err)
	}
	appendFixtureStream(t, s, run, edgeID, nodeA, nodeB)
	live, err := s.ProjectionDigest(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	report, err := s.RebuildProjections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Diverged {
		t.Errorf("healthy rebuild reported divergence: before %s after %s", report.Before, report.After)
	}
	if report.After != live {
		t.Errorf("rebuilt digest %s != live digest %s", report.After, live)
	}
	report, err = s.RebuildProjections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Diverged || report.After != live {
		t.Errorf("second rebuild = %+v, want stable digest %s", report, live)
	}
}

func TestRebuildDetectsInjectedDivergence(t *testing.T) {
	s := openTestStore(t)
	versionID, edgeID, nodeA, nodeB := fixtureVersion(t, s)
	run, err := s.CreateRun(context.Background(), versionID)
	if err != nil {
		t.Fatal(err)
	}
	appendFixtureStream(t, s, run, edgeID, nodeA, nodeB)
	healthy, err := s.ProjectionDigest(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.db.Exec(
		`UPDATE run_node SET status = 'failed', attempt_count = 99 WHERE run_id = ?`, run.ID); err != nil {
		t.Fatal(err)
	}
	report, err := s.RebuildProjections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Diverged {
		t.Error("injected divergence must be detected")
	}
	if report.After != healthy {
		t.Errorf("post-rebuild digest %s != healthy %s", report.After, healthy)
	}
	var status string
	var attempts int
	if err := s.db.QueryRow(
		`SELECT status, attempt_count FROM run_node WHERE run_id = ? AND node_key = ?`, run.ID, nodeA).
		Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "succeeded" || attempts != 1 {
		t.Errorf("run_node = %s/%d after rebuild, want succeeded/1", status, attempts)
	}
}

func rawInsertEvent(t *testing.T, s *Store, ev Event) {
	t.Helper()
	if ev.EventID == "" {
		t.Fatal("raw event needs an id")
	}
	_, err := s.db.Exec(`
INSERT INTO event (event_id, run_id, sequence, schema_version, type, occurred_at, recorded_at,
                   actor_type, actor_id, causation_id, correlation_id, idempotency_key,
                   payload_digest, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.EventID, ev.RunID, ev.Sequence, ev.SchemaVersion, ev.Type, ev.OccurredAt,
		time.Now().UnixMilli(), ev.ActorType, ev.ActorID, nullable(ev.CausationID),
		nullable(ev.CorrelationID), nullable(ev.IdempotencyKey),
		payloadDigest(ev.Payload), ev.Payload)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRebuildConvergesAfterCrashBeforeProjection(t *testing.T) {
	s := openTestStore(t)
	versionID, _, nodeA, _ := fixtureVersion(t, s)
	run, err := s.CreateRun(context.Background(), versionID)
	if err != nil {
		t.Fatal(err)
	}
	app(t, s, run, 2, "node_started", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"executor":"shell","side_effect_contract":"pure","operation_key":"op-1"}`, nodeA))

	crashed := Event{
		EventID:       "01CRASH0000000000000000000",
		RunID:         run.ID,
		Sequence:      3,
		SchemaVersion: eventSchemaVersion,
		Type:          "artifact_published",
		OccurredAt:    time.Now().UnixMilli(),
		ActorType:     "executor",
		ActorID:       "shell",
		Payload: fmt.Sprintf(
			`{"node_key":%q,"name":"stdout","path":"artifacts/bb","content_hash":"cafebabe","media_type":"text/plain","size_bytes":7}`,
			nodeA),
	}
	rawInsertEvent(t, s, crashed)
	if n := count(t, s, "SELECT COUNT(*) FROM artifact WHERE id = ?", crashed.EventID); n != 0 {
		t.Fatal("crash simulation broken: artifact projection exists before rebuild")
	}

	report, err := s.RebuildProjections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Diverged {
		t.Error("missing projection after crash must be reported as divergence")
	}
	if n := count(t, s, "SELECT COUNT(*) FROM artifact WHERE id = ?", crashed.EventID); n != 1 {
		t.Errorf("artifact projection missing after rebuild")
	}
	report, err = s.RebuildProjections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Diverged {
		t.Error("rebuild must converge: second rebuild reports divergence")
	}

	app(t, s, run, 4, "node_finished", fmt.Sprintf(`{"node_key":%q,"attempt_no":1}`, nodeA))
	app(t, s, run, 5, "run_completed", `{"detail":"done"}`)
}

func TestUnknownEventTypeStoredAndIgnored(t *testing.T) {
	s := openTestStore(t)
	versionID, _, nodeA, _ := fixtureVersion(t, s)
	run, err := s.CreateRun(context.Background(), versionID)
	if err != nil {
		t.Fatal(err)
	}
	app(t, s, run, 2, "future_migration_marker", `{"anything":true}`)
	app(t, s, run, 3, "node_started", fmt.Sprintf(`{"node_key":%q}`, nodeA))
	if n := count(t, s, "SELECT COUNT(*) FROM event WHERE run_id = ?", run.ID); n != 3 {
		t.Errorf("event rows = %d, want 3", n)
	}
}

func TestRunTerminalProjections(t *testing.T) {
	s := openTestStore(t)
	versionID, edgeID, nodeA, nodeB := fixtureVersion(t, s)
	run, err := s.CreateRun(context.Background(), versionID)
	if err != nil {
		t.Fatal(err)
	}
	appendFixtureStream(t, s, run, edgeID, nodeA, nodeB)

	var status string
	var finished any
	if err := s.db.QueryRow("SELECT status, finished_at FROM graph_run WHERE id = ?", run.ID).
		Scan(&status, &finished); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || finished == nil {
		t.Errorf("graph_run = %s/%v", status, finished)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM outcome WHERE run_id = ?", run.ID); n != 1 {
		t.Errorf("outcome rows = %d, want 1", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM anchor"); n != 1 {
		t.Errorf("anchor rows = %d, want 1", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM decision WHERE run_id = ?", run.ID); n != 1 {
		t.Errorf("decision rows = %d, want 1", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM causal_link"); n != 1 {
		t.Errorf("causal_link rows = %d, want 1", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM run_edge WHERE run_id = ?", run.ID); n != 1 {
		t.Errorf("run_edge rows = %d, want 1", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM node_attempt"); n != 2 {
		t.Errorf("node_attempt rows = %d, want 2", n)
	}
}
