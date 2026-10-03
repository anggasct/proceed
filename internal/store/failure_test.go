package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func failureFixture(t *testing.T) (*Store, Run, string, string) {
	t.Helper()
	s := openTestStore(t)
	versionID, _, nodeA, nodeB := fixtureVersion(t, s)
	run, err := s.CreateRun(context.Background(), versionID, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return s, run, nodeA, nodeB
}

func nodeFailureFor(t *testing.T, s *Store, runID, nodeKey string) *NodeFailure {
	t.Helper()
	graph, err := s.RuntimeGraph(context.Background(), runID)
	if err != nil {
		t.Fatalf("runtime graph: %v", err)
	}
	for _, n := range graph.Nodes {
		if n.NodeKey == nodeKey {
			return n.Failure
		}
	}
	t.Fatalf("node %q missing from run graph", nodeKey)
	return nil
}

func startNodeAttempt(t *testing.T, s *Store, run Run, seq int64, nodeKey string, attemptNo int64) int64 {
	t.Helper()
	app(t, s, run, seq, "node_started", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":%d,"executor":"shell","side_effect_contract":"pure","operation_key":"op-%d"}`,
		nodeKey, attemptNo, attemptNo))
	return seq + 1
}

func TestRuntimeGraphSurfacesExecutorFailureFromTerminalEvent(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	seq := startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, seq, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"NODE_FAILED: command exited with status 2"}`, nodeA))

	var stored *string
	if err := s.db.QueryRow(`SELECT result FROM node_attempt na JOIN run_node rn ON rn.id = na.run_node_id
		WHERE rn.run_id = ? AND rn.node_key = ?`, run.ID, nodeA).Scan(&stored); err == nil && stored != nil {
		t.Fatalf("fixture precondition: node_attempt.result = %q, want NULL", *stored)
	}

	failure := nodeFailureFor(t, s, run.ID, nodeA)
	if failure == nil || failure.Code == nil || *failure.Code != "NODE_FAILED" {
		t.Fatalf("failure = %+v, want code NODE_FAILED", failure)
	}
	if failure.Message != "command exited with status 2" {
		t.Errorf("message = %q, want %q", failure.Message, "command exited with status 2")
	}
}

func TestRuntimeGraphFailureCodeNullWhenPrefixIsNotAStableClass(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	seq := startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, seq, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"agent CLI crashed"}`, nodeA))

	failure := nodeFailureFor(t, s, run.ID, nodeA)
	if failure == nil || failure.Code != nil {
		t.Fatalf("failure = %+v, want nil code", failure)
	}
	if failure.Message != "agent CLI crashed" {
		t.Errorf("message = %q, want the full text", failure.Message)
	}
}

func TestRuntimeGraphFallsBackToAttemptResultWhenEventCarriesNoCause(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, 3, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"executor":"shell","side_effect_contract":"pure","operation_key":"op-1","result":{"error":"POLICY_DENIED: sandbox is unavailable"}}`, nodeA))

	failure := nodeFailureFor(t, s, run.ID, nodeA)
	if failure == nil || failure.Code == nil || *failure.Code != "POLICY_DENIED" {
		t.Fatalf("failure = %+v, want code POLICY_DENIED from attempt result", failure)
	}
	if failure.Message != "sandbox is unavailable" {
		t.Errorf("message = %q, want %q", failure.Message, "sandbox is unavailable")
	}
}

func TestRuntimeGraphFailureWithoutAttemptRow(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	app(t, s, run, 2, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"NODE_TIMEOUT: node exceeded its timeout budget"}`, nodeA))

	if n := count(t, s, `SELECT COUNT(*) FROM node_attempt na JOIN run_node rn ON rn.id = na.run_node_id
		WHERE rn.run_id = ? AND rn.node_key = ?`, run.ID, nodeA); n != 0 {
		t.Fatalf("attempt rows = %d, want 0", n)
	}

	failure := nodeFailureFor(t, s, run.ID, nodeA)
	if failure == nil || failure.Code == nil || *failure.Code != "NODE_TIMEOUT" {
		t.Fatalf("failure = %+v, want code NODE_TIMEOUT without an attempt row", failure)
	}
}

func TestRuntimeGraphUncertainCauseComesFromEventReason(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	seq := startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, seq, "node_uncertain", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"reason":"EFFECT_UNCERTAIN: receipt missing"}`, nodeA))

	var stored sql.NullString
	if err := s.db.QueryRow(`SELECT na.result FROM node_attempt na JOIN run_node rn ON rn.id = na.run_node_id
		WHERE rn.run_id = ? AND rn.node_key = ? AND na.attempt_no = 1`, run.ID, nodeA).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored.Valid {
		t.Fatalf("fixture precondition: attempt result = %q, want NULL so the cause can only come from the event", stored.String)
	}

	failure := nodeFailureFor(t, s, run.ID, nodeA)
	if failure == nil || failure.Code == nil || *failure.Code != "EFFECT_UNCERTAIN" {
		t.Fatalf("failure = %+v, want code EFFECT_UNCERTAIN", failure)
	}
	if failure.Message != "receipt missing" {
		t.Errorf("message = %q, want the uncertain reason", failure.Message)
	}
}

func TestRuntimeGraphUncertainReasonWithoutClassPrefix(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, 3, "node_uncertain", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"reason":"provider callback never arrived"}`, nodeA))

	failure := nodeFailureFor(t, s, run.ID, nodeA)
	if failure == nil || failure.Code != nil || failure.Message != "provider callback never arrived" {
		t.Fatalf("failure = %+v, want nil code and the full reason", failure)
	}
}

func TestRuntimeGraphEventCauseWinsOverAttemptResult(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, 3, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"NODE_FAILED: command exited with status 2","result":{"error":"POLICY_DENIED: sandbox is unavailable"}}`, nodeA))

	failure := nodeFailureFor(t, s, run.ID, nodeA)
	if failure == nil || failure.Message != "command exited with status 2" {
		t.Fatalf("failure = %+v, want the event cause", failure)
	}
}

func TestRuntimeGraphIgnoresTerminalEventFromSupersededAttempt(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	seq := startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, seq, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"NODE_FAILED: command exited with status 2"}`, nodeA))
	seq = startNodeAttempt(t, s, run, seq+1, nodeA, 2)
	app(t, s, run, seq, "node_finished", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":2,"result":{"ok":true}}`, nodeA))

	if failure := nodeFailureFor(t, s, run.ID, nodeA); failure != nil {
		t.Fatalf("failure = %+v, want null for a recovered node", failure)
	}
}

func TestRuntimeGraphLatestTerminalEventForSameAttemptWins(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	seq := startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, seq, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"NODE_FAILED: first failure"}`, nodeA))
	app(t, s, run, seq+1, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"NODE_FAILED: second failure"}`, nodeA))

	failure := nodeFailureFor(t, s, run.ID, nodeA)
	if failure == nil || failure.Message != "second failure" {
		t.Fatalf("failure = %+v, want the highest-sequence terminal event", failure)
	}
}

func TestRuntimeGraphFailureNullForUnusableStoredValues(t *testing.T) {
	cases := []struct {
		name   string
		result *string
	}{
		{name: "attempt result null", result: nil},
		{name: "attempt result empty string", result: strptr("")},
		{name: "attempt result whitespace", result: strptr("   ")},
		{name: "attempt result not json", result: strptr("not json")},
		{name: "attempt result without error key", result: strptr(`{"ok":true}`)},
		{name: "attempt result with empty error", result: strptr(`{"error":""}`)},
		{name: "attempt result error not a string", result: strptr(`{"error":{"detail":"nested"}}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, run, nodeA, _ := failureFixture(t)
			startNodeAttempt(t, s, run, 2, nodeA, 1)
			app(t, s, run, 3, "node_failed", fmt.Sprintf(`{"node_key":%q,"attempt_no":1}`, nodeA))
			result := "NULL"
			if tc.result != nil {
				result = *tc.result
			}
			if _, err := s.db.Exec(`UPDATE node_attempt SET result = ? WHERE id IN (
				SELECT na.id FROM node_attempt na JOIN run_node rn ON rn.id = na.run_node_id
				WHERE rn.run_id = ? AND rn.node_key = ? AND na.attempt_no = 1)`, tc.result, run.ID, nodeA); err != nil {
				t.Fatal(err)
			}
			if failure := nodeFailureFor(t, s, run.ID, nodeA); failure != nil {
				t.Fatalf("result %s yielded failure = %+v, want null", result, failure)
			}
		})
	}
}

func TestRuntimeGraphEmptyEventCauseFallsBackToAttemptResult(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, 3, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"executor":"shell","side_effect_contract":"pure","operation_key":"op-1","result":{"error":"POLICY_DENIED: sandbox is unavailable"}}`, nodeA))

	failure := nodeFailureFor(t, s, run.ID, nodeA)
	if failure == nil || failure.Code == nil || *failure.Code != "POLICY_DENIED" {
		t.Fatalf("failure = %+v, want the attempt-result fallback", failure)
	}
}

func TestRuntimeGraphSurvivesCorruptStoredPayloads(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, 3, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"NODE_FAILED: command exited with status 2"}`, nodeA))
	if _, err := s.db.Exec(`UPDATE event SET payload = '{"node_key":' WHERE run_id = ? AND sequence = 3`, run.ID); err != nil {
		t.Fatal(err)
	}

	if failure := nodeFailureFor(t, s, run.ID, nodeA); failure != nil {
		t.Fatalf("failure = %+v, want null for an undecodable stored payload", failure)
	}
	graph, err := s.RuntimeGraph(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("runtime graph must not fail on corrupt history: %v", err)
	}
	if len(graph.Nodes) == 0 {
		t.Error("graph lost its nodes")
	}
}

func TestRuntimeGraphNonStringCauseIsNotClassified(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	app(t, s, run, 2, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":{"detail":"nested"}}`, nodeA))

	failure := nodeFailureFor(t, s, run.ID, nodeA)
	if failure == nil || failure.Code != nil {
		t.Fatalf("failure = %+v, want nil code for a non-string cause", failure)
	}
	if failure.Message != `{"detail":"nested"}` {
		t.Errorf("message = %q, want the JSON-encoded cause", failure.Message)
	}
}

func TestRuntimeGraphFailureKeyPresentOnEveryNode(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	seq := startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, seq, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"NODE_FAILED: command exited with status 2"}`, nodeA))

	graph, err := s.RuntimeGraph(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(graph)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(encoded), `"failure":null`); n != len(graph.Nodes)-1 {
		t.Fatalf("null failure keys = %d, want %d (one per non-failed node): %s", n, len(graph.Nodes)-1, encoded)
	}
	if !strings.Contains(string(encoded), `"failure":{"code":"NODE_FAILED"`) {
		t.Fatalf("payload lacks the failed node failure: %s", encoded)
	}
	if graph.Nodes[0].NodeKey != nodeA && graph.Nodes[len(graph.Nodes)-1].NodeKey != nodeA {
		t.Fatalf("nodes = %+v, want %q present", graph.Nodes, nodeA)
	}
	for i := 1; i < len(graph.Nodes); i++ {
		if graph.Nodes[i-1].NodeKey >= graph.Nodes[i].NodeKey {
			t.Fatalf("nodes = %+v, want ascending node_key ordering", graph.Nodes)
		}
	}
	if graph.Edges[0].Type == "" {
		t.Errorf("edge type missing: %+v", graph.Edges)
	}
}

func TestRuntimeGraphReadsAreStableAndSideEffectFree(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	seq := startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, seq, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"NODE_FAILED: command exited with status 2"}`, nodeA))

	before := map[string]int{
		"event":        count(t, s, `SELECT COUNT(*) FROM event WHERE run_id = ?`, run.ID),
		"node_attempt": count(t, s, `SELECT COUNT(*) FROM node_attempt`),
		"run_node":     count(t, s, `SELECT COUNT(*) FROM run_node WHERE run_id = ?`, run.ID),
	}
	first, err := s.RuntimeGraph(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.RuntimeGraph(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("reads differ:\n%s\n%s", firstJSON, secondJSON)
	}
	for _, check := range []struct {
		label string
		query string
		args  []any
		want  int
	}{
		{"event", `SELECT COUNT(*) FROM event WHERE run_id = ?`, []any{run.ID}, before["event"]},
		{"node_attempt", `SELECT COUNT(*) FROM node_attempt`, nil, before["node_attempt"]},
		{"run_node", `SELECT COUNT(*) FROM run_node WHERE run_id = ?`, []any{run.ID}, before["run_node"]},
	} {
		if got := count(t, s, check.query, check.args...); got != check.want {
			t.Errorf("%s rows after reads = %d, want %d", check.label, got, check.want)
		}
	}
}

func TestFailureMessageTruncatesOnRuneBoundary(t *testing.T) {
	cases := []struct {
		name string
		text string
		want int
	}{
		{name: "two byte runes", text: strings.Repeat("é", 400), want: failureMessageCap},
		{name: "three byte runes", text: strings.Repeat("€", 400), want: failureMessageCap / 3 * 3},
		{name: "ascii", text: strings.Repeat("x", failureMessageCap+10), want: failureMessageCap},
		{name: "short", text: "short", want: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateFailureMessage(tc.text)
			if len(got) != tc.want {
				t.Errorf("length = %d, want %d", len(got), tc.want)
			}
			if len(got) > failureMessageCap {
				t.Errorf("length = %d, want <= %d", len(got), failureMessageCap)
			}
			if !utf8.ValidString(got) {
				t.Errorf("truncation split a rune: %q", got)
			}
		})
	}
}

func TestRuntimeGraphWhitespaceOnlyCauseYieldsNull(t *testing.T) {
	s, run, nodeA, _ := failureFixture(t)
	startNodeAttempt(t, s, run, 2, nodeA, 1)
	app(t, s, run, 3, "node_failed", fmt.Sprintf(
		`{"node_key":%q,"attempt_no":1,"error":"   "}`, nodeA))

	if failure := nodeFailureFor(t, s, run.ID, nodeA); failure != nil {
		t.Fatalf("failure = %+v, want null for a whitespace-only cause", failure)
	}
}

func TestFailureMessageCapAppliesToUnclassifiedText(t *testing.T) {
	overlong := strings.Repeat("x", failureMessageCap+100)
	failure := newNodeFailure(overlong)
	if failure.Code != nil {
		t.Errorf("code = %v, want nil", *failure.Code)
	}
	if len(failure.Message) != failureMessageCap {
		t.Errorf("message length = %d, want %d", len(failure.Message), failureMessageCap)
	}
}

func TestNodeAttemptSchemaIsUnchanged(t *testing.T) {
	s := openTestStore(t)
	want := []string{
		"id", "run_node_id", "attempt_no", "operation_key", "executor", "side_effect_contract",
		"lease_token", "lease_expires_at", "status", "result", "started_at", "finished_at",
	}
	rows, err := s.db.Query(`PRAGMA table_info(node_attempt)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var dflt any
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			t.Fatal(err)
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("node_attempt columns = %v, want %v", got, want)
	}
}

func strptr(s string) *string { return &s }
