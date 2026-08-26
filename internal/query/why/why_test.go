package why

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"proceed/internal/compiler"
	"proceed/internal/store"
)

const fixtureGraph = `schema: proceed/v1
name: why-fixture
nodes:
  - id: a
    type: task
    executor: { kind: shell, command: [bin/a] }
    contract: pure
  - id: b
    type: task
    executor: { kind: shell, command: [bin/b] }
    contract: pure
    terminal: true
edges:
  - { from: a, to: b, type: depends_on }
`

type fixture struct {
	t       *testing.T
	st      *store.Store
	q       *Query
	runID   string
	version string
	seq     int64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	doc, err := compiler.Parse([]byte(fixtureGraph))
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(ctx, "fixture.yaml", []byte(fixtureGraph), doc)
	if err != nil {
		t.Fatal(err)
	}
	run, err := st.CreateRun(ctx, frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, st: st, q: New(st), runID: run.ID, version: frozen.GraphVersionID, seq: 1}
}

func (f *fixture) append(typ string, payload any) {
	f.t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		f.t.Fatal(err)
	}
	f.seq++
	_, err = f.st.Append(context.Background(), store.Event{
		RunID:         f.runID,
		Sequence:      f.seq,
		SchemaVersion: "proceed/v1",
		Type:          typ,
		OccurredAt:    1700000000000 + f.seq,
		ActorType:     "controller",
		ActorID:       "fixture",
		Payload:       string(body),
	})
	if err != nil {
		f.t.Fatal(err)
	}
}

func TestExplainUnknownRunIsTypedError(t *testing.T) {
	f := newFixture(t)
	_, err := f.q.Explain(context.Background(), "does-not-exist", "a")
	if err == nil {
		t.Fatal("expected error for unknown run")
	}
	if code := store.ErrorCode(err); code != "RUN_NOT_FOUND" {
		t.Errorf("error code = %q, want RUN_NOT_FOUND", code)
	}
}

func TestExplainUnknownNodeNamesRunAndNode(t *testing.T) {
	f := newFixture(t)
	_, err := f.q.Explain(context.Background(), f.runID, "nope")
	if err == nil {
		t.Fatal("expected error for unknown node")
	}
	if code := store.ErrorCode(err); code == "" || code == "RUN_NOT_FOUND" {
		t.Errorf("error code = %q, want a distinct node-level class", code)
	}
	msg := err.Error()
	if !strings.Contains(msg, f.runID) || !strings.Contains(msg, "nope") {
		t.Errorf("error %q must name both the run id and the node id", msg)
	}
}

func TestPendingNodeListsCandidatesWithoutSelection(t *testing.T) {
	f := newFixture(t)

	var edgeID string
	if err := f.st.DB().QueryRow(
		"SELECT id FROM graph_edge WHERE graph_version_id = ? AND from_node_key = 'a'",
		f.version).Scan(&edgeID); err != nil {
		t.Fatal(err)
	}

	explanation, err := f.q.Explain(context.Background(), f.runID, "b")
	if err != nil {
		t.Fatal(err)
	}
	if explanation.Recorded.NodeStatus != "pending" {
		t.Errorf("status = %q, want pending", explanation.Recorded.NodeStatus)
	}
	if len(explanation.Recorded.CandidateEdges) != 1 || explanation.Recorded.CandidateEdges[0] != edgeID {
		t.Errorf("candidates = %v, want [%s]", explanation.Recorded.CandidateEdges, edgeID)
	}
	if len(explanation.Recorded.Decisions) != 0 {
		t.Errorf("decisions = %d, want 0", len(explanation.Recorded.Decisions))
	}
	if !explanation.Inference.Pending {
		t.Error("inference.pending = false, want true")
	}
	if len(explanation.Inference.Attributions) != 0 {
		t.Errorf("attributions = %v, want empty", explanation.Inference.Attributions)
	}
}

func TestEvidenceJoinIncludesArtifactsEvaluationsAndApprovals(t *testing.T) {
	f := newFixture(t)
	f.append("node_started", map[string]any{"node_key": "b", "attempt_no": 1})
	f.append("artifact_published", map[string]any{
		"node_key":     "b",
		"name":         "report",
		"path":         "out/report",
		"content_hash": "sha256:c0ffee",
		"media_type":   "text/markdown",
		"size_bytes":   42,
	})
	var artifactEventID string
	if err := f.st.DB().QueryRow(
		"SELECT id FROM artifact WHERE run_id = ? AND name = 'report'", f.runID).Scan(&artifactEventID); err != nil {
		t.Fatal(err)
	}
	f.append("evaluation_failed", map[string]any{
		"artifact_id":           artifactEventID,
		"evaluated_by_node_key": "reviewer",
		"evidence_ref":          "eval-ref-1",
	})
	f.append("approval_requested", map[string]any{
		"node_key":            "b",
		"requested_action":    json.RawMessage(`{"action":"publish"}`),
		"evidence_references": json.RawMessage(`["eval-ref-1"]`),
		"required_scope":      "deploy",
		"expires_at":          1800000000000,
	})
	var approvalID string
	if err := f.st.DB().QueryRow(
		"SELECT id FROM approval WHERE run_id = ? AND required_scope = 'deploy'", f.runID).Scan(&approvalID); err != nil {
		t.Fatal(err)
	}
	f.append("approval_granted", map[string]any{
		"approval_id": approvalID,
		"decided_by":  "alice",
	})

	explanation, err := f.q.Explain(context.Background(), f.runID, "b")
	if err != nil {
		t.Fatal(err)
	}
	ev := explanation.Recorded.Evidence
	if len(ev.Artifacts) != 1 || ev.Artifacts[0].ContentHash != "sha256:c0ffee" {
		t.Errorf("artifacts = %+v, want one with hash sha256:c0ffee", ev.Artifacts)
	}
	if len(ev.Evaluations) != 1 || ev.Evaluations[0].Verdict != "failed" {
		t.Errorf("evaluations = %+v, want one failed verdict", ev.Evaluations)
	}
	if len(ev.Approvals) != 1 {
		t.Fatalf("approvals = %+v, want one", ev.Approvals)
	}
	if ev.Approvals[0].Decision != "grant" || ev.Approvals[0].DecidedBy != "alice" {
		t.Errorf("approval decision = %s by %s, want grant by alice",
			ev.Approvals[0].Decision, ev.Approvals[0].DecidedBy)
	}
}

func TestPartialProjectionFallbackReplaysCausalLinksAndEvidence(t *testing.T) {
	f := newFixture(t)
	f.append("node_started", map[string]any{"node_key": "a", "attempt_no": 1})
	f.append("artifact_published", map[string]any{
		"node_key": "a", "name": "result", "content_hash": "sha256:abc", "media_type": "text/plain", "size_bytes": 3,
	})
	var artifactID string
	if err := f.st.DB().QueryRow("SELECT id FROM artifact WHERE run_id = ?", f.runID).Scan(&artifactID); err != nil {
		t.Fatal(err)
	}
	f.append("decision_recorded", map[string]any{
		"node_key": "b", "kind": "routing", "candidate_edges": []string{},
		"predicate_snapshot": map[string]any{}, "input_references": []string{"artifact:" + artifactID},
		"policy_version": "v1", "causal_links": []map[string]any{{
			"target_node_key": "b", "attribution": "necessary", "source_kind": "event", "source_id": "event-source",
		}},
	})
	if _, err := f.st.DB().Exec("DELETE FROM causal_link"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.st.DB().Exec("DELETE FROM artifact"); err != nil {
		t.Fatal(err)
	}

	explanation, err := f.q.Explain(context.Background(), f.runID, "b")
	if err != nil {
		t.Fatal(err)
	}
	if explanation.Recorded.Source != "events" {
		t.Fatalf("source = %q, want events", explanation.Recorded.Source)
	}
	if len(explanation.Recorded.CausalLinks) != 1 {
		t.Fatalf("causal links = %d, want 1", len(explanation.Recorded.CausalLinks))
	}
	if len(explanation.Recorded.Evidence.Artifacts) != 1 || explanation.Recorded.Evidence.Artifacts[0].ID != artifactID {
		t.Fatalf("artifacts = %+v, want cited artifact %s", explanation.Recorded.Evidence.Artifacts, artifactID)
	}
}

func TestCompletedRootIsNotPending(t *testing.T) {
	f := newFixture(t)
	f.append("node_started", map[string]any{"node_key": "a", "attempt_no": 1})
	f.append("node_finished", map[string]any{"node_key": "a", "attempt_no": 1})

	explanation, err := f.q.Explain(context.Background(), f.runID, "a")
	if err != nil {
		t.Fatal(err)
	}
	if explanation.Recorded.NodeStatus != "succeeded" {
		t.Fatalf("status = %q, want succeeded", explanation.Recorded.NodeStatus)
	}
	if explanation.Inference.Pending {
		t.Fatal("completed root was reported as pending")
	}
}
