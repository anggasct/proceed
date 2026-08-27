package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"proceed/internal/executor"
	"proceed/internal/query/why"
	"proceed/internal/store"
)

func runToCompletion(t *testing.T, st *store.Store, frozen store.FrozenVersion, pool map[executor.Kind]executor.Executor) string {
	t.Helper()
	c := newController(t, st, pool)
	runID, err := c.Run(context.Background(), RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	return runID
}

func explainNode(t *testing.T, st *store.Store, runID, nodeKey string) *why.Explanation {
	t.Helper()
	explanation, err := why.New(st).Explain(context.Background(), runID, nodeKey)
	if err != nil {
		t.Fatal(err)
	}
	return explanation
}

func edgeIDBetween(t *testing.T, st *store.Store, versionID, from, to, typ string) string {
	t.Helper()
	var id string
	if err := st.DB().QueryRow(
		"SELECT id FROM graph_edge WHERE graph_version_id = ? AND from_node_key = ? AND to_node_key = ? AND type = ?",
		versionID, from, to, typ).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func artifactIDsByNode(t *testing.T, st *store.Store, runID, nodeKey string) []string {
	t.Helper()
	rows, err := st.DB().Query(
		"SELECT id FROM artifact WHERE run_id = ? AND produced_by_node_key = ? ORDER BY id", runID, nodeKey)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func artifactProducingResult() *executor.Result {
	return &executor.Result{
		Route: "requires_code",
		Artifacts: []executor.ArtifactRef{{
			Name:        "classification",
			Path:        "out/classification",
			ContentHash: "sha256:deadbeef",
			MediaType:   "text/plain",
			SizeBytes:   11,
		}},
	}
}

const routingWhyGraph = `schema: proceed/v1
name: routing-why
nodes:
  - id: classify
    type: model
    executor: { kind: shell, command: [bin/c] }
    contract: pure
  - id: code_path
    type: task
    executor: { kind: shell, command: [bin/code] }
    contract: pure
    terminal: true
  - id: docs_path
    type: task
    executor: { kind: shell, command: [bin/docs] }
    contract: pure
    terminal: true
edges:
  - { from: classify, to: code_path, type: routes_to, when: requires_code }
  - { from: classify, to: docs_path, type: routes_to, when: requires_docs }
`

func TestWhyRoutingDecisionRecordsSelectionAndInputs(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, routingWhyGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "classify" {
				return artifactProducingResult(), nil
			}
			return &executor.Result{}, nil
		}),
	}
	runID := runToCompletion(t, st, frozen, pool)

	codeEdge := edgeIDBetween(t, st, frozen.GraphVersionID, "classify", "code_path", "routes_to")
	docsEdge := edgeIDBetween(t, st, frozen.GraphVersionID, "classify", "docs_path", "routes_to")

	explanation := explainNode(t, st, runID, "classify")
	rec := explanation.Recorded
	if len(rec.Decisions) != 1 {
		t.Fatalf("decisions = %d, want 1", len(rec.Decisions))
	}
	d := rec.Decisions[0]
	gotCandidates := append([]string(nil), d.CandidateEdges...)
	sort.Strings(gotCandidates)
	wantCandidates := []string{codeEdge, docsEdge}
	sort.Strings(wantCandidates)
	if len(gotCandidates) != 2 || gotCandidates[0] != wantCandidates[0] || gotCandidates[1] != wantCandidates[1] {
		t.Errorf("candidates = %v, want %v", d.CandidateEdges, wantCandidates)
	}
	if d.SelectedEdgeID != codeEdge {
		t.Errorf("selected = %q, want %q", d.SelectedEdgeID, codeEdge)
	}
	if d.PolicyVersion != frozen.Digest {
		t.Errorf("policy_version = %q, want digest %q", d.PolicyVersion, frozen.Digest)
	}
	var snapshot map[string]any
	if err := json.Unmarshal(d.PredicateSnapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot["route"] != "requires_code" {
		t.Errorf("predicate route = %v, want requires_code", snapshot["route"])
	}
	wantRef := "artifact:" + artifactIDsByNode(t, st, runID, "classify")[0]
	foundInputRef := false
	for _, ref := range d.InputReferences {
		if ref == wantRef {
			foundInputRef = true
		}
	}
	if !foundInputRef {
		t.Errorf("input_references %v missing %q", d.InputReferences, wantRef)
	}
	if explanation.Inference.SelectedEdge != codeEdge {
		t.Errorf("inference.selected_edge = %q, want %q", explanation.Inference.SelectedEdge, codeEdge)
	}
}

func TestWhyOutputSeparatesFactAndInference(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, routingWhyGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "classify" {
				return artifactProducingResult(), nil
			}
			return &executor.Result{}, nil
		}),
	}
	runID := runToCompletion(t, st, frozen, pool)

	raw, err := json.Marshal(explainNode(t, st, runID, "classify"))
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	if len(top) != 2 {
		t.Fatalf("top-level keys = %d (%v), want exactly recorded + inference", len(top), top)
	}
	if _, ok := top["recorded"]; !ok {
		t.Error("missing recorded key")
	}
	if _, ok := top["inference"]; !ok {
		t.Error("missing inference key")
	}
}

func TestWhyBlockedByCitesDecidingNode(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, routingWhyGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "classify" {
				return artifactProducingResult(), nil
			}
			return &executor.Result{}, nil
		}),
	}
	runID := runToCompletion(t, st, frozen, pool)

	explanation := explainNode(t, st, runID, "docs_path")
	if explanation.Recorded.NodeStatus != "skipped" {
		t.Errorf("docs_path status = %q, want skipped", explanation.Recorded.NodeStatus)
	}
	blocked := false
	for _, link := range explanation.Recorded.CausalLinks {
		if link.Attribution == "blocked_by" && link.DecidedByNode == "classify" && link.TargetNodeKey == "docs_path" {
			blocked = true
		}
	}
	if !blocked {
		t.Errorf("no blocked_by link citing classify: %+v", explanation.Recorded.CausalLinks)
	}
	strengths := map[string]bool{}
	for _, a := range explanation.Inference.Attributions {
		strengths[a.Strength] = true
	}
	if !strengths["blocked_by"] {
		t.Errorf("attributions %v missing blocked_by", explanation.Inference.Attributions)
	}
}

func TestWhyConjunctiveGroupSufficientMembersContributing(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, fanOutGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "source" || req.NodeKey == "join" {
				return &executor.Result{}, nil
			}
			return &executor.Result{Artifacts: []executor.ArtifactRef{{
				Name:        "out",
				Path:        "out/" + req.NodeKey,
				ContentHash: "sha256:" + req.NodeKey,
				MediaType:   "text/plain",
				SizeBytes:   4,
			}}}, nil
		}),
	}
	runID := runToCompletion(t, st, frozen, pool)

	explanation := explainNode(t, st, runID, "join")
	groupLinks := map[string][]string{}
	for _, link := range explanation.Recorded.CausalLinks {
		if link.GroupKey != "" {
			groupLinks[link.GroupKey] = append(groupLinks[link.GroupKey], link.Attribution+"("+link.SourceID+")")
		}
	}
	if len(groupLinks) != 1 {
		t.Fatalf("group keys = %v, want exactly one conjunctive group", groupLinks)
	}
	var groupKey string
	members := 0
	for key, attrs := range groupLinks {
		groupKey = key
		members = len(attrs)
		for _, a := range attrs {
			if !contains(a, "contributing") {
				t.Errorf("group member attribution %q is not contributing (final state must reflect both branches succeeded)", a)
			}
		}
	}
	if members != 2 {
		t.Errorf("group members = %d, want 2", members)
	}
	sufficientGroups := 0
	for _, a := range explanation.Inference.Attributions {
		if a.Strength == "sufficient" {
			sufficientGroups++
			if a.GroupKey != groupKey || a.GroupMembers != 2 {
				t.Errorf("sufficient attribution = %+v, want group %s with 2 members", a, groupKey)
			}
		}
	}
	if sufficientGroups != 1 {
		t.Errorf("sufficient attributions = %d, want exactly the group (members never individually sufficient)", sufficientGroups)
	}
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}

func TestWhySoleDependencyNecessaryWithEvidenceUnknownWithout(t *testing.T) {
	for _, tc := range []struct {
		name         string
		withArtifact bool
		wantStrength string
	}{
		{"artifact evidence", true, "necessary"},
		{"no evidence", false, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			frozen := compileAndFreeze(t, st, linearGraph)
			pool := map[executor.Kind]executor.Executor{
				"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
					if req.NodeKey != "a" || !tc.withArtifact {
						return &executor.Result{}, nil
					}
					return &executor.Result{Artifacts: []executor.ArtifactRef{{
						Name:        "out",
						Path:        "out/a",
						ContentHash: "sha256:aa",
						MediaType:   "text/plain",
						SizeBytes:   2,
					}}}, nil
				}),
			}
			runID := runToCompletion(t, st, frozen, pool)

			explanation := explainNode(t, st, runID, "b")
			if len(explanation.Recorded.Decisions) != 1 {
				t.Fatalf("decisions = %d, want 1", len(explanation.Recorded.Decisions))
			}
			var snapshot map[string]any
			if err := json.Unmarshal(explanation.Recorded.Decisions[0].PredicateSnapshot, &snapshot); err != nil {
				t.Fatal(err)
			}
			if tc.wantStrength == "necessary" {
				if basis, _ := snapshot["counterfactual_basis"].(string); basis == "" {
					t.Errorf("necessary attribution without counterfactual basis: %v", snapshot)
				}
			}
			strengths := map[string]bool{}
			for _, a := range explanation.Inference.Attributions {
				strengths[a.Strength] = true
			}
			if !strengths[tc.wantStrength] {
				t.Errorf("attributions %v, want %s", explanation.Inference.Attributions, tc.wantStrength)
			}
			if strengths["necessary"] != (tc.wantStrength == "necessary") {
				t.Errorf("necessary present=%v, want %v", strengths["necessary"], tc.wantStrength == "necessary")
			}
		})
	}
}

func TestWhyUnconditionalSelectionNeverNecessary(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: unconditional-route
nodes:
  - id: pick
    type: router
    executor: { kind: shell, command: [bin/p] }
    contract: pure
  - id: only
    type: task
    executor: { kind: shell, command: [bin/o] }
    contract: pure
    terminal: true
edges:
  - { from: pick, to: only, type: routes_to }
`)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{Route: "whatever"}, nil
		}),
	}
	runID := runToCompletion(t, st, frozen, pool)

	explanation := explainNode(t, st, runID, "only")
	for _, a := range explanation.Inference.Attributions {
		if a.Strength == "necessary" {
			t.Errorf("unconditional selection rendered necessary: %+v", explanation.Inference.Attributions)
		}
	}
	contributing := false
	for _, a := range explanation.Inference.Attributions {
		if a.Strength == "contributing" {
			contributing = true
		}
	}
	if !contributing {
		t.Errorf("attributions %v missing contributing for unconditional selection", explanation.Inference.Attributions)
	}
}

func TestWhyUnconditionalSelectionWithArtifactStaysContributing(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: unconditional-artifact-route
nodes:
  - id: pick
    type: router
    executor: { kind: shell, command: [bin/p] }
    contract: pure
  - id: only
    type: task
    executor: { kind: shell, command: [bin/o] }
    contract: pure
    terminal: true
edges:
  - { from: pick, to: only, type: routes_to }
`)
	runID := runToCompletion(t, st, frozen, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "pick" {
				result := artifactProducingResult()
				result.Route = "requires_code"
				return result, nil
			}
			return &executor.Result{}, nil
		}),
	})

	explanation := explainNode(t, st, runID, "only")
	for _, attribution := range explanation.Inference.Attributions {
		if attribution.Strength == "necessary" {
			t.Fatalf("selection backed only by an unrelated artifact rendered necessary: %+v", explanation.Inference.Attributions)
		}
	}
	if len(explanation.Inference.Attributions) != 1 || explanation.Inference.Attributions[0].Strength != "contributing" {
		t.Fatalf("attributions = %+v, want one contributing attribution", explanation.Inference.Attributions)
	}
	var basisCount int
	if err := st.DB().QueryRow(`
SELECT COUNT(*) FROM decision d JOIN run_node rn ON rn.id = d.run_node_id
WHERE d.run_id = ? AND rn.node_key = 'pick'
  AND json_extract(d.predicate_snapshot, '$.counterfactual_basis') LIKE 'unconditional%'`,
		runID).Scan(&basisCount); err != nil {
		t.Fatal(err)
	}
	if basisCount == 0 {
		t.Fatal("unconditional selection did not record the absence of a counterfactual basis")
	}
}

func TestWhyConditionalSelectionRecordsCounterfactualBasis(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, routingWhyGraph)
	runID := runToCompletion(t, st, frozen, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "classify" {
				result := artifactProducingResult()
				result.Route = "requires_code"
				return result, nil
			}
			return &executor.Result{}, nil
		}),
	})

	explanation := explainNode(t, st, runID, "code_path")
	found := false
	for _, attribution := range explanation.Inference.Attributions {
		if attribution.Strength != "necessary" {
			t.Fatalf("conditional selection with recorded basis rendered %q: %+v", attribution.Strength, explanation.Inference.Attributions)
		}
		found = true
	}
	if !found {
		t.Fatalf("no attribution rendered for conditional selection: %+v", explanation.Inference.Attributions)
	}
	var basis string
	if err := st.DB().QueryRow(`
SELECT json_extract(d.predicate_snapshot, '$.counterfactual_basis') FROM decision d
JOIN run_node rn ON rn.id = d.run_node_id
WHERE d.run_id = ? AND rn.node_key = 'classify' AND d.selected_edge_id IS NOT NULL`, runID).Scan(&basis); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(basis, "single cited input") {
		t.Fatalf("counterfactual basis = %q, want single-input basis", basis)
	}
}

func TestWhyConditionalSelectionWithoutCounterfactualInputIsContributing(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, routingWhyGraph)
	runID := runToCompletion(t, st, frozen, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "classify" {
				return &executor.Result{Route: "requires_code"}, nil
			}
			return &executor.Result{}, nil
		}),
	})

	explanation := explainNode(t, st, runID, "code_path")
	if len(explanation.Inference.Attributions) != 1 || explanation.Inference.Attributions[0].Strength != "contributing" {
		t.Fatalf("attributions = %+v, want one contributing attribution", explanation.Inference.Attributions)
	}
}

func TestWhyFallbackEquivalenceAfterProjectionRebuild(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, routingWhyGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "classify" {
				return artifactProducingResult(), nil
			}
			return &executor.Result{}, nil
		}),
	}
	runID := runToCompletion(t, st, frozen, pool)

	q := why.New(st)
	beforeCode, err := json.Marshal(mustExplain(t, q, runID, "code_path"))
	if err != nil {
		t.Fatal(err)
	}
	beforeDocs, err := json.Marshal(mustExplain(t, q, runID, "docs_path"))
	if err != nil {
		t.Fatal(err)
	}

	digestBefore, err := st.ProjectionDigest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	report, err := st.RebuildProjections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Diverged {
		t.Fatalf("projection rebuild diverged: before=%s after=%s", report.Before, report.After)
	}
	digestAfter, err := st.ProjectionDigest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if digestBefore != digestAfter {
		t.Errorf("projection digest changed across rebuild")
	}

	afterCode, err := json.Marshal(mustExplain(t, q, runID, "code_path"))
	if err != nil {
		t.Fatal(err)
	}
	afterDocs, err := json.Marshal(mustExplain(t, q, runID, "docs_path"))
	if err != nil {
		t.Fatal(err)
	}
	if string(beforeCode) != string(afterCode) {
		t.Errorf("code_path explanation changed across event-stream rebuild:\nbefore: %s\nafter:  %s", beforeCode, afterCode)
	}
	if string(beforeDocs) != string(afterDocs) {
		t.Errorf("docs_path explanation changed across event-stream rebuild:\nbefore: %s\nafter:  %s", beforeDocs, afterDocs)
	}
}

func mustExplain(t *testing.T, q *why.Query, runID, nodeKey string) *why.Explanation {
	t.Helper()
	explanation, err := q.Explain(context.Background(), runID, nodeKey)
	if err != nil {
		t.Fatal(err)
	}
	return explanation
}

// Reviewer verification 1: with projections wiped and NOT rebuilt, the
// event-backed explanation matches the projection-backed answer.
func TestWhyEventFallbackWithoutRebuild(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, routingWhyGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "classify" {
				return &executor.Result{Route: "requires_code"}, nil
			}
			return &executor.Result{}, nil
		}),
	}
	runID := runToCompletion(t, st, frozen, pool)

	projected := explainNode(t, st, runID, "code_path")
	if projected.Recorded.Source != "projection" {
		t.Fatalf("source = %q, want projection", projected.Recorded.Source)
	}

	for _, table := range []string{"causal_link", "decision", "node_attempt", "run_edge", "run_node", "artifact", "evaluation"} {
		if _, err := st.DB().Exec("DELETE FROM " + table); err != nil {
			t.Fatal(err)
		}
	}

	fromEvents := explainNode(t, st, runID, "code_path")
	if fromEvents.Recorded.Source != "events" {
		t.Fatalf("source = %q, want events", fromEvents.Recorded.Source)
	}
	if len(projected.Recorded.Decisions) == 0 || len(projected.Recorded.Decisions) != len(fromEvents.Recorded.Decisions) {
		t.Fatalf("decisions projected=%d events=%d", len(projected.Recorded.Decisions), len(fromEvents.Recorded.Decisions))
	}
	if projected.Inference.SelectedEdge != fromEvents.Inference.SelectedEdge || projected.Inference.SelectedEdge == "" {
		t.Fatalf("selected edge projected=%q events=%q", projected.Inference.SelectedEdge, fromEvents.Inference.SelectedEdge)
	}
	if len(projected.Recorded.CausalLinks) != len(fromEvents.Recorded.CausalLinks) {
		t.Fatalf("links projected=%d events=%d", len(projected.Recorded.CausalLinks), len(fromEvents.Recorded.CausalLinks))
	}
	for i := range projected.Recorded.CausalLinks {
		if projected.Recorded.CausalLinks[i].Attribution != fromEvents.Recorded.CausalLinks[i].Attribution ||
			projected.Recorded.CausalLinks[i].GroupKey != fromEvents.Recorded.CausalLinks[i].GroupKey {
			t.Fatalf("link %d mismatch: %+v vs %+v", i, projected.Recorded.CausalLinks[i], fromEvents.Recorded.CausalLinks[i])
		}
	}
	pStrengths := attributionStrengths(projected)
	eStrengths := attributionStrengths(fromEvents)
	if !reflect.DeepEqual(pStrengths, eStrengths) {
		t.Fatalf("attribution strengths differ: %v vs %v", pStrengths, eStrengths)
	}
}

func attributionStrengths(e *why.Explanation) []string {
	out := make([]string, 0, len(e.Inference.Attributions))
	for _, a := range e.Inference.Attributions {
		out = append(out, a.Strength)
	}
	sort.Strings(out)
	return out
}

// Reviewer verification 2: downstream node explanations carry the upstream
// evidence the decision cited.
func TestWhyDownstreamSeesUpstreamEvidence(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: evidence-chain
nodes:
  - id: produce
    type: task
    executor: { kind: shell, command: [bin/p] }
    contract: pure
  - id: consume
    type: task
    executor: { kind: shell, command: [bin/c] }
    contract: pure
    terminal: true
edges:
  - { from: produce, to: consume, type: depends_on }
`)
	runID := runToCompletion(t, st, frozen, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "produce" {
				return artifactProducingResult(), nil
			}
			return &executor.Result{}, nil
		}),
	})

	explanation := explainNode(t, st, runID, "consume")
	if len(explanation.Recorded.Evidence.Artifacts) == 0 {
		t.Fatalf("downstream evidence artifacts = %+v, want the cited upstream artifact", explanation.Recorded.Evidence.Artifacts)
	}
	upstream := artifactIDsByNode(t, st, runID, "produce")
	found := map[string]bool{}
	for _, a := range explanation.Recorded.Evidence.Artifacts {
		found[a.ID] = true
	}
	for _, id := range upstream {
		if !found[id] {
			t.Fatalf("cited upstream artifact %s missing from downstream evidence %+v", id, explanation.Recorded.Evidence.Artifacts)
		}
		if a := findArtifact(explanation, id); a != nil && a.ContentHash == "" {
			t.Fatalf("artifact %s missing content hash", id)
		}
	}
}

func TestWhyEligibilityRecordsAllReleaseEdgeTypes(t *testing.T) {
	for _, edgeType := range []string{"produces", "consumes"} {
		t.Run(edgeType, func(t *testing.T) {
			st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: release-edge-why
nodes:
  - id: source
    type: task
    executor: { kind: shell, command: [bin/source] }
    contract: pure
  - id: target
    type: task
    executor: { kind: shell, command: [bin/target] }
    contract: pure
    terminal: true
edges:
  - { from: source, to: target, type: `+edgeType+` }
`)
			runID := runToCompletion(t, st, frozen, map[executor.Kind]executor.Executor{
				"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
					return &executor.Result{}, nil
				}),
			})

			explanation := explainNode(t, st, runID, "target")
			if len(explanation.Recorded.Decisions) != 1 {
				t.Fatalf("decisions = %d, want 1", len(explanation.Recorded.Decisions))
			}
			edgeID := edgeIDBetween(t, st, frozen.GraphVersionID, "source", "target", edgeType)
			if len(explanation.Recorded.Decisions[0].CandidateEdges) != 1 || explanation.Recorded.Decisions[0].CandidateEdges[0] != edgeID {
				t.Fatalf("candidate edges = %v, want [%s]", explanation.Recorded.Decisions[0].CandidateEdges, edgeID)
			}
		})
	}
}

func findArtifact(e *why.Explanation, id string) *why.ArtifactEvidence {
	for i := range e.Recorded.Evidence.Artifacts {
		if e.Recorded.Evidence.Artifacts[i].ID == id {
			return &e.Recorded.Evidence.Artifacts[i]
		}
	}
	return nil
}

// Reviewer verification 3: every decision/event source id in causal links
// resolves to its row or event, including after a retry produced several
// finish events.
func TestWhySourceIDsResolve(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: resolve-sources
nodes:
  - id: flaky
    type: task
    executor: { kind: shell, command: [bin/f] }
    contract: pure
    retry: { max_attempts: 2, backoff_ms: 0 }
  - id: after
    type: task
    executor: { kind: shell, command: [bin/a] }
    contract: pure
    terminal: true
edges:
  - { from: flaky, to: after, type: depends_on }
`)
	attempts := 0
	runID := runToCompletion(t, st, frozen, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "flaky" {
				attempts++
				if attempts == 1 {
					return nil, errors.New("transient")
				}
			}
			return &executor.Result{}, nil
		}),
	})
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}

	explanation := explainNode(t, st, runID, "after")
	flakyProjected := explainNode(t, st, runID, "flaky")
	if flakyProjected.Recorded.AttemptCount != 2 {
		t.Fatalf("projected flaky attempts = %d, want 2", flakyProjected.Recorded.AttemptCount)
	}
	for _, link := range explanation.Recorded.CausalLinks {
		switch link.SourceKind {
		case "decision":
			var count int
			if err := st.DB().QueryRow("SELECT COUNT(*) FROM decision WHERE id = ?", link.SourceID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Errorf("decision source %q does not resolve", link.SourceID)
			}
		case "event":
			var count int
			if err := st.DB().QueryRow("SELECT COUNT(*) FROM event WHERE event_id = ?", link.SourceID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Errorf("event source %q does not resolve", link.SourceID)
			}
		}
	}

	// The fallback path must resolve the same identifiers.
	if _, err := st.DB().Exec("DELETE FROM causal_link"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec("DELETE FROM decision"); err != nil {
		t.Fatal(err)
	}
	fallback := explainNode(t, st, runID, "after")
	flakyFallback := explainNode(t, st, runID, "flaky")
	if flakyFallback.Recorded.AttemptCount != flakyProjected.Recorded.AttemptCount {
		t.Fatalf("fallback flaky attempts = %d, projected = %d", flakyFallback.Recorded.AttemptCount, flakyProjected.Recorded.AttemptCount)
	}
	for _, link := range fallback.Recorded.CausalLinks {
		if link.SourceKind != "event" {
			continue
		}
		var count int
		if err := st.DB().QueryRow("SELECT COUNT(*) FROM event WHERE event_id = ?", link.SourceID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Errorf("fallback event source %q does not resolve", link.SourceID)
		}
	}
}

// Reviewer verification 4: two matching route edges produce recorded
// decisions and traversals for both, without a false blocked_by.
func TestWhyDualRouteEdgesBothRecorded(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: dual-route
nodes:
  - id: classify
    type: model
    executor: { kind: shell, command: [bin/c] }
    contract: pure
  - id: path_a
    type: task
    executor: { kind: shell, command: [bin/a] }
    contract: pure
    terminal: true
  - id: path_b
    type: task
    executor: { kind: shell, command: [bin/b] }
    contract: pure
    terminal: true
edges:
  - { from: classify, to: path_a, type: routes_to, when: go }
  - { from: classify, to: path_b, type: routes_to, when: go }
`)
	runID := runToCompletion(t, st, frozen, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "classify" {
				result := artifactProducingResult()
				result.Route = "go"
				return result, nil
			}
			return &executor.Result{}, nil
		}),
	})

	for _, key := range []string{"path_a", "path_b"} {
		if s := nodeStatus(t, st, runID, key); s != "succeeded" {
			t.Fatalf("%s status = %q, want succeeded", key, s)
		}
		explanation := explainNode(t, st, runID, key)
		selected := map[string]bool{}
		for _, link := range explanation.Recorded.CausalLinks {
			if link.Attribution == "blocked_by" {
				t.Fatalf("false blocked_by for %s: %+v", key, link)
			}
			if link.Attribution == "necessary" {
				selected[key] = true
			}
		}
		if !selected[key] {
			t.Fatalf("%s has no selection attribution: %+v", key, explanation.Recorded.CausalLinks)
		}
	}

	var decisions, traversed int
	if err := st.DB().QueryRow(`
SELECT (SELECT COUNT(*) FROM decision d JOIN run_node rn ON rn.id = d.run_node_id WHERE d.run_id = ? AND rn.node_key = 'classify'),
       (SELECT COUNT(*) FROM run_edge re JOIN graph_edge ge ON ge.id = re.edge_id WHERE re.run_id = ? AND ge.from_node_key = 'classify')`,
		runID, runID).Scan(&decisions, &traversed); err != nil {
		t.Fatal(err)
	}
	if decisions < 2 || traversed != 2 {
		t.Fatalf("decisions = %d (want >= 2), traversed = %d (want 2)", decisions, traversed)
	}
}

func TestIndependentRootsFanInWithSerialConcurrency(t *testing.T) {
	st := newStore(t)
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: serial-fan-in
nodes:
  - id: left
    type: task
    executor: { kind: shell, command: [bin/left] }
    contract: pure
  - id: right
    type: task
    executor: { kind: shell, command: [bin/right] }
    contract: pure
  - id: join
    type: task
    executor: { kind: shell, command: [bin/join] }
    contract: pure
    terminal: true
edges:
  - { from: left, to: join, type: depends_on }
  - { from: right, to: join, type: depends_on }
`)
	cfg := DefaultConfig()
	cfg.MaxConcurrent = 1
	c, err := New(st, cfg, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := c.Run(context.Background(), RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	if got := nodeStatus(t, st, runID, "join"); got != "succeeded" {
		t.Fatalf("join status = %q, want succeeded", got)
	}
}

func TestUnmatchedRouteWithDependencySkipsTarget(t *testing.T) {
	st := newStore(t)
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: mixed-incoming
nodes:
  - id: chooser
    type: router
    executor: { kind: shell, command: [bin/chooser] }
    contract: pure
  - id: prerequisite
    type: task
    executor: { kind: shell, command: [bin/prerequisite] }
    contract: pure
  - id: target
    type: task
    executor: { kind: shell, command: [bin/target] }
    contract: pure
    terminal: true
edges:
  - { from: chooser, to: target, type: routes_to, when: expected }
  - { from: prerequisite, to: target, type: depends_on }
`)
	cfg := DefaultConfig()
	cfg.MaxConcurrent = 1
	c, err := New(st, cfg, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "chooser" {
				return &executor.Result{Route: "other"}, nil
			}
			return &executor.Result{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := c.Run(context.Background(), RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	if got := nodeStatus(t, st, runID, "target"); got != "skipped" {
		t.Fatalf("target status = %q, want skipped", got)
	}
	explanation := explainNode(t, st, runID, "target")
	blocked := false
	for _, link := range explanation.Recorded.CausalLinks {
		if link.Attribution == "blocked_by" {
			blocked = true
		}
	}
	if !blocked {
		t.Fatalf("target links = %+v, want blocked_by", explanation.Recorded.CausalLinks)
	}
}

func TestSkippedDependencyCitesSkipEvent(t *testing.T) {
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)
	c := newController(t, st, map[executor.Kind]executor.Executor{})
	runID, err := st.CreateRun(context.Background(), frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}
	skipped, err := st.Append(context.Background(), store.Event{
		RunID: runID.ID, Sequence: 2, SchemaVersion: "proceed/v1", Type: "node_skipped",
		OccurredAt: 1700000000001, ActorType: "controller", ActorID: "test",
		Payload: `{"node_key":"a"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := st.DB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.latestNodeFinishedEvent(context.Background(), tx, runID.ID, "a")
	if rollbackErr := tx.Rollback(); err != nil {
		t.Fatal(err)
	} else if rollbackErr != nil && rollbackErr != sql.ErrTxDone {
		t.Fatal(rollbackErr)
	}
	if got != skipped.EventID {
		t.Fatalf("source event = %q, want skipped event %q", got, skipped.EventID)
	}
}

// Reviewer verification F-002: route-node artifact evidence appears once
// per id, and the fallback path returns the same evidence arrays.
func TestWhyEvidenceDeduplicatedAcrossPaths(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: route-evidence
nodes:
  - id: decide
    type: model
    executor: { kind: shell, command: [bin/d] }
    contract: pure
  - id: target
    type: task
    executor: { kind: shell, command: [bin/t] }
    contract: pure
    terminal: true
edges:
  - { from: decide, to: target, type: routes_to, when: go }
`)
	runID := runToCompletion(t, st, frozen, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "decide" {
				result := artifactProducingResult()
				result.Route = "go"
				return result, nil
			}
			return &executor.Result{}, nil
		}),
	})

	// Explaining the deciding node: its own artifact is both the decision
	// input reference and its own output — it must appear exactly once.
	projectionBacked := explainNode(t, st, runID, "decide")
	seen := map[string]int{}
	for _, a := range projectionBacked.Recorded.Evidence.Artifacts {
		seen[a.ID]++
		if seen[a.ID] > 1 {
			t.Fatalf("artifact %s duplicated in projection output: %+v", a.ID, projectionBacked.Recorded.Evidence.Artifacts)
		}
	}
	if len(seen) == 0 {
		t.Fatal("expected at least one artifact")
	}

	for _, table := range []string{"causal_link", "decision", "node_attempt", "run_edge", "run_node", "artifact", "evaluation"} {
		if _, err := st.DB().Exec("DELETE FROM " + table); err != nil {
			t.Fatal(err)
		}
	}
	fallback := explainNode(t, st, runID, "decide")
	fallbackSeen := map[string]bool{}
	for _, a := range fallback.Recorded.Evidence.Artifacts {
		fallbackSeen[a.ID] = true
	}
	for id := range seen {
		if !fallbackSeen[id] {
			t.Fatalf("artifact %s present in projection output but missing from fallback: %+v", id, fallback.Recorded.Evidence.Artifacts)
		}
	}
	for id := range fallbackSeen {
		if _, ok := seen[id]; !ok {
			t.Fatalf("artifact %s present in fallback but not projection output", id)
		}
	}
}

// Reviewer verification F-003: pending artifact-gated nodes list their
// produces/consumes candidate edges before any decision exists.
func TestWhyPendingArtifactGatedCandidates(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: artifact-gated
nodes:
  - id: builder
    type: task
    executor: { kind: shell, command: [bin/b] }
    contract: pure
  - id: verifier
    type: verifier
    executor: { kind: shell, command: [bin/v] }
    contract: pure
    terminal: true
edges:
  - { from: builder, to: verifier, type: produces }
`)
	c := newController(t, st, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	})
	runID, err := c.Run(context.Background(), RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}

	explanation := explainNode(t, st, runID, "verifier")
	if explanation.Recorded.NodeStatus != "pending" && explanation.Recorded.NodeStatus != "eligible" {
		t.Fatalf("verifier status = %q, want pending/eligible", explanation.Recorded.NodeStatus)
	}
	if len(explanation.Recorded.CandidateEdges) != 1 {
		t.Fatalf("candidate edges = %+v, want the produces edge", explanation.Recorded.CandidateEdges)
	}
	want := edgeIDBetween(t, st, frozen.GraphVersionID, "builder", "verifier", "produces")
	if explanation.Recorded.CandidateEdges[0] != want {
		t.Fatalf("candidate edge = %q, want %q", explanation.Recorded.CandidateEdges[0], want)
	}
}
