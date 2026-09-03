package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"proceed/internal/executor"
	approvalexec "proceed/internal/executor/approval"
	"proceed/internal/store"
)

const approvalGateGraph = `schema: proceed/v1
name: approval-gate-flow
nodes:
  - id: work
    type: task
    contract: pure
    executor:
      kind: shell
      command: ["echo", "work"]
  - id: review
    type: task
    contract: idempotent
    executor:
      kind: human_approval
      scope: deploy-prod
      expires_in_ms: 60000
  - id: ship
    type: task
    contract: pure
    terminal: true
    executor:
      kind: shell
      command: ["echo", "ship"]
edges:
  - { from: work, to: review, type: depends_on }
  - { from: review, to: ship, type: routes_to, when: success }
`

func approvalTestPool() map[executor.Kind]executor.Executor {
	return map[executor.Kind]executor.Executor{
		executor.Shell:         &testShellExecutor{},
		executor.HumanApproval: approvalexec.New(),
	}
}

func openApprovalGateRun(t *testing.T, graphYAML string) (*store.Store, *Controller, string) {
	t.Helper()
	st, _, runID := openTestStoreWithGraph(t, graphYAML)
	ctrl := newTestController(t, st, approvalTestPool(), "ctrl-approval")
	for i := 0; i < 10; i++ {
		progressed, err := ctrl.Step(context.Background(), runID)
		if err != nil {
			t.Fatalf("Step to gate: %v", err)
		}
		var status string
		if err := st.DB().QueryRow(
			"SELECT status FROM run_node WHERE run_id = ? AND node_key = 'review'", runID).
			Scan(&status); err == nil && status == "waiting" {
			return st, ctrl, runID
		}
		if !progressed {
			break
		}
	}
	t.Fatal("gate never reached waiting")
	return nil, nil, ""
}

func pendingApprovalID(t *testing.T, st *store.Store, runID string) string {
	t.Helper()
	var id string
	if err := st.DB().QueryRow(
		"SELECT id FROM approval WHERE run_id = ? AND decision IS NULL", runID).Scan(&id); err != nil {
		t.Fatalf("pending approval: %v", err)
	}
	return id
}

func optionalNodeStatus(st *store.Store, runID, nodeKey string) string {
	var status string
	if err := st.DB().QueryRow(
		"SELECT status FROM run_node WHERE run_id = ? AND node_key = ?", runID, nodeKey).
		Scan(&status); err != nil {
		return ""
	}
	return status
}

func nodeStatusOf(t *testing.T, st *store.Store, runID, nodeKey string) string {
	t.Helper()
	var status string
	if err := st.DB().QueryRow(
		"SELECT status FROM run_node WHERE run_id = ? AND node_key = ?", runID, nodeKey).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func decide(t *testing.T, ctrl *Controller, runID, approvalID, decision, actor, key string) *ApprovalDecisionResult {
	t.Helper()
	res, err := ctrl.DecideApproval(context.Background(), ApprovalDecisionRequest{
		ApprovalID:     approvalID,
		RunID:          runID,
		Decision:       decision,
		Actor:          actor,
		IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("DecideApproval(%s): %v", decision, err)
	}
	return res
}

// Reaching the gate persists the full request and blocks downstream nodes.
func TestApprovalGateRegistrationWaits(t *testing.T) {
	ctx := context.Background()
	st, ctrl, runID := openApprovalGateRun(t, approvalGateGraph)

	if status := nodeStatusOf(t, st, runID, "review"); status != "waiting" {
		t.Fatalf("review status = %q, want waiting", status)
	}
	if status := optionalNodeStatus(st, runID, "ship"); status != "" {
		t.Errorf("ship status = %q, want untouched", status)
	}

	var approvalID, scope, action, evidence string
	var expiresAt int64
	var decision, decidedBy any
	if err := st.DB().QueryRow(`
SELECT id, required_scope, requested_action, evidence_references, expires_at, decision, decided_by
FROM approval WHERE run_id = ?`, runID).
		Scan(&approvalID, &scope, &action, &evidence, &expiresAt, &decision, &decidedBy); err != nil {
		t.Fatal(err)
	}
	if scope != "deploy-prod" {
		t.Errorf("scope = %q, want deploy-prod", scope)
	}
	if decision != nil || decidedBy != nil {
		t.Errorf("decision/decided_by = %v/%v, want NULL", decision, decidedBy)
	}
	var actionMap map[string]any
	if err := json.Unmarshal([]byte(action), &actionMap); err != nil {
		t.Fatalf("requested_action not JSON: %v", err)
	}
	if actionMap["scope"] != "deploy-prod" || actionMap["action"] != "human_approval" {
		t.Errorf("requested_action = %v", actionMap)
	}
	var refs []string
	if err := json.Unmarshal([]byte(evidence), &refs); err != nil {
		t.Fatalf("evidence_references not JSON array: %v", err)
	}
	if expiresAt <= time.Now().UnixMilli() {
		t.Errorf("expires_at = %d, want in the future", expiresAt)
	}

	progressed, err := ctrl.Step(ctx, runID)
	if err != nil {
		t.Fatalf("Step while waiting: %v", err)
	}
	if progressed {
		t.Errorf("Step while a gate is open must not progress")
	}
}

// A grant routes the success edge; downstream nodes run only after it.
func TestApprovalGrantResumesRun(t *testing.T) {
	st, ctrl, runID := openApprovalGateRun(t, approvalGateGraph)
	approvalID := pendingApprovalID(t, st, runID)

	res := decide(t, ctrl, runID, approvalID, "grant", "alice", "key-grant-1")
	if res.Code != ApprovalDecided || res.HTTPStatus != http.StatusAccepted {
		t.Fatalf("res = %+v, want APPROVAL_DECIDED (202)", res)
	}

	if _, err := ctrl.Step(context.Background(), runID); err != nil {
		t.Fatalf("Step after grant: %v", err)
	}
	_, _ = ctrl.Step(context.Background(), runID)
	if status := runStatusOf(ctrl, context.Background(), runID); status != "completed" {
		t.Errorf("run status = %q, want completed", status)
	}

	var decidedBy, decision string
	if err := st.DB().QueryRow(
		"SELECT decided_by, decision FROM approval WHERE id = ?", approvalID).
		Scan(&decidedBy, &decision); err != nil {
		t.Fatal(err)
	}
	if decidedBy != "alice" || decision != "grant" {
		t.Errorf("approval decided_by/decision = %q/%q, want alice/grant", decidedBy, decision)
	}
}

// The approval row must carry the caller's decision idempotency key, not the
// decision event id, so the documented unique column enforces replay defense.
func TestApprovalProjectionStoresDecisionIdempotencyKey(t *testing.T) {
	for _, tc := range []struct {
		decision string
		key      string
	}{
		{"grant", "idem-grant-key-1"},
		{"deny", "idem-deny-key-1"},
	} {
		st, ctrl, runID := openApprovalGateRun(t, approvalGateGraph)
		approvalID := pendingApprovalID(t, st, runID)

		res := decide(t, ctrl, runID, approvalID, tc.decision, "alice", tc.key)
		if res.Code != ApprovalDecided {
			t.Fatalf("%s: res = %+v, want APPROVAL_DECIDED", tc.decision, res)
		}

		var storedKey string
		if err := st.DB().QueryRow(
			"SELECT decision_idempotency_key FROM approval WHERE id = ?", approvalID).
			Scan(&storedKey); err != nil {
			t.Fatal(err)
		}
		if storedKey != tc.key {
			t.Errorf("%s: decision_idempotency_key = %q, want %q", tc.decision, storedKey, tc.key)
		}
	}
}

// Deny terminates the gate node; no declared alternate route means failure.
func TestApprovalDenyTerminatesPerPolicy(t *testing.T) {
	st, ctrl, runID := openApprovalGateRun(t, approvalGateGraph)
	approvalID := pendingApprovalID(t, st, runID)

	res := decide(t, ctrl, runID, approvalID, "deny", "bob", "key-deny-1")
	if res.Code != ApprovalDecided {
		t.Fatalf("res = %+v, want APPROVAL_DECIDED", res)
	}

	if status := nodeStatusOf(t, st, runID, "review"); status != "failed" {
		t.Fatalf("review status = %q, want failed", status)
	}
	if status := optionalNodeStatus(st, runID, "ship"); status != "" {
		t.Errorf("ship status = %q, want not run", status)
	}

	var decidedBy, decision string
	if err := st.DB().QueryRow(
		"SELECT decided_by, decision FROM approval WHERE id = ?", approvalID).
		Scan(&decidedBy, &decision); err != nil {
		t.Fatal(err)
	}
	if decidedBy != "bob" || decision != "deny" {
		t.Errorf("approval decided_by/decision = %q/%q, want bob/deny", decidedBy, decision)
	}
}

// Deny routes the declared alternate edge when the definition has one.
func TestApprovalDenyRoutesAlternateEdge(t *testing.T) {
	graphYAML := `schema: proceed/v1
name: approval-deny-route
nodes:
  - id: review
    type: task
    contract: idempotent
    executor:
      kind: human_approval
      scope: deploy-prod
      expires_in_ms: 60000
  - id: rollback
    type: task
    contract: pure
    terminal: true
    executor:
      kind: shell
      command: ["echo", "rollback"]
edges:
  - { from: review, to: rollback, type: routes_to, when: denied }
`
	st, _, runID := openTestStoreWithGraph(t, graphYAML)
	ctrl := newTestController(t, st, approvalTestPool(), "ctrl-approval")
	for i := 0; i < 10; i++ {
		progressed, err := ctrl.Step(context.Background(), runID)
		if err != nil {
			t.Fatalf("Step to gate: %v", err)
		}
		var status string
		if err := st.DB().QueryRow(
			"SELECT status FROM run_node WHERE run_id = ? AND node_key = 'review'", runID).
			Scan(&status); err == nil && status == "waiting" {
			break
		}
		if !progressed {
			break
		}
	}
	approvalID := pendingApprovalID(t, st, runID)

	res := decide(t, ctrl, runID, approvalID, "deny", "bob", "key-deny-route")
	if res.Code != ApprovalDecided {
		t.Fatalf("res = %+v, want APPROVAL_DECIDED", res)
	}
	if _, err := ctrl.Step(context.Background(), runID); err != nil {
		t.Fatalf("Step after deny: %v", err)
	}
	if status := nodeStatusOf(t, st, runID, "review"); status != "succeeded" {
		t.Errorf("review status = %q, want succeeded via denied route", status)
	}
	if status := nodeStatusOf(t, st, runID, "rollback"); status != "succeeded" {
		t.Errorf("rollback status = %q, want succeeded", status)
	}
}

// Kill + relaunch mid-wait preserves the wait and never re-runs the
// preceding node.
func TestApprovalRestartPreservesWait(t *testing.T) {
	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, "proceed.db")
	src := []byte(approvalGateGraph)
	doc := compileFixture(t, src)

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(context.Background(), "graph.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	first := newTestController(t, st, approvalTestPool(), "ctrl-1")
	run, err := st.CreateRun(context.Background(), frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		progressed, err := first.Step(context.Background(), run.ID)
		if err != nil {
			t.Fatal(err)
		}
		var status string
		if err := st.DB().QueryRow(
			"SELECT status FROM run_node WHERE run_id = ? AND node_key = 'review'", run.ID).
			Scan(&status); err == nil && status == "waiting" {
			break
		}
		if !progressed {
			break
		}
	}
	attemptBefore := int64(0)
	if err := st.DB().QueryRow(
		"SELECT attempt_count FROM run_node WHERE run_id = ? AND node_key = 'work'", run.ID).
		Scan(&attemptBefore); err != nil {
		t.Fatal(err)
	}
	first.releaseLease(context.Background())
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	second := newTestController(t, reopened, approvalTestPool(), "ctrl-2")

	if status := nodeStatusOf(t, reopened, run.ID, "review"); status != "waiting" {
		t.Fatalf("review status after restart = %q, want waiting", status)
	}
	attemptAfter := int64(-1)
	if err := reopened.DB().QueryRow(
		"SELECT attempt_count FROM run_node WHERE run_id = ? AND node_key = 'work'", run.ID).
		Scan(&attemptAfter); err != nil {
		t.Fatal(err)
	}
	if attemptAfter != attemptBefore {
		t.Errorf("work attempt_count = %d after restart, want %d (no re-run)", attemptAfter, attemptBefore)
	}

	var started int
	if err := reopened.DB().QueryRow(`
SELECT COUNT(*) FROM event WHERE run_id = ? AND type = 'node_started' AND json_extract(payload, '$.node_key') = 'work'`,
		run.ID).Scan(&started); err != nil {
		t.Fatal(err)
	}
	if started != 1 {
		t.Errorf("work node_started events = %d, want 1", started)
	}

	progressed, err := second.Step(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("Step after restart: %v", err)
	}
	if progressed {
		t.Errorf("Step after restart must not progress while the gate is open")
	}

	approvalID := pendingApprovalID(t, reopened, run.ID)
	res := decide(t, second, run.ID, approvalID, "grant", "carol", "key-restart")
	if res.Code != ApprovalDecided {
		t.Fatalf("res = %+v, want APPROVAL_DECIDED", res)
	}
}

// Simulated-clock expiry terminates the gate deterministically and a late
// decision cannot revive it.
func TestApprovalExpiry(t *testing.T) {
	st, ctrl, runID := openApprovalGateRun(t, approvalGateGraph)
	approvalID := pendingApprovalID(t, st, runID)

	future := time.Now().Add(2 * time.Hour).UnixMilli()
	if err := ctrl.ExpireApprovals(context.Background(), future); err != nil {
		t.Fatalf("ExpireApprovals: %v", err)
	}

	if status := nodeStatusOf(t, st, runID, "review"); status != "failed" {
		t.Fatalf("review status = %q, want failed", status)
	}
	var failures int
	if err := st.DB().QueryRow(`
SELECT COUNT(*) FROM event WHERE run_id = ? AND type = 'node_failed' AND json_extract(payload, '$.error') = 'APPROVAL_EXPIRED'`,
		runID).Scan(&failures); err != nil {
		t.Fatal(err)
	}
	if failures != 1 {
		t.Errorf("APPROVAL_EXPIRED node_failed events = %d, want 1", failures)
	}

	res := decide(t, ctrl, runID, approvalID, "grant", "dave", "key-late")
	if res.Code != ApprovalExpired || res.HTTPStatus != http.StatusAccepted {
		t.Fatalf("late decision res = %+v, want APPROVAL_EXPIRED (202)", res)
	}

	if err := ctrl.ExpireApprovals(context.Background(), future); err != nil {
		t.Fatalf("ExpireApprovals again: %v", err)
	}
	var expiryEvents int
	if err := st.DB().QueryRow(`
SELECT COUNT(*) FROM event WHERE run_id = ? AND type = 'approval_expired'`, runID).
		Scan(&expiryEvents); err != nil {
		t.Fatal(err)
	}
	if expiryEvents != 1 {
		t.Errorf("approval_expired events = %d, want 1 (idempotent scan)", expiryEvents)
	}
}

// The same idempotency key decides exactly once; both calls succeed.
func TestApprovalDuplicateDecisionIdempotent(t *testing.T) {
	st, ctrl, runID := openApprovalGateRun(t, approvalGateGraph)
	approvalID := pendingApprovalID(t, st, runID)

	first := decide(t, ctrl, runID, approvalID, "grant", "erin", "key-dup")
	if first.Code != ApprovalDecided {
		t.Fatalf("first = %+v, want APPROVAL_DECIDED", first)
	}
	second := decide(t, ctrl, runID, approvalID, "grant", "erin", "key-dup")
	if second.Code != ApprovalAlreadyDecided || second.HTTPStatus != http.StatusOK {
		t.Fatalf("second = %+v, want APPROVAL_ALREADY_DECIDED (200)", second)
	}
	if second.Decision != "grant" || second.Actor != "erin" {
		t.Errorf("second decision/actor = %q/%q, want grant/erin", second.Decision, second.Actor)
	}

	var events int
	if err := st.DB().QueryRow(`
SELECT COUNT(*) FROM event WHERE run_id = ? AND type = 'approval_granted'`, runID).
		Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Errorf("approval_granted events = %d, want 1", events)
	}
}

// A different key on an already-decided approval conflicts.
func TestApprovalConflictingDecision(t *testing.T) {
	st, ctrl, runID := openApprovalGateRun(t, approvalGateGraph)
	approvalID := pendingApprovalID(t, st, runID)

	if res := decide(t, ctrl, runID, approvalID, "grant", "finn", "key-a"); res.Code != ApprovalDecided {
		t.Fatalf("first = %+v, want APPROVAL_DECIDED", res)
	}
	res := decide(t, ctrl, runID, approvalID, "deny", "finn", "key-b")
	if res.Code != ApprovalConflict || res.HTTPStatus != http.StatusConflict {
		t.Fatalf("res = %+v, want APPROVAL_CONFLICT (409)", res)
	}
	var decision string
	if err := st.DB().QueryRow(
		"SELECT decision FROM approval WHERE id = ?", approvalID).Scan(&decision); err != nil {
		t.Fatal(err)
	}
	if decision != "grant" {
		t.Errorf("decision = %q, want grant (original preserved)", decision)
	}
}

// An approval bound to version D cannot authorize a run of D'.
func TestApprovalVersionMismatchRejected(t *testing.T) {
	st, ctrl, runID := openApprovalGateRun(t, approvalGateGraph)
	approvalID := pendingApprovalID(t, st, runID)

	otherDoc := compileFixture(t, []byte(`schema: proceed/v1
name: other-definition
nodes:
  - id: solo
    type: task
    contract: pure
    terminal: true
    executor:
      kind: shell
      command: ["echo", "solo"]
edges: []
`))
	frozen, err := st.FreezeDefinition(context.Background(), "other.yaml", []byte("other"), otherDoc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(
		"UPDATE approval SET graph_version_id = ? WHERE id = ?", frozen.GraphVersionID, approvalID); err != nil {
		t.Fatal(err)
	}

	res := decide(t, ctrl, runID, approvalID, "grant", "gina", "key-version")
	if res.Code != "POLICY_DENIED" || res.HTTPStatus != http.StatusConflict {
		t.Fatalf("res = %+v, want POLICY_DENIED (409)", res)
	}
	var decision any
	if err := st.DB().QueryRow(
		"SELECT decision FROM approval WHERE id = ?", approvalID).Scan(&decision); err != nil {
		t.Fatal(err)
	}
	if decision != nil {
		t.Errorf("decision = %v, want NULL (no mutation)", decision)
	}
}

// Anonymous decisions are rejected before any state mutation.
func TestApprovalAnonymousDecisionRejected(t *testing.T) {
	st, ctrl, runID := openApprovalGateRun(t, approvalGateGraph)
	approvalID := pendingApprovalID(t, st, runID)

	res, err := ctrl.DecideApproval(context.Background(), ApprovalDecisionRequest{
		ApprovalID:     approvalID,
		Decision:       "grant",
		IdempotencyKey: "key-anon",
	})
	if err == nil {
		t.Fatalf("anonymous decision = %+v, want error", res)
	}
	if store.ErrorCode(err) != store.CodeGraphInvalid {
		t.Errorf("err code = %q, want GRAPH_INVALID", store.ErrorCode(err))
	}
	var decision any
	if err := st.DB().QueryRow(
		"SELECT decision FROM approval WHERE id = ?", approvalID).Scan(&decision); err != nil {
		t.Fatal(err)
	}
	if decision != nil {
		t.Errorf("decision = %v, want NULL", decision)
	}
}

// Unknown approval identity maps to the RUN_NOT_FOUND class.
func TestApprovalUnknownIdentity(t *testing.T) {
	_, ctrl, runID := openApprovalGateRun(t, approvalGateGraph)

	res, err := ctrl.DecideApproval(context.Background(), ApprovalDecisionRequest{
		ApprovalID:     "01UNKNOWNAPPROVAL0000000000",
		Decision:       "grant",
		Actor:          "hana",
		IdempotencyKey: "key-unknown",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != "RUN_NOT_FOUND" || res.HTTPStatus != http.StatusNotFound {
		t.Errorf("res = %+v, want RUN_NOT_FOUND (404)", res)
	}

	res, err = ctrl.DecideApproval(context.Background(), ApprovalDecisionRequest{
		ApprovalID:     "01MISMATCHEDRUN00000000000000",
		RunID:          runID,
		Decision:       "grant",
		Actor:          "hana",
		IdempotencyKey: "key-mismatch",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != "RUN_NOT_FOUND" {
		t.Errorf("mismatched run res = %+v, want RUN_NOT_FOUND", res)
	}
}

// Two open gates in one run decide independently.
func TestApprovalIndependentGates(t *testing.T) {
	graphYAML := `schema: proceed/v1
name: two-gates
nodes:
  - id: gate_a
    type: task
    contract: idempotent
    executor:
      kind: human_approval
      scope: scope-a
      expires_in_ms: 60000
  - id: gate_b
    type: task
    contract: idempotent
    terminal: true
    executor:
      kind: human_approval
      scope: scope-b
      expires_in_ms: 60000
edges:
  - { from: gate_a, to: gate_b, type: depends_on }
`
	st, _, runID := openTestStoreWithGraph(t, graphYAML)
	ctrl := newTestController(t, st, approvalTestPool(), "ctrl-gates")
	if _, err := ctrl.Step(context.Background(), runID); err != nil {
		t.Fatalf("Step: %v", err)
	}

	var approvals int
	if err := st.DB().QueryRow(
		"SELECT COUNT(*) FROM approval WHERE run_id = ? AND decision IS NULL", runID).
		Scan(&approvals); err != nil {
		t.Fatal(err)
	}
	if approvals != 1 {
		t.Fatalf("open approvals = %d, want 1 (gate_b blocked behind gate_a)", approvals)
	}

	first := pendingApprovalID(t, st, runID)
	if res := decide(t, ctrl, runID, first, "grant", "ivan", "key-gate-a"); res.Code != ApprovalDecided {
		t.Fatalf("gate_a res = %+v, want APPROVAL_DECIDED", res)
	}
	if _, err := ctrl.Step(context.Background(), runID); err != nil {
		t.Fatalf("Step to gate_b: %v", err)
	}

	if err := st.DB().QueryRow(
		"SELECT COUNT(*) FROM approval WHERE run_id = ? AND decision IS NULL", runID).
		Scan(&approvals); err != nil {
		t.Fatal(err)
	}
	if approvals != 1 {
		t.Fatalf("open approvals after gate_a = %d, want 1 (gate_b)", approvals)
	}

	second := pendingApprovalID(t, st, runID)
	if second == first {
		t.Fatalf("gate_b reused gate_a's approval row")
	}
	if res := decide(t, ctrl, runID, second, "deny", "ivan", "key-gate-b"); res.Code != ApprovalDecided {
		t.Fatalf("gate_b res = %+v, want APPROVAL_DECIDED", res)
	}
}
