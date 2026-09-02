package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"proceed/internal/compiler"
	"proceed/internal/config"
	"proceed/internal/controller"
	"proceed/internal/executor"
	approvalexec "proceed/internal/executor/approval"
	"proceed/internal/store"
)

func setupApprovalAPIServer(t *testing.T) (*Server, *store.Store, *controller.Controller, string, string) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := config.Config{
		Tokens: []config.Token{
			{Name: "approver", Token: "approve-secret-token", Scopes: []string{"approve", "read"}},
			{Name: "read-only", Token: "read-secret-token", Scopes: []string{"read"}},
		},
	}

	pool := map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewFuncExecutor(executor.Shell, executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{Output: map[string]any{"executed": req.NodeKey}, Route: "success"}, nil
		}),
		executor.HumanApproval: approvalexec.New(),
	}
	ctrl, err := controller.New(st, controller.DefaultConfig(), pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.AcquireLease(context.Background()); err != nil {
		t.Fatal(err)
	}

	srv := NewServer(Deps{Store: st, Controller: ctrl, Config: cfg})

	graphYAML := `schema: proceed/v1
name: test-api-approval
nodes:
  - id: work
    type: task
    contract: pure
    executor: { kind: shell, command: ["echo", "1"] }
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
    executor: { kind: shell, command: ["echo", "2"] }
edges:
  - { from: work, to: review, type: depends_on }
  - { from: review, to: ship, type: routes_to, when: success }
`
	src := []byte(graphYAML)
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(context.Background(), "test.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	run, err := st.CreateRun(context.Background(), frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}

	return srv, st, ctrl, run.ID, frozen.GraphVersionID
}

func postDecision(t *testing.T, srv *Server, token, approvalID, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	url := "/v1/approvals/" + approvalID + "/" + path
	req := httptest.NewRequest(http.MethodPost, url, bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	var decoded map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("response not JSON: %v (%s)", err, rec.Body.String())
	}
	return rec, decoded
}

func TestAPIApprovalDecisionEndpoint(t *testing.T) {
	ctx := context.Background()
	srv, st, ctrl, runID, _ := setupApprovalAPIServer(t)

	for i := 0; i < 10; i++ {
		progressed, err := ctrl.Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
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
	var approvalID string
	if err := st.DB().QueryRow(
		"SELECT id FROM approval WHERE run_id = ? AND decision IS NULL", runID).Scan(&approvalID); err != nil {
		t.Fatal(err)
	}

	rec, decoded := postDecision(t, srv, "read-secret-token", approvalID, "decision",
		`{"decision":"grant","actor":"alice","decision_idempotency_key":"api-key-1"}`)
	if rec.Code != http.StatusForbidden {
		t.Errorf("read token = %d, want 403", rec.Code)
	}
	if decoded["error"].(map[string]any)["code"] != "POLICY_DENIED" {
		t.Errorf("read token code = %v, want POLICY_DENIED", decoded)
	}

	rec, _ = postDecision(t, srv, "", approvalID, "decision",
		`{"decision":"grant","actor":"alice","decision_idempotency_key":"api-key-1"}`)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("missing token = %d, want 401", rec.Code)
	}

	rec, _ = postDecision(t, srv, "approve-secret-token", approvalID, "decision", `{"decision":"grant"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("missing actor = %d, want 400", rec.Code)
	}

	rec, decoded = postDecision(t, srv, "approve-secret-token", approvalID, "decision",
		`{"decision":"grant","actor":"alice","decision_idempotency_key":"api-key-1"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("decision = %d (%s), want 202", rec.Code, rec.Body.String())
	}
	if decoded["status"] != "APPROVAL_DECIDED" || decoded["decision"] != "grant" || decoded["actor"] != "alice" {
		t.Errorf("envelope = %v", decoded)
	}
	if decoded["run_id"] != runID || decoded["node_key"] != "review" {
		t.Errorf("envelope run/node = %v/%v", decoded["run_id"], decoded["node_key"])
	}

	rec, decoded = postDecision(t, srv, "approve-secret-token", approvalID, "decision",
		`{"decision":"grant","actor":"alice","decision_idempotency_key":"api-key-1"}`)
	if rec.Code != http.StatusOK || decoded["status"] != "APPROVAL_ALREADY_DECIDED" {
		t.Errorf("duplicate = %d %v, want 200 APPROVAL_ALREADY_DECIDED", rec.Code, decoded["status"])
	}

	rec, decoded = postDecision(t, srv, "approve-secret-token", approvalID, "grant",
		`{"actor":"alice","decision_idempotency_key":"api-key-2"}`)
	if rec.Code != http.StatusConflict {
		t.Errorf("decided via alias = %d %v, want 409", rec.Code, decoded)
	}

	for i := 0; i < 10; i++ {
		progressed, err := ctrl.Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		if !progressed {
			break
		}
	}
	var runStatus string
	if err := st.DB().QueryRow("SELECT status FROM graph_run WHERE id = ?", runID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "completed" {
		t.Errorf("run status = %q, want completed", runStatus)
	}
}

func TestAPIApprovalUnknownAndExpired(t *testing.T) {
	ctx := context.Background()
	srv, st, ctrl, runID, _ := setupApprovalAPIServer(t)

	rec, decoded := postDecision(t, srv, "approve-secret-token", "01UNKNOWN000000000000000APPROVAL", "decision",
		`{"decision":"grant","actor":"alice","decision_idempotency_key":"api-key-unknown"}`)
	if rec.Code != http.StatusNotFound || decoded["error"].(map[string]any)["code"] != "RUN_NOT_FOUND" {
		t.Errorf("unknown = %d %v, want 404 RUN_NOT_FOUND", rec.Code, decoded)
	}

	for i := 0; i < 10; i++ {
		progressed, err := ctrl.Step(ctx, runID)
		if err != nil {
			t.Fatal(err)
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
	var approvalID string
	if err := st.DB().QueryRow(
		"SELECT id FROM approval WHERE run_id = ? AND decision IS NULL", runID).Scan(&approvalID); err != nil {
		t.Fatal(err)
	}

	future := int64(4000000000000)
	if err := ctrl.ExpireApprovals(ctx, future); err != nil {
		t.Fatal(err)
	}

	rec, decoded = postDecision(t, srv, "approve-secret-token", approvalID, "decision",
		`{"decision":"grant","actor":"alice","decision_idempotency_key":"api-key-late"}`)
	if rec.Code != http.StatusAccepted || decoded["status"] != "APPROVAL_EXPIRED" {
		t.Errorf("late = %d %v, want 202 APPROVAL_EXPIRED", rec.Code, decoded["status"])
	}
}
