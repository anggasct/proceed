package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/api"
	"proceed/internal/compiler"
	"proceed/internal/config"
	"proceed/internal/controller"
	"proceed/internal/executor"
	"proceed/internal/store"
)

// Webhook signature verification
func TestWebhookSignatureVerification(t *testing.T) {
	secret := "super-secret-webhook-key-12345"
	payload := []byte(`{"action":"completed","check_run":{"id":101,"status":"completed"}}`)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if !VerifyWebhookSignature(secret, validSig, payload) {
		t.Errorf("VerifyWebhookSignature failed for valid signature")
	}

	// Invalid signature
	if VerifyWebhookSignature(secret, "sha256:badsignature", payload) {
		t.Errorf("VerifyWebhookSignature succeeded for invalid signature")
	}

	// Tampered payload
	tampered := []byte(`{"action":"completed","check_run":{"id":999,"status":"completed"}}`)
	if VerifyWebhookSignature(secret, validSig, tampered) {
		t.Errorf("VerifyWebhookSignature succeeded for tampered payload")
	}

	// Missing secret or signature
	if VerifyWebhookSignature("", validSig, payload) {
		t.Errorf("VerifyWebhookSignature succeeded for empty secret")
	}
	if VerifyWebhookSignature(secret, "", payload) {
		t.Errorf("VerifyWebhookSignature succeeded for empty signature")
	}
}

// Normalization of check run events
func TestNormalizeCheckRun(t *testing.T) {
	waitID := ulid.Make().String()
	event := &CheckRunEvent{
		Action: "completed",
		CheckRun: CheckRunPayload{
			ID:          98765,
			Name:        "test-and-lint",
			HeadSHA:     "sha256:abc123456789",
			Status:      "completed",
			Conclusion:  "success",
			CompletedAt: "2026-08-28T06:00:00Z",
			PullRequests: []PullRequestRef{
				{Number: 42},
			},
		},
		Repository: RepoRef{
			FullName: "proceed/app",
		},
	}

	req, err := NormalizeCheckRun(event, waitID, 0)
	if err != nil {
		t.Fatalf("NormalizeCheckRun failed: %v", err)
	}

	if req.WaitID != waitID {
		t.Errorf("req.WaitID = %q, want %q", req.WaitID, waitID)
	}
	if req.EventType != "ci.completed" || req.Source != "github" {
		t.Errorf("EventType=%q, Source=%q", req.EventType, req.Source)
	}
	if req.CorrelationKey != "repo=proceed/app;pr=42;head=sha256:abc123456789" {
		t.Errorf("req.CorrelationKey = %q", req.CorrelationKey)
	}
	if req.Status != "success" {
		t.Errorf("req.Status = %q, want success", req.Status)
	}
	if req.ProviderEventID != "github:check_run:98765" {
		t.Errorf("req.ProviderEventID = %q", req.ProviderEventID)
	}
}

// Verified webhook processing enforcing signature and delivery replay prevention
func TestProcessVerifiedWebhook(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Config{
		Tokens: []config.Token{
			{Name: "gh-token", Token: "secret-token", Scopes: []string{"event", "read"}},
		},
	}
	ctrl, err := controller.New(st, controller.DefaultConfig(), map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewFuncExecutor(executor.Shell, executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{Output: map[string]any{"executed": req.NodeKey}, Route: "success"}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	srv := api.NewServer(api.Deps{Store: st, Controller: ctrl, Config: cfg})
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	adapter := NewAdapter(httpSrv.URL, "secret-token")
	secret := "webhook-secret-999"

	graphYAML := `schema: proceed/v1
name: test-webhook-verify
nodes:
  - id: wait_ci
    type: task
    contract: pure
    terminal: true
    executor: { kind: shell, command: ["echo", "w"] }
edges: []
`
	src := []byte(graphYAML)
	doc, _ := compiler.Parse(src)
	_ = compiler.Validate(doc)
	frozen, _ := st.FreezeDefinition(ctx, "test.yaml", src, doc)
	run, _ := st.CreateRun(ctx, frozen.GraphVersionID)

	waitID := ulid.Make().String()
	corrKey := "repo=proceed/core;pr=5;head=sha256:wh_sha1"
	_, err = ctrl.RegisterExternalWait(ctx, controller.ExternalWaitRequest{
		RunID:             run.ID,
		NodeKey:           "wait_ci",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	rawPayload := []byte(`{
		"action": "completed",
		"check_run": {
			"id": 9901,
			"name": "build",
			"head_sha": "sha256:wh_sha1",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": [{"number": 5}]
		},
		"repository": {
			"full_name": "proceed/core"
		}
	}`)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(rawPayload)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	deliveryID := "gh-delivery-uuid-001"

	// 1. Invalid signature fails before calling API
	_, err = adapter.ProcessVerifiedWebhook(ctx, secret, "sha256:invalid", deliveryID, rawPayload, waitID, 0)
	if err == nil {
		t.Fatalf("Expected error for invalid signature, got nil")
	}

	// 2. Valid signature + first delivery -> succeeds
	res, err := adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, waitID, 0)
	if err != nil {
		t.Fatalf("ProcessVerifiedWebhook failed: %v", err)
	}
	if res.Code != "WAIT_COMPLETED" || res.HTTPStatus != 202 {
		t.Errorf("res = %+v, want WAIT_COMPLETED (202)", res)
	}

	// 3. Replay with same delivery ID -> rejected
	_, err = adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, waitID, 0)
	if err == nil {
		t.Fatalf("Expected error for replayed delivery ID, got nil")
	}
}

// Transient HTTP failures trigger automatic client retries with identical provider event identity
func TestAdapterRetryOnTransientFailure(t *testing.T) {
	ctx := context.Background()
	var attempts int32
	receivedProviderEventIDs := []string{}

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt32(&attempts, 1)

		var req controller.CompleteWaitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		receivedProviderEventIDs = append(receivedProviderEventIDs, req.ProviderEventID)

		if att < 3 {
			http.Error(w, `{"error":{"code":"SERVICE_UNAVAILABLE","message":"transient error"}}`, http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"wait_id": req.WaitID,
			"status":  "WAIT_COMPLETED",
		})
	}))
	defer mockServer.Close()

	adapter := NewAdapter(mockServer.URL, "token")
	adapter.BackoffBase = 5 * time.Millisecond

	req := controller.CompleteWaitRequest{
		WaitID:          "wait-retry-123",
		ProviderEventID: "github:check_run:retry_999",
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  "repo=org/app;pr=1;head=sha256:abc",
		OccurredAt:      time.Now().UnixMilli(),
		Status:          "success",
		PayloadDigest:   "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		Payload:         json.RawMessage("{}"),
	}

	res, err := adapter.CompleteWait(ctx, req)
	if err != nil {
		t.Fatalf("CompleteWait with retry failed: %v", err)
	}
	if res.Code != "WAIT_COMPLETED" || res.HTTPStatus != 202 {
		t.Errorf("res = %+v, want WAIT_COMPLETED", res)
	}

	if atomic.LoadInt32(&attempts) != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}

	// Verify all 3 attempts used the exact same ProviderEventID
	for i, id := range receivedProviderEventIDs {
		if id != "github:check_run:retry_999" {
			t.Errorf("attempt %d sent provider event ID %s, want github:check_run:retry_999", i, id)
		}
	}
}

// Full GitHub CI fixture
// Maps completed required-check to wait, routes success to merge node, routes failure to fix node, rejects older head SHA
func TestGitHubAdapterIntegration(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cfg := config.Config{
		Tokens: []config.Token{
			{Name: "github-webhook-adapter", Token: "gh-adapter-token-xyz", Scopes: []string{"event", "read"}},
		},
	}

	executedNodes := map[string]bool{}
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewFuncExecutor(executor.Shell, executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			executedNodes[req.NodeKey] = true
			return &executor.Result{Output: map[string]any{"executed": req.NodeKey}, Route: "success"}, nil
		}),
	}

	ctrl, err := controller.New(st, controller.DefaultConfig(), pool)
	if err != nil {
		t.Fatal(err)
	}

	server := api.NewServer(api.Deps{
		Store:      st,
		Controller: ctrl,
		Config:     cfg,
	})
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	adapter := NewAdapter(httpServer.URL, "gh-adapter-token-xyz")

	graphYAML := `schema: proceed/v1
name: github-ci-flow
nodes:
  - id: create_pr
    type: task
    contract: pure
    executor: { kind: shell, command: ["echo", "pr"] }
  - id: wait_ci
    type: task
    contract: pure
    executor: { kind: shell, command: ["echo", "wait"] }
  - id: merge_pr
    type: task
    contract: pure
    terminal: true
    executor: { kind: shell, command: ["echo", "merge"] }
  - id: fix_pr
    type: task
    contract: pure
    terminal: true
    executor: { kind: shell, command: ["echo", "fix"] }
edges:
  - { from: create_pr, to: wait_ci, type: depends_on }
  - { from: wait_ci, to: merge_pr, type: routes_to, when: success }
  - { from: wait_ci, to: fix_pr, type: routes_to, when: failure }
`
	src := []byte(graphYAML)
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(ctx, "ci.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	run, err := st.CreateRun(ctx, frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}

	// Step initial node (create_pr)
	_, _ = ctrl.Step(ctx, run.ID)
	if !executedNodes["create_pr"] {
		t.Fatalf("create_pr was not executed")
	}

	// Register wait for PR 100 on current HEAD SHA
	currentHeadSHA := "sha256:current_commit_sha_123"
	oldHeadSHA := "sha256:stale_commit_sha_000"
	waitID := ulid.Make().String()
	corrKey := CorrelationKey("proceed/app", 100, currentHeadSHA)

	_, err = ctrl.RegisterExternalWait(ctx, controller.ExternalWaitRequest{
		RunID:             run.ID,
		NodeKey:           "wait_ci",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		ExpiresAt:         time.Now().UnixMilli() + 60000,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1. Deliver webhook check run for OLD HEAD SHA -> rejected with WAIT_CONFLICT
	oldEvent := &CheckRunEvent{
		Action: "completed",
		CheckRun: CheckRunPayload{
			ID:           11111,
			Name:         "ci/build",
			HeadSHA:      oldHeadSHA,
			Status:       "completed",
			Conclusion:   "success",
			PullRequests: []PullRequestRef{{Number: 100}},
		},
		Repository: RepoRef{FullName: "proceed/app"},
	}
	oldReq, err := NormalizeCheckRun(oldEvent, waitID, 0)
	if err != nil {
		t.Fatal(err)
	}
	oldRes, err := adapter.CompleteWait(ctx, *oldReq)
	if err != nil {
		t.Fatalf("adapter.CompleteWait old: %v", err)
	}
	if oldRes.Code != "WAIT_CONFLICT" || oldRes.HTTPStatus != 409 {
		t.Errorf("oldRes = %+v, want WAIT_CONFLICT (409)", oldRes)
	}

	// Verify graph has not advanced to merge_pr
	if executedNodes["merge_pr"] {
		t.Fatalf("merge_pr executed on stale SHA completion")
	}

	// 2. Deliver webhook check run for CURRENT HEAD SHA -> succeeds and advances to merge_pr
	curEvent := &CheckRunEvent{
		Action: "completed",
		CheckRun: CheckRunPayload{
			ID:           22222,
			Name:         "ci/build",
			HeadSHA:      currentHeadSHA,
			Status:       "completed",
			Conclusion:   "success",
			PullRequests: []PullRequestRef{{Number: 100}},
		},
		Repository: RepoRef{FullName: "proceed/app"},
	}
	curReq, err := NormalizeCheckRun(curEvent, waitID, 0)
	if err != nil {
		t.Fatal(err)
	}
	curRes, err := adapter.CompleteWait(ctx, *curReq)
	if err != nil {
		t.Fatalf("adapter.CompleteWait cur: %v", err)
	}
	if curRes.Code != "WAIT_COMPLETED" || curRes.HTTPStatus != 202 {
		t.Fatalf("curRes = %+v, want WAIT_COMPLETED (202)", curRes)
	}

	// Step controller to run merge_pr
	_, err = ctrl.Step(ctx, run.ID)
	if err != nil {
		t.Fatalf("Step merge_pr: %v", err)
	}
	_, _ = ctrl.Step(ctx, run.ID)

	if !executedNodes["merge_pr"] {
		t.Errorf("merge_pr was not executed after matching CI completion")
	}
	if executedNodes["fix_pr"] {
		t.Errorf("fix_pr should not have executed on success")
	}

	// 3. Retry same provider event ID -> returns WAIT_ALREADY_COMPLETED
	retryRes, err := adapter.CompleteWait(ctx, *curReq)
	if err != nil {
		t.Fatalf("adapter.CompleteWait retry: %v", err)
	}
	if retryRes.Code != "WAIT_ALREADY_COMPLETED" || retryRes.HTTPStatus != 200 {
		t.Errorf("retryRes = %+v, want WAIT_ALREADY_COMPLETED (200)", retryRes)
	}
}
