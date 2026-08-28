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
	"strconv"
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

// Webhook delivery outage recovery allows resubmission of the same delivery ID and completes exactly once
func TestProcessVerifiedWebhookRetryAfterOutage(t *testing.T) {
	ctx := context.Background()
	var serviceAvailable int32
	var completionCalls int32

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&serviceAvailable) == 0 {
			http.Error(w, `{"error":{"code":"SERVICE_UNAVAILABLE","message":"server down"}}`, http.StatusServiceUnavailable)
			return
		}

		atomic.AddInt32(&completionCalls, 1)
		var req controller.CompleteWaitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"wait_id": req.WaitID,
			"status":  "WAIT_COMPLETED",
		})
	}))
	defer mockServer.Close()

	adapter := NewAdapter(mockServer.URL, "token")
	adapter.MaxRetries = 2
	adapter.BackoffBase = 5 * time.Millisecond

	secret := "test-secret-123"
	deliveryID := "gh-delivery-uuid-outage-001"
	rawPayload := []byte(`{
		"action": "completed",
		"check_run": {
			"id": 88001,
			"name": "build",
			"head_sha": "sha256:abc1234",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": [{"number": 10}]
		},
		"repository": {
			"full_name": "org/repo"
		}
	}`)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(rawPayload)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	// 1. Initial attempt fails due to service outage (all retries exhausted)
	_, err := adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, "wait-outage-1", 0)
	if err == nil {
		t.Fatalf("expected initial delivery to fail during outage, got nil")
	}

	// 2. Service recovers
	atomic.StoreInt32(&serviceAvailable, 1)

	// 3. Resubmit exact same signed payload and delivery ID -> must succeed and complete
	res, err := adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, "wait-outage-1", 0)
	if err != nil {
		t.Fatalf("ProcessVerifiedWebhook failed after service recovery: %v", err)
	}
	if res.Code != "WAIT_COMPLETED" || res.HTTPStatus != 202 {
		t.Errorf("res = %+v, want WAIT_COMPLETED (202)", res)
	}

	if atomic.LoadInt32(&completionCalls) != 1 {
		t.Errorf("completionCalls = %d, want 1", completionCalls)
	}

	// 4. Subsequent submission of the same delivery ID is rejected as replay
	_, err = adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, "wait-outage-1", 0)
	if err == nil {
		t.Fatalf("expected replay rejection for already completed delivery ID, got nil")
	}
}

// Non-completion actions and non-completed check run statuses are rejected; only the polling (empty action) case is allowed
func TestNormalizeCheckRunRejectsNonCompletion(t *testing.T) {
	waitID := ulid.Make().String()
	base := func(action, status string) *CheckRunEvent {
		return &CheckRunEvent{
			Action: action,
			CheckRun: CheckRunPayload{
				ID:           555,
				Name:         "ci/build",
				HeadSHA:      "sha256:norm123",
				Status:       status,
				Conclusion:   "success",
				PullRequests: []PullRequestRef{{Number: 7}},
			},
			Repository: RepoRef{FullName: "org/repo"},
		}
	}

	if _, err := NormalizeCheckRun(base("rerequested", "completed"), waitID, 0); err == nil {
		t.Errorf("expected error for action=rerequested with completed status")
	}
	if _, err := NormalizeCheckRun(base("created", "completed"), waitID, 0); err == nil {
		t.Errorf("expected error for action=created with completed status")
	}
	if _, err := NormalizeCheckRun(base("completed", "in_progress"), waitID, 0); err == nil {
		t.Errorf("expected error for completed action with status=in_progress")
	}
	if _, err := NormalizeCheckRun(base("", "queued"), waitID, 0); err == nil {
		t.Errorf("expected error for polling payload with status=queued")
	}

	req, err := NormalizeCheckRun(base("", "completed"), waitID, 0)
	if err != nil {
		t.Fatalf("polling payload without action must normalize: %v", err)
	}
	if req.Status != "success" || req.ProviderEventID != "github:check_run:555" {
		t.Errorf("req = %+v, want success status and stable provider event id", req)
	}
}

// Malformed or untyped HTTP responses surface errors instead of terminal completion outcomes
func TestCompleteWaitRejectsMalformedResponses(t *testing.T) {
	ctx := context.Background()
	var mode int32
	modeMalformed2xx, modeEmpty2xx, modeUntyped4xx, modeMalformed4xx, modeOK := int32(0), int32(1), int32(2), int32(3), int32(4)

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch atomic.LoadInt32(&mode) {
		case modeMalformed2xx:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("<html>proxy error page</html>"))
		case modeEmpty2xx:
			w.WriteHeader(http.StatusOK)
		case modeUntyped4xx:
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":{"message":"no code"}}`))
		case modeMalformed4xx:
			http.Error(w, "plain text gateway error", http.StatusConflict)
		default:
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"wait_id": "wait-malformed-1",
				"status":  "WAIT_COMPLETED",
			})
		}
	}))
	defer mockServer.Close()

	adapter := NewAdapter(mockServer.URL, "token")
	req := controller.CompleteWaitRequest{
		WaitID:          "wait-malformed-1",
		ProviderEventID: "github:check_run:malformed_1",
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  "repo=org/app;pr=3;head=sha256:mal1",
		OccurredAt:      time.Now().UnixMilli(),
		Status:          "success",
		PayloadDigest:   "sha256:" + hexDigestOf("{}"),
		Payload:         json.RawMessage("{}"),
	}

	for i, m := range []int32{modeMalformed2xx, modeEmpty2xx, modeUntyped4xx, modeMalformed4xx} {
		atomic.StoreInt32(&mode, m)
		if _, err := adapter.CompleteWait(ctx, req); err == nil {
			t.Errorf("case %d: expected error for malformed response mode %d", i, m)
		}
	}

	atomic.StoreInt32(&mode, modeOK)
	res, err := adapter.CompleteWait(ctx, req)
	if err != nil {
		t.Fatalf("valid response after malformed ones must succeed with the same provider event id: %v", err)
	}
	if res.Code != "WAIT_COMPLETED" || res.HTTPStatus != 202 {
		t.Errorf("res = %+v, want WAIT_COMPLETED (202)", res)
	}
}

// A malformed success response must not mark the delivery completed; the same delivery stays retryable
func TestProcessVerifiedWebhookMalformedResponseRetryable(t *testing.T) {
	ctx := context.Background()
	var respondValid int32

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&respondValid) == 0 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("truncated response body"))
			return
		}
		var req controller.CompleteWaitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"wait_id": req.WaitID,
			"status":  "WAIT_COMPLETED",
		})
	}))
	defer mockServer.Close()

	adapter := NewAdapter(mockServer.URL, "token")

	secret := "webhook-secret-malformed"
	deliveryID := "gh-delivery-uuid-malformed-001"
	rawPayload := []byte(`{
		"action": "completed",
		"check_run": {
			"id": 77001,
			"name": "build",
			"head_sha": "sha256:mal_sha",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": [{"number": 3}]
		},
		"repository": {"full_name": "org/repo"}
	}`)

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(rawPayload)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	if _, err := adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, "wait-malformed-delivery", 0); err == nil {
		t.Fatalf("expected error for malformed success response")
	}

	atomic.StoreInt32(&respondValid, 1)
	res, err := adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, "wait-malformed-delivery", 0)
	if err != nil {
		t.Fatalf("same delivery must remain retryable after malformed response: %v", err)
	}
	if res.Code != "WAIT_COMPLETED" || res.HTTPStatus != 202 {
		t.Errorf("res = %+v, want WAIT_COMPLETED (202)", res)
	}

	if _, err := adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, "wait-malformed-delivery", 0); err == nil {
		t.Fatalf("expected replay rejection after successful completion")
	}
}

func hexDigestOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// Only recognized HTTP/status pairings are accepted as typed outcomes; unknown
// outcomes surface errors and stay retryable
func TestCompleteWaitValidatesOutcomeCodes(t *testing.T) {
	ctx := context.Background()
	type response struct {
		status int
		body   string
	}
	cases := []struct {
		name     string
		response response
		wantErr  bool
		wantCode string
	}{
		{"accepted completion", response{http.StatusAccepted, `{"wait_id":"w","status":"WAIT_COMPLETED"}`}, false, "WAIT_COMPLETED"},
		{"idempotent duplicate", response{http.StatusOK, `{"wait_id":"w","status":"WAIT_ALREADY_COMPLETED"}`}, false, "WAIT_ALREADY_COMPLETED"},
		{"typed rejection", response{http.StatusAccepted, `{"wait_id":"w","status":"WAIT_REJECTED"}`}, false, "WAIT_REJECTED"},
		{"typed conflict", response{http.StatusConflict, `{"error":{"code":"WAIT_CONFLICT","message":"stale"}}`}, false, "WAIT_CONFLICT"},
		{"typed not found", response{http.StatusNotFound, `{"error":{"code":"WAIT_NOT_FOUND","message":"nope"}}`}, false, "WAIT_NOT_FOUND"},
		{"unknown success status", response{http.StatusAccepted, `{"wait_id":"w","status":"GREAT_SUCCESS"}`}, true, ""},
		{"mismatched pairing", response{http.StatusOK, `{"wait_id":"w","status":"WAIT_COMPLETED"}`}, true, ""},
		{"success body with error code", response{http.StatusAccepted, `{"status":"WAIT_CONFLICT"}`}, true, ""},
		{"unknown error code", response{http.StatusBadRequest, `{"error":{"code":"MYSTERY","message":"?"}}`}, true, ""},
		{"error code with wrong status", response{http.StatusTeapot, `{"error":{"code":"WAIT_CONFLICT","message":"?"}}`}, true, ""},
	}

	for i, tc := range cases {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.response.status)
			_, _ = w.Write([]byte(tc.response.body))
		}))
		adapter := NewAdapter(mockServer.URL, "token")
		req := controller.CompleteWaitRequest{
			WaitID:          "wait-outcome-" + strconv.Itoa(i),
			ProviderEventID: "github:check_run:outcome_" + strconv.Itoa(i),
			EventType:       "ci.completed",
			Source:          "github",
			CorrelationKey:  "repo=org/app;pr=9;head=sha256:outc1",
			OccurredAt:      time.Now().UnixMilli(),
			Status:          "success",
			PayloadDigest:   "sha256:" + hexDigestOf("{}"),
			Payload:         json.RawMessage("{}"),
		}
		res, err := adapter.CompleteWait(ctx, req)
		mockServer.Close()
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: expected error, got %+v", tc.name, res)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
			continue
		}
		if res.Code != tc.wantCode {
			t.Errorf("%s: res.Code = %q, want %q", tc.name, res.Code, tc.wantCode)
		}
	}
}

// A typed but non-accepted outcome must not mark the delivery completed; the
// same delivery stays retryable until an accepted outcome arrives
func TestProcessVerifiedWebhookNonAcceptedOutcomeStaysRetryable(t *testing.T) {
	ctx := context.Background()
	var mode int32
	modeRejected, modeNotFound, modeAccepted, modeReplay := int32(0), int32(1), int32(2), int32(3)
	var acceptedCalls int32

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch atomic.LoadInt32(&mode) {
		case modeRejected:
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"wait_id":"w","status":"WAIT_REJECTED","message":"condition unsatisfied"}`))
		case modeNotFound:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"WAIT_NOT_FOUND","message":"unknown wait"}}`))
		case modeAccepted:
			atomic.AddInt32(&acceptedCalls, 1)
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"wait_id":"w","status":"WAIT_COMPLETED"}`))
		default:
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"wait_id":"w","status":"WAIT_ALREADY_COMPLETED"}`))
		}
	}))
	defer mockServer.Close()

	adapter := NewAdapter(mockServer.URL, "token")

	secret := "webhook-secret-outcome"
	deliveryID := "gh-delivery-uuid-outcome-001"
	rawPayload := []byte(`{
		"action": "completed",
		"check_run": {
			"id": 81001,
			"name": "build",
			"head_sha": "sha256:outc_sha",
			"status": "completed",
			"conclusion": "success",
			"pull_requests": [{"number": 9}]
		},
		"repository": {"full_name": "org/repo"}
	}`)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(rawPayload)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	// WAIT_REJECTED is a typed outcome but not an accepted completion
	atomic.StoreInt32(&mode, modeRejected)
	res, err := adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, "wait-outcome-1", 0)
	if err != nil {
		t.Fatalf("rejected outcome: %v", err)
	}
	if res.Code != "WAIT_REJECTED" || res.Accepted() {
		t.Fatalf("res = %+v, want non-accepted WAIT_REJECTED", res)
	}

	// WAIT_NOT_FOUND likewise leaves the delivery retryable
	atomic.StoreInt32(&mode, modeNotFound)
	res, err = adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, "wait-outcome-1", 0)
	if err != nil {
		t.Fatalf("not-found outcome: %v", err)
	}
	if res.Code != "WAIT_NOT_FOUND" || res.Accepted() {
		t.Fatalf("res = %+v, want non-accepted WAIT_NOT_FOUND", res)
	}

	// The same delivery still reaches the API and completes exactly once
	atomic.StoreInt32(&mode, modeAccepted)
	res, err = adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, "wait-outcome-1", 0)
	if err != nil {
		t.Fatalf("accepted outcome after rejections: %v", err)
	}
	if res.Code != "WAIT_COMPLETED" || !res.Accepted() {
		t.Fatalf("res = %+v, want WAIT_COMPLETED", res)
	}

	// Once accepted, the delivery is replay-protected (idempotent duplicate class)
	atomic.StoreInt32(&mode, modeReplay)
	res, err = adapter.ProcessVerifiedWebhook(ctx, secret, validSig, deliveryID, rawPayload, "wait-outcome-1", 0)
	if err == nil {
		t.Fatalf("expected replay rejection after accepted outcome, got %+v", res)
	}

	if atomic.LoadInt32(&acceptedCalls) != 1 {
		t.Errorf("acceptedCalls = %d, want 1", acceptedCalls)
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

// A completed failing required check routes to the fix node while the merge node does not run
func TestGitHubAdapterIntegrationFailurePath(t *testing.T) {
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

	server := api.NewServer(api.Deps{Store: st, Controller: ctrl, Config: cfg})
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	adapter := NewAdapter(httpServer.URL, "gh-adapter-token-xyz")

	graphYAML := `schema: proceed/v1
name: github-ci-failure-flow
nodes:
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
	frozen, err := st.FreezeDefinition(ctx, "ci-failure.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	run, err := st.CreateRun(ctx, frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}

	failingHeadSHA := "sha256:failing_commit_sha_999"
	waitID := ulid.Make().String()
	corrKey := CorrelationKey("proceed/app", 200, failingHeadSHA)

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

	failEvent := &CheckRunEvent{
		Action: "completed",
		CheckRun: CheckRunPayload{
			ID:           33333,
			Name:         "ci/build",
			HeadSHA:      failingHeadSHA,
			Status:       "completed",
			Conclusion:   "failure",
			PullRequests: []PullRequestRef{{Number: 200}},
		},
		Repository: RepoRef{FullName: "proceed/app"},
	}
	failReq, err := NormalizeCheckRun(failEvent, waitID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if failReq.Status != "failure" {
		t.Fatalf("failReq.Status = %q, want failure", failReq.Status)
	}

	failRes, err := adapter.CompleteWait(ctx, *failReq)
	if err != nil {
		t.Fatalf("adapter.CompleteWait failing check: %v", err)
	}
	if failRes.Code != "WAIT_COMPLETED" || failRes.HTTPStatus != 202 {
		t.Fatalf("failRes = %+v, want WAIT_COMPLETED (202)", failRes)
	}

	if _, err := ctrl.Step(ctx, run.ID); err != nil {
		t.Fatalf("Step fix_pr: %v", err)
	}
	_, _ = ctrl.Step(ctx, run.ID)

	if !executedNodes["fix_pr"] {
		t.Errorf("fix_pr was not executed after failing CI completion")
	}
	if executedNodes["merge_pr"] {
		t.Errorf("merge_pr must not execute after failing CI completion")
	}
}
