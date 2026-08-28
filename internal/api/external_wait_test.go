package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/compiler"
	"proceed/internal/config"
	"proceed/internal/controller"
	"proceed/internal/executor"
	"proceed/internal/store"
)

func hexDigest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func setupAPITestServer(t *testing.T) (*Server, *store.Store, *controller.Controller, string, string) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := config.Config{
		Tokens: []config.Token{
			{Name: "admin-user", Token: "admin-secret-token", Scopes: []string{"admin", "run", "read", "event"}},
			{Name: "event-service", Token: "event-secret-token", Scopes: []string{"event"}},
			{Name: "read-only-user", Token: "read-secret-token", Scopes: []string{"read"}},
		},
	}

	pool := map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewFuncExecutor(executor.Shell, executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{Output: map[string]any{"executed": req.NodeKey}, Route: "success"}, nil
		}),
	}
	ctrl, err := controller.New(st, controller.DefaultConfig(), pool)
	if err != nil {
		t.Fatal(err)
	}

	srv := NewServer(Deps{
		Store:      st,
		Controller: ctrl,
		Config:     cfg,
	})

	graphYAML := `schema: proceed/v1
name: test-api-wait
nodes:
  - id: step_one
    type: task
    contract: pure
    executor: { kind: shell, command: ["echo", "1"] }
  - id: wait_node
    type: task
    contract: pure
    executor: { kind: shell, command: ["echo", "w"] }
  - id: step_two
    type: task
    contract: pure
    terminal: true
    executor: { kind: shell, command: ["echo", "2"] }
edges:
  - { from: step_one, to: wait_node, type: depends_on }
  - { from: wait_node, to: step_two, type: routes_to, when: success }
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

// Token auth with event scope, rejection of missing/unauthorized tokens, and secret safety
func TestAPICompleteWaitAuthAndCompletion(t *testing.T) {
	ctx := context.Background()
	srv, _, ctrl, runID, _ := setupAPITestServer(t)

	// Step initial node
	_, _ = ctrl.Step(ctx, runID)

	// Register wait
	waitID := ulid.Make().String()
	corrKey := "repo=proceed/api;pr=12;head=sha256:abc888"
	_, err := ctrl.RegisterExternalWait(ctx, controller.ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_node",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	payloadJSON := `{"conclusion":"success"}`
	body := controller.CompleteWaitRequest{
		ProviderEventID: "github:check_run:5555",
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  corrKey,
		OccurredAt:      time.Now().UnixMilli(),
		Status:          "success",
		PayloadDigest:   "sha256:" + hexDigest(payloadJSON),
		Payload:         json.RawMessage(payloadJSON),
	}
	bodyBytes, _ := json.Marshal(body)

	// 1. Missing Authorization header -> 401 UNAUTHORIZED
	req1 := httptest.NewRequest(http.MethodPost, "/v1/waits/"+waitID+"/complete", bytes.NewReader(bodyBytes))
	rec1 := httptest.NewRecorder()
	srv.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusUnauthorized {
		t.Errorf("missing token status = %d, want 401", rec1.Code)
	}

	// 2. Token without 'event' scope -> 403 POLICY_DENIED
	req2 := httptest.NewRequest(http.MethodPost, "/v1/waits/"+waitID+"/complete", bytes.NewReader(bodyBytes))
	req2.Header.Set("Authorization", "Bearer read-secret-token")
	rec2 := httptest.NewRecorder()
	srv.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusForbidden {
		t.Errorf("read-only token status = %d, want 403", rec2.Code)
	}

	// 3. Valid token with 'event' scope -> 202 WAIT_COMPLETED
	req3 := httptest.NewRequest(http.MethodPost, "/v1/waits/"+waitID+"/complete", bytes.NewReader(bodyBytes))
	req3.Header.Set("Authorization", "Bearer event-secret-token")
	rec3 := httptest.NewRecorder()
	srv.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusAccepted {
		t.Fatalf("valid token status = %d, want 202. Body: %s", rec3.Code, rec3.Body.String())
	}
	var resp3 struct {
		WaitID string `json:"wait_id"`
		Status string `json:"status"`
	}
	_ = json.NewDecoder(rec3.Body).Decode(&resp3)
	if resp3.Status != "WAIT_COMPLETED" {
		t.Errorf("status = %q, want WAIT_COMPLETED", resp3.Status)
	}

	// 4. Duplicate completion -> 200 WAIT_ALREADY_COMPLETED
	req4 := httptest.NewRequest(http.MethodPost, "/v1/waits/"+waitID+"/complete", bytes.NewReader(bodyBytes))
	req4.Header.Set("Authorization", "Bearer event-secret-token")
	rec4 := httptest.NewRecorder()
	srv.ServeHTTP(rec4, req4)
	if rec4.Code != http.StatusOK {
		t.Fatalf("duplicate status = %d, want 200. Body: %s", rec4.Code, rec4.Body.String())
	}
	var resp4 struct {
		Status string `json:"status"`
	}
	_ = json.NewDecoder(rec4.Body).Decode(&resp4)
	if resp4.Status != "WAIT_ALREADY_COMPLETED" {
		t.Errorf("duplicate status = %q, want WAIT_ALREADY_COMPLETED", resp4.Status)
	}

	// 5. Unknown wait ID -> 404 WAIT_NOT_FOUND
	bodyUnknown := body
	bodyUnknown.ProviderEventID = "github:check_run:9999"
	bodyUnknownBytes, _ := json.Marshal(bodyUnknown)
	req5 := httptest.NewRequest(http.MethodPost, "/v1/waits/01UNKNOWN000000000000000000/complete", bytes.NewReader(bodyUnknownBytes))
	req5.Header.Set("Authorization", "Bearer event-secret-token")
	rec5 := httptest.NewRecorder()
	srv.ServeHTTP(rec5, req5)
	if rec5.Code != http.StatusNotFound {
		t.Errorf("unknown wait status = %d, want 404", rec5.Code)
	}
}

// Proves payloads carrying secret fields are rejected outright and secret values never reach the event log
func TestAPICompleteWaitRejectsSecretPayloads(t *testing.T) {
	ctx := context.Background()
	srv, st, ctrl, runID, _ := setupAPITestServer(t)

	_, _ = ctrl.Step(ctx, runID)

	waitID := ulid.Make().String()
	corrKey := "repo=proceed/security;pr=77;head=sha256:sec777"
	_, err := ctrl.RegisterExternalWait(ctx, controller.ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_node",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	secretSentinel1 := "super-secret-token-value-XYZ"
	secretSentinel2 := "super-private-key-12345"
	rawPayload := fmt.Sprintf(`{
		"conclusion": "success",
		"secret_token": %q,
		"nested": {"api_key": %q, "check_name": "build"}
	}`, secretSentinel1, secretSentinel2)

	body := controller.CompleteWaitRequest{
		WaitID:          waitID,
		ProviderEventID: "github:check_run:sec_999",
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  corrKey,
		OccurredAt:      time.Now().UnixMilli(),
		Status:          "success",
		PayloadDigest:   "sha256:" + hexDigest(rawPayload),
		Payload:         json.RawMessage(rawPayload),
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/v1/waits/"+waitID+"/complete", bytes.NewReader(bodyBytes))
	req.Header.Set("Authorization", "Bearer event-secret-token")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400. Body: %s", rec.Code, rec.Body.String())
	}

	assertNoEventContains(t, st, ctx, runID, secretSentinel1, secretSentinel2)

	w, _ := st.GetExternalWait(ctx, waitID)
	if w == nil || w.Status != "pending" {
		t.Fatalf("wait must remain pending after payload rejection, got %+v", w)
	}
}

// Proves a secret sentinel under an innocuous key that matches no credential
// pattern is still rejected and absent from every persisted event payload
func TestAPICompleteWaitRejectsUnprovablePayloadFields(t *testing.T) {
	ctx := context.Background()
	srv, st, ctrl, runID, _ := setupAPITestServer(t)

	_, _ = ctrl.Step(ctx, runID)

	waitID := ulid.Make().String()
	corrKey := "repo=proceed/security;pr=78;head=sha256:sec778"
	_, err := ctrl.RegisterExternalWait(ctx, controller.ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_node",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	innocuousSentinel := "hunter2-my-plain-phrase-secret"
	credentialSentinel := "ghp_AbCdEf0123456789BaSe64ToKeN"

	cases := []struct {
		name    string
		payload string
	}{
		{"innocuous key with plain secret", fmt.Sprintf(`{"metadata": %q}`, innocuousSentinel)},
		{"log output with plain secret", fmt.Sprintf(`{"log": %q}`, innocuousSentinel)},
		{"allowlisted field with credential value", fmt.Sprintf(`{"check_name": %q}`, credentialSentinel)},
		{"array root", `[1,2,3]`},
		{"scalar root", `"just a string"`},
		{"oversized string", `{"check_name": "` + strings.Repeat("x", 5000) + `"}`},
	}
	for i, tc := range cases {
		body := controller.CompleteWaitRequest{
			WaitID:          waitID,
			ProviderEventID: fmt.Sprintf("github:check_run:unprov_%d", i),
			EventType:       "ci.completed",
			Source:          "github",
			CorrelationKey:  corrKey,
			OccurredAt:      time.Now().UnixMilli(),
			Status:          "success",
			PayloadDigest:   "sha256:" + hexDigest(tc.payload),
			Payload:         json.RawMessage(tc.payload),
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/v1/waits/"+waitID+"/complete", bytes.NewReader(bodyBytes))
		req.Header.Set("Authorization", "Bearer event-secret-token")
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400. Body: %s", tc.name, rec.Code, rec.Body.String())
		}
	}

	assertNoEventContains(t, st, ctx, runID, innocuousSentinel, credentialSentinel)
}

func assertNoEventContains(t *testing.T, st *store.Store, ctx context.Context, runID string, sentinels ...string) {
	t.Helper()
	rows, err := st.DB().QueryContext(ctx, "SELECT payload FROM event WHERE run_id = ?", runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var payloadStr string
		if err := rows.Scan(&payloadStr); err != nil {
			t.Fatal(err)
		}
		for _, sentinel := range sentinels {
			if strings.Contains(payloadStr, sentinel) {
				t.Fatalf("Found sentinel %q in persisted event payload: %s", sentinel, payloadStr)
			}
		}
	}
}

// Payloads outside the bounded normalized object contract are rejected before persistence
func TestAPICompleteWaitRejectsNonNormalizedPayloads(t *testing.T) {
	ctx := context.Background()
	srv, _, ctrl, runID, _ := setupAPITestServer(t)

	_, _ = ctrl.Step(ctx, runID)

	waitID := ulid.Make().String()
	corrKey := "repo=proceed/security;pr=79;head=sha256:sec779"
	_, err := ctrl.RegisterExternalWait(ctx, controller.ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_node",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	deepValue := map[string]any{"v": "leaf"}
	for i := 0; i < 10; i++ {
		deepValue = map[string]any{"nested": deepValue}
	}
	deepBytes, _ := json.Marshal(deepValue)

	cases := []struct {
		name    string
		payload string
	}{
		{"array root", `[1,2,3]`},
		{"scalar root", `"just a string"`},
		{"bad key charset", `{"bad key!": "value"}`},
		{"oversized string", `{"log": "` + strings.Repeat("x", 5000) + `"}`},
		{"too deep", string(deepBytes)},
	}
	for i, tc := range cases {
		body := controller.CompleteWaitRequest{
			WaitID:          waitID,
			ProviderEventID: fmt.Sprintf("github:check_run:bad_%d", i),
			EventType:       "ci.completed",
			Source:          "github",
			CorrelationKey:  corrKey,
			OccurredAt:      time.Now().UnixMilli(),
			Status:          "success",
			PayloadDigest:   "sha256:" + hexDigest(tc.payload),
			Payload:         json.RawMessage(tc.payload),
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/v1/waits/"+waitID+"/complete", bytes.NewReader(bodyBytes))
		req.Header.Set("Authorization", "Bearer event-secret-token")
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400. Body: %s", tc.name, rec.Code, rec.Body.String())
		}
	}
}
