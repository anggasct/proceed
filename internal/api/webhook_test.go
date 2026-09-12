package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"proceed/internal/config"
	"proceed/internal/store"
)

const fireSecret = "deploy-hmac-secret"

var fireClock = time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)

func fireTestConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := testConfig(t)
	cfg.Triggers = []config.Trigger{{Name: "deploy", Secret: fireSecret}}
	return cfg
}

func triggerGraphFile(t *testing.T, dir, targetURL string) string {
	t.Helper()
	path := filepath.Join(dir, "trigger-graph.yaml")
	yaml := fmt.Sprintf(`schema: proceed/v1
name: trigger-fire
params:
  - { name: env, type: string, default: staging }
  - { name: retries, type: int, default: 1 }
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: GET
      url: %s/deploy?env={{ params.env }}
    contract: reconcilable
    terminal: true
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`, targetURL)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type fireFixture struct {
	server    *Server
	st        *store.Store
	target    *httptest.Server
	graphPath string
	seenEnv   *string
}

func newFireFixture(t *testing.T, cfg config.Config) *fireFixture {
	t.Helper()
	fx := &fireFixture{}
	fx.server, fx.st, _ = testServer(t, cfg)
	fx.server.now = func() time.Time { return fireClock }
	fx.server.limiter = newTriggerLimiter(fx.server.now)

	var seenEnv string
	fx.target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenEnv = r.URL.Query().Get("env")
		fmt.Fprint(w, "ok")
	}))
	t.Cleanup(fx.target.Close)
	fx.seenEnv = &seenEnv

	fx.graphPath = triggerGraphFile(t, cfg.DataDir, fx.target.URL)
	frozen, err := freezeGraph(context.Background(), fx.st, fx.graphPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := fx.st.AddWebhookTrigger(context.Background(), "deploy", frozen); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fx.st.Close() })
	return fx
}

func signedFire(t *testing.T, handler http.Handler, name, secret string, ts time.Time, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	req := httptest.NewRequest("POST", "/v1/triggers/"+name+"/fire", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Proceed-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	req.Header.Set("X-Proceed-Timestamp", strconv.FormatInt(ts.Unix(), 10))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func waitForTerminal(t *testing.T, st *store.Store, runID string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		g, err := st.RuntimeGraph(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if g.Status != "running" {
			return g.Status
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("run did not reach terminal state")
	return ""
}

func TestFireTriggerCreatesAndDrainsRun(t *testing.T) {
	fx := newFireFixture(t, fireTestConfig(t))
	rec := signedFire(t, fx.server.Handler(), "deploy", fireSecret, fireClock, []byte(`{"env":"prod"}`))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("fire status = %d body = %s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	runID := payload["run_id"].(string)
	if status := waitForTerminal(t, fx.st, runID); status != "completed" {
		t.Fatalf("run status = %s", status)
	}
	if *fx.seenEnv != "prod" {
		t.Fatalf("target saw env = %q", *fx.seenEnv)
	}
	var triggerName string
	if err := fx.st.DB().QueryRow("SELECT trigger_name FROM graph_run WHERE id = ?", runID).Scan(&triggerName); err != nil {
		t.Fatal(err)
	}
	if triggerName != "deploy" {
		t.Fatalf("trigger_name = %q", triggerName)
	}
	var payloadText string
	if err := fx.st.DB().QueryRow(
		"SELECT payload FROM event WHERE run_id = ? AND type = 'run_started'", runID).Scan(&payloadText); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payloadText, `"trigger_name":"deploy"`) {
		t.Fatalf("run_started payload = %q", payloadText)
	}
	assertSecretAbsent(t, fx.st, fireSecret)
}

func TestFireTriggerUnknownName(t *testing.T) {
	fx := newFireFixture(t, fireTestConfig(t))
	rec := signedFire(t, fx.server.Handler(), "nope", fireSecret, fireClock, []byte(`{}`))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if n := runCount(t, fx.st); n != 0 {
		t.Fatalf("created %d runs", n)
	}
}

func TestFireTriggerBadSignatureAndStaleTimestamp(t *testing.T) {
	fx := newFireFixture(t, fireTestConfig(t))
	rec := signedFire(t, fx.server.Handler(), "deploy", "wrong-secret", fireClock, []byte(`{}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong signature status = %d", rec.Code)
	}
	rec = signedFire(t, fx.server.Handler(), "deploy", fireSecret, fireClock.Add(-301*time.Second), []byte(`{}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("stale timestamp status = %d", rec.Code)
	}
	rec = signedFire(t, fx.server.Handler(), "deploy", fireSecret, fireClock.Add(301*time.Second), []byte(`{}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("future timestamp status = %d", rec.Code)
	}
	req := httptest.NewRequest("POST", "/v1/triggers/deploy/fire", strings.NewReader(`{}`))
	req.Header.Set("X-Proceed-Signature", "not-a-signature")
	req.Header.Set("X-Proceed-Timestamp", strconv.FormatInt(fireClock.Unix(), 10))
	r2 := httptest.NewRecorder()
	fx.server.Handler().ServeHTTP(r2, req)
	if r2.Code != http.StatusUnauthorized {
		t.Fatalf("malformed signature status = %d", r2.Code)
	}
	if n := runCount(t, fx.st); n != 0 {
		t.Fatalf("created %d runs", n)
	}
	assertSecretAbsent(t, fx.st, fireSecret)
}

func TestFireTriggerWithoutConfigSecretFailsClosed(t *testing.T) {
	cfg := fireTestConfig(t)
	cfg.Triggers = nil
	fx := newFireFixture(t, cfg)
	rec := signedFire(t, fx.server.Handler(), "deploy", fireSecret, fireClock, []byte(`{}`))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", rec.Code)
	}
	if n := runCount(t, fx.st); n != 0 {
		t.Fatalf("created %d runs", n)
	}
}

func TestFireTriggerBodyLimits(t *testing.T) {
	fx := newFireFixture(t, fireTestConfig(t))
	rec := signedFire(t, fx.server.Handler(), "deploy", fireSecret, fireClock, []byte(`not json`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("non-JSON status = %d body = %s", rec.Code, rec.Body.String())
	}
	rec = signedFire(t, fx.server.Handler(), "deploy", fireSecret, fireClock, []byte(`[1,2,3]`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("non-object status = %d", rec.Code)
	}
	big := bytes.Repeat([]byte("a"), 256<<10+1)
	rec = signedFire(t, fx.server.Handler(), "deploy", fireSecret, fireClock, big)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize status = %d", rec.Code)
	}
	if n := runCount(t, fx.st); n != 0 {
		t.Fatalf("created %d runs", n)
	}
}

func TestFireTriggerRateLimit(t *testing.T) {
	fx := newFireFixture(t, fireTestConfig(t))
	handler := fx.server.Handler()
	for i := 0; i < 60; i++ {
		rec := signedFire(t, handler, "deploy", fireSecret, fireClock, []byte(`{}`))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("fire %d status = %d body = %s", i+1, rec.Code, rec.Body.String())
		}
	}
	rec := signedFire(t, handler, "deploy", fireSecret, fireClock, []byte(`{}`))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("61st fire status = %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 must carry Retry-After")
	}
	if n := runCount(t, fx.st); n != 60 {
		t.Fatalf("created %d runs, want 60", n)
	}
}

func TestFireTriggerAfterRemove(t *testing.T) {
	fx := newFireFixture(t, fireTestConfig(t))
	rec := signedFire(t, fx.server.Handler(), "deploy", fireSecret, fireClock, []byte(`{}`))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("fire status = %d", rec.Code)
	}
	var payload map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &payload)
	runID := payload["run_id"].(string)
	removed, err := fx.st.RemoveWebhookTrigger(context.Background(), "deploy")
	if err != nil || !removed {
		t.Fatalf("remove = %v %v", removed, err)
	}
	rec = signedFire(t, fx.server.Handler(), "deploy", fireSecret, fireClock, []byte(`{}`))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("post-remove status = %d", rec.Code)
	}
	g, err := fx.st.RuntimeGraph(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	_ = g
}

func TestFireTriggerParamBinding(t *testing.T) {
	fx := newFireFixture(t, fireTestConfig(t))
	rec := signedFire(t, fx.server.Handler(), "deploy", fireSecret, fireClock,
		[]byte(`{"env":"prod","retries":5,"ignored_field":{"nested":true}}`))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("fire status = %d body = %s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &payload)
	runID := payload["run_id"].(string)
	rows, err := fx.st.RunParams(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if rows["env"].Value.String != "prod" || rows["retries"].Value.String != "5" {
		t.Fatalf("bound rows = %+v", rows)
	}
	if len(rows) != 2 {
		t.Fatalf("undeclared fields must be ignored, rows = %+v", rows)
	}

	fx2 := newFireFixture(t, fireTestConfig(t))
	rec = signedFire(t, fx2.server.Handler(), "deploy", fireSecret, fireClock, []byte(`{"retries":"abc"}`))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid binding status = %d body = %s", rec.Code, rec.Body.String())
	}
	errBody := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &errBody)
	if errBody["error"].(map[string]any)["code"] != "GRAPH_INVALID" {
		t.Fatalf("error body = %v", errBody)
	}
	if n := runCount(t, fx2.st); n != 0 {
		t.Fatalf("invalid binding created %d runs", n)
	}
}

func TestFireTriggerRouteDoesNotShadowRuns(t *testing.T) {
	fx := newFireFixture(t, fireTestConfig(t))
	handler := fx.server.Handler()
	req := httptest.NewRequest("GET", "/v1/runs", nil)
	req.Header.Set("Authorization", "Bearer viewer-secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/runs status = %d", rec.Code)
	}
	req = httptest.NewRequest("GET", "/v1/triggers/deploy/fire", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET fire status = %d", rec.Code)
	}
}

func runCount(t *testing.T, st *store.Store) int {
	t.Helper()
	var n int
	if err := st.DB().QueryRow("SELECT COUNT(*) FROM graph_run").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func assertSecretAbsent(t *testing.T, st *store.Store, secret string) {
	t.Helper()
	for _, table := range []string{"event", "graph_run", "run_param", "webhook_trigger", "node_attempt"} {
		rows, err := st.DB().Query("SELECT * FROM " + table)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			for i, v := range vals {
				if s, ok := v.(string); ok && strings.Contains(s, secret) {
					t.Fatalf("secret leaked into %s.%s", table, cols[i])
				}
			}
		}
		rows.Close()
	}
}
