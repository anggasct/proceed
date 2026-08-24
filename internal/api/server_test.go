package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"proceed/internal/config"
	"proceed/internal/controller"
	"proceed/internal/executor"
	httpexec "proceed/internal/executor/http"
	"proceed/internal/store"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{
		DataDir: t.TempDir(),
		Bind:    config.DefaultBind,
		Tokens: []config.Token{
			{Name: "viewer", Token: "viewer-secret", Scopes: []string{"read"}},
			{Name: "operator", Token: "operator-secret", Scopes: []string{"read", "run", "approve", "admin"}},
		},
	}
}

func testServer(t *testing.T, cfg config.Config) (*Server, *store.Store, *controller.Controller) {
	t.Helper()
	st, err := store.Open(filepath.Join(cfg.DataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	pool := map[executor.Kind]executor.Executor{
		executor.HTTP: httpexec.New(),
	}
	c, err := controller.New(st, controller.DefaultConfig(), pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AcquireLease(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.ReleaseLease)
	return NewServer(Deps{Store: st, Controller: c, Config: cfg}), st, c
}

func doJSON(t *testing.T, handler http.Handler, method, target, token string, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	req := httptest.NewRequest(method, target, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var payload map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &payload)
	}
	return rec, payload
}

func TestAPITokenRequired(t *testing.T) {
	cfg := testConfig(t)
	server, _, _ := testServer(t, cfg)
	rec, payload := doJSON(t, server.Handler(), "GET", "/v1/runs/01X", "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no-token status = %d, want 401", rec.Code)
	}
	if payload["error"].(map[string]any)["code"] != "UNAUTHORIZED" {
		t.Fatalf("envelope = %v", payload)
	}

	rec, _ = doJSON(t, server.Handler(), "GET", "/v1/runs/01X", "wrong-token", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unknown-token status = %d, want 401", rec.Code)
	}
}

func TestAPIScopeMatrix(t *testing.T) {
	cfg := testConfig(t)
	server, _, _ := testServer(t, cfg)

	rec, payload := doJSON(t, server.Handler(), "POST", "/v1/runs", "viewer-secret", `{"graph":"nope.yaml"}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("read-token run status = %d, want 403", rec.Code)
	}
	errBody := payload["error"].(map[string]any)
	if errBody["code"] != "POLICY_DENIED" {
		t.Fatalf("error code = %v", errBody["code"])
	}
	details := errBody["details"].(map[string]any)
	if details["required_scope"] != "run" {
		t.Fatalf("details = %v, want required_scope run", details)
	}

	rec, _ = doJSON(t, server.Handler(), "GET", "/v1/runs/01X", "operator-secret", "")
	if rec.Code == http.StatusForbidden {
		t.Fatalf("operator read should pass scope check")
	}

	rec, _ = doJSON(t, server.Handler(), "GET", "/v1/runs/01X", "viewer-secret", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("viewer unknown run status = %d, want 404", rec.Code)
	}
}

func TestAPICreateRunDrainsAndInspectMirrorsCLI(t *testing.T) {
	cfg := testConfig(t)
	server, _, _ := testServer(t, cfg)

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer target.Close()

	graphPath := filepath.Join(cfg.DataDir, "graph.yaml")
	graphYAML := fmt.Sprintf(`schema: proceed/v1
name: api-run
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: GET
      url: %s
    contract: reconcilable
    terminal: true
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`, target.URL)
	if err := os.WriteFile(graphPath, []byte(graphYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	rec, payload := doJSON(t, server.Handler(), "POST", "/v1/runs", "operator-secret",
		`{"graph":`+jsonString(graphPath)+`}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d body = %v", rec.Code, payload)
	}
	runID := payload["run_id"].(string)
	if payload["status"] != "completed" {
		t.Fatalf("run status = %v", payload["status"])
	}

	rec, payload = doJSON(t, server.Handler(), "GET", "/v1/runs/"+runID, "viewer-secret", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d", rec.Code)
	}
	if payload["run_id"] != runID || payload["status"] != "completed" {
		t.Fatalf("run payload = %v", payload)
	}

	rec, payload = doJSON(t, server.Handler(), "GET", "/v1/runs/"+runID+"/graph", "viewer-secret", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("graph status = %d", rec.Code)
	}
	nodes := payload["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["status"] != "succeeded" {
		t.Fatalf("graph nodes = %v", nodes)
	}
}

func TestAPIReservedRoutesReturn501(t *testing.T) {
	cfg := testConfig(t)
	server, _, _ := testServer(t, cfg)
	for _, route := range []string{
		"POST /v1/runs/01X/approve operator-secret",
		"POST /v1/runs/01X/reconcile operator-secret",
		"GET /v1/runs/01X/export operator-secret",
	} {
		parts := strings.SplitN(route, " ", 3)
		rec, _ := doJSON(t, server.Handler(), parts[0], parts[1], parts[2], "")
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("%s status = %d, want 501", route, rec.Code)
		}
	}
	rec, _ := doJSON(t, server.Handler(), "POST", "/v1/runs/01X/approve", "viewer-secret", "")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("reserved route must still enforce scope: %d", rec.Code)
	}
}

func TestAPIGraphInvalidSurfacesAs400(t *testing.T) {
	cfg := testConfig(t)
	server, _, _ := testServer(t, cfg)
	badPath := filepath.Join(cfg.DataDir, "bad.yaml")
	if err := os.WriteFile(badPath, []byte("schema: proceed/v1\nname: bad\nnodes: []\nedges: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rec, payload := doJSON(t, server.Handler(), "POST", "/v1/runs", "operator-secret",
		`{"graph":`+jsonString(badPath)+`}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %v", rec.Code, payload)
	}
	if payload["error"].(map[string]any)["code"] != "GRAPH_INVALID" {
		t.Fatalf("envelope = %v", payload)
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
