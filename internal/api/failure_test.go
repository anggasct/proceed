package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"proceed/internal/config"
)

func failingRunID(t *testing.T, cfg config.Config, server *Server) string {
	t.Helper()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer target.Close()

	graphPath := filepath.Join(cfg.DataDir, "failing-graph.yaml")
	graphYAML := fmt.Sprintf(`schema: proceed/v1
name: api-failing-run
nodes:
  - id: call
    type: task
    contract: reconcilable
    terminal: true
    executor:
      kind: http
      method: GET
      url: %s
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
	if payload["status"] != "failed" {
		t.Fatalf("run status = %v, want failed", payload["status"])
	}
	return payload["run_id"].(string)
}

func TestAPIRunStateSurfacesNodeFailureCause(t *testing.T) {
	cfg := testConfig(t)
	server, st, _ := testServer(t, cfg)
	runID := failingRunID(t, cfg, server)

	rec, payload := doJSON(t, server.Handler(), "GET", "/v1/runs/"+runID, "viewer-secret", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d", rec.Code)
	}
	nodes := payload["nodes"].([]any)
	if len(nodes) != 1 {
		t.Fatalf("nodes = %v, want 1", nodes)
	}
	failure, ok := nodes[0].(map[string]any)["failure"].(map[string]any)
	if !ok {
		t.Fatalf("node failure missing: %v", nodes[0])
	}
	if failure["code"] != "NODE_FAILED" {
		t.Errorf("failure code = %v, want NODE_FAILED", failure["code"])
	}
	if failure["message"] == "" || failure["message"] == nil {
		t.Errorf("failure message = %v, want a cause", failure["message"])
	}

	graphRec, graphPayload := doJSON(t, server.Handler(), "GET", "/v1/runs/"+runID+"/graph", "viewer-secret", "")
	if graphRec.Code != http.StatusOK {
		t.Fatalf("graph status = %d", graphRec.Code)
	}
	if rec.Body.String() != graphRec.Body.String() {
		t.Errorf("run and graph payloads differ:\n%s\n%s", rec.Body.String(), graphRec.Body.String())
	}
	if len(graphPayload["nodes"].([]any)) != 1 {
		t.Errorf("graph nodes = %v, want 1", graphPayload["nodes"])
	}

	graph, err := st.RuntimeGraph(context.Background(), runID)
	if err != nil {
		t.Fatalf("runtime graph: %v", err)
	}
	if graph.Nodes[0].Failure == nil || graph.Nodes[0].Failure.Code == nil {
		t.Fatalf("store graph failure = %+v, want a cause", graph.Nodes[0].Failure)
	}
	if graph.Nodes[0].NodeKey != "call" {
		t.Errorf("node key = %q, want call", graph.Nodes[0].NodeKey)
	}
}

func TestAPIUnknownRunAndScopeUnchangedForRunState(t *testing.T) {
	cfg := testConfig(t)
	cfg.Tokens = append(cfg.Tokens, config.Token{Name: "noread", Token: "noread-secret", Scopes: []string{"run"}})
	server, _, _ := testServer(t, cfg)

	rec, payload := doJSON(t, server.Handler(), "GET", "/v1/runs/01MISSING", "viewer-secret", "")
	if rec.Code != http.StatusNotFound || payload["error"].(map[string]any)["code"] != "RUN_NOT_FOUND" {
		t.Fatalf("unknown run = %d %v, want 404 RUN_NOT_FOUND", rec.Code, payload)
	}

	rec, _ = doJSON(t, server.Handler(), "GET", "/v1/runs/01MISSING", "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token = %d, want 401", rec.Code)
	}

	for _, target := range []string{"/v1/runs/01MISSING", "/v1/runs/01MISSING/graph"} {
		rec, payload := doJSON(t, server.Handler(), "GET", target, "noread-secret", "")
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s read-less token = %d, want 403", target, rec.Code)
		}
		errBody, ok := payload["error"].(map[string]any)
		if !ok || errBody["code"] != "POLICY_DENIED" {
			t.Fatalf("%s error envelope = %v, want POLICY_DENIED", target, payload)
		}
		details, ok := errBody["details"].(map[string]any)
		if !ok || details["required_scope"] != "read" {
			t.Fatalf("%s details = %v, want required_scope read", target, errBody["details"])
		}
	}
}
