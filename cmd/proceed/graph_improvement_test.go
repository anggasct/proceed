package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIGraphImprovement(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Effect-Status", "confirmed")
		w.Header().Set("X-Effect-Digest", "sha256:tamper-evidence")
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()

	graph := writeGraph(t, dir, httpNodeGraph(server.URL, "reconcilable"))

	// Run graph to completion
	code, stdout, stderr := runCLI(t, "run", graph, "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("run exit = %d, stdout = %q stderr = %q", code, stdout, stderr)
	}

	// Test proceed graph improvement <graph-name>
	code, stdout, stderr = runCLI(t, "graph", "improvement", "cli-e2e", "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("graph improvement exit = %d, stderr = %q", code, stderr)
	}

	var overview map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stdout), &overview); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
	}
	if _, ok := overview["graph_id"]; !ok {
		t.Errorf("missing graph_id key: %s", stdout)
	}
	if _, ok := overview["graph_name"]; !ok {
		t.Errorf("missing graph_name key: %s", stdout)
	}
	if _, ok := overview["versions"]; !ok {
		t.Errorf("missing versions key: %s", stdout)
	}
	if _, ok := overview["proposals"]; !ok {
		t.Errorf("missing proposals key: %s", stdout)
	}

	// Test unknown graph name returns GRAPH_INVALID error
	code, _, stderr = runCLI(t, "graph", "improvement", "non-existent-graph", "--data-dir", dataDir)
	if code != 10 {
		t.Errorf("unknown graph exit = %d, want 10 (stderr: %q)", code, stderr)
	}
	if !strings.Contains(stderr, "GRAPH_INVALID") {
		t.Errorf("stderr = %q, want GRAPH_INVALID class", stderr)
	}
}
