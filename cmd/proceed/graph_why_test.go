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

func TestCLIGraphWhy(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Effect-Status", "confirmed")
		w.Header().Set("X-Effect-Digest", "sha256:tamper-evidence")
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := writeGraph(t, dir, httpNodeGraph(server.URL, "reconcilable"))

	code, stdout, stderr := runCLI(t, "run", graph, "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("run exit = %d, stdout = %q stderr = %q", code, stdout, stderr)
	}
	fields := strings.Fields(stdout)
	if len(fields) < 2 {
		t.Fatalf("run stdout = %q, want run id", stdout)
	}
	runID := fields[1]

	code, stdout, stderr = runCLI(t, "graph", "why", runID, "call", "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("graph why exit = %d, stderr = %q", code, stderr)
	}
	var explanation map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stdout), &explanation); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout)
	}
	if _, ok := explanation["recorded"]; !ok {
		t.Errorf("missing recorded key: %s", stdout)
	}
	if _, ok := explanation["inference"]; !ok {
		t.Errorf("missing inference key: %s", stdout)
	}

	code, _, stderr = runCLI(t, "graph", "why", "missing-run", "call", "--data-dir", dataDir)
	if code != 12 {
		t.Errorf("unknown run exit = %d, want 12 (stderr %q)", code, stderr)
	}
	if !strings.Contains(stderr, "RUN_NOT_FOUND") {
		t.Errorf("stderr = %q, want RUN_NOT_FOUND class", stderr)
	}

	code, _, stderr = runCLI(t, "graph", "why", runID, "ghost-node", "--data-dir", dataDir)
	if code == 0 || code == 12 {
		t.Errorf("unknown node exit = %d, want distinct non-zero non-RUN_NOT_FOUND", code)
	}
	if !strings.Contains(stderr, runID) || !strings.Contains(stderr, "ghost-node") {
		t.Errorf("stderr = %q, want both run id and node id", stderr)
	}

	code, _, _ = runCLI(t, "graph", "why", runID, "--data-dir", dataDir)
	if code != exitUsage {
		t.Errorf("missing node arg exit = %d, want usage exit %d", code, exitUsage)
	}
}
