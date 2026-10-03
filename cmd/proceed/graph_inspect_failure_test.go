package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"proceed/internal/store"
)

func TestCLIGraphInspectShowsFailedNodeCause(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	graph := writeGraph(t, dir, httpNodeGraph(server.URL, "reconcilable"))

	if code, _, stderr := runCLI(t, "run", graph, "--data-dir", dataDir); code != 14 {
		t.Fatalf("run exit = %d, want 14 (NODE_FAILED), stderr = %q", code, stderr)
	}

	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var runID string
	if err := st.DB().QueryRow("SELECT id FROM graph_run LIMIT 1").Scan(&runID); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runCLI(t, "graph", "inspect", runID, "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("inspect exit = %d stderr = %q", code, stderr)
	}

	var payload struct {
		Status string `json:"status"`
		Nodes  []struct {
			NodeKey string `json:"node_key"`
			Status  string `json:"status"`
			Failure *struct {
				Code    *string `json:"code"`
				Message string  `json:"message"`
			} `json:"failure"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("inspect output not JSON: %q", stdout)
	}
	if payload.Status != "failed" || len(payload.Nodes) != 1 {
		t.Fatalf("inspect payload = %+v", payload)
	}
	failure := payload.Nodes[0].Failure
	if failure == nil || failure.Code == nil || *failure.Code != "NODE_FAILED" {
		t.Fatalf("inspect failure = %+v, want code NODE_FAILED", failure)
	}
	if !strings.Contains(failure.Message, "500") {
		t.Errorf("failure message = %q, want the upstream status", failure.Message)
	}

	if !strings.Contains(stdout, `"failure"`) {
		t.Errorf("inspect output omits the failure key: %s", stdout)
	}
	if code, _, _ := runCLI(t, "graph", "inspect", runID, "--data-dir", dataDir); code != 0 {
		t.Errorf("repeat inspect exit = %d", code)
	}
}
