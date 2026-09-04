package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

type listPayload struct {
	Runs []struct {
		RunID      string `json:"run_id"`
		GraphName  string `json:"graph_name"`
		Status     string `json:"status"`
		CreatedAt  int64  `json:"created_at"`
		StartedAt  *int64 `json:"started_at"`
		FinishedAt *int64 `json:"finished_at"`
	} `json:"runs"`
}

func decodeList(t *testing.T, stdout string) listPayload {
	t.Helper()
	var payload listPayload
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("decode list output: %v (stdout = %q)", err, stdout)
	}
	if payload.Runs == nil {
		t.Fatalf("runs is null, want [] (stdout = %q)", stdout)
	}
	return payload
}

func TestCLIGraphListEmpty(t *testing.T) {
	dir := t.TempDir()
	code, stdout, stderr := runCLI(t, "graph", "list", "--data-dir", filepath.Join(dir, "data"))
	if code != 0 {
		t.Fatalf("list exit = %d, stderr = %q", code, stderr)
	}
	if payload := decodeList(t, stdout); len(payload.Runs) != 0 {
		t.Fatalf("runs = %+v, want empty", payload.Runs)
	}
}

func TestCLIGraphListOrdersFiltersAndValidates(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer okServer.Close()
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failServer.Close()
	okGraph := writeGraph(t, dir, httpNodeGraph(okServer.URL, "reconcilable"))
	failGraph := writeGraph(t, dir, httpNodeGraph(failServer.URL, "reconcilable"))

	if code, _, stderr := runCLI(t, "run", okGraph, "--data-dir", dataDir); code != 0 {
		t.Fatalf("seed run exit = %d, stderr = %q", code, stderr)
	}
	if code, _, _ := runCLI(t, "run", failGraph, "--data-dir", dataDir); code != 14 {
		t.Fatalf("seed failed run exit = %d, want 14", code)
	}

	code, stdout, stderr := runCLI(t, "graph", "list", "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("list exit = %d, stderr = %q", code, stderr)
	}
	payload := decodeList(t, stdout)
	if len(payload.Runs) != 2 {
		t.Fatalf("runs = %+v, want 2", payload.Runs)
	}
	if payload.Runs[0].Status != "failed" || payload.Runs[1].Status != "completed" {
		t.Fatalf("order = [%s %s], want [failed completed]", payload.Runs[0].Status, payload.Runs[1].Status)
	}
	if payload.Runs[0].GraphName == "" || payload.Runs[0].RunID == "" {
		t.Fatalf("run entry missing id/name: %+v", payload.Runs[0])
	}

	code, stdout, _ = runCLI(t, "graph", "list", "--status", "completed", "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("filtered list exit = %d", code)
	}
	if payload := decodeList(t, stdout); len(payload.Runs) != 1 || payload.Runs[0].Status != "completed" {
		t.Fatalf("filtered runs = %+v", payload.Runs)
	}

	code, stdout, _ = runCLI(t, "graph", "list", "--limit", "1", "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("limited list exit = %d", code)
	}
	if payload := decodeList(t, stdout); len(payload.Runs) != 1 {
		t.Fatalf("limited runs = %+v, want 1", payload.Runs)
	}

	for _, args := range [][]string{
		{"graph", "list", "--status", "waiting", "--data-dir", dataDir},
		{"graph", "list", "--limit", "0", "--data-dir", dataDir},
		{"graph", "list", "--limit", "501", "--data-dir", dataDir},
		{"graph", "list", "--limit", "abc", "--data-dir", dataDir},
	} {
		if code, _, stderr := runCLI(t, args...); code != 10 {
			t.Fatalf("%v exit = %d, want 10 (GRAPH_INVALID), stderr = %q", args, code, stderr)
		}
	}
	if !strings.Contains(stdout, "runs") {
		t.Fatalf("stdout = %q", stdout)
	}
}
