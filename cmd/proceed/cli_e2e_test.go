package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proceed/internal/controller"
	"proceed/internal/executor"
	"proceed/internal/store"
)

func writeGraph(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("graph-%d.yaml", time.Now().UnixNano()))
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func httpNodeGraph(url, contract string) string {
	return fmt.Sprintf(`schema: proceed/v1
name: cli-e2e
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: GET
      url: %s
    contract: %s
    terminal: true
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`, url, contract)
}

func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr strings.Builder
	done := make(chan int, 1)
	go func() { done <- run(args, &stdout, &stderr) }()
	select {
	case code := <-done:
		return code, stdout.String(), stderr.String()
	case <-time.After(15 * time.Second):
		t.Fatal("CLI command did not return")
		return 1, "", ""
	}
}

func TestCLIRunCompletesAndRecordsEventStream(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := writeGraph(t, dir, httpNodeGraph(server.URL, "reconcilable"))

	code, stdout, stderr := runCLI(t, "run", graph, "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("run exit = %d, stdout = %q stderr = %q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "run ") || !strings.Contains(stdout, "completed") {
		t.Fatalf("run stdout = %q", stdout)
	}

	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var types []string
	rows, err := st.DB().Query("SELECT type FROM event ORDER BY sequence")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			t.Fatal(err)
		}
		types = append(types, typ)
	}
	joined := strings.Join(types, ",")
	for _, want := range []string{"run_started", "node_started", "node_finished", "run_completed"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("event stream %q missing %q", joined, want)
		}
	}

	var leaseRows int
	if err := st.DB().QueryRow("SELECT COUNT(*) FROM controller_lease").Scan(&leaseRows); err != nil {
		t.Fatal(err)
	}
	if leaseRows != 0 {
		t.Fatalf("controller_lease rows after run = %d, want 0 (released)", leaseRows)
	}
}

func TestCLIRunFailsBusyWhenServeHoldsLease(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := writeGraph(t, dir, httpNodeGraph(server.URL, "reconcilable"))

	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	holder, err := controller.New(st, controller.DefaultConfig(), map[executor.Kind]executor.Executor{})
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.AcquireLease(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer holder.ReleaseLease()

	var eventsBefore int
	if err := st.DB().QueryRow("SELECT COUNT(*) FROM event").Scan(&eventsBefore); err != nil {
		t.Fatal(err)
	}

	code, _, stderr := runCLI(t, "run", graph, "--data-dir", dataDir)
	if code != 19 {
		t.Fatalf("run exit = %d, want 19 (STORE_BUSY), stderr = %q", code, stderr)
	}
	if !strings.Contains(stderr, "STORE_BUSY") {
		t.Fatalf("stderr = %q, want STORE_BUSY class token", stderr)
	}
	var eventsAfter int
	if err := st.DB().QueryRow("SELECT COUNT(*) FROM event").Scan(&eventsAfter); err != nil {
		t.Fatal(err)
	}
	if eventsAfter != eventsBefore {
		t.Fatalf("events mutated: before = %d after = %d", eventsBefore, eventsAfter)
	}
}

func TestCLIRunNodeFailedExitsNodeFailed(t *testing.T) {
	dir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	graph := writeGraph(t, dir, httpNodeGraph(server.URL, "reconcilable"))

	code, _, stderr := runCLI(t, "run", graph, "--data-dir", filepath.Join(dir, "data"))
	if code != 14 {
		t.Fatalf("run exit = %d, want 14 (NODE_FAILED), stderr = %q", code, stderr)
	}
}

func TestCLIRunUncertainEffectExitsUncertain(t *testing.T) {
	dir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("server cannot hijack")
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()
	graph := writeGraph(t, dir, httpNodeGraph(server.URL, "reconcilable"))

	code, _, stderr := runCLI(t, "run", graph, "--data-dir", filepath.Join(dir, "data"))
	if code != 15 {
		t.Fatalf("run exit = %d, want 15 (EFFECT_UNCERTAIN), stderr = %q", code, stderr)
	}
}

func TestCLIRunCancelledExitsRunCancelled(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	var started int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&started, 1)
		time.Sleep(400 * time.Millisecond)
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := writeGraph(t, dir, httpNodeGraph(server.URL, "reconcilable"))

	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if atomic.LoadInt32(&started) > 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
		if err != nil {
			return
		}
		defer st.Close()
		canceller, err := controller.New(st, controller.DefaultConfig(), map[executor.Kind]executor.Executor{})
		if err != nil {
			return
		}
		var runID string
		for time.Now().Before(deadline) {
			if err := st.DB().QueryRow("SELECT id FROM graph_run LIMIT 1").Scan(&runID); err == nil && runID != "" {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if runID != "" {
			_ = canceller.CancelRun(context.Background(), runID)
		}
	}()

	code, _, stderr := runCLI(t, "run", graph, "--data-dir", dataDir)
	if code != 18 {
		t.Fatalf("run exit = %d, want 18 (RUN_CANCELLED), stderr = %q", code, stderr)
	}
}

func TestCLIGraphInspect(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := writeGraph(t, dir, httpNodeGraph(server.URL, "reconcilable"))

	code, stdout, _ := runCLI(t, "run", graph, "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("run exit = %d", code)
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
		RunID  string `json:"run_id"`
		Status string `json:"status"`
		Nodes  []struct {
			NodeKey string `json:"node_key"`
			Status  string `json:"status"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("inspect output not JSON: %q", stdout)
	}
	if payload.RunID != runID || payload.Status != "completed" {
		t.Fatalf("inspect payload = %+v", payload)
	}
	if len(payload.Nodes) != 1 || payload.Nodes[0].Status != "succeeded" {
		t.Fatalf("inspect nodes = %+v", payload.Nodes)
	}

	code, _, stderr = runCLI(t, "graph", "inspect", "01NOSUCHRUN", "--data-dir", dataDir)
	if code != 12 {
		t.Fatalf("inspect unknown exit = %d, want 12, stderr = %q", code, stderr)
	}
	if !strings.Contains(stderr, "RUN_NOT_FOUND") {
		t.Fatalf("stderr = %q, want RUN_NOT_FOUND", stderr)
	}
}

func TestExitCodeMatrixComplete(t *testing.T) {
	canon := []string{
		"GRAPH_INVALID", "POLICY_DENIED", "RUN_NOT_FOUND",
		"NODE_TIMEOUT", "NODE_FAILED", "EFFECT_UNCERTAIN",
		"APPROVAL_REQUIRED", "APPROVAL_EXPIRED", "RUN_CANCELLED",
		"STORE_BUSY",
	}
	seen := map[int]string{0: "success", 1: "unclassified", 2: "usage"}
	for _, class := range canon {
		code := exitCodeForClass(class)
		if code <= 2 {
			t.Fatalf("%s exit = %d, must be > 2", class, code)
		}
		if other, dup := seen[code]; dup {
			t.Fatalf("%s and %s share exit %d", class, other, code)
		}
		seen[code] = class
		err := store.NewCodeError(class, "trigger %s", class)
		if got := exitCodeForError(err); got != code {
			t.Fatalf("exitCodeForError(%s) = %d, want %d", class, got, code)
		}
	}
}

func TestCLIServeBindsLoopbackAndRequiresTokens(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	configPath := filepath.Join(dir, "proceed.yaml")
	cfgYAML := "tokens:\n  - name: viewer\n    token: viewer-secret\n    scopes: [read]\n"
	if err := os.WriteFile(configPath, []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	ready := make(chan struct{}, 1)
	_ = ready
	go func() {
		_ = run([]string{"serve", "--data-dir", dataDir, "--config", configPath}, io.Discard, io.Discard)
	}()

	deadline := time.Now().Add(5 * time.Second)
	dialed := ""
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:7331", 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			dialed = "127.0.0.1:7331"
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if dialed == "" {
		t.Fatal("serve did not bind 127.0.0.1:7331")
	}

	resp, err := http.Get("http://127.0.0.1:7331/v1/runs/01X")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", resp.StatusCode)
	}

	req, _ := http.NewRequest("GET", "http://127.0.0.1:7331/v1/runs/01X", nil)
	req.Header.Set("Authorization", "Bearer viewer-secret")
	authed, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer authed.Body.Close()
	if authed.StatusCode != http.StatusNotFound {
		t.Fatalf("viewer status = %d, want 404 (scope passed, run unknown)", authed.StatusCode)
	}
}

func TestCLIServeRefusesWithoutTokens(t *testing.T) {
	dir := t.TempDir()
	code, _, stderr := runCLI(t, "serve", "--data-dir", filepath.Join(dir, "data"), "--config", filepath.Join(dir, "absent.yaml"))
	if code != 1 {
		t.Fatalf("serve exit = %d, want 1", code)
	}
	if !strings.Contains(stderr, "tokens") {
		t.Fatalf("stderr = %q, want token requirement message", stderr)
	}
}
