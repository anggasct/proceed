package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"proceed/internal/controller"
	"proceed/internal/executor"
	agentexec "proceed/internal/executor/agent"
	httpexec "proceed/internal/executor/http"
	"proceed/internal/store"
)

func readDogfood(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("..", "..", "dogfood", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func TestDogfoodFixturesValidate(t *testing.T) {
	for _, name := range []string{"delivery-loop.yaml", "content-publish.yaml"} {
		path := filepath.Join("..", "..", "dogfood", name)
		if code, _, stderr := runCLI(t, "validate", path); code != 0 {
			t.Fatalf("validate %s exit = %d, stderr = %q", name, code, stderr)
		}
	}
}

func TestDogfoodFixturesUseShippedKinds(t *testing.T) {
	shipped := map[string]bool{"shell": true, "http": true, "human_approval": true, "agent_cli": true}
	for _, name := range []string{"delivery-loop.yaml", "content-publish.yaml"} {
		for _, line := range strings.Split(readDogfood(t, name), "\n") {
			trimmed := strings.TrimSpace(line)
			if !strings.HasPrefix(trimmed, "kind:") {
				continue
			}
			kind := strings.TrimSpace(strings.TrimPrefix(trimmed, "kind:"))
			if !shipped[kind] {
				t.Fatalf("%s uses unshipped executor kind %q", name, kind)
			}
		}
	}
}

func substituteURL(t *testing.T, dir, name, placeholder, url string) string {
	t.Helper()
	content := strings.ReplaceAll(readDogfood(t, name), placeholder, url)
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func queryApproval(t *testing.T, dataDir string) (runID, approvalID string) {
	t.Helper()
	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.DB().QueryRow(`
SELECT r.id, a.id FROM graph_run r
JOIN approval a ON a.run_id = r.id AND a.decision IS NULL
WHERE r.status = 'running' LIMIT 1`).Scan(&runID, &approvalID); err != nil {
		t.Fatalf("no waiting approval: %v", err)
	}
	return runID, approvalID
}

func TestDogfoodContentPipelineCompletes(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	withFakeSandboxOnPath(t, dir)

	var published atomic.Bool
	publishServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		published.Store(true)
		fmt.Fprint(w, "published")
	}))
	defer publishServer.Close()

	cli := filepath.Join(dir, "agent-cli")
	writeExecutable(t, cli, "#!/bin/sh\nprintf 'draft-ok'")
	graph := substituteURL(t, dir, "content-publish.yaml", "http://127.0.0.1:1/publish", publishServer.URL)
	cfgPath := writeAgentConfig(t, dir, "agent_clis:\n  drafter: "+cli+"\n")

	code, _, stderr := runCLI(t, "run", graph, "--data-dir", dataDir, "--config", cfgPath)
	if code != 16 {
		t.Fatalf("run exit = %d, stderr = %q, want 16 (waiting at editor review)", code, stderr)
	}
	runID, approvalID := queryApproval(t, dataDir)

	code, stdout, stderr := runCLI(t, "approve", runID, approvalID,
		"--decision", "grant", "--actor", "editor", "--idempotency-key", "dogfood-1",
		"--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("approve exit = %d, stdout = %q stderr = %q", code, stdout, stderr)
	}

	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	pool := map[executor.Kind]executor.Executor{
		executor.HTTP:     httpexec.New(),
		executor.AgentCLI: agentexec.New(map[string]string{"drafter": cli}),
	}
	resumeCfg := controller.DefaultConfig()
	if err := st.DB().QueryRow("SELECT owner_id FROM controller_lease WHERE store_id = 'default'").Scan(&resumeCfg.OwnerID); err != nil {
		t.Fatalf("lease owner: %v", err)
	}
	c, err := controller.New(st, resumeCfg, pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ResumeRun(context.Background(), runID); err != nil {
		t.Fatalf("ResumeRun() error = %v", err)
	}
	var status string
	if err := st.DB().QueryRow("SELECT status FROM graph_run WHERE id = ?", runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("run status = %q, want completed", status)
	}
	if !published.Load() {
		t.Fatal("publish target never received the request")
	}
}

func TestDogfoodDeliveryLoopReachesApproval(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	withFakeSandboxOnPath(t, dir)

	deployServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "deployed")
	}))
	defer deployServer.Close()
	graph := substituteURL(t, dir, "delivery-loop.yaml", "http://127.0.0.1:1/deploy", deployServer.URL)

	code, _, stderr := runCLI(t, "run", graph, "--data-dir", dataDir)
	if code != 16 {
		t.Fatalf("run exit = %d, stderr = %q, want 16 (waiting at approve-deploy)", code, stderr)
	}
	runID, _ := queryApproval(t, dataDir)

	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var lint, test string
	if err := st.DB().QueryRow(
		"SELECT status FROM run_node WHERE run_id = ? AND node_key = 'lint'", runID).Scan(&lint); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(
		"SELECT status FROM run_node WHERE run_id = ? AND node_key = 'test'", runID).Scan(&test); err != nil {
		t.Fatal(err)
	}
	if lint != "succeeded" || test != "succeeded" {
		t.Fatalf("lint = %q test = %q, want both succeeded", lint, test)
	}
}
