package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"proceed/internal/store"
)

func approvalGateGraphFile(t *testing.T, dir, url string) string {
	t.Helper()
	return writeGraph(t, dir, `schema: proceed/v1
name: cli-approval-gate
nodes:
  - id: work
    type: task
    contract: idempotent
    executor:
      kind: http
      method: GET
      url: `+url+`
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
  - id: review
    type: task
    contract: idempotent
    terminal: true
    executor:
      kind: human_approval
      scope: deploy-prod
      expires_in_ms: 60000
edges:
  - { from: work, to: review, type: depends_on }
`)
}

func TestCLIApproveGrantAndIdempotentReplay(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := approvalGateGraphFile(t, dir, server.URL)

	code, stdout, stderr := runCLI(t, "run", graph, "--data-dir", dataDir)
	if code != 16 {
		t.Fatalf("run exit = %d (stdout %q stderr %q), want 16 APPROVAL_REQUIRED", code, stdout, stderr)
	}

	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	var runID, approvalID string
	if err := st.DB().QueryRow(`
SELECT r.id, a.id FROM graph_run r
JOIN approval a ON a.run_id = r.id AND a.decision IS NULL
WHERE r.status = 'running' LIMIT 1`).Scan(&runID, &approvalID); err != nil {
		t.Fatal(err)
	}
	st.Close()

	code, stdout, stderr = runCLI(t, "approve", runID, approvalID,
		"--decision", "grant", "--actor", "tester", "--idempotency-key", "cli-key-1",
		"--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("approve exit = %d (stdout %q stderr %q), want 0", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "APPROVAL_DECIDED") || !strings.Contains(stdout, "decision=grant") {
		t.Errorf("approve stdout = %q", stdout)
	}

	code, stdout, _ = runCLI(t, "approve", runID, approvalID,
		"--decision", "grant", "--actor", "tester", "--idempotency-key", "cli-key-1",
		"--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("replay exit = %d (stdout %q), want 0", code, stdout)
	}
	if !strings.Contains(stdout, "APPROVAL_ALREADY_DECIDED") {
		t.Errorf("replay stdout = %q", stdout)
	}

	code, _, stderr = runCLI(t, "approve", runID, approvalID,
		"--decision", "deny", "--actor", "tester", "--idempotency-key", "cli-key-2",
		"--data-dir", dataDir)
	if code != 20 {
		t.Fatalf("conflicting decision exit = %d (stderr %q), want 20", code, stderr)
	}

	code, _, stderr = runCLI(t, "approve", "01WRONGRUN000000000000000000", approvalID,
		"--decision", "grant", "--actor", "tester", "--idempotency-key", "cli-key-3",
		"--data-dir", dataDir)
	if code != 12 {
		t.Fatalf("mismatched run exit = %d (stderr %q), want 12", code, stderr)
	}

	code, _, _ = runCLI(t, "approve", runID, approvalID, "--decision", "maybe", "--data-dir", dataDir)
	if code != 2 {
		t.Fatalf("invalid decision exit = %d, want 2 (usage)", code)
	}
}
