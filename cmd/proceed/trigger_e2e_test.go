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

func triggerGraphFile(t *testing.T, dir, url string) string {
	t.Helper()
	return writeGraph(t, dir, fmt.Sprintf(`schema: proceed/v1
name: trigger-cli
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: GET
      url: %s/ping
    contract: pure
    terminal: true
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`, url))
}

func TestCLITriggerAddListRemove(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := triggerGraphFile(t, dir, server.URL)

	code, stdout, stderr := runCLI(t, "trigger", "list", "--data-dir", dataDir)
	if code != 0 || !strings.Contains(stdout, `"triggers":[]`) {
		t.Fatalf("empty list exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}

	code, stdout, stderr = runCLI(t, "trigger", "add", "--name", "deploy", "--graph", graph, "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("add exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "trigger deploy bound") {
		t.Fatalf("add stdout = %q", stdout)
	}

	code, stdout, stderr = runCLI(t, "trigger", "add", "--name", "deploy", "--graph", graph, "--data-dir", dataDir)
	if code != 10 {
		t.Fatalf("duplicate add exit = %d stderr = %q", code, stderr)
	}

	code, stdout, stderr = runCLI(t, "trigger", "list", "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("list exit = %d stderr = %q", code, stderr)
	}
	var listed struct {
		Triggers []struct {
			Name string `json:"name"`
		} `json:"triggers"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &listed); err != nil {
		t.Fatalf("list stdout = %q: %v", stdout, err)
	}
	if len(listed.Triggers) != 1 || listed.Triggers[0].Name != "deploy" {
		t.Fatalf("listed = %+v", listed)
	}

	code, stdout, stderr = runCLI(t, "trigger", "remove", "deploy", "--data-dir", dataDir)
	if code != 0 || !strings.Contains(stdout, "trigger deploy removed") {
		t.Fatalf("remove exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
	code, _, stderr = runCLI(t, "trigger", "remove", "deploy", "--data-dir", dataDir)
	if code != 1 || !strings.Contains(stderr, "not found") {
		t.Fatalf("unknown remove exit = %d stderr = %q", code, stderr)
	}
	code, stdout, _ = runCLI(t, "trigger", "list", "--data-dir", dataDir)
	if code != 0 || !strings.Contains(stdout, `"triggers":[]`) {
		t.Fatalf("post-remove list stdout = %q", stdout)
	}
}

func TestCLITriggerAddUsage(t *testing.T) {
	code, _, stderr := runCLI(t, "trigger", "add", "--name", "x")
	if code != 2 {
		t.Fatalf("missing --graph exit = %d stderr = %q", code, stderr)
	}
	code, _, _ = runCLI(t, "trigger", "bogus")
	if code != 2 {
		t.Fatalf("bogus subcommand exit = %d", code)
	}
}
