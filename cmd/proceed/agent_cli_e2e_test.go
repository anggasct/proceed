package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"proceed/internal/store"
)

func agentNodeGraph(cli string) string {
	return `schema: proceed/v1
name: agent-cli-e2e
nodes:
  - id: delegate
    type: task
    executor:
      kind: agent_cli
      cli: ` + cli + `
      args: [do, the-thing]
    contract: pure
    terminal: true
    capability:
      process: declared-command
edges: []
`
}

func writeAgentConfig(t *testing.T, dir, mapping string) string {
	t.Helper()
	path := filepath.Join(dir, "proceed.yaml")
	content := "data_dir: " + filepath.Join(dir, "data") + "\n" + mapping
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeExecutable(t *testing.T, path, script string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func withFakeSandboxOnPath(t *testing.T, dir string) {
	t.Helper()
	writeExecutable(t, filepath.Join(dir, "bin", "bwrap"), `#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    --setenv) export "$2=$3"; shift 3 ;;
    --) shift; exec "$@" ;;
    *) shift ;;
  esac
done
exit 127
`)
	t.Setenv("PATH", filepath.Join(dir, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestCLIRunAgentNodeCompletes(t *testing.T) {
	dir := t.TempDir()
	withFakeSandboxOnPath(t, dir)
	cli := filepath.Join(dir, "agent-cli")
	writeExecutable(t, cli, "#!/bin/sh\nprintf 'agent-e2e-ok'")
	graph := writeGraph(t, dir, agentNodeGraph("helper"))
	cfgPath := writeAgentConfig(t, dir, "agent_clis:\n  helper: "+cli+"\n")

	code, stdout, stderr := runCLI(t, "run", graph, "--data-dir", filepath.Join(dir, "data"), "--config", cfgPath)
	if code != 0 {
		t.Fatalf("run exit = %d, stdout = %q stderr = %q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "completed") {
		t.Fatalf("run stdout = %q, want completion", stdout)
	}
}

func TestCLIRunAgentNodeRejectsUnlistedCLI(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	graph := writeGraph(t, dir, agentNodeGraph("ghost"))
	cfgPath := writeAgentConfig(t, dir, "")

	code, _, stderr := runCLI(t, "run", graph, "--data-dir", dataDir, "--config", cfgPath)
	if code != 14 {
		t.Fatalf("run exit = %d, want 14 (NODE_FAILED), stderr = %q", code, stderr)
	}

	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var payload string
	if err := st.DB().QueryRow("SELECT payload FROM event WHERE type = 'node_failed' ORDER BY sequence DESC LIMIT 1").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, "POLICY_DENIED") {
		t.Fatalf("node_failed payload = %q, want POLICY_DENIED cause", payload)
	}
}
