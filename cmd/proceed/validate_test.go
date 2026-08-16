package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validGraph = `schema: proceed/v1
name: smoke
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
edges: []
`

const invalidGraph = `schema: proceed/v1
name: smoke
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
edges:
  - { from: work, to: work, type: routes_to, when: again }
`

func TestValidateCommand(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	valid := filepath.Join(dir, "g.yaml")
	invalid := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(valid, []byte(validGraph), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(invalid, []byte(invalidGraph), 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"validate", valid, "--data-dir", dataDir}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d, stderr = %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "smoke") || !strings.Contains(out, "nodes=1") || !strings.Contains(out, "frozen") {
		t.Errorf("stdout = %q", out)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"validate", valid, "--data-dir", dataDir}, &stdout, &stderr); code != 0 {
		t.Fatalf("re-validate exit = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "already frozen") {
		t.Errorf("re-validate stdout = %q", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"validate", invalid, "--data-dir", dataDir}, &stdout, &stderr); code != 1 {
		t.Fatalf("invalid exit = %d", code)
	}
	if !strings.Contains(stderr.String(), "GRAPH_INVALID") || !strings.Contains(stderr.String(), "E107") {
		t.Errorf("stderr = %q", stderr.String())
	}

	stderr.Reset()
	if code := run([]string{"validate"}, &stdout, &stderr); code != 2 {
		t.Errorf("missing arg exit = %d", code)
	}
	if code := run([]string{"validate", filepath.Join(dir, "missing.yaml"), "--data-dir", dataDir}, &stdout, &stderr); code != 1 {
		t.Errorf("missing file exit = %d", code)
	}
}
