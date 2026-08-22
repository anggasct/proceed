package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"proceed/internal/executor"
	"proceed/internal/executor/shell"
	"proceed/internal/store"
)

func TestShellPublishesArtifactsBeforeNodeFinish(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: shell
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [/bin/sh, -c, "printf hello"] }
    contract: pure
    terminal: true
    capability:
      filesystem: none
      process: declared-command
      network: none
edges: []
`)

	pool := map[executor.Kind]executor.Executor{
		executor.Shell: &shell.Executor{Launcher: shell.Launcher{Path: fakeBubblewrap(t)}},
	}
	c := newController(t, st, pool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := st.DB().QueryRow("SELECT COUNT(*) FROM artifact WHERE run_id = ?", runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("artifact count = %d, want 2", count)
	}
	var path, contentHash string
	if err := st.DB().QueryRow("SELECT path, content_hash FROM artifact WHERE run_id = ? AND name = 'stdout'", runID).Scan(&path, &contentHash); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(st.DataDir(), filepath.FromSlash(path)))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "hello" {
		t.Fatalf("artifact content = %q, want hello", content)
	}
	digest := sha256.Sum256(content)
	if contentHash != hex.EncodeToString(digest[:]) {
		t.Fatalf("artifact hash = %q, want %q", contentHash, hex.EncodeToString(digest[:]))
	}

	events, err := st.Events(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	approvalSeq, startedSeq, artifactSeq, finishSeq := int64(0), int64(0), int64(0), int64(0)
	for _, event := range events {
		switch event.Type {
		case "capability_approved":
			approvalSeq = event.Sequence
		case "node_started":
			startedSeq = event.Sequence
		case "artifact_published":
			artifactSeq = event.Sequence
		case "node_finished":
			finishSeq = event.Sequence
		}
	}
	if approvalSeq == 0 || startedSeq == 0 || artifactSeq == 0 || finishSeq == 0 || approvalSeq <= startedSeq || approvalSeq >= artifactSeq || artifactSeq >= finishSeq {
		t.Fatalf("event order approval=%d node_started=%d artifact=%d node_finished=%d", approvalSeq, startedSeq, artifactSeq, finishSeq)
	}
}

func TestShellAdmissionFailureRecordsAttempt(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: denied-shell
nodes:
  - id: denied
    type: task
    executor: { kind: shell, command: [/bin/true] }
    contract: pure
    terminal: true
edges: []
`)
	c := newController(t, st, map[executor.Kind]executor.Executor{
		executor.Shell: &shell.Executor{Launcher: shell.Launcher{Path: "/missing/bwrap"}},
	})
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}

	var attemptStatus, nodeStatus string
	if err := st.DB().QueryRow(`
SELECT na.status, rn.status
FROM node_attempt na
JOIN run_node rn ON rn.id = na.run_node_id
WHERE rn.run_id = ? AND rn.node_key = 'denied'`, runID).Scan(&attemptStatus, &nodeStatus); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "failed" || nodeStatus != "failed" {
		t.Fatalf("attempt/node status = %s/%s, want failed/failed", attemptStatus, nodeStatus)
	}
	events, err := st.Events(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == "capability_approved" {
			t.Fatal("capability approval recorded for a rejected shell")
		}
	}
}

func fakeBubblewrap(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bwrap")
	script := `#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    --setenv) export "$2=$3"; shift 3 ;;
    --) shift; exec "$@" ;;
    *) shift ;;
  esac
done
exit 127
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}
