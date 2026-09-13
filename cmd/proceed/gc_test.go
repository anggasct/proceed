package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"proceed/internal/store"
)

func gcSeedRun(t *testing.T, dataDir string, age time.Duration) {
	t.Helper()
	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	t.Cleanup(server.Close)
	graph := writeGraph(t, t.TempDir(), httpNodeGraph(server.URL, "reconcilable"))
	code, stdout, stderr := runCLI(t, "run", graph, "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("run exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
	var runID string
	rows, qerr := st.DB().Query(`SELECT id FROM graph_run ORDER BY created_at DESC`)
	if qerr != nil {
		t.Fatal(qerr)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		if runID == "" {
			runID = id
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`UPDATE event SET occurred_at = occurred_at - ? WHERE run_id = ?`,
		age.Milliseconds(), runID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RebuildProjections(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func gcCLIResult(t *testing.T, stdout string) (int, map[string]int) {
	t.Helper()
	line := strings.TrimSpace(stdout)
	start := strings.Index(line, "tables=")
	if start < 0 {
		t.Fatalf("gc stdout missing tables JSON: %q", stdout)
	}
	raw := strings.TrimSpace(line[start+len("tables="):])
	end := strings.LastIndex(raw, " record=")
	if end < 0 {
		t.Fatalf("gc stdout missing record id: %q", stdout)
	}
	var tables map[string]int
	if err := json.Unmarshal([]byte(raw[:end]), &tables); err != nil {
		t.Fatalf("gc stdout = %q: %v", stdout, err)
	}
	runs := -1
	for _, field := range strings.Fields(line) {
		field = strings.TrimSuffix(field, ",")
		if strings.HasPrefix(field, "runs=") {
			if _, err := fmt.Sscanf(field, "runs=%d", &runs); err != nil {
				t.Fatalf("gc stdout = %q: %v", stdout, err)
			}
		}
	}
	if runs < 0 {
		t.Fatalf("gc stdout missing runs count: %q", stdout)
	}
	return runs, tables
}

func TestCLIGCDryRunThenDelete(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	gcSeedRun(t, dataDir, 90*24*time.Hour)
	gcSeedRun(t, dataDir, 90*24*time.Hour)
	gcSeedRun(t, dataDir, time.Hour)

	code, stdout, stderr := runCLI(t, "gc", "--keep-runs", "2", "--older-than", "30d",
		"--dry-run", "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("dry-run exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
	runs, tables := gcCLIResult(t, stdout)
	if runs != 1 || tables["graph_run"] != 1 {
		t.Fatalf("dry-run = runs %d tables %v, want 1 run with graph_run 1", runs, tables)
	}
	if !strings.Contains(stdout, "would delete") {
		t.Fatalf("dry-run output missing preview marker: %q", stdout)
	}

	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	var before int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM graph_run`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	st.Close()
	if before != 3 {
		t.Fatalf("graph_run rows = %d, want 3 after dry-run", before)
	}

	code, stdout, stderr = runCLI(t, "gc", "--keep-runs", "2", "--older-than", "30d",
		"--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("gc exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
	runs, tables = gcCLIResult(t, stdout)
	if runs != 1 || tables["graph_run"] != 1 {
		t.Fatalf("gc = runs %d tables %v, want 1 run with graph_run 1", runs, tables)
	}

	st2, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	var after int
	if err := st2.DB().QueryRow(`SELECT COUNT(*) FROM graph_run`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != 2 {
		t.Fatalf("graph_run rows = %d, want 2 after GC", after)
	}

	code, stdout, stderr = runCLI(t, "store", "export", "--data-dir", dataDir,
		"--output", filepath.Join(dir, "post-gc.tgz"))
	if code != 0 {
		t.Fatalf("export exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
	code, stdout, stderr = runCLI(t, "graph", "list", "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("graph list exit = %d stderr = %q", code, stderr)
	}
}

func TestCLIGCInvalidDuration(t *testing.T) {
	dir := t.TempDir()
	code, _, stderr := runCLI(t, "gc", "--keep-runs", "2", "--older-than", "not-a-duration",
		"--data-dir", filepath.Join(dir, "data"))
	if code != 2 {
		t.Fatalf("exit = %d, want 2 (usage error before any snapshot)", code)
	}
	if !strings.Contains(stderr, "invalid --older-than") {
		t.Fatalf("stderr = %q, want usage error", stderr)
	}
}

func TestCLIGCOverflowDayMagnitudeRejected(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	gcSeedRun(t, dataDir, 90*24*time.Hour)
	code, _, stderr := runCLI(t, "gc", "--keep-runs", "2", "--older-than", "200000d",
		"--data-dir", dataDir)
	if code != 2 {
		t.Fatalf("exit = %d, want 2 (unrepresentable day magnitude is a usage error)", code)
	}
	if !strings.Contains(stderr, "invalid --older-than") {
		t.Fatalf("stderr = %q, want usage error", stderr)
	}
	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var n int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM graph_run`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("graph_run rows = %d, want 1 (no deletion on usage error)", n)
	}
}

func TestCLIGCZeroKeepRequiresYes(t *testing.T) {
	dir := t.TempDir()
	gcSeedRun(t, filepath.Join(dir, "data"), 90*24*time.Hour)
	code, _, _ := runCLI(t, "gc", "--keep-runs", "0", "--older-than", "30d",
		"--data-dir", filepath.Join(dir, "data"))
	if code != 2 {
		t.Fatalf("exit = %d, want 2 (destructive-confirm refusal)", code)
	}
	code, stdout, stderr := runCLI(t, "gc", "--keep-runs", "0", "--older-than", "30d", "--yes",
		"--data-dir", filepath.Join(dir, "data"))
	if code != 0 {
		t.Fatalf("exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
	runs, _ := gcCLIResult(t, stdout)
	if runs != 1 {
		t.Fatalf("deleted runs = %d, want 1", runs)
	}
}

func TestCLIGCEmptyStore(t *testing.T) {
	dir := t.TempDir()
	code, stdout, stderr := runCLI(t, "gc", "--keep-runs", "100", "--older-than", "90d",
		"--data-dir", filepath.Join(dir, "data"))
	if code != 0 {
		t.Fatalf("exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
	runs, _ := gcCLIResult(t, stdout)
	if runs != 0 {
		t.Fatalf("deleted runs = %d, want 0", runs)
	}
}

func TestCLIGCRegressionGate(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		if e.Name() == "gc.go" {
			continue
		}
		content, err := os.ReadFile(filepath.Join(".", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, sym := range []string{"GCRuns", "GCPolicy", "GCResult"} {
			if strings.Contains(string(content), sym) {
				t.Fatalf("%s references GC symbol %q outside the gc call chain", e.Name(), sym)
			}
		}
	}
}
