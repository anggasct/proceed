package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"proceed/internal/controller"
	"proceed/internal/store"
)

func scheduleGraphFile(t *testing.T, dir, url string) string {
	t.Helper()
	return writeGraph(t, dir, fmt.Sprintf(`schema: proceed/v1
name: schedule-cli
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

func TestCLIScheduleLifecycle(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := scheduleGraphFile(t, dir, server.URL)

	code, stdout, _ := runCLI(t, "schedule", "list", "--data-dir", dataDir)
	if code != 0 || !strings.Contains(stdout, `"schedules":[]`) {
		t.Fatalf("empty list = %d %q", code, stdout)
	}

	before := time.Now().UTC()
	code, stdout, stderr := runCLI(t, "schedule", "add", "--name", "daily", "--graph", graph,
		"--cron", "0 7 * * *", "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("add exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
	after := time.Now().UTC()
	nextCandidates := map[int64]bool{}
	for _, base := range []time.Time{before, after} {
		candidate := time.Date(base.Year(), base.Month(), base.Day(), 7, 0, 0, 0, time.UTC)
		if !candidate.After(base) {
			candidate = candidate.AddDate(0, 0, 1)
		}
		nextCandidates[candidate.UnixMilli()] = true
	}

	code, stdout, stderr = runCLI(t, "schedule", "list", "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("list exit = %d stderr = %q", code, stderr)
	}
	var listed struct {
		Schedules []struct {
			Name       string `json:"name"`
			Cron       string `json:"cron"`
			Enabled    bool   `json:"enabled"`
			NextFireAt int64  `json:"next_fire_at"`
		} `json:"schedules"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &listed); err != nil {
		t.Fatalf("list stdout = %q: %v", stdout, err)
	}
	if len(listed.Schedules) != 1 || listed.Schedules[0].Name != "daily" {
		t.Fatalf("listed = %+v", listed)
	}
	if !listed.Schedules[0].Enabled || listed.Schedules[0].Cron != "0 7 * * *" {
		t.Fatalf("row = %+v", listed.Schedules[0])
	}
	if !nextCandidates[listed.Schedules[0].NextFireAt] {
		t.Fatalf("next_fire_at = %d, want one of %v", listed.Schedules[0].NextFireAt, nextCandidates)
	}

	code, stdout, _ = runCLI(t, "schedule", "pause", "daily", "--data-dir", dataDir)
	if code != 0 || !strings.Contains(stdout, "paused") {
		t.Fatalf("pause = %d %q", code, stdout)
	}
	code, stdout, _ = runCLI(t, "schedule", "list", "--data-dir", dataDir)
	if !strings.Contains(stdout, `"enabled":false`) {
		t.Fatalf("paused list = %q", stdout)
	}
	code, stdout, _ = runCLI(t, "schedule", "resume", "daily", "--data-dir", dataDir)
	if code != 0 || !strings.Contains(stdout, "resumed") {
		t.Fatalf("resume = %d %q", code, stdout)
	}
	code, _, stderr = runCLI(t, "schedule", "remove", "daily", "--data-dir", dataDir)
	if code != 0 || !strings.Contains(stderr, "") {
		t.Fatalf("remove = %d %q", code, stderr)
	}
	code, _, stderr = runCLI(t, "schedule", "remove", "daily", "--data-dir", dataDir)
	if code != 1 || !strings.Contains(stderr, "not found") {
		t.Fatalf("double remove = %d %q", code, stderr)
	}
}

func TestCLIScheduleInvalidInputs(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := scheduleGraphFile(t, dir, server.URL)

	for _, cron := range []string{"99 * * * *", "not-a-cron", "0 0 31 2 *"} {
		code, _, stderr := runCLI(t, "schedule", "add", "--name", "bad", "--graph", graph,
			"--cron", cron, "--data-dir", dataDir)
		if code != 10 {
			t.Fatalf("cron %q exit = %d stderr = %q", cron, code, stderr)
		}
	}

	code, _, _ := runCLI(t, "schedule", "add", "--name", "ghost", "--graph", filepath.Join(dir, "missing.yaml"),
		"--cron", "* * * * *", "--data-dir", dataDir)
	if code != 10 {
		t.Fatalf("missing graph exit = %d", code)
	}

	if code, _, _ := runCLI(t, "schedule", "add", "--name", "ok1", "--graph", graph,
		"--cron", "0 7 * * *", "--data-dir", dataDir); code != 0 {
		t.Fatalf("valid add failed")
	}
	code, _, stderr := runCLI(t, "schedule", "add", "--name", "ok1", "--graph", graph,
		"--cron", "0 8 * * *", "--data-dir", dataDir)
	if code != 10 || !strings.Contains(stderr, "already exists") {
		t.Fatalf("duplicate exit = %d stderr = %q", code, stderr)
	}
}

func TestCLIScheduleHonorsConfigDataDir(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "cfg-data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := scheduleGraphFile(t, dir, server.URL)
	cfg := filepath.Join(dir, "proceed.yaml")
	if err := os.WriteFile(cfg, []byte("data_dir: "+dataDir+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	code, _, stderr := runCLI(t, "schedule", "add", "--name", "daily", "--graph", graph,
		"--cron", "0 7 * * *", "--config", cfg)
	if code != 0 {
		t.Fatalf("add exit = %d stderr = %q", code, stderr)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "proceed.db")); err != nil {
		t.Fatalf("schedule not written to the configured data dir: %v", err)
	}

	code, stdout, stderr := runCLI(t, "schedule", "list", "--config", cfg)
	if code != 0 || !strings.Contains(stdout, "daily") {
		t.Fatalf("list exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}

	code, stdout, stderr = runCLI(t, "schedule", "remove", "daily", "--config", cfg)
	if code != 0 || !strings.Contains(stdout, "removed") {
		t.Fatalf("remove exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
}

func TestCLIScheduleHonorsDataDirEnv(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "env-data")
	t.Setenv("PROCEED_DATA_DIR", dataDir)
	t.Setenv("PROCEED_CONFIG", filepath.Join(dir, "absent.yaml"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := scheduleGraphFile(t, dir, server.URL)

	code, _, stderr := runCLI(t, "schedule", "add", "--name", "daily", "--graph", graph,
		"--cron", "0 7 * * *")
	if code != 0 {
		t.Fatalf("add exit = %d stderr = %q", code, stderr)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "proceed.db")); err != nil {
		t.Fatalf("schedule not written to the env-configured data dir: %v", err)
	}

	code, stdout, stderr := runCLI(t, "schedule", "list")
	if code != 0 || !strings.Contains(stdout, "daily") {
		t.Fatalf("list exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}

	code, stdout, stderr = runCLI(t, "schedule", "remove", "daily")
	if code != 0 || !strings.Contains(stdout, "removed") {
		t.Fatalf("remove exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
}

func TestCLIScheduleManagementWhileLeaseHeld(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := scheduleGraphFile(t, dir, server.URL)

	if code, _, stderr := runCLI(t, "schedule", "list", "--data-dir", dataDir); code != 0 {
		t.Fatalf("initial list exit = %d stderr = %q", code, stderr)
	}
	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := controller.DefaultConfig()
	cfg.Mode = "serve"
	c, err := controller.New(st, cfg, buildPool(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ValidateLease(context.Background()); err != nil {
		t.Fatalf("lease acquire: %v", err)
	}
	defer c.ReleaseLease()

	if code, _, stderr := runCLI(t, "schedule", "add", "--name", "daily", "--graph", graph,
		"--cron", "0 7 * * *", "--data-dir", dataDir); code != 0 {
		t.Fatalf("add under lease exit = %d stderr = %q", code, stderr)
	}
	if code, _, stderr := runCLI(t, "schedule", "pause", "daily", "--data-dir", dataDir); code != 0 {
		t.Fatalf("pause under lease exit = %d stderr = %q", code, stderr)
	}
	if code, _, stderr := runCLI(t, "schedule", "resume", "daily", "--data-dir", dataDir); code != 0 {
		t.Fatalf("resume under lease exit = %d stderr = %q", code, stderr)
	}
	if code, _, stderr := runCLI(t, "schedule", "remove", "daily", "--data-dir", dataDir); code != 0 {
		t.Fatalf("remove under lease exit = %d stderr = %q", code, stderr)
	}

	var owner string
	if err := st.DB().QueryRow(
		"SELECT owner_id FROM controller_lease WHERE store_id = 'default'").Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != c.OwnerID() {
		t.Fatalf("lease owner = %q, want %q (schedule management must not disturb the lease)", owner, c.OwnerID())
	}
}
