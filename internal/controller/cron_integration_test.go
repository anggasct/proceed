package controller

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"proceed/internal/compiler"
	"proceed/internal/executor"
	"proceed/internal/store"
)

const scheduledGraph = `schema: proceed/v1
name: scheduled-run
nodes:
  - id: call
    type: task
    executor: { kind: shell, command: [bin/call] }
    contract: pure
    terminal: true
edges: []
`

func TestControllerFireDueSchedulesCreatesRun(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		time.Sleep(50 * time.Millisecond)
		_ = st.Close()
	})
	src := []byte(scheduledGraph)
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(context.Background(), "sched.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	tick := time.Date(2026, 9, 12, 10, 1, 0, 0, time.UTC)
	if err := st.AddSchedule(context.Background(), "every", frozen.GraphVersionID, "* * * * *", tick.UnixMilli()); err != nil {
		t.Fatal(err)
	}

	pool := map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewAppendFileExecutor(),
	}
	c := newController(t, st, pool)

	if err := c.FireDueSchedules(context.Background(), tick.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	var runID, schedID string
	var schedTick int64
	if err := st.DB().QueryRow(`
SELECT id, schedule_id, schedule_tick FROM graph_run WHERE schedule_id IS NOT NULL`).
		Scan(&runID, &schedID, &schedTick); err != nil {
		t.Fatal(err)
	}
	if schedTick != tick.UnixMilli() {
		t.Fatalf("schedule_tick = %d", schedTick)
	}
	var payload string
	if err := st.DB().QueryRow(
		"SELECT payload FROM event WHERE run_id = ? AND type = 'run_started'", runID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, `"schedule_id":"`) {
		t.Fatalf("payload = %s", payload)
	}

	if err := c.FireDueSchedules(context.Background(), tick.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := st.DB().QueryRow("SELECT COUNT(*) FROM graph_run WHERE schedule_id = ?", schedID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("double fire: %d runs", count)
	}
}

func TestControllerFireDueSchedulesDowntimeSkip(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		time.Sleep(50 * time.Millisecond)
		_ = st.Close()
	})
	src := []byte(scheduledGraph)
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(context.Background(), "sched.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	tick := time.Date(2026, 9, 12, 7, 0, 0, 0, time.UTC)
	if err := st.AddSchedule(context.Background(), "daily", frozen.GraphVersionID, "0 7 * * *", tick.UnixMilli()); err != nil {
		t.Fatal(err)
	}

	pool := map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewAppendFileExecutor(),
	}
	c := newController(t, st, pool)

	wake := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	if err := c.FireDueSchedules(context.Background(), wake); err != nil {
		t.Fatal(err)
	}
	var runs int
	if err := st.DB().QueryRow("SELECT COUNT(*) FROM graph_run").Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("downtime created %d catch-up runs", runs)
	}
	schedules, err := st.ListSchedules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(schedules) != 1 || schedules[0].SkippedCount != 4 {
		t.Fatalf("schedules = %+v", schedules)
	}
	want := time.Date(2026, 9, 16, 7, 0, 0, 0, time.UTC)
	if schedules[0].NextFireAt != want.UnixMilli() {
		t.Fatalf("next_fire_at = %d", schedules[0].NextFireAt)
	}
}
