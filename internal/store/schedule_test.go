package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"proceed/internal/compiler"
)

const scheduleGraph = `schema: proceed/v1
name: schedule-store
nodes:
  - id: call
    type: task
    executor: { kind: http, method: GET, url: "http://example.test/ping" }
    contract: pure
    terminal: true
    capability:
      network:
        allowlisted_hosts: [example.test]
edges: []
`

func freezeScheduleGraph(t *testing.T, s *Store) string {
	t.Helper()
	doc, err := compiler.Parse([]byte(scheduleGraph))
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := s.FreezeDefinition(context.Background(), "schedule.yaml", []byte(scheduleGraph), doc)
	if err != nil {
		t.Fatal(err)
	}
	return frozen.GraphVersionID
}

func minuteMs(t *testing.T, s string) int64 {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v.UnixMilli()
}

func testNextFunc(t *testing.T) CronNextFunc {
	t.Helper()
	return func(expr string, after time.Time) (time.Time, bool, error) {
		cur := after.UTC().Truncate(time.Minute).Add(time.Minute)
		for i := 0; i < 527040; i++ {
			if cronMatchesTest(expr, cur) {
				return cur, true, nil
			}
			cur = cur.Add(time.Minute)
		}
		return time.Time{}, false, nil
	}
}

func cronMatchesTest(expr string, t time.Time) bool {
	switch expr {
	case "* * * * *":
		return true
	case "0 7 * * *":
		return t.Minute() == 0 && t.Hour() == 7
	}
	return false
}

func scheduleRows(t *testing.T, s *Store, name string) *Schedule {
	t.Helper()
	rows, err := s.ListSchedules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if rows[i].Name == name {
			return &rows[i]
		}
	}
	t.Fatalf("schedule %s not found", name)
	return nil
}

func runIDsForSchedule(t *testing.T, s *Store, scheduleID string) []string {
	t.Helper()
	rows, err := s.db.Query(
		"SELECT id FROM graph_run WHERE schedule_id = ? ORDER BY created_at", scheduleID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

func TestScheduleCRUD(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeScheduleGraph(t, s)
	next := minuteMs(t, "2026-09-13T07:00:00Z")

	if err := s.AddSchedule(context.Background(), "daily", versionID, "0 7 * * *", next); err != nil {
		t.Fatal(err)
	}
	err = s.AddSchedule(context.Background(), "daily", versionID, "0 7 * * *", next)
	if !IsCode(err, CodeGraphInvalid) || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("duplicate error = %v", err)
	}
	err = s.AddSchedule(context.Background(), "ghost", "01NOPE", "* * * * *", next)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("unknown version error = %v", err)
	}

	sched := scheduleRows(t, s, "daily")
	if !sched.Enabled || sched.NextFireAt != next || sched.Cron != "0 7 * * *" {
		t.Fatalf("row = %+v", sched)
	}

	updated, err := s.SetScheduleEnabled(context.Background(), "daily", false)
	if err != nil || !updated {
		t.Fatalf("pause = %v %v", updated, err)
	}
	if scheduleRows(t, s, "daily").Enabled {
		t.Fatal("schedule must be paused")
	}
	updated, err = s.SetScheduleEnabled(context.Background(), "ghost", true)
	if err != nil || updated {
		t.Fatalf("unknown pause = %v %v", updated, err)
	}

	removed, err := s.RemoveSchedule(context.Background(), "daily")
	if err != nil || !removed {
		t.Fatalf("remove = %v %v", removed, err)
	}
	if list, _ := s.ListSchedules(context.Background()); len(list) != 0 {
		t.Fatalf("list = %+v", list)
	}
}

func TestFireDueSchedulesFiresOncePerTick(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeScheduleGraph(t, s)

	tick := minuteMs(t, "2026-09-12T10:01:00Z")
	if err := s.AddSchedule(context.Background(), "every", versionID, "* * * * *", tick); err != nil {
		t.Fatal(err)
	}
	next := testNextFunc(t)

	outcomes, err := s.FireDueSchedules(context.Background(), minuteMs(t, "2026-09-12T10:01:02Z"), next)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 1 || outcomes[0].RunID == "" || outcomes[0].Tick != tick {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	runID := outcomes[0].RunID

	sched := scheduleRows(t, s, "every")
	if sched.NextFireAt != minuteMs(t, "2026-09-12T10:02:00Z") {
		t.Fatalf("next_fire_at = %d", sched.NextFireAt)
	}
	if sched.LastRunID != runID || sched.SkippedCount != 0 {
		t.Fatalf("row = %+v", sched)
	}

	var graphDigest, schedID string
	var schedTick int64
	var digest string
	if err := s.db.QueryRow(`
SELECT g.definition_digest, g.schedule_id, g.schedule_tick, v.definition_digest
FROM graph_run g JOIN graph_version v ON v.id = g.graph_version_id WHERE g.id = ?`, runID).
		Scan(&graphDigest, &schedID, &schedTick, &digest); err != nil {
		t.Fatal(err)
	}
	if graphDigest != digest || schedID != sched.ID || schedTick != tick {
		t.Fatalf("run row digest=%s sched=%s tick=%d", graphDigest, schedID, schedTick)
	}
	var payload string
	if err := s.db.QueryRow(
		"SELECT payload FROM event WHERE run_id = ? AND type = 'run_started'", runID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, `"schedule_id":"`) || !strings.Contains(payload, `"schedule_tick":`) {
		t.Fatalf("payload = %s", payload)
	}
	var idem string
	if err := s.db.QueryRow(
		"SELECT idempotency_key FROM event WHERE run_id = ?", runID).Scan(&idem); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(idem, "schedule:") {
		t.Fatalf("idempotency key = %s", idem)
	}

	if _, err := s.FireDueSchedules(context.Background(), minuteMs(t, "2026-09-12T10:01:30Z"), next); err != nil {
		t.Fatal(err)
	}
	if got := runIDsForSchedule(t, s, sched.ID); len(got) != 1 {
		t.Fatalf("double fire: runs = %v", got)
	}

	outcomes, err = s.FireDueSchedules(context.Background(), minuteMs(t, "2026-09-12T10:02:01Z"), next)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 1 || outcomes[0].Tick != minuteMs(t, "2026-09-12T10:02:00Z") {
		t.Fatalf("second tick outcomes = %+v", outcomes)
	}
	if got := runIDsForSchedule(t, s, sched.ID); len(got) != 2 {
		t.Fatalf("runs after two ticks = %v", got)
	}

	removed, err := s.RemoveSchedule(context.Background(), "every")
	if err != nil || !removed {
		t.Fatalf("remove after fire = %v %v", removed, err)
	}
	if got := runIDsForSchedule(t, s, sched.ID); len(got) != 2 {
		t.Fatalf("runs must survive schedule removal, got %v", got)
	}
}

func TestFireDueSchedulesSkipsDowntime(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeScheduleGraph(t, s)

	tick := minuteMs(t, "2026-09-12T10:00:00Z")
	if err := s.AddSchedule(context.Background(), "daily7", versionID, "0 7 * * *", tick); err != nil {
		t.Fatal(err)
	}
	next := testNextFunc(t)

	outcomes, err := s.FireDueSchedules(context.Background(), minuteMs(t, "2026-09-15T10:00:00Z"), next)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 1 || outcomes[0].RunID != "" || outcomes[0].Skipped != 4 {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	sched := scheduleRows(t, s, "daily7")
	if got := runIDsForSchedule(t, s, sched.ID); len(got) != 0 {
		t.Fatalf("catch-up runs created: %v", got)
	}
	if sched.SkippedCount != 4 {
		t.Fatalf("skipped_count = %d", sched.SkippedCount)
	}
	if sched.NextFireAt != minuteMs(t, "2026-09-16T07:00:00Z") {
		t.Fatalf("next_fire_at = %d", sched.NextFireAt)
	}
	if !strings.Contains(sched.LastSkipWindow, `"count":4`) || sched.LastSkippedAt == 0 {
		t.Fatalf("window = %s skipped_at = %d", sched.LastSkipWindow, sched.LastSkippedAt)
	}
}

func TestFireDueSchedulesSkipsPausedAndFuture(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeScheduleGraph(t, s)

	past := minuteMs(t, "2026-09-12T10:00:00Z")
	future := minuteMs(t, "2030-01-01T00:00:00Z")
	if err := s.AddSchedule(context.Background(), "paused", versionID, "* * * * *", past); err != nil {
		t.Fatal(err)
	}
	if err := s.AddSchedule(context.Background(), "future", versionID, "* * * * *", future); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetScheduleEnabled(context.Background(), "paused", false); err != nil {
		t.Fatal(err)
	}
	outcomes, err := s.FireDueSchedules(context.Background(), minuteMs(t, "2026-09-12T10:00:30Z"), testNextFunc(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 0 {
		t.Fatalf("outcomes = %+v", outcomes)
	}
}

func TestFireDueSchedulesIdempotencyGuard(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeScheduleGraph(t, s)

	tick := minuteMs(t, "2026-09-12T10:01:00Z")
	if err := s.AddSchedule(context.Background(), "every", versionID, "* * * * *", tick); err != nil {
		t.Fatal(err)
	}
	sched := scheduleRows(t, s, "every")
	sched.NextFireAt = tick
	next := testNextFunc(t)
	if _, err := s.fireOneSchedule(context.Background(), sched, minuteMs(t, "2026-09-12T10:01:02Z"), next); err != nil {
		t.Fatal(err)
	}

	schedAgain := scheduleRows(t, s, "every")
	schedAgain.NextFireAt = tick
	if _, err := s.fireOneSchedule(context.Background(), schedAgain, minuteMs(t, "2026-09-12T10:01:02Z"), next); err == nil {
		t.Fatal("expected idempotency conflict on tick replay")
	}
	if got := runIDsForSchedule(t, s, sched.ID); len(got) != 1 {
		t.Fatalf("replay created runs: %v", got)
	}
}

func TestScheduledRunProjectionRebuild(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeScheduleGraph(t, s)

	tick := minuteMs(t, "2026-09-12T10:01:00Z")
	if err := s.AddSchedule(context.Background(), "every", versionID, "* * * * *", tick); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FireDueSchedules(context.Background(), minuteMs(t, "2026-09-12T10:01:02Z"), testNextFunc(t)); err != nil {
		t.Fatal(err)
	}
	report, err := s.RebuildProjections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Diverged {
		t.Fatal("projection rebuild diverged")
	}
	var count int
	if err := s.db.QueryRow(
		"SELECT COUNT(*) FROM graph_run WHERE schedule_id IS NOT NULL AND schedule_tick = ?", tick).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rebuilt scheduled runs = %d", count)
	}
}

func TestFireDueSchedulesIsolatesPerScheduleErrors(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeScheduleGraph(t, s)

	tick := minuteMs(t, "2026-09-12T10:00:00Z")
	if err := s.AddSchedule(context.Background(), "a-bad", versionID, "0 0 31 2 *", tick); err != nil {
		t.Fatal(err)
	}
	if err := s.AddSchedule(context.Background(), "healthy", versionID, "* * * * *", tick); err != nil {
		t.Fatal(err)
	}

	outcomes, err := s.FireDueSchedules(context.Background(), tick+2000, testNextFunc(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(outcomes) != 2 {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	bad, healthy := outcomes[0], outcomes[1]
	if bad.Name != "a-bad" || bad.Error == "" || bad.RunID != "" {
		t.Fatalf("bad outcome = %+v", bad)
	}
	if healthy.Name != "healthy" || healthy.Error != "" || healthy.RunID == "" || healthy.Tick != tick {
		t.Fatalf("healthy outcome = %+v", healthy)
	}
	healthySched := scheduleRows(t, s, "healthy")
	if len(runIDsForSchedule(t, s, healthySched.ID)) != 1 {
		t.Fatal("healthy schedule must still fire after a failing schedule")
	}
	badSched := scheduleRows(t, s, "a-bad")
	if len(runIDsForSchedule(t, s, badSched.ID)) != 0 || badSched.NextFireAt != tick {
		t.Fatalf("bad row = %+v", badSched)
	}
}

func TestFreshStoreHasScheduleColumns(t *testing.T) {
	s := openTestStore(t)
	defer s.Close()
	for _, col := range []string{"schedule_id", "schedule_tick"} {
		var n int
		if err := s.db.QueryRow(
			`SELECT COUNT(*) FROM pragma_table_info('graph_run') WHERE name = ?`, col).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("column %s missing in fresh baseline store", col)
		}
	}
	var tables int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schedule'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 1 {
		t.Fatal("schedule table missing in fresh baseline store")
	}
}
