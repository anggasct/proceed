package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func seedGCRun(t *testing.T, s *Store, versionID string, seq int, status string, finishedAgo time.Duration) string {
	t.Helper()
	ctx := context.Background()
	run, err := s.CreateRun(ctx, versionID, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	statusUpdate := map[string]string{
		"completed": "run_completed",
		"failed":    "run_failed",
		"cancelled": "run_cancelled",
		"abandoned": "run_abandoned",
	}
	evType, ok := statusUpdate[status]
	if status != "running" && !ok {
		t.Fatalf("unknown seed status %q", status)
	}
	if status != "running" {
		if _, err := s.Append(ctx, Event{
			RunID:         run.ID,
			Sequence:      2,
			SchemaVersion: eventSchemaVersion,
			Type:          evType,
			OccurredAt:    now.UnixMilli(),
			ActorType:     "controller",
			ActorID:       "controller-1",
			Payload:       `{"detail":"seed"}`,
		}); err != nil {
			t.Fatal(err)
		}
	}
	shift := finishedAgo + time.Duration(seq)*time.Hour
	if status == "running" {
		shift = time.Duration(seq) * time.Hour
	}
	if _, err := s.db.Exec(`UPDATE event SET occurred_at = occurred_at - ? WHERE run_id = ?`,
		shift.Milliseconds(), run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RebuildProjections(ctx); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := s.db.QueryRow(`SELECT status FROM graph_run WHERE id = ?`, run.ID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != status {
		t.Fatalf("seed run status = %q, want %q", got, status)
	}
	return run.ID
}

func seedGCStore(t *testing.T) (*Store, string) {
	t.Helper()
	s := openTestStore(t)
	versionID, _, _, _ := fixtureVersion(t, s)
	return s, versionID
}

func assertNoFKViolations(t *testing.T, s *Store) {
	t.Helper()
	rows, err := s.db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		var rowid int64
		var target, fkindex int64
		if err := rows.Scan(&table, &rowid, &target, &fkindex); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("foreign key violation: table %s rowid %d references %d (fk %d)", table, rowid, target, fkindex)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestGCDryRunChangesNothingButRecord(t *testing.T) {
	s, versionID := seedGCStore(t)
	ctx := context.Background()
	old1 := seedGCRun(t, s, versionID, 10, "completed", 90*24*time.Hour)
	old2 := seedGCRun(t, s, versionID, 9, "failed", 90*24*time.Hour)
	live := seedGCRun(t, s, versionID, 1, "running", 0)
	_ = live

	beforeRuns := count(t, s, `SELECT COUNT(*) FROM graph_run`)
	beforeEvents := count(t, s, `SELECT COUNT(*) FROM event`)
	digestBefore, err := s.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}

	res, err := s.GCRuns(ctx, GCPolicy{KeepRuns: 2, OlderThan: 30 * 24 * time.Hour, OlderThanS: "30d", DryRun: true}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedRuns != 0 {
		t.Fatalf("dry-run deleted runs = %d, want 0", res.DeletedRuns)
	}
	if res.Counts["graph_run"] != 1 {
		t.Fatalf("dry-run graph_run count = %d, want 1 (only oldest terminal beyond keep-2)", res.Counts["graph_run"])
	}
	if after := count(t, s, `SELECT COUNT(*) FROM graph_run`); after != beforeRuns {
		t.Fatalf("graph_run rows = %d, want %d", after, beforeRuns)
	}
	if after := count(t, s, `SELECT COUNT(*) FROM event`); after != beforeEvents {
		t.Fatalf("event rows = %d, want %d", after, beforeEvents)
	}
	for _, id := range []string{old1, old2} {
		var n int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM graph_run WHERE id = ?`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("seed run %s missing after dry-run", id)
		}
	}
	digestAfter, err := s.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digestAfter != digestBefore {
		t.Fatal("projection digest changed across dry-run")
	}
	if n := count(t, s, `SELECT COUNT(*) FROM gc_record`); n != 1 {
		t.Fatalf("gc_record rows = %d, want 1", n)
	}
}

func TestGCDeletesExactGatedSet(t *testing.T) {
	s, versionID := seedGCStore(t)
	ctx := context.Background()
	victim := seedGCRun(t, s, versionID, 10, "completed", 90*24*time.Hour)
	recent := seedGCRun(t, s, versionID, 2, "failed", 24*time.Hour)
	live := seedGCRun(t, s, versionID, 1, "running", 0)

	res, err := s.GCRuns(ctx, GCPolicy{KeepRuns: 2, OlderThan: 30 * 24 * time.Hour, OlderThanS: "30d"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedRuns != 1 {
		t.Fatalf("deleted runs = %d, want 1", res.DeletedRuns)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM graph_run WHERE id = ?`, victim).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("gated run survived GC")
	}
	for _, id := range []string{recent, live} {
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM graph_run WHERE id = ?`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("survivor %s missing after GC", id)
		}
	}
	if n := count(t, s, `SELECT COUNT(*) FROM event WHERE run_id = ?`, victim); n != 0 {
		t.Fatalf("victim events = %d, want 0", n)
	}
	if n := count(t, s, `SELECT COUNT(*) FROM outcome WHERE run_id = ?`, victim); n != 0 {
		t.Fatalf("victim outcome = %d, want 0", n)
	}
	if n := count(t, s, `SELECT COUNT(*) FROM metric WHERE anchor_id NOT IN (SELECT anchor_id FROM outcome)`); n != 0 {
		t.Fatalf("orphan metrics = %d, want 0", n)
	}
	if n := count(t, s, `SELECT COUNT(*) FROM anchor WHERE id NOT IN (SELECT anchor_id FROM outcome)`); n != 0 {
		t.Fatalf("orphan anchors = %d, want 0", n)
	}
	if n := count(t, s, `SELECT COUNT(*) FROM gc_record`); n != 1 {
		t.Fatalf("gc_record rows = %d, want 1", n)
	}
	assertNoFKViolations(t, s)
}

func TestGCPendingApprovalSurvives(t *testing.T) {
	s, versionID := seedGCStore(t)
	ctx := context.Background()
	run, err := s.CreateRun(ctx, versionID, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	versionID, _, nodeA, _ := fixtureVersion(t, s)
	_ = versionID
	if _, err := s.Append(ctx, Event{
		RunID:         run.ID,
		Sequence:      2,
		SchemaVersion: eventSchemaVersion,
		Type:          "node_started",
		OccurredAt:    time.Now().Add(-100 * 24 * time.Hour).UnixMilli(),
		ActorType:     "controller",
		ActorID:       "controller-1",
		Payload:       `{"node_key":"` + nodeA + `","attempt_no":1,"executor":"human_approval","side_effect_contract":"reconcilable","operation_key":"op-gc"}`,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, Event{
		RunID:         run.ID,
		Sequence:      3,
		SchemaVersion: eventSchemaVersion,
		Type:          "approval_requested",
		OccurredAt:    time.Now().Add(-90 * 24 * time.Hour).UnixMilli(),
		ActorType:     "controller",
		ActorID:       "controller-1",
		Payload:       `{"node_key":"` + nodeA + `","requested_action":{},"evidence_references":[],"required_scope":"ops","expires_at":9999999999999}`,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE graph_run SET created_at = ? WHERE id = ?`,
		time.Now().Add(-100*24*time.Hour).UnixMilli(), run.ID); err != nil {
		t.Fatal(err)
	}
	if n := count(t, s, `SELECT COUNT(*) FROM approval WHERE run_id = ? AND decision IS NULL`, run.ID); n != 1 {
		t.Fatalf("pending approvals = %d, want 1", n)
	}
	res, err := s.GCRuns(ctx, GCPolicy{KeepRuns: 0, OlderThan: 30 * 24 * time.Hour, OlderThanS: "30d"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedRuns != 0 {
		t.Fatalf("deleted runs = %d, want 0 (pending approval run is non-terminal)", res.DeletedRuns)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM graph_run WHERE id = ?`, run.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("pending-approval run deleted by GC")
	}
}

func TestGCConcurrentCreationSurvives(t *testing.T) {
	s, versionID := seedGCStore(t)
	ctx := context.Background()
	victim := seedGCRun(t, s, versionID, 10, "completed", 90*24*time.Hour)
	res, err := s.GCRuns(ctx, GCPolicy{KeepRuns: 0, OlderThan: 30 * 24 * time.Hour, OlderThanS: "30d"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedRuns != 1 {
		t.Fatalf("deleted runs = %d, want 1", res.DeletedRuns)
	}
	newRun, err := s.CreateRun(ctx, versionID, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE graph_run SET created_at = ?, finished_at = ? WHERE id = ?`,
		time.Now().Add(-100*24*time.Hour).UnixMilli(), time.Now().Add(-90*24*time.Hour).UnixMilli(), newRun.ID); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM graph_run WHERE id = ?`, newRun.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("run created after the candidate snapshot is missing")
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM graph_run WHERE id = ?`, victim).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("gated run survived GC")
	}
	res2, err := s.GCRuns(ctx, GCPolicy{KeepRuns: 0, OlderThan: 30 * 24 * time.Hour, OlderThanS: "30d"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res2.DeletedRuns != 0 {
		t.Fatalf("second GC deleted runs = %d, want 0 (new run is non-terminal)", res2.DeletedRuns)
	}
}

func TestGCRollbackOnMidTransactionFailure(t *testing.T) {
	s, versionID := seedGCStore(t)
	ctx := context.Background()
	victim := seedGCRun(t, s, versionID, 10, "completed", 90*24*time.Hour)
	before, err := s.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM event WHERE run_id = ?`, victim); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}); err == nil {
		t.Fatal("expected mid-transaction failure, got nil")
	}
	after, err := s.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatal("projection digest changed after rolled-back transaction")
	}
	if n := count(t, s, `SELECT COUNT(*) FROM event WHERE run_id = ?`, victim); n == 0 {
		t.Fatal("victim events missing after rollback")
	}
	res, err := s.GCRuns(ctx, GCPolicy{KeepRuns: 0, OlderThan: 30 * 24 * time.Hour, OlderThanS: "30d"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedRuns != 1 {
		t.Fatalf("rerun deleted runs = %d, want 1 (rerun safe)", res.DeletedRuns)
	}
}

func TestGCScheduleLastRunSurvivesAsNull(t *testing.T) {
	s, versionID := seedGCStore(t)
	ctx := context.Background()
	victim := seedGCRun(t, s, versionID, 10, "completed", 90*24*time.Hour)
	if _, err := s.db.Exec(`INSERT INTO schedule (id, name, graph_version_id, definition_digest, cron, enabled, next_fire_at, last_run_id, created_at)
VALUES ('01SCHEDGC000000000000000001', 'nightly', ?, 'digest', '0 7 * * *', 1, ?, ?, ?)`,
		versionID, time.Now().Add(time.Hour).UnixMilli(), victim, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	res, err := s.GCRuns(ctx, GCPolicy{KeepRuns: 0, OlderThan: 30 * 24 * time.Hour, OlderThanS: "30d"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedRuns != 1 {
		t.Fatalf("deleted runs = %d, want 1", res.DeletedRuns)
	}
	var lastRun any
	if err := s.db.QueryRow(`SELECT last_run_id FROM schedule WHERE name = 'nightly'`).Scan(&lastRun); err != nil {
		t.Fatal(err)
	}
	if lastRun != nil {
		t.Fatalf("schedule last_run_id = %v, want NULL after GC", lastRun)
	}
	assertNoFKViolations(t, s)
}

func TestGCGovernanceAnchorSurvives(t *testing.T) {
	s, versionID := seedGCStore(t)
	ctx := context.Background()
	victim := seedGCRun(t, s, versionID, 10, "completed", 90*24*time.Hour)
	var anchorID string
	if err := s.db.QueryRow(`SELECT anchor_id FROM outcome WHERE run_id = ?`, victim).Scan(&anchorID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO metric (id, anchor_id, name, value, unit, recorded_at)
VALUES ('01METRICGC000000000000000001', ?, 'score', 0.9, 'ratio', ?)`, anchorID, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO policy_change_proposal (id, target_graph_version_id, status, rationale, proposed_change, created_at)
VALUES ('01PROPOSALGC0000000000000001', ?, 'proposed', 'tune', '{}', ?)`,
		versionID, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	var pid string
	if err := s.db.QueryRow(`SELECT id FROM policy_change_proposal LIMIT 1`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO proposal_metric (proposal_id, metric_id) VALUES (?, '01METRICGC000000000000000001')`, pid); err != nil {
		t.Fatal(err)
	}
	res, err := s.GCRuns(ctx, GCPolicy{KeepRuns: 0, OlderThan: 30 * 24 * time.Hour, OlderThanS: "30d"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedRuns != 0 {
		t.Fatalf("deleted runs = %d, want 0 (anchor cited by governance survives)", res.DeletedRuns)
	}
	for _, q := range []string{
		`SELECT COUNT(*) FROM anchor WHERE id = '` + anchorID + `'`,
		`SELECT COUNT(*) FROM metric WHERE id = '01METRICGC000000000000000001'`,
		`SELECT COUNT(*) FROM policy_change_proposal`,
		`SELECT COUNT(*) FROM proposal_metric`,
		`SELECT COUNT(*) FROM graph_run WHERE id = '` + victim + `'`,
	} {
		var n int
		if err := s.db.QueryRow(q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("%s = %d, want 1", q, n)
		}
	}
	assertNoFKViolations(t, s)
}

func TestGCNullFinishedAtSurvivesAsAnomaly(t *testing.T) {
	s, versionID := seedGCStore(t)
	ctx := context.Background()
	victim := seedGCRun(t, s, versionID, 10, "completed", 90*24*time.Hour)
	if _, err := s.db.Exec(`UPDATE graph_run SET finished_at = NULL WHERE id = ?`, victim); err != nil {
		t.Fatal(err)
	}
	res, err := s.GCRuns(ctx, GCPolicy{KeepRuns: 0, OlderThan: 30 * 24 * time.Hour, OlderThanS: "30d"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.Anomalies != 1 {
		t.Fatalf("anomalies = %d, want 1", res.Anomalies)
	}
	if res.DeletedRuns != 0 {
		t.Fatalf("deleted runs = %d, want 0 (NULL finished_at never selected)", res.DeletedRuns)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM graph_run WHERE id = ?`, victim).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("anomaly run deleted by GC")
	}
}

func TestGCEmptyStoreAndKeepAll(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	res, err := s.GCRuns(ctx, GCPolicy{KeepRuns: 100, OlderThan: 90 * 24 * time.Hour, OlderThanS: "90d"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedRuns != 0 {
		t.Fatalf("deleted runs = %d, want 0", res.DeletedRuns)
	}
	for table, n := range res.Counts {
		if n != 0 {
			t.Fatalf("table %s count = %d, want 0", table, n)
		}
	}
}

func TestGCMigrationAddsRecordTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "proceed.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DROP TABLE gc_record`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("PRAGMA user_version = 6"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	backup := migrationBackupPath(path)
	_ = os.Remove(backup)
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var tables int
	if err := s2.db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'gc_record'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 1 {
		t.Fatal("gc_record table missing after migration")
	}
	var v int
	if err := s2.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != storeSchemaVersion {
		t.Fatalf("user_version = %d, want %d", v, storeSchemaVersion)
	}
}
