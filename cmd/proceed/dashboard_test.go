package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proceed/internal/compiler"
	"proceed/internal/controller"
	"proceed/internal/executor"
	"proceed/internal/store"
)

func seedDashboardStore(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	t.Cleanup(okServer.Close)
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(failServer.Close)

	if code, _, stderr := runCLI(t, "run", writeGraph(t, dir, httpNodeGraph(okServer.URL, "reconcilable")), "--data-dir", dataDir); code != 0 {
		t.Fatalf("seed completed run exit = %d, stderr = %q", code, stderr)
	}
	if code, _, _ := runCLI(t, "run", writeGraph(t, dir, httpNodeGraph(failServer.URL, "reconcilable")), "--data-dir", dataDir); code != 14 {
		t.Fatalf("seed failed run exit = %d, want 14", code)
	}
	if code, _, stderr := runCLI(t, "run", approvalGateGraphFile(t, dir, okServer.URL), "--data-dir", dataDir); code != 16 {
		t.Fatalf("seed approval run exit = %d (stderr %q), want 16", code, stderr)
	}

	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewFuncExecutor(executor.Shell, executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{Output: map[string]any{"executed": req.NodeKey}, Route: "success"}, nil
		}),
	}
	ctrl, err := controller.New(st, controller.DefaultConfig(), pool)
	if err != nil {
		t.Fatal(err)
	}
	src := []byte(`schema: proceed/v1
name: dashboard-wait-seed
nodes:
  - id: step_one
    type: task
    contract: pure
    executor: { kind: shell, command: ["echo", "1"] }
  - id: wait_node
    type: task
    contract: pure
    executor: { kind: shell, command: ["echo", "w"] }
  - id: step_two
    type: task
    contract: pure
    terminal: true
    executor: { kind: shell, command: ["echo", "2"] }
edges:
  - { from: step_one, to: wait_node, type: depends_on }
  - { from: wait_node, to: step_two, type: routes_to, when: success }
`)
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	frozen, err := st.FreezeDefinition(ctx, "wait-seed.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	run, err := st.CreateRun(ctx, frozen.GraphVersionID, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ctrl.Step(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := ctrl.RegisterExternalWait(ctx, controller.ExternalWaitRequest{
		RunID:             run.ID,
		NodeKey:           "wait_node",
		EventType:         "ci.completed",
		CorrelationKey:    "repo=proceed/dashboard;pr=1",
		ExpectedCondition: `{"status":"success"}`,
	}); err != nil {
		t.Fatal(err)
	}
	return dataDir
}

func openDashboardStore(t *testing.T, dataDir string) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func dashboardFrameIDs(t *testing.T, st *store.Store) (runIDs map[string]string, approvalID, waitID string) {
	t.Helper()
	ctx := context.Background()
	runIDs = map[string]string{}
	rows, err := st.DB().QueryContext(ctx, `SELECT id, status FROM graph_run`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, status string
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatal(err)
		}
		runIDs[status] = id
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `SELECT id FROM approval WHERE decision IS NULL LIMIT 1`).Scan(&approvalID); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRowContext(ctx, `SELECT id FROM external_wait WHERE status = 'pending' LIMIT 1`).Scan(&waitID); err != nil {
		t.Fatal(err)
	}
	return runIDs, approvalID, waitID
}

func lastDashboardFrame(out string) string {
	parts := strings.Split(out, dashboardClearScreen)
	return parts[len(parts)-1]
}

func fixedDashboardTime() time.Time { return time.UnixMilli(0).UTC() }

func TestDashboardSnapshotSections(t *testing.T) {
	dataDir := seedDashboardStore(t)
	st := openDashboardStore(t, dataDir)
	snap, err := st.DashboardSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.TotalRuns != 4 {
		t.Fatalf("total runs = %d, want 4", snap.TotalRuns)
	}
	if snap.TotalApprovals != 1 || len(snap.Approvals) != 1 {
		t.Fatalf("approvals = %+v total %d, want 1 pending", snap.Approvals, snap.TotalApprovals)
	}
	if snap.TotalWaits != 1 || len(snap.Waits) != 1 {
		t.Fatalf("waits = %+v total %d, want 1 pending", snap.Waits, snap.TotalWaits)
	}
	if snap.TotalFailedRuns != 1 || len(snap.FailedRuns) != 1 {
		t.Fatalf("failed runs = %+v total %d, want 1", snap.FailedRuns, snap.TotalFailedRuns)
	}
	if snap.TotalFailedNodes < 1 || len(snap.FailedNodes) < 1 {
		t.Fatalf("failed nodes = %+v total %d, want >= 1", snap.FailedNodes, snap.TotalFailedNodes)
	}

	runIDs, approvalID, waitID := dashboardFrameIDs(t, st)
	frame := renderDashboardFrame(snap, 0, 80, fixedDashboardTime(), false)
	for _, id := range runIDs {
		if !strings.Contains(frame, shortDashboardID(id)) {
			t.Errorf("frame missing run %s:\n%s", id, frame)
		}
	}
	for _, want := range []string{
		shortDashboardID(approvalID), "review", "deploy-prod",
		shortDashboardID(waitID), "wait_node", "ci.completed",
		"running", "completed", "failed", "call",
	} {
		if !strings.Contains(frame, want) {
			t.Errorf("frame missing %q:\n%s", want, frame)
		}
	}
	runsIdx := strings.Index(frame, "RUNS (")
	pendingIdx := strings.Index(frame, "PENDING (")
	failuresIdx := strings.Index(frame, "FAILURES (")
	if runsIdx < 0 || pendingIdx < 0 || failuresIdx < 0 || !(runsIdx < pendingIdx && pendingIdx < failuresIdx) {
		t.Fatalf("section order wrong:\n%s", frame)
	}
}

func TestDashboardRefreshPicksUpNewRun(t *testing.T) {
	v1 := &store.DashboardSnapshot{
		Runs:      []store.RunSummary{{RunID: "01OLDRUN000000000000000000", GraphName: "g", Status: "running"}},
		TotalRuns: 1,
	}
	v2 := &store.DashboardSnapshot{
		Runs: []store.RunSummary{
			{RunID: "01NEWRUN000000000000000000", GraphName: "g", Status: "running"},
			{RunID: "01OLDRUN000000000000000000", GraphName: "g", Status: "running"},
		},
		TotalRuns: 2,
	}
	var calls atomic.Int64
	query := func(ctx context.Context) (*store.DashboardSnapshot, error) {
		if calls.Add(1) == 1 {
			return v1, nil
		}
		return v2, nil
	}
	var out strings.Builder
	keys := make(chan dashboardKey, 2)
	ticks := make(chan time.Time, 1)
	result := make(chan int, 1)
	go func() {
		result <- runDashboardLoop(dashboardLoopDeps{
			query: query, out: &out, keys: keys, ticks: ticks,
			width: func() int { return 80 }, now: fixedDashboardTime,
		})
	}()
	ticks <- time.Now()
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatal("refresh tick never re-queried")
	}
	keys <- dashboardKeyQuit
	select {
	case code := <-result:
		if code != 0 {
			t.Fatalf("loop exit = %d, want 0", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not quit")
	}
	frame := lastDashboardFrame(out.String())
	if !strings.Contains(frame, shortDashboardID("01NEWRUN000000000000000000")) {
		t.Fatalf("refreshed frame missing new run:\n%s", frame)
	}
}

func TestDashboardRefreshDropsResolvedApproval(t *testing.T) {
	v1 := &store.DashboardSnapshot{
		Approvals:      []store.DashboardApproval{{ID: "01APPROVAL00000000000000000", RunID: "01RUN0000000000000000000000", NodeKey: "review", Scope: "deploy"}},
		TotalApprovals: 1,
	}
	v2 := &store.DashboardSnapshot{}
	var calls atomic.Int64
	query := func(ctx context.Context) (*store.DashboardSnapshot, error) {
		if calls.Add(1) == 1 {
			return v1, nil
		}
		return v2, nil
	}
	var out strings.Builder
	keys := make(chan dashboardKey, 2)
	ticks := make(chan time.Time, 1)
	result := make(chan int, 1)
	go func() {
		result <- runDashboardLoop(dashboardLoopDeps{
			query: query, out: &out, keys: keys, ticks: ticks,
			width: func() int { return 80 }, now: fixedDashboardTime,
		})
	}()
	ticks <- time.Now()
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatal("refresh tick never re-queried")
	}
	keys <- dashboardKeyQuit
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not quit")
	}
	frame := lastDashboardFrame(out.String())
	if strings.Contains(frame, shortDashboardID("01APPROVAL00000000000000000")) {
		t.Fatalf("refreshed frame still shows resolved approval:\n%s", frame)
	}
	if !strings.Contains(frame, "(no pending items)") {
		t.Fatalf("refreshed frame missing pending empty state:\n%s", frame)
	}
}

func TestDashboardEmptyStore(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	st := openDashboardStore(t, dataDir)
	snap, err := st.DashboardSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.TotalRuns != 0 || snap.TotalApprovals != 0 || snap.TotalWaits != 0 || snap.TotalFailedRuns != 0 || snap.TotalFailedNodes != 0 {
		t.Fatalf("empty snapshot totals nonzero: %+v", snap)
	}
	var out strings.Builder
	keys := make(chan dashboardKey, 1)
	keys <- dashboardKeyQuit
	code := runDashboardLoop(dashboardLoopDeps{
		query: st.DashboardSnapshot, out: &out, keys: keys, ticks: make(chan time.Time),
		width: func() int { return 80 }, now: fixedDashboardTime,
	})
	if code != 0 {
		t.Fatalf("loop exit = %d, want 0", code)
	}
	frame := lastDashboardFrame(out.String())
	for _, want := range []string{"(no runs)", "(no pending items)", "(no failures)"} {
		if !strings.Contains(frame, want) {
			t.Errorf("empty frame missing %q:\n%s", want, frame)
		}
	}
}

func TestDashboardReadOnlyProof(t *testing.T) {
	dataDir := seedDashboardStore(t)
	st := openDashboardStore(t, dataDir)
	ctx := context.Background()
	baseline := func() (string, string, int, int) {
		digest, err := st.ProjectionDigest(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var events int
		if err := st.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM event`).Scan(&events); err != nil {
			t.Fatal(err)
		}
		var lease int
		if err := st.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM controller_lease`).Scan(&lease); err != nil {
			t.Fatal(err)
		}
		var runs string
		rows, err := st.DB().QueryContext(ctx, `SELECT id, status FROM graph_run ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id, status string
			if err := rows.Scan(&id, &status); err != nil {
				t.Fatal(err)
			}
			runs += id + "=" + status + ";"
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return digest, runs, events, lease
	}
	beforeDigest, beforeRuns, beforeEvents, beforeLease := baseline()

	var out strings.Builder
	keys := make(chan dashboardKey, 3)
	keys <- dashboardKeyRefresh
	keys <- dashboardKeyRefresh
	keys <- dashboardKeyQuit
	code := runDashboardLoop(dashboardLoopDeps{
		query: st.DashboardSnapshot, out: &out, keys: keys, ticks: make(chan time.Time),
		width: func() int { return 80 }, now: fixedDashboardTime,
	})
	if code != 0 {
		t.Fatalf("loop exit = %d, want 0", code)
	}
	afterDigest, afterRuns, afterEvents, afterLease := baseline()
	if beforeDigest != afterDigest {
		t.Fatalf("projection digest changed: %q -> %q", beforeDigest, afterDigest)
	}
	if beforeRuns != afterRuns {
		t.Fatalf("run rows changed: %q -> %q", beforeRuns, afterRuns)
	}
	if beforeEvents != afterEvents {
		t.Fatalf("event count changed: %d -> %d", beforeEvents, afterEvents)
	}
	if beforeLease != afterLease {
		t.Fatalf("lease rows changed: %d -> %d", beforeLease, afterLease)
	}
}

func TestDashboardNonTTYRefuses(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	code, stdout, stderr := runCLI(t, "dashboard", "--data-dir", dataDir)
	if code != 2 {
		t.Fatalf("dashboard exit = %d (stdout %q stderr %q), want 2", code, stdout, stderr)
	}
	if strings.ContainsRune(stdout, 0x1b) {
		t.Fatalf("stdout contains escape bytes: %q", stdout)
	}
	if !strings.Contains(stderr, "not a terminal") {
		t.Fatalf("stderr = %q, want terminal refusal", stderr)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "proceed.db")); !os.IsNotExist(err) {
		t.Fatalf("refused dashboard created a store file")
	}
}

func TestDashboardNarrowTerminal(t *testing.T) {
	snap := &store.DashboardSnapshot{
		Runs: []store.RunSummary{{
			RunID: "01VERYLONGRUNIDTHATEXCEEDSWIDTH000000", GraphName: "an-exceptionally-long-graph-name-for-truncation", Status: "running",
		}},
		TotalRuns:        1,
		Approvals:        []store.DashboardApproval{{ID: "01APPROVALID00000000000000000", RunID: "01RUN0000000000000000000000", NodeKey: "a-very-long-node-key-name", Scope: "deploy-prod"}},
		TotalApprovals:   1,
		Waits:            []store.DashboardWait{{ID: "01WAITID00000000000000000000", RunID: "01RUN0000000000000000000000", NodeKey: "wait-node", EventType: "ci.completed", CorrelationKey: "repo=proceed/very-long-correlation-key-value"}},
		TotalWaits:       1,
		FailedRuns:       []store.RunSummary{{RunID: "01FAILRUN0000000000000000000", GraphName: "g", Status: "failed"}},
		TotalFailedRuns:  1,
		FailedNodes:      []store.DashboardFailedNode{{RunID: "01FAILRUN0000000000000000000", NodeKey: "call"}},
		TotalFailedNodes: 1,
	}
	for _, width := range []int{80, 79, 40} {
		frame := renderDashboardFrame(snap, 0, width, fixedDashboardTime(), false)
		for _, line := range strings.Split(strings.TrimSuffix(frame, "\n"), "\n") {
			if n := len([]rune(line)); n > width {
				t.Fatalf("width %d: line %q is %d runes", width, line, n)
			}
		}
	}
}

func TestDashboardSanitizesHostileFields(t *testing.T) {
	snap := &store.DashboardSnapshot{
		Runs: []store.RunSummary{{
			RunID: "01RUNAAAAAAAAAAAAAAAAAAAAA", GraphName: "evil\x1b[2Jgraph\x00name — héllo",
			Status: "running",
		}},
		TotalRuns:      1,
		Approvals:      []store.DashboardApproval{{ID: "01APPROVALID00000000000000000", RunID: "01RUN0000000000000000000000", NodeKey: "node\x1b]0;pwned\x07key", Scope: "s"}},
		TotalApprovals: 1,
	}
	frame := renderDashboardFrame(snap, 0, 80, fixedDashboardTime(), false)
	if strings.ContainsRune(frame, 0x1b) || strings.ContainsRune(frame, 0x00) || strings.ContainsRune(frame, 0x07) {
		t.Fatalf("frame leaks control bytes: %q", frame)
	}
	if !strings.Contains(frame, "h?llo") && !strings.Contains(frame, "h\xc3\xa9llo") {
		t.Fatalf("frame dropped multibyte text: %q", frame)
	}
	for _, line := range strings.Split(strings.TrimSuffix(frame, "\n"), "\n") {
		if n := len([]rune(line)); n > 80 {
			t.Fatalf("line %q is %d runes", line, n)
		}
	}
}

func TestDashboardStaleOnQueryError(t *testing.T) {
	v1 := &store.DashboardSnapshot{
		Runs:      []store.RunSummary{{RunID: "01OLDRUN000000000000000000", GraphName: "g", Status: "running"}},
		TotalRuns: 1,
	}
	var calls atomic.Int64
	query := func(ctx context.Context) (*store.DashboardSnapshot, error) {
		if calls.Add(1) == 1 {
			return v1, nil
		}
		return nil, fmt.Errorf("store locked")
	}
	var out strings.Builder
	keys := make(chan dashboardKey, 2)
	ticks := make(chan time.Time, 1)
	result := make(chan int, 1)
	go func() {
		result <- runDashboardLoop(dashboardLoopDeps{
			query: query, out: &out, keys: keys, ticks: ticks,
			width: func() int { return 80 }, now: fixedDashboardTime,
		})
	}()
	ticks <- time.Now()
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	keys <- dashboardKeyQuit
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not quit")
	}
	frame := lastDashboardFrame(out.String())
	if !strings.Contains(frame, "stale") {
		t.Fatalf("error frame missing stale marker:\n%s", frame)
	}
	if !strings.Contains(frame, shortDashboardID("01OLDRUN000000000000000000")) {
		t.Fatalf("error frame dropped last snapshot:\n%s", frame)
	}
}

func TestDashboardKeyNavigation(t *testing.T) {
	snap := &store.DashboardSnapshot{
		Runs: []store.RunSummary{
			{RunID: "01RUNAAAAAAAAAAAAAAAAAAAAA", GraphName: "g", Status: "running"},
			{RunID: "01RUNBBBBBBBBBBBBBBBBBBBBB", GraphName: "g", Status: "completed"},
		},
		TotalRuns: 2,
	}
	query := func(ctx context.Context) (*store.DashboardSnapshot, error) { return snap, nil }
	var out strings.Builder
	keys := make(chan dashboardKey, 3)
	keys <- dashboardKeyDown
	keys <- dashboardKeyUp
	keys <- dashboardKeyQuit
	code := runDashboardLoop(dashboardLoopDeps{
		query: query, out: &out, keys: keys, ticks: make(chan time.Time),
		width: func() int { return 80 }, now: fixedDashboardTime,
	})
	if code != 0 {
		t.Fatalf("loop exit = %d, want 0", code)
	}
	frames := strings.Split(out.String(), dashboardClearScreen)
	if len(frames) != 4 {
		t.Fatalf("frames = %d, want 3 renders + leading empty", len(frames))
	}
	markerLine := func(frame string) string {
		for _, line := range strings.Split(frame, "\n") {
			if strings.HasPrefix(line, "> ") {
				return line
			}
		}
		return ""
	}
	if got := markerLine(frames[1]); !strings.Contains(got, shortDashboardID("01RUNAAAAAAAAAAAAAAAAAAAAA")) {
		t.Fatalf("initial cursor = %q", got)
	}
	if got := markerLine(frames[2]); !strings.Contains(got, shortDashboardID("01RUNBBBBBBBBBBBBBBBBBBBBB")) {
		t.Fatalf("after down cursor = %q", got)
	}
	if got := markerLine(frames[3]); !strings.Contains(got, shortDashboardID("01RUNAAAAAAAAAAAAAAAAAAAAA")) {
		t.Fatalf("after up cursor = %q", got)
	}
}

func TestDashboardKeyPump(t *testing.T) {
	keys := pumpDashboardKeys(strings.NewReader("rq\x1b[A\x1b[Bx"))
	var got []dashboardKey
	for k := range keys {
		got = append(got, k)
	}
	want := []dashboardKey{dashboardKeyRefresh, dashboardKeyQuit, dashboardKeyUp, dashboardKeyDown, dashboardKeyQuit}
	if len(got) != len(want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("keys = %v, want %v", got, want)
		}
	}
}

func TestDashboardUsageError(t *testing.T) {
	code, _, stderr := runCLI(t, "dashboard", "extra-arg", "--data-dir", t.TempDir())
	if code != 2 {
		t.Fatalf("dashboard exit = %d (stderr %q), want 2", code, stderr)
	}
}
