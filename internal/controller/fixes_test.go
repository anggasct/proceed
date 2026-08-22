package controller

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"proceed/internal/executor"
	"proceed/internal/store"
)

func TestCancellationSignalsInflightExecutor(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: blocking
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [bin/w] }
    contract: pure
    terminal: true
edges: []
`)

	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			select {
			case <-req.Cancellation:
				return nil, executor.ErrCancelled
			case <-ctx.Done():
				return nil, executor.ErrCancelled
			}
		}),
	}
	c := newController(t, st, pool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}

	drained := make(chan error, 1)
	go func() {
		drained <- c.Drain(ctx, runID)
	}()
	for {
		var attempts int
		if err := st.DB().QueryRowContext(ctx,
			"SELECT COUNT(*) FROM node_attempt").Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if attempts > 0 {
			break
		}
		select {
		case err := <-drained:
			t.Fatalf("drain finished before the executor was cancelled: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}

	if err := c.CancelRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if err := c.CancelRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain never finished after cancellation")
	}

	if s := nodeStatus(t, st, runID, "work"); s != "cancelled" {
		t.Fatalf("node = %q, want cancelled", s)
	}
	if s := runStatus(t, st, runID); s != "cancelled" {
		t.Fatalf("run = %q, want cancelled", s)
	}
	events, err := st.Events(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	finishedAfterCancel := false
	cancelSeen := false
	for _, ev := range events {
		if ev.Type == "node_cancelled" {
			cancelSeen = true
		}
		if cancelSeen && ev.Type == "node_finished" {
			finishedAfterCancel = true
		}
	}
	if finishedAfterCancel {
		t.Error("node_finished recorded after cancellation — executor outcome overrode durable cancel")
	}

	assertReplayStable(t, st)
}

func TestCancellationSignalsRequestChannel(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: request-cancel
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [bin/w] }
    contract: pure
    terminal: true
edges: []
`)
	p := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			<-req.Cancellation
			return nil, executor.ErrCancelled
		}),
	}
	c := newController(t, st, p)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	drained := make(chan error, 1)
	go func() { drained <- c.Drain(ctx, runID) }()
	for {
		var attempts int
		if err := st.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM node_attempt").Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if attempts > 0 {
			break
		}
		select {
		case err := <-drained:
			t.Fatalf("drain finished before cancellation: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := c.CancelRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not observe Request.Cancellation")
	}
	if s := runStatus(t, st, runID); s != "cancelled" {
		t.Fatalf("run = %q, want cancelled", s)
	}
	events, err := st.Events(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	var requests int
	for _, event := range events {
		if event.Type == "node_cancel_requested" {
			requests++
		}
	}
	if requests != 1 {
		t.Fatalf("node_cancel_requested events = %d, want 1", requests)
	}
	assertReplayStable(t, st)
}

func assertReplayStable(t *testing.T, st *store.Store) {
	t.Helper()
	before, err := st.ProjectionDigest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	report, err := st.RebuildProjections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Diverged {
		t.Fatalf("projections diverged from event stream: %+v", report)
	}
	after, err := st.ProjectionDigest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Error("rebuild changed projection digest")
	}
}

func TestRoutingSkipsUnselectedBranches(t *testing.T) {
	for _, tc := range []struct {
		route string
		want  string
	}{
		{"requires_code", "code_path"},
		{"requires_docs", "docs_path"},
	} {
		t.Run(tc.route, func(t *testing.T) {
			ctx := context.Background()
			st := newStore(t)
			frozen := compileAndFreeze(t, st, routingGraph)
			var mu sync.Mutex
			executed := map[string]int{}
			pool := map[executor.Kind]executor.Executor{
				"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
					mu.Lock()
					executed[req.NodeKey]++
					mu.Unlock()
					if req.NodeKey == "classify" {
						return &executor.Result{Route: tc.route}, nil
					}
					return &executor.Result{}, nil
				}),
			}
			c := newController(t, st, pool)
			runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Drain(ctx, runID); err != nil {
				t.Fatal(err)
			}

			if executed[tc.want] != 1 {
				t.Errorf("selected branch %s executed %d times, want 1", tc.want, executed[tc.want])
			}
			unselected := "docs_path"
			if tc.want == "docs_path" {
				unselected = "code_path"
			}
			if executed[unselected] != 0 {
				t.Errorf("unselected branch %s executed %d times, want 0", unselected, executed[unselected])
			}
			if s := nodeStatus(t, st, runID, unselected); s != "skipped" {
				t.Errorf("unselected branch status = %q, want skipped", s)
			}
			if s := runStatus(t, st, runID); s != "completed" {
				t.Errorf("run = %q, want completed", s)
			}
			assertReplayStable(t, st)
		})
	}
}

func TestRetryAndFailureScenariosReplayStable(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, retryGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Idempotent, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return nil, errBoom
		}),
	}
	c := newController(t, st, pool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := runStatus(t, st, runID); s != "failed" {
		t.Fatalf("run = %q, want failed", s)
	}
	assertReplayStable(t, st)
}

var errBoom = &testErr{}

type testErr struct{}

func (*testErr) Error() string { return "boom" }

func TestEventsCarryPayloadDigest(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	}
	c := newController(t, st, pool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	var empty int
	if err := st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM event WHERE run_id = ? AND (payload_digest IS NULL OR payload_digest = '')",
		runID).Scan(&empty); err != nil {
		t.Fatal(err)
	}
	if empty != 0 {
		t.Errorf("%d events stored without payload_digest", empty)
	}
}

func TestLongExecutorKeepsLeaseAlive(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			time.Sleep(2 * time.Second)
			return &executor.Result{}, nil
		}),
	}
	cfg := DefaultConfig()
	cfg.LeaseTTL = time.Second
	cfg.HeartbeatPeriod = 200 * time.Millisecond
	c, err := New(st, cfg, pool)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := runStatus(t, st, runID); s != "completed" {
		t.Fatalf("run = %q, want completed with heartbeat keeping the lease alive", s)
	}
}

func TestDrainReleasesLeaseOnCompletion(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	}
	c1 := newController(t, st, pool)
	runID, err := c1.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c1.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}

	c2 := newController(t, st, pool)
	if err := c2.acquireLease(ctx, time.Now()); err != nil {
		t.Fatalf("lease must be released after drain completes: %v", err)
	}
}

func TestAttemptLeasePersisted(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	}
	c := newController(t, st, pool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	var nullLease int
	if err := st.DB().QueryRowContext(ctx, `
SELECT COUNT(*) FROM node_attempt na
JOIN run_node rn ON rn.id = na.run_node_id
WHERE rn.run_id = ? AND (na.lease_token IS NULL OR na.lease_expires_at IS NULL)`, runID).Scan(&nullLease); err != nil {
		t.Fatal(err)
	}
	if nullLease != 0 {
		t.Errorf("%d attempts without persisted lease token/expiry", nullLease)
	}
	assertReplayStable(t, st)
}
