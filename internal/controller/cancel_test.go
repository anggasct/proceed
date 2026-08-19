package controller

import (
	"context"
	"sync"
	"testing"

	"proceed/internal/executor"
	"proceed/internal/store"
)

func TestCancelRunBeforeNodesRun(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, fanOutGraph)
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

	err = c.CancelRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if runStatus(t, st, runID) != "completed" {
		t.Fatalf("cancelling a completed run must not change status, got %q", runStatus(t, st, runID))
	}
}

func TestCancelRunUnknown(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	}
	c := newController(t, st, pool)
	err := c.CancelRun(ctx, "missing-run")
	if store.ErrorCode(err) != "RUN_NOT_FOUND" {
		t.Fatalf("error = %v, want RUN_NOT_FOUND", err)
	}
}

func TestProjectionsMatchEventStreamAfterRuns(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, fanOutGraph)
	var mu sync.Mutex
	served := map[string]int{}
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			mu.Lock()
			served[req.NodeKey]++
			mu.Unlock()
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

	before, err := st.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	report, err := st.RebuildProjections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Diverged {
		t.Fatalf("projections diverged from event stream: %+v", report)
	}
	after, err := st.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Error("rebuild changed projection digest — replay is not deterministic")
	}
}

func TestActiveRunIgnoresLaterCompile(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)

	blocking := make(chan struct{})
	release := make(chan struct{})
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "a" {
				<-release
			}
			return &executor.Result{}, nil
		}),
	}
	c := newController(t, st, pool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		<-blocking
	}()
	_ = blocking

	modified := linear2Graph()
	frozen2 := compileAndFreeze(t, st, modified)
	if frozen2.GraphVersionID == frozen.GraphVersionID {
		t.Fatal("modified definition must produce a new version")
	}

	var digest string
	if err := st.DB().QueryRow("SELECT definition_digest FROM graph_run WHERE id = ?", runID).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if digest != frozen.Digest {
		t.Errorf("active run digest = %q, want the original %q", digest, frozen.Digest)
	}
	close(release)
}

func linear2Graph() string {
	return `schema: proceed/v1
name: linear
nodes:
  - id: a
    type: task
    executor: { kind: shell, command: [bin/a] }
    contract: pure
  - id: b
    type: task
    executor: { kind: shell, command: [bin/b] }
    contract: pure
    terminal: true
  - id: c
    type: task
    executor: { kind: shell, command: [bin/c] }
    contract: pure
    terminal: true
edges:
  - { from: a, to: b, type: depends_on }
  - { from: b, to: c, type: depends_on }
`
}
