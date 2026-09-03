package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"proceed/internal/executor"
)

const reconcilableGraph = `schema: proceed/v1
name: recon
nodes:
  - id: side
    type: tool
    executor: { kind: http, method: POST, url: "https://api.example/do" }
    contract: reconcilable
    terminal: true
    capability:
      network: { allowlisted_hosts: [api.example] }
edges: []
`

func TestReconcileUsesPersistedOperationKey(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, reconcilableGraph)

	var executed atomic.Int64
	var confirmedKeys sync.Map
	pool := map[executor.Kind]executor.Executor{
		"http": executor.NewFuncExecutor("http", executor.Reconcilable, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			executed.Add(1)
			confirmedKeys.Store(req.OperationKey, true)
			return nil, executor.ErrUncertain
		}),
	}
	cfg := DefaultConfig()
	cfg.LeaseTTL = 40 * time.Millisecond
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
	c.releaseLease(ctx)
	time.Sleep(60 * time.Millisecond)

	if nodeStatus(t, st, runID, "side") != "uncertain" {
		t.Fatalf("node = %q, want uncertain", nodeStatus(t, st, runID, "side"))
	}

	var reconcileCalls atomic.Int64
	reconPool := map[executor.Kind]executor.Executor{
		"http": &reconcileByKeyExecutor{
			confirmed: &confirmedKeys,
			calls:     &reconcileCalls,
			extraExec: &executed,
		},
	}
	recovered := newController(t, st, reconPool)
	if err := recovered.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if nodeStatus(t, st, runID, "side") != "succeeded" {
		t.Fatalf("node = %q, want succeeded after keyed reconcile", nodeStatus(t, st, runID, "side"))
	}
	if reconcileCalls.Load() == 0 {
		t.Error("reconcile never invoked")
	}
	if executed.Load() != 1 {
		t.Errorf("executions = %d, want 1 (keyed reconcile must not re-execute)", executed.Load())
	}
	assertReplayStable(t, st)
}

type reconcileByKeyExecutor struct {
	confirmed *sync.Map
	calls     *atomic.Int64
	extraExec *atomic.Int64
}

func (r *reconcileByKeyExecutor) Kind() executor.Kind { return "http" }

func (r *reconcileByKeyExecutor) Execute(ctx context.Context, req *executor.Request) (*executor.Result, error) {
	r.extraExec.Add(1)
	return &executor.Result{}, nil
}

func (r *reconcileByKeyExecutor) Reconcile(ctx context.Context, req *executor.Request) (executor.EffectState, error) {
	r.calls.Add(1)
	if req.OperationKey == "" {
		return executor.EffectUnknown, nil
	}
	if _, ok := r.confirmed.Load(req.OperationKey); ok {
		return executor.EffectConfirmed, nil
	}
	return executor.EffectAbsent, nil
}

func TestCancelRunWithoutDrainFinalizesRun(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)

	block := make(chan struct{})
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			<-block
			return &executor.Result{}, nil
		}),
	}
	c := newController(t, st, pool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.CancelRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := runStatus(t, st, runID); s != "cancelled" {
		t.Fatalf("run = %q, want cancelled without Drain", s)
	}
	events, err := st.Events(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	var runCancelled bool
	for _, ev := range events {
		if ev.Type == "run_cancelled" {
			runCancelled = true
		}
	}
	if !runCancelled {
		t.Error("run_cancelled event missing")
	}
	close(block)
	assertReplayStable(t, st)
}

func TestConcurrentStepsDoNotDoubleExecute(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)

	var executions atomic.Int64
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			executions.Add(1)
			time.Sleep(20 * time.Millisecond)
			return &executor.Result{}, nil
		}),
	}
	c := newController(t, st, pool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Step(ctx, runID)
		}()
	}
	wg.Wait()
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if n := executions.Load(); n != 2 {
		t.Errorf("executions = %d, want 2 (each node exactly once)", n)
	}
	assertReplayStable(t, st)
}

func TestFanOutBranchesOverlap(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, fanOutGraph)

	var running atomic.Int64
	var maxOverlap atomic.Int64
	overlapCh := make(chan struct{})
	var closeOnce sync.Once
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			cur := running.Add(1)
			for {
				old := maxOverlap.Load()
				if cur <= old || maxOverlap.CompareAndSwap(old, cur) {
					break
				}
			}
			if req.NodeKey == "left" || req.NodeKey == "right" {
				if cur >= 2 {
					closeOnce.Do(func() { close(overlapCh) })
				} else {
					select {
					case <-overlapCh:
					case <-time.After(2 * time.Second):
					}
				}
			}
			running.Add(-1)
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
	if s := runStatus(t, st, runID); s != "completed" {
		t.Fatalf("run = %q, want completed", s)
	}
	if maxOverlap.Load() < 2 {
		t.Errorf("max overlap = %d, want >= 2 (independent branches must run in parallel)", maxOverlap.Load())
	}
	assertReplayStable(t, st)
}
