package controller

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"proceed/internal/executor"
)

func TestRecoveryIdempotentResumesPersistedOperationKey(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: idem
nodes:
  - id: effect
    type: tool
    executor: { kind: http, method: POST, url: "https://api.example/do" }
    contract: idempotent
    terminal: true
    capability:
      network: { allowlisted_hosts: [api.example] }
edges: []
`)

	cfg := DefaultConfig()
	cfg.LeaseTTL = 40 * time.Millisecond
	var mu sync.Mutex
	keys := map[string]int{}
	pool := map[executor.Kind]executor.Executor{
		"http": executor.NewFuncExecutor("http", executor.Idempotent, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			mu.Lock()
			keys[req.OperationKey]++
			mu.Unlock()
			return nil, executor.ErrUncertain
		}),
	}
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

	seen := map[string]int{}
	recovered := newController(t, st, map[executor.Kind]executor.Executor{
		"http": executor.NewFuncExecutor("http", executor.Idempotent, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			mu.Lock()
			seen[req.OperationKey]++
			mu.Unlock()
			return &executor.Result{}, nil
		}),
	})
	if err := recovered.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}

	if nodeStatus(t, st, runID, "effect") != "succeeded" {
		t.Fatalf("node = %q, want succeeded", nodeStatus(t, st, runID, "effect"))
	}
	for k, v := range keys {
		if seen[k] == 0 {
			t.Errorf("recovery used a new operation key; persisted key %s… was not reused", k[:10])
		}
		_ = v
	}
	if len(seen) != len(keys) || len(seen) != 1 {
		t.Errorf("distinct operation keys: first=%d resumed=%d, want 1", len(keys), len(seen))
	}
	assertReplayStable(t, st)
}

func TestRecoveryMatrixEffectBeforeReceipt(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: nonreplay2
nodes:
  - id: charge
    type: tool
    executor: { kind: http, method: POST, url: "https://api.example/charge" }
    contract: non_replayable
    terminal: true
    capability:
      network: { allowlisted_hosts: [api.example] }
edges: []
`)
	cfg := DefaultConfig()
	cfg.LeaseTTL = 40 * time.Millisecond
	pool := map[executor.Kind]executor.Executor{
		"http": executor.NewFuncExecutor("http", executor.NonReplayable, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return nil, executor.ErrUncertain
		}),
	}
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

	recovered := newController(t, st, pool)
	if err := recovered.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "charge"); s != "waiting" {
		t.Errorf("node = %q, want waiting (escalated, no blind retry)", s)
	}
	var attempts int
	if err := st.DB().QueryRowContext(ctx, `
SELECT COUNT(*) FROM node_attempt na JOIN run_node rn ON rn.id = na.run_node_id
WHERE rn.run_id = ? AND rn.node_key = 'charge'`, runID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (no second execution)", attempts)
	}
	assertReplayStable(t, st)
}

func TestRecoveryMatrixReceiptBeforeFinish(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, reconcilableGraph)
	cfg := DefaultConfig()
	cfg.LeaseTTL = 40 * time.Millisecond

	pool := map[executor.Kind]executor.Executor{
		"http": executor.NewFuncExecutor("http", executor.Reconcilable, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return nil, executor.ErrUncertain
		}),
	}
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

	reconPool := map[executor.Kind]executor.Executor{
		"http": &confirmAllReconciler{},
	}
	recovered := newController(t, st, reconPool)
	if err := recovered.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "side"); s != "succeeded" {
		t.Errorf("node = %q, want succeeded (receipt authoritative — completes idempotently)", s)
	}
	assertReplayStable(t, st)
}

type confirmAllReconciler struct{}

func (*confirmAllReconciler) Kind() executor.Kind { return "http" }
func (*confirmAllReconciler) Execute(ctx context.Context, req *executor.Request) (*executor.Result, error) {
	return &executor.Result{}, nil
}
func (*confirmAllReconciler) Reconcile(ctx context.Context, req *executor.Request) (executor.EffectState, error) {
	return executor.EffectConfirmed, nil
}

func TestRecoveryMatrixHumanWaitPreserved(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: gated
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [bin/w] }
    contract: pure
  - id: approval
    type: gate
edges:
  - { from: work, to: approval, type: routes_to, when: done }
`)
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{Route: "done"}, nil
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
	c.releaseLease(ctx)

	if s := nodeStatus(t, st, runID, "approval"); s != "waiting" {
		t.Fatalf("gate = %q, want waiting", s)
	}
	recovered := newController(t, st, pool)
	if err := recovered.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "approval"); s != "waiting" {
		t.Errorf("gate = %q after restart, want waiting preserved (no rerun of work)", s)
	}
	if s := nodeStatus(t, st, runID, "work"); s != "succeeded" {
		t.Errorf("work = %q, want succeeded (never rerun)", s)
	}
	assertReplayStable(t, st)
}

func TestRecoveryMatrixSQLiteContention(t *testing.T) {
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

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; i < 50; i++ {
			_, _ = st.DB().ExecContext(ctx, `
INSERT INTO event (event_id, run_id, sequence, schema_version, type, occurred_at, recorded_at,
                   actor_type, actor_id, idempotency_key, payload_digest, payload)
VALUES (?, 'contention-probe', 1, 'proceed/v1', 'probe', ?, ?, 'controller', 'probe', ?, '{}', '{}')`,
				"probe-"+time.Now().Format("150405.000000000")+"-"+jsonSafe(i), time.Now().UnixMilli(), time.Now().UnixMilli(),
				"probe-key-"+time.Now().Format("150405.000000000")+"-"+jsonSafe(i))
		}
	}()

	if err := c.Drain(ctx, runID); err != nil {
		t.Fatalf("drain under concurrent writer failed: %v", err)
	}
	<-writerDone
	if s := runStatus(t, st, runID); s != "completed" {
		t.Errorf("run = %q, want completed under contention", s)
	}
	assertReplayStable(t, st)
}

func jsonSafe(i int) string {
	b, _ := json.Marshal(i)
	return string(b)
}

func TestDefinitionDigestImmutabilityDuringRun(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)

	block := make(chan struct{})
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "a" {
				<-block
			}
			return &executor.Result{}, nil
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
		var n int
		if err := st.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM node_attempt").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	frozen2 := compileAndFreeze(t, st, linear2Graph())
	if frozen2.Digest == frozen.Digest {
		t.Fatal("recompiled definition must differ")
	}

	var runDigest string
	if err := st.DB().QueryRowContext(ctx,
		"SELECT definition_digest FROM graph_run WHERE id = ?", runID).Scan(&runDigest); err != nil {
		t.Fatal(err)
	}
	if runDigest != frozen.Digest {
		t.Errorf("active run digest = %q, want original %q", runDigest, frozen.Digest)
	}

	close(block)
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	if s := runStatus(t, st, runID); s != "completed" {
		t.Errorf("run = %q, want completed on the original definition", s)
	}
	var nodeCount int
	if err := st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM run_node WHERE run_id = ?", runID).Scan(&nodeCount); err != nil {
		t.Fatal(err)
	}
	if nodeCount != 2 {
		t.Errorf("nodes materialized = %d, want 2 (new version's node c must not appear)", nodeCount)
	}
	assertReplayStable(t, st)
}

func TestCancelPreservesTerminalHistory(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, fanOutGraph)

	block := make(chan struct{})
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "source" {
				return &executor.Result{}, nil
			}
			<-block
			return &executor.Result{}, nil
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
		if s := nodeStatus(t, st, runID, "source"); s == "succeeded" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if err := c.CancelRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "source"); s != "succeeded" {
		t.Errorf("completed sibling rewritten to %q — terminal history must be preserved", s)
	}
	close(block)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain never returned")
	}
	assertReplayStable(t, st)
}

func TestConcurrentTerminalizationSingleEvent(t *testing.T) {
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

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- c.tryCompleteRun(ctx, runID, frozen.GraphVersionID)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}

	var terminals int
	if err := st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM event WHERE run_id = ? AND type IN ('run_completed','run_failed','run_cancelled')",
		runID).Scan(&terminals); err != nil {
		t.Fatal(err)
	}
	if terminals != 1 {
		t.Errorf("terminal run events = %d, want exactly 1", terminals)
	}
	assertReplayStable(t, st)
}
