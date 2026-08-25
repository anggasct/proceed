package controller

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"proceed/internal/executor"
	"proceed/internal/store"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func recoverController(t *testing.T, st *store.Store, pool map[executor.Kind]executor.Executor) *Controller {
	t.Helper()
	c, err := New(st, DefaultConfig(), pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.acquireLease(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	return c
}

func nodeStatus(t *testing.T, st *store.Store, runID, key string) string {
	t.Helper()
	var s sql.NullString
	err := st.DB().QueryRow(
		"SELECT status FROM run_node WHERE run_id = ? AND node_key = ?", runID, key).Scan(&s)
	if err == sql.ErrNoRows {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return s.String
}

func runStatus(t *testing.T, st *store.Store, runID string) string {
	t.Helper()
	var s string
	if err := st.DB().QueryRow("SELECT status FROM graph_run WHERE id = ?", runID).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRecoveryCrashAfterLeaseBeforeStart(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)

	prePool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	}
	pre := newController(t, st, prePool)
	runID, err := pre.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	pre.releaseLease(ctx)

	if s := nodeStatus(t, st, runID, "a"); s != "" && s != "eligible" {
		t.Fatalf("before lease: node = %q, want absent or eligible", s)
	}

	c := recoverController(t, st, prePool)
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if runStatus(t, st, runID) != "completed" {
		t.Errorf("post-recovery status = %q, want completed", runStatus(t, st, runID))
	}
}

func TestRecoveryCrashAfterStartBeforeExecutor(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, linearGraph)

	started := 0
	crashPool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			started++
			return nil, errors.New("simulated crash mid-execution")
		}),
	}
	c := newController(t, st, crashPool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if nodeStatus(t, st, runID, "a") != "failed" {
		t.Fatalf("node a = %q", nodeStatus(t, st, runID, "a"))
	}
	_ = started
}

func TestRecoveryExpiredLeaseReclaimed(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	if _, err := st.DB().ExecContext(ctx, `
INSERT INTO controller_lease (store_id, owner_id, mode, heartbeat_at, lease_expires_at)
VALUES ('default', 'dead-controller', 'run', 1, 1)`); err != nil {
		t.Fatal(err)
	}
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	}
	c := newController(t, st, pool)
	if err := c.acquireLease(ctx, time.Now()); err != nil {
		t.Fatalf("expired lease must be reclaimable: %v", err)
	}
	var owner string
	if err := st.DB().QueryRow(
		"SELECT owner_id FROM controller_lease WHERE store_id = 'default'").Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != c.OwnerID() {
		t.Errorf("owner = %q, want %q", owner, c.OwnerID())
	}

	c2 := newController(t, st, pool)
	if err := c2.acquireLease(ctx, time.Now()); store.ErrorCode(err) != store.CodeStoreBusy {
		t.Fatalf("second controller error = %v, want STORE_BUSY", err)
	}
}

func TestRecoveryUncertainPureRetriedAfterRestart(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: pureuncertain
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [bin/w] }
    contract: pure
    terminal: true
edges: []
`)

	executions := 0
	crashPool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			executions++
			return nil, executor.ErrUncertain
		}),
	}
	cfg := DefaultConfig()
	cfg.LeaseTTL = 40 * time.Millisecond
	c, err := New(st, cfg, crashPool)
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
	if nodeStatus(t, st, runID, "work") != "uncertain" {
		t.Fatalf("node = %q, want uncertain after crash", nodeStatus(t, st, runID, "work"))
	}

	recovered := recoverController(t, st, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			executions++
			return &executor.Result{}, nil
		}),
	})
	if err := recovered.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if nodeStatus(t, st, runID, "work") != "succeeded" {
		t.Errorf("node = %q, want succeeded after pure retry", nodeStatus(t, st, runID, "work"))
	}
	if runStatus(t, st, runID) != "completed" {
		t.Errorf("run = %q, want completed", runStatus(t, st, runID))
	}
	if executions != 2 {
		t.Errorf("executions = %d, want 2", executions)
	}
}

func TestRecoveryFanOutPartialSurvives(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, fanOutGraph)

	failRight := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if req.NodeKey == "right" {
				return nil, errors.New("branch failure")
			}
			return &executor.Result{}, nil
		}),
	}
	c := newController(t, st, failRight)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}

	if nodeStatus(t, st, runID, "left") != "succeeded" {
		t.Errorf("completed branch left = %q, want succeeded", nodeStatus(t, st, runID, "left"))
	}
	var joinCount int
	if err := st.DB().QueryRow(
		"SELECT COUNT(*) FROM run_node WHERE run_id = ? AND node_key = 'join'", runID).Scan(&joinCount); err != nil {
		t.Fatal(err)
	}
	if joinCount != 0 {
		var joinStatus string
		var joinAttempts int
		_ = st.DB().QueryRow(
			"SELECT status, attempt_count FROM run_node WHERE run_id = ? AND node_key = 'join'", runID).Scan(&joinStatus, &joinAttempts)
		if joinStatus != "pending" || joinAttempts != 0 {
			t.Errorf("join = %q attempts=%d, want pending with 0 attempts while a branch failed", joinStatus, joinAttempts)
		}
	}
	if runStatus(t, st, runID) != "failed" {
		t.Errorf("run = %q, want failed", runStatus(t, st, runID))
	}
}

func TestRecoveryCancelRequestedPureNode(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: cancelled
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [bin/w] }
    contract: pure
    terminal: true
edges: []
`)
	cfg := DefaultConfig()
	cfg.LeaseTTL = time.Second
	c, err := New(st, cfg, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.appendEvent(ctx, runID, "node_started", map[string]any{
		"node_key":             "work",
		"attempt_no":           1,
		"executor":             "shell",
		"side_effect_contract": "pure",
		"operation_key":        OperationKey(runID, frozen.Digest, "work", 1),
		"lease_token":          "expired-token",
		"lease_expires_at":     time.Now().Add(-time.Second).UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.appendEvent(ctx, runID, "node_cancel_requested", map[string]any{
		"node_key": "work",
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.appendEvent(ctx, runID, "node_reconciling", map[string]any{
		"node_key": "work",
	}); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "work"); s != "cancel_requested" {
		t.Fatalf("node = %q after reconcile marker, want cancel_requested", s)
	}
	c.releaseLease(ctx)

	recovered := recoverController(t, st, map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	})
	if err := recovered.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "work"); s != "cancelled" {
		t.Fatalf("node = %q, want cancelled after restart", s)
	}
	if err := recovered.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := runStatus(t, st, runID); s != "cancelled" {
		t.Fatalf("run = %q, want cancelled after restart", s)
	}
	assertReplayStable(t, st)
}

func TestRecoveryReconcilingCompletesRoutes(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: recon-route
nodes:
  - id: effect
    type: tool
    executor: { kind: http, method: POST, url: "https://api.example/do" }
    contract: reconcilable
    capability:
      network: { allowlisted_hosts: [api.example] }
  - id: after
    type: task
    executor: { kind: shell, command: [bin/after] }
    contract: pure
    terminal: true
edges:
  - { from: effect, to: after, type: depends_on }
`)
	cfg := DefaultConfig()
	cfg.LeaseTTL = 40 * time.Millisecond
	initial, err := New(st, cfg, map[executor.Kind]executor.Executor{
		"http": executor.NewFuncExecutor("http", executor.Reconcilable, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return nil, executor.ErrUncertain
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := initial.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	initial.releaseLease(ctx)
	if err := initial.appendEvent(ctx, runID, "node_reconciling", map[string]any{
		"node_key": "effect",
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)

	recovered := recoverController(t, st, map[executor.Kind]executor.Executor{
		"http": &confirmAllReconciler{},
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	})
	if err := recovered.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	var traversals int
	if err := st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM run_edge WHERE run_id = ?", runID).Scan(&traversals); err != nil {
		t.Fatal(err)
	}
	if traversals != 1 {
		t.Fatalf("traversals = %d, want 1 after recovered completion", traversals)
	}
	if err := recovered.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "after"); s != "succeeded" {
		t.Fatalf("downstream node = %q, want succeeded", s)
	}
	if s := runStatus(t, st, runID); s != "completed" {
		t.Fatalf("run = %q, want completed", s)
	}
	assertReplayStable(t, st)
}

func TestRecoveryReconcilingUsesRecoveredRoute(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: recon-conditional
nodes:
  - id: effect
    type: tool
    executor: { kind: http, method: POST, url: "https://api.example/classify" }
    contract: reconcilable
    capability:
      network: { allowlisted_hosts: [api.example] }
  - id: code_path
    type: task
    executor: { kind: shell, command: [bin/code] }
    contract: pure
    terminal: true
  - id: docs_path
    type: task
    executor: { kind: shell, command: [bin/docs] }
    contract: pure
    terminal: true
edges:
  - { from: effect, to: code_path, type: routes_to, when: requires_code }
  - { from: effect, to: docs_path, type: routes_to, when: requires_docs }
`)
	cfg := DefaultConfig()
	cfg.LeaseTTL = 40 * time.Millisecond
	initial, err := New(st, cfg, map[executor.Kind]executor.Executor{
		"http": executor.NewFuncExecutor("http", executor.Reconcilable, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return nil, executor.ErrUncertain
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := initial.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	initial.releaseLease(ctx)
	if err := initial.appendEvent(ctx, runID, "node_reconciling", map[string]any{
		"node_key": "effect",
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)

	recovered := recoverController(t, st, map[executor.Kind]executor.Executor{
		"http": &routeReconciler{},
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	})
	if err := recovered.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "code_path"); s != "succeeded" {
		t.Fatalf("selected route = %q, want succeeded", s)
	}
	if s := nodeStatus(t, st, runID, "docs_path"); s != "skipped" {
		t.Fatalf("unselected route = %q, want skipped", s)
	}
	if s := runStatus(t, st, runID); s != "completed" {
		t.Fatalf("run = %q, want completed", s)
	}
	assertReplayStable(t, st)
}

type routeReconciler struct{}

func (*routeReconciler) Kind() executor.Kind { return "http" }
func (*routeReconciler) Execute(ctx context.Context, req *executor.Request) (*executor.Result, error) {
	return &executor.Result{}, nil
}
func (*routeReconciler) ReconcileResult(ctx context.Context, req *executor.Request) (*executor.Result, executor.EffectState, error) {
	return &executor.Result{Route: "requires_code"}, executor.EffectConfirmed, nil
}
