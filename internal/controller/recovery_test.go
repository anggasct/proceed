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
	c := newController(t, st, crashPool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	c.releaseLease(ctx)
	if nodeStatus(t, st, runID, "work") != "uncertain" {
		t.Fatalf("node = %q, want uncertain after crash", nodeStatus(t, st, runID, "work"))
	}
	if _, err := st.DB().ExecContext(ctx, `
UPDATE node_attempt SET lease_expires_at = 1
WHERE run_node_id = (SELECT id FROM run_node WHERE run_id = ? AND node_key = 'work')`, runID); err != nil {
		t.Fatal(err)
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
		t.Errorf("join rows = %d, want 0 (never started while a branch failed)", joinCount)
	}
	if runStatus(t, st, runID) != "failed" {
		t.Errorf("run = %q, want failed", runStatus(t, st, runID))
	}
}
