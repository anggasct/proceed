package controller

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"proceed/internal/compiler"
	"proceed/internal/executor"
	"proceed/internal/store"
)

func compileAndFreeze(t *testing.T, st *store.Store, src string) store.FrozenVersion {
	t.Helper()
	doc, err := compiler.Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(context.Background(), "test.yaml", []byte(src), doc)
	if err != nil {
		t.Fatal(err)
	}
	return frozen
}

func newController(t *testing.T, st *store.Store, pool map[executor.Kind]executor.Executor) *Controller {
	t.Helper()
	c, err := New(st, DefaultConfig(), pool)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const linearGraph = `schema: proceed/v1
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
edges:
  - { from: a, to: b, type: depends_on }
`

func TestHappyPathLinearRun(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, linearGraph)

	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{Output: map[string]any{"node": req.NodeKey}}, nil
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

	var status string
	if err := st.DB().QueryRow("SELECT status FROM graph_run WHERE id = ?", runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("run status = %q, want completed", status)
	}

	var succeeded int
	if err := st.DB().QueryRow(
		"SELECT COUNT(*) FROM run_node WHERE run_id = ? AND status = 'succeeded'", runID).Scan(&succeeded); err != nil {
		t.Fatal(err)
	}
	if succeeded != 2 {
		t.Errorf("succeeded nodes = %d, want 2", succeeded)
	}

	var traversals int
	if err := st.DB().QueryRow(
		"SELECT COUNT(*) FROM run_edge WHERE run_id = ?", runID).Scan(&traversals); err != nil {
		t.Fatal(err)
	}
	if traversals != 1 {
		t.Errorf("traversed edges = %d, want 1", traversals)
	}

	events, err := st.Events(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	var lastSeq int64
	seen := map[string]int{}
	for _, ev := range events {
		if ev.Sequence <= lastSeq {
			t.Fatalf("event sequence not monotonic: %d after %d", ev.Sequence, lastSeq)
		}
		lastSeq = ev.Sequence
		seen[ev.Type]++
	}
	for _, want := range []string{"run_started", "node_started", "node_finished", "edge_traversed", "run_completed"} {
		if seen[want] == 0 {
			t.Errorf("missing %s event (seen: %v)", want, seen)
		}
	}
	if seen["run_completed"] != 1 {
		t.Errorf("run_completed events = %d, want 1", seen["run_completed"])
	}
}

func TestOperationKeyDeterminism(t *testing.T) {
	a1 := OperationKey("run1", "digest1", "nodeA", 1)
	a2 := OperationKey("run1", "digest1", "nodeA", 1)
	if a1 != a2 {
		t.Error("operation key must be deterministic for the same logical attempt")
	}
	if OperationKey("run1", "digest1", "nodeA", 2) == a1 {
		t.Error("operation key must differ across attempts")
	}
	if OperationKey("run1", "digest1", "nodeB", 1) == a1 {
		t.Error("operation key must differ across nodes")
	}
	if OperationKey("run2", "digest1", "nodeA", 1) == a1 {
		t.Error("operation key must differ across runs")
	}
	if OperationKey("run1", "digest2", "nodeA", 1) == a1 {
		t.Error("operation key must differ across digests")
	}
}

func TestSecondControllerStoreBusy(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	}
	c1 := newController(t, st, pool)
	frozen := compileAndFreeze(t, st, linearGraph)
	runID, err := c1.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	_ = runID

	c2 := newController(t, st, pool)
	_, err = c2.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if store.ErrorCode(err) != store.CodeStoreBusy {
		t.Fatalf("error = %v, want STORE_BUSY", err)
	}
}

func TestRunUnknownGraphVersion(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			return &executor.Result{}, nil
		}),
	}
	c := newController(t, st, pool)
	_, err = c.Run(ctx, RunInput{GraphVersionID: "missing"})
	if store.ErrorCode(err) != store.CodeGraphInvalid {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
}

var _ = os.Getenv
var _ = sql.ErrNoRows
