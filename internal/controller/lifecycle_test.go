package controller

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"proceed/internal/executor"
	"proceed/internal/store"
)

const fanOutGraph = `schema: proceed/v1
name: fanout
nodes:
  - id: source
    type: task
    executor: { kind: shell, command: [bin/s] }
    contract: pure
  - id: left
    type: task
    executor: { kind: shell, command: [bin/l] }
    contract: pure
  - id: right
    type: task
    executor: { kind: shell, command: [bin/r] }
    contract: pure
  - id: join
    type: task
    executor: { kind: shell, command: [bin/j] }
    contract: pure
    terminal: true
edges:
  - { from: source, to: left, type: depends_on }
  - { from: source, to: right, type: depends_on }
  - { from: left, to: join, type: depends_on }
  - { from: right, to: join, type: depends_on }
`

func TestParallelFanOutJoin(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
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
	var status string
	if err := st.DB().QueryRow("SELECT status FROM graph_run WHERE id = ?", runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("status = %q, want completed", status)
	}
	var joinStatus string
	if err := st.DB().QueryRow(
		"SELECT status FROM run_node WHERE run_id = ? AND node_key = 'join'", runID).Scan(&joinStatus); err != nil {
		t.Fatal(err)
	}
	if joinStatus != "succeeded" {
		t.Errorf("join = %q, want succeeded (join must wait for both branches)", joinStatus)
	}
}

const retryGraph = `schema: proceed/v1
name: retry
nodes:
  - id: flaky
    type: task
    executor: { kind: shell, command: [bin/f] }
    contract: idempotent
    retry: { max_attempts: 3, backoff_ms: 0 }
    terminal: true
edges: []
`

func TestRetryBudgetExhaustionFails(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, retryGraph)

	calls := 0
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Idempotent, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			calls++
			return nil, errors.New("boom")
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

	if calls != 3 {
		t.Errorf("executor calls = %d, want 3 (max_attempts)", calls)
	}
	var runStatus, nodeStatus string
	if err := st.DB().QueryRow("SELECT status FROM graph_run WHERE id = ?", runID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(
		"SELECT status FROM run_node WHERE run_id = ? AND node_key = 'flaky'", runID).Scan(&nodeStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "failed" || nodeStatus != "failed" {
		t.Errorf("run/node = %s/%s, want failed/failed", runStatus, nodeStatus)
	}
	var attempts int
	if err := st.DB().QueryRow(
		"SELECT COUNT(*) FROM node_attempt WHERE run_node_id = (SELECT id FROM run_node WHERE run_id = ? AND node_key = 'flaky')",
		runID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Errorf("node_attempt rows = %d, want 3 (history preserved)", attempts)
	}
}

func TestRetrySucceedsOnLaterAttempt(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, retryGraph)

	calls := 0
	pool := map[executor.Kind]executor.Executor{
		"shell": executor.NewFuncExecutor("shell", executor.Idempotent, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			calls++
			if calls < 2 {
				return nil, errors.New("transient")
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
	var status string
	if err := st.DB().QueryRow("SELECT status FROM graph_run WHERE id = ?", runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("status = %q, want completed after retry", status)
	}
}

const routingGraph = `schema: proceed/v1
name: routing
nodes:
  - id: classify
    type: model
    executor: { kind: shell, command: [bin/c] }
    contract: pure
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
  - { from: classify, to: code_path, type: routes_to, when: requires_code }
  - { from: classify, to: docs_path, type: routes_to, when: requires_docs }
`

func TestConditionalRouting(t *testing.T) {
	for _, tc := range []struct {
		route string
		want  string
	}{
		{"requires_code", "code_path"},
		{"requires_docs", "docs_path"},
	} {
		t.Run(tc.route, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			frozen := compileAndFreeze(t, st, routingGraph)
			pool := map[executor.Kind]executor.Executor{
				"shell": executor.NewFuncExecutor("shell", executor.Pure, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
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
			var taken string
			err = st.DB().QueryRow(`
SELECT gn.node_key FROM run_edge re
JOIN graph_edge ge ON ge.id = re.edge_id
JOIN graph_node gn ON gn.node_key = ge.to_node_key AND gn.graph_version_id = ge.graph_version_id
WHERE re.run_id = ?`, runID).Scan(&taken)
			if err != nil {
				t.Fatal(err)
			}
			if taken != tc.want {
				t.Errorf("routed to %q, want %q", taken, tc.want)
			}
			var status string
			if err := st.DB().QueryRow("SELECT status FROM graph_run WHERE id = ?", runID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != "completed" {
				t.Errorf("status = %q, want completed (unselected branch skipped)", status)
			}
		})
	}
}

func TestUncertainNonReplayableNotRetried(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: nonreplay
nodes:
  - id: pay
    type: tool
    executor: { kind: http, method: POST, url: "https://api.example/charge" }
    contract: non_replayable
    terminal: true
    capability:
      network: { allowlisted_hosts: [api.example] }
edges: []
`)

	calls := 0
	pool := map[executor.Kind]executor.Executor{
		"http": executor.NewFuncExecutor("http", executor.NonReplayable, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			calls++
			return nil, executor.ErrUncertain
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

	if calls != 1 {
		t.Errorf("executor calls = %d, want 1 (uncertain must not be retried)", calls)
	}
	var nodeStatus string
	if err := st.DB().QueryRow(
		"SELECT status FROM run_node WHERE run_id = ? AND node_key = 'pay'", runID).Scan(&nodeStatus); err != nil {
		t.Fatal(err)
	}
	if nodeStatus != "uncertain" {
		t.Fatalf("node status = %q, want uncertain", nodeStatus)
	}
	var runStatus string
	if err := st.DB().QueryRow("SELECT status FROM graph_run WHERE id = ?", runID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus == "completed" {
		t.Error("run with an uncertain node must not complete")
	}
	events, err := st.Events(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	hasUncertain := false
	for _, ev := range events {
		if ev.Type == "node_uncertain" {
			hasUncertain = true
		}
	}
	if !hasUncertain {
		t.Error("node_uncertain event missing")
	}
}
