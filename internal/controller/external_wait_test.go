package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/compiler"
	"proceed/internal/executor"
	"proceed/internal/store"
)

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func openTestStoreWithGraph(t *testing.T, graphYAML string) (*store.Store, string, string) {
	t.Helper()
	s := openTestStore(t)
	src := []byte(graphYAML)
	doc := compileFixture(t, src)
	frozen, err := s.FreezeDefinition(context.Background(), "test.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.CreateRun(context.Background(), frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}
	return s, frozen.GraphVersionID, run.ID
}

func newTestController(t *testing.T, st *store.Store, pool map[executor.Kind]executor.Executor, ownerID string) *Controller {
	t.Helper()
	ctrl, err := New(st, testConfig(ownerID), pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.acquireLease(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	return ctrl
}

// Registration, matching completion, and duplicate idempotency
func TestExternalWaitRegistrationAndCompletion(t *testing.T) {
	ctx := context.Background()
	graphYAML := `schema: proceed/v1
name: test-external-wait
nodes:
  - id: create_pr
    type: task
    contract: pure
    executor:
      kind: shell
      command: ["echo", "pr_created"]
  - id: wait_ci
    type: task
    contract: pure
    executor:
      kind: shell
      command: ["echo", "waiting"]
  - id: merge_pr
    type: task
    contract: pure
    terminal: true
    executor:
      kind: shell
      command: ["echo", "pr_merged"]
edges:
  - { from: create_pr, to: wait_ci, type: depends_on }
  - { from: wait_ci, to: merge_pr, type: routes_to, when: success }
`
	st, _, runID := openTestStoreWithGraph(t, graphYAML)
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: &testShellExecutor{},
	}
	ctrl := newTestController(t, st, pool, "ctrl-1")

	// 1. Run create_pr node
	progressed, err := ctrl.Step(ctx, runID)
	if err != nil || !progressed {
		t.Fatalf("Step create_pr: progressed=%v, err=%v", progressed, err)
	}

	// Register external wait on wait_ci
	waitID := ulid.Make().String()
	corrKey := "repo=proceed/app;pr=42;head=sha256:abc1234"
	wait, err := ctrl.RegisterExternalWait(ctx, ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_ci",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		ExpiresAt:         time.Now().UnixMilli() + 60000,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatalf("RegisterExternalWait: %v", err)
	}
	if wait.Status != "pending" || wait.ID != waitID {
		t.Errorf("wait = %+v, want pending with ID %s", wait, waitID)
	}

	// Verify node is waiting in store
	var nodeStatus string
	if err := st.DB().QueryRow("SELECT status FROM run_node WHERE run_id = ? AND node_key = 'wait_ci'", runID).Scan(&nodeStatus); err != nil {
		t.Fatal(err)
	}
	if nodeStatus != "waiting" {
		t.Errorf("nodeStatus = %q, want waiting", nodeStatus)
	}

	// Stepping the controller while waiting makes no progress (controller does not block/busy loop)
	progressed, err = ctrl.Step(ctx, runID)
	if err != nil {
		t.Fatalf("Step while waiting: %v", err)
	}
	if progressed {
		t.Errorf("Step while waiting should not progress")
	}

	// Complete the wait with matching correlation key and event
	payload := `{"check_run_id":12345,"conclusion":"success"}`
	payloadDigest := "sha256:" + hexDigest(payload)
	res, err := ctrl.CompleteExternalWait(ctx, CompleteWaitRequest{
		WaitID:          waitID,
		ProviderEventID: "github:check_run:12345",
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  corrKey,
		OccurredAt:      time.Now().UnixMilli(),
		Status:          "success",
		PayloadDigest:   payloadDigest,
		Payload:         json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("CompleteExternalWait: %v", err)
	}
	if res.Code != "WAIT_COMPLETED" || res.HTTPStatus != http.StatusAccepted {
		t.Errorf("res = %+v, want WAIT_COMPLETED (202)", res)
	}

	// Repeating the same provider_event_id is idempotent (WAIT_ALREADY_COMPLETED)
	dupRes, err := ctrl.CompleteExternalWait(ctx, CompleteWaitRequest{
		WaitID:          waitID,
		ProviderEventID: "github:check_run:12345",
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  corrKey,
		OccurredAt:      time.Now().UnixMilli(),
		Status:          "success",
		PayloadDigest:   payloadDigest,
		Payload:         json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("Duplicate CompleteExternalWait: %v", err)
	}
	if dupRes.Code != "WAIT_ALREADY_COMPLETED" || dupRes.HTTPStatus != http.StatusOK {
		t.Errorf("dupRes = %+v, want WAIT_ALREADY_COMPLETED (200)", dupRes)
	}

	// Now step should run merge_pr and complete the graph!
	progressed, err = ctrl.Step(ctx, runID)
	if err != nil || !progressed {
		t.Fatalf("Step merge_pr: progressed=%v, err=%v", progressed, err)
	}
	_, _ = ctrl.Step(ctx, runID)

	// Verify run completed
	runStatus := runStatusOf(ctrl, ctx, runID)
	if runStatus != "completed" {
		t.Errorf("runStatus = %q, want completed", runStatus)
	}
}

// Crash after wait registration and restart preserve exactly one pending wait
func TestExternalWaitCrashAndRestartPreservation(t *testing.T) {
	ctx := context.Background()
	graphYAML := `schema: proceed/v1
name: test-crash-restart
nodes:
  - id: step_one
    type: task
    contract: pure
    executor:
      kind: shell
      command: ["echo", "one"]
  - id: wait_step
    type: task
    contract: pure
    executor:
      kind: shell
      command: ["echo", "wait"]
  - id: step_two
    type: task
    contract: pure
    terminal: true
    executor:
      kind: shell
      command: ["echo", "two"]
edges:
  - { from: step_one, to: wait_step, type: depends_on }
  - { from: wait_step, to: step_two, type: routes_to, when: success }
`
	st, _, runID := openTestStoreWithGraph(t, graphYAML)
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: &testShellExecutor{},
	}
	ctrl1 := newTestController(t, st, pool, "ctrl-1")

	// Execute step_one
	_, err := ctrl1.Step(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	// Register external wait
	waitID := ulid.Make().String()
	corrKey := "repo=org/app;pr=10;head=sha256:deadbeef"
	_, err = ctrl1.RegisterExternalWait(ctx, ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_step",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate crash: release lease, drop controller instance
	ctrl1.releaseLease(ctx)

	// New controller instance on restart
	ctrl2 := newTestController(t, st, pool, "ctrl-2")

	// Recover run
	if err := ctrl2.Recover(ctx, runID); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Verify pending wait is preserved
	wait, err := st.GetExternalWait(ctx, waitID)
	if err != nil || wait == nil {
		t.Fatalf("GetExternalWait after restart: %v", err)
	}
	if wait.Status != "pending" {
		t.Errorf("wait.Status = %q, want pending", wait.Status)
	}

	// Verify preceding node (step_one) was NOT re-executed (attempt count remains 1)
	var stepOneAttempts int
	if err := st.DB().QueryRow("SELECT attempt_count FROM run_node WHERE run_id = ? AND node_key = 'step_one'", runID).Scan(&stepOneAttempts); err != nil {
		t.Fatal(err)
	}
	if stepOneAttempts != 1 {
		t.Errorf("step_one attempt_count = %d, want 1", stepOneAttempts)
	}
}

// Mismatched correlation (e.g. older head SHA), unknown wait, and state conflicts
func TestExternalWaitMismatchedCorrelationAndStaleSHA(t *testing.T) {
	ctx := context.Background()
	graphYAML := `schema: proceed/v1
name: test-stale-sha
nodes:
  - id: wait_ci
    type: task
    contract: pure
    executor:
      kind: shell
      command: ["echo", "wait"]
  - id: done
    type: task
    contract: pure
    terminal: true
    executor:
      kind: shell
      command: ["echo", "done"]
edges:
  - { from: wait_ci, to: done, type: routes_to, when: success }
`
	st, _, runID := openTestStoreWithGraph(t, graphYAML)
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: &testShellExecutor{},
	}
	ctrl := newTestController(t, st, pool, "ctrl-1")

	waitID := ulid.Make().String()
	newHeadSHA := "sha256:new_sha_2222"
	oldHeadSHA := "sha256:old_sha_1111"
	_, err := ctrl.RegisterExternalWait(ctx, ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_ci",
		EventType:         "ci.completed",
		CorrelationKey:    fmt.Sprintf("repo=org/app;pr=1;head=%s", newHeadSHA),
		ExpectedCondition: `{"status":"success"}`,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1. Unknown wait identity -> 404 WAIT_NOT_FOUND
	resUnknown, err := ctrl.CompleteExternalWait(ctx, CompleteWaitRequest{
		WaitID:          "01UNKNOWN999999999999999999",
		ProviderEventID: "github:check:0",
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  fmt.Sprintf("repo=org/app;pr=1;head=%s", newHeadSHA),
		OccurredAt:      time.Now().UnixMilli(),
		Status:          "success",
		PayloadDigest:   "sha256:" + hexDigest("{}"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resUnknown.Code != "WAIT_NOT_FOUND" || resUnknown.HTTPStatus != http.StatusNotFound {
		t.Errorf("resUnknown = %+v, want WAIT_NOT_FOUND (404)", resUnknown)
	}

	// 2. Stale Head SHA completion -> 409 WAIT_CONFLICT
	resStale, err := ctrl.CompleteExternalWait(ctx, CompleteWaitRequest{
		WaitID:          waitID,
		ProviderEventID: "github:check:1",
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  fmt.Sprintf("repo=org/app;pr=1;head=%s", oldHeadSHA),
		OccurredAt:      time.Now().UnixMilli(),
		Status:          "success",
		PayloadDigest:   "sha256:" + hexDigest("{}"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resStale.Code != "WAIT_CONFLICT" || resStale.HTTPStatus != http.StatusConflict {
		t.Errorf("resStale = %+v, want WAIT_CONFLICT (409)", resStale)
	}

	// Verify graph did NOT advance; wait remains pending
	w, _ := st.GetExternalWait(ctx, waitID)
	if w.Status != "pending" {
		t.Errorf("w.Status = %q, want pending", w.Status)
	}
}

// Expiry and late completion rejection
func TestExternalWaitExpiryAndLateEvent(t *testing.T) {
	ctx := context.Background()
	graphYAML := `schema: proceed/v1
name: test-expiry
nodes:
  - id: wait_ci
    type: task
    contract: pure
    executor:
      kind: shell
      command: ["echo", "wait"]
  - id: on_timeout
    type: task
    contract: pure
    terminal: true
    executor:
      kind: shell
      command: ["echo", "timeout_route"]
edges:
  - { from: wait_ci, to: on_timeout, type: routes_to, when: timeout }
`
	st, _, runID := openTestStoreWithGraph(t, graphYAML)
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: &testShellExecutor{},
	}
	ctrl := newTestController(t, st, pool, "ctrl-1")

	now := time.Now().UnixMilli() - 1000
	waitID := ulid.Make().String()
	corrKey := "repo=org/app;pr=5;head=sha256:exp1"
	_, err := ctrl.RegisterExternalWait(ctx, ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_ci",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		ExpiresAt:         now + 100,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Trigger expiry check after expiry time
	if err := ctrl.ExpireExternalWaits(ctx, time.Now().UnixMilli()); err != nil {
		t.Fatalf("ExpireExternalWaits: %v", err)
	}

	w, _ := st.GetExternalWait(ctx, waitID)
	if w.Status != "expired" {
		t.Fatalf("w.Status = %q, want expired", w.Status)
	}

	// Late completion after expiry -> 409 WAIT_CONFLICT, does not revive wait
	lateRes, err := ctrl.CompleteExternalWait(ctx, CompleteWaitRequest{
		WaitID:          waitID,
		ProviderEventID: "github:check:late1",
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  corrKey,
		OccurredAt:      now + 300,
		Status:          "success",
		PayloadDigest:   "sha256:" + hexDigest("{}"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if lateRes.Code != "WAIT_CONFLICT" || lateRes.HTTPStatus != http.StatusConflict {
		t.Errorf("lateRes = %+v, want WAIT_CONFLICT (409)", lateRes)
	}

	// Step controller to run timeout route
	progressed, err := ctrl.Step(ctx, runID)
	if err != nil || !progressed {
		t.Fatalf("Step timeout: %v, %v", progressed, err)
	}
	_, _ = ctrl.Step(ctx, runID)

	runStatus := runStatusOf(ctrl, ctx, runID)
	if runStatus != "completed" {
		t.Errorf("runStatus = %q, want completed", runStatus)
	}
}

// Run cancellation and late completion rejection
func TestExternalWaitCancellationAndLateEvent(t *testing.T) {
	ctx := context.Background()
	graphYAML := `schema: proceed/v1
name: test-cancel
nodes:
  - id: wait_ci
    type: task
    contract: pure
    executor:
      kind: shell
      command: ["echo", "wait"]
  - id: next_step
    type: task
    contract: pure
    terminal: true
    executor:
      kind: shell
      command: ["echo", "next"]
edges:
  - { from: wait_ci, to: next_step, type: routes_to, when: success }
`
	st, _, runID := openTestStoreWithGraph(t, graphYAML)
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: &testShellExecutor{},
	}
	ctrl := newTestController(t, st, pool, "ctrl-1")

	waitID := ulid.Make().String()
	corrKey := "repo=org/app;pr=9;head=sha256:can1"
	_, err := ctrl.RegisterExternalWait(ctx, ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_ci",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Cancel the run
	if err := ctrl.CancelRun(ctx, runID); err != nil {
		t.Fatalf("CancelRun: %v", err)
	}

	w, _ := st.GetExternalWait(ctx, waitID)
	if w.Status != "cancelled" {
		t.Fatalf("w.Status = %q, want cancelled", w.Status)
	}

	// Late completion after cancellation -> 409 WAIT_CONFLICT
	lateRes, err := ctrl.CompleteExternalWait(ctx, CompleteWaitRequest{
		WaitID:          waitID,
		ProviderEventID: "github:check:late_cancel",
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  corrKey,
		OccurredAt:      time.Now().UnixMilli(),
		Status:          "success",
		PayloadDigest:   "sha256:" + hexDigest("{}"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if lateRes.Code != "WAIT_CONFLICT" || lateRes.HTTPStatus != http.StatusConflict {
		t.Errorf("lateRes = %+v, want WAIT_CONFLICT (409)", lateRes)
	}

	// Verify run remains cancelled
	runStatus := runStatusOf(ctrl, ctx, runID)
	if runStatus != "cancelled" {
		t.Errorf("runStatus = %q, want cancelled", runStatus)
	}
}

// Concurrent completions with distinct provider event IDs: exactly one wins
func TestExternalWaitConcurrentCompletions(t *testing.T) {
	ctx := context.Background()
	graphYAML := `schema: proceed/v1
name: test-concurrent-complete
nodes:
  - id: wait_ci
    type: task
    contract: pure
    executor:
      kind: shell
      command: ["echo", "wait"]
  - id: next_step
    type: task
    contract: pure
    terminal: true
    executor:
      kind: shell
      command: ["echo", "next"]
edges:
  - { from: wait_ci, to: next_step, type: routes_to, when: success }
`
	st, _, runID := openTestStoreWithGraph(t, graphYAML)
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: &testShellExecutor{},
	}
	ctrl := newTestController(t, st, pool, "ctrl-1")

	waitID := ulid.Make().String()
	corrKey := "repo=org/concurrency;pr=10;head=sha256:conc123"
	_, err := ctrl.RegisterExternalWait(ctx, ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_ci",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	concurrency := 10
	results := make([]*CompletionResult, concurrency)
	errs := make([]error, concurrency)

	var wg sync.WaitGroup
	startCh := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startCh
			res, err := ctrl.CompleteExternalWait(ctx, CompleteWaitRequest{
				WaitID:          waitID,
				ProviderEventID: fmt.Sprintf("github:check_run:concurrent_%d", idx),
				EventType:       "ci.completed",
				Source:          "github",
				CorrelationKey:  corrKey,
				OccurredAt:      time.Now().UnixMilli(),
				Status:          "success",
				PayloadDigest:   "sha256:" + hexDigest("{}"),
				Payload:         json.RawMessage("{}"),
			})
			results[idx] = res
			errs[idx] = err
		}(i)
	}

	close(startCh)
	wg.Wait()

	var successCount, conflictCount int
	for i := 0; i < concurrency; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d returned error: %v", i, errs[i])
		}
		if results[i].Code == "WAIT_COMPLETED" && results[i].HTTPStatus == http.StatusAccepted {
			successCount++
		} else if results[i].Code == "WAIT_CONFLICT" && results[i].HTTPStatus == http.StatusConflict {
			conflictCount++
		} else {
			t.Errorf("goroutine %d unexpected result: %+v", i, results[i])
		}
	}

	if successCount != 1 {
		t.Errorf("successCount = %d, want exactly 1", successCount)
	}
	if conflictCount != concurrency-1 {
		t.Errorf("conflictCount = %d, want %d", conflictCount, concurrency-1)
	}

	// Verify only 1 external_wait_completed event was recorded
	var completedEventCount int
	if err := st.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM event WHERE run_id = ? AND type = 'external_wait_completed'", runID).Scan(&completedEventCount); err != nil {
		t.Fatal(err)
	}
	if completedEventCount != 1 {
		t.Errorf("completedEventCount = %d, want 1", completedEventCount)
	}

	// Verify only 1 node_finished event was recorded
	var finishedNodeCount int
	if err := st.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM event WHERE run_id = ? AND type = 'node_finished'", runID).Scan(&finishedNodeCount); err != nil {
		t.Fatal(err)
	}
	if finishedNodeCount != 1 {
		t.Errorf("finishedNodeCount = %d, want 1", finishedNodeCount)
	}
}

// Completion racing expiry cannot revive terminal expired status
func TestExternalWaitCompletionRacingExpiry(t *testing.T) {
	ctx := context.Background()
	graphYAML := `schema: proceed/v1
name: test-race-expiry
nodes:
  - id: wait_ci
    type: task
    contract: pure
    executor:
      kind: shell
      command: ["echo", "wait"]
  - id: next_step
    type: task
    contract: pure
    terminal: true
    executor:
      kind: shell
      command: ["echo", "next"]
edges:
  - { from: wait_ci, to: next_step, type: routes_to, when: success }
`
	st, _, runID := openTestStoreWithGraph(t, graphYAML)
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: &testShellExecutor{},
	}
	ctrl := newTestController(t, st, pool, "ctrl-1")

	now := time.Now().UnixMilli() - 500
	waitID := ulid.Make().String()
	corrKey := "repo=org/race;pr=11;head=sha256:race456"
	_, err := ctrl.RegisterExternalWait(ctx, ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           "wait_ci",
		EventType:         "ci.completed",
		CorrelationKey:    corrKey,
		ExpectedCondition: `{"status":"success"}`,
		ExpiresAt:         now + 50,
		WaitID:            waitID,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Concurrently expire and complete
	var wg sync.WaitGroup
	startCh := make(chan struct{})

	var compRes *CompletionResult
	var compErr error
	var expireErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-startCh
		expireErr = ctrl.ExpireExternalWaits(ctx, time.Now().UnixMilli())
	}()

	go func() {
		defer wg.Done()
		<-startCh
		compRes, compErr = ctrl.CompleteExternalWait(ctx, CompleteWaitRequest{
			WaitID:          waitID,
			ProviderEventID: "github:check_run:race_expiry",
			EventType:       "ci.completed",
			Source:          "github",
			CorrelationKey:  corrKey,
			OccurredAt:      now + 20,
			Status:          "success",
			PayloadDigest:   "sha256:" + hexDigest("{}"),
			Payload:         json.RawMessage("{}"),
		})
	}()

	close(startCh)
	wg.Wait()

	if expireErr != nil {
		t.Fatalf("ExpireExternalWaits error: %v", expireErr)
	}
	if compErr != nil {
		t.Fatalf("CompleteExternalWait error: %v", compErr)
	}

	w, _ := st.GetExternalWait(ctx, waitID)
	if w.Status != "completed" && w.Status != "expired" {
		t.Fatalf("Unexpected wait status after race: %s", w.Status)
	}

	// If expired won, completion must have been rejected as conflict
	if w.Status == "expired" {
		if compRes.Code != "WAIT_CONFLICT" || compRes.HTTPStatus != http.StatusConflict {
			t.Errorf("Expected WAIT_CONFLICT when expiry won, got %+v", compRes)
		}
	}
}

type testShellExecutor struct{}

func (e *testShellExecutor) Kind() executor.Kind { return executor.Shell }

func (e *testShellExecutor) Contract() executor.Contract { return executor.Pure }

func (e *testShellExecutor) Execute(ctx context.Context, req *executor.Request) (*executor.Result, error) {
	return &executor.Result{
		Output: map[string]any{"executed": req.NodeKey},
		Route:  "success",
	}, nil
}

func testConfig(ownerID string) Config {
	cfg := DefaultConfig()
	cfg.OwnerID = ownerID
	return cfg
}

func compileFixture(t *testing.T, src []byte) *compiler.Document {
	t.Helper()
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	return doc
}
