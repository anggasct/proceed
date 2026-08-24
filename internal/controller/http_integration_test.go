package controller

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"proceed/internal/executor"
	httpexec "proceed/internal/executor/http"
	"proceed/internal/store"
)

type fixedSecrets map[string]string

func (s fixedSecrets) Resolve(_ context.Context, name string) ([]byte, error) {
	value, ok := s[name]
	if !ok {
		return nil, errors.New("missing secret")
	}
	return []byte(value), nil
}

func effectStatuses(t *testing.T, st *store.Store, runID, nodeKey string) []string {
	t.Helper()
	rows, err := st.DB().Query(`
SELECT e.status FROM effect e
JOIN node_attempt na ON na.id = e.node_attempt_id
JOIN run_node rn ON rn.id = na.run_node_id
WHERE rn.run_id = ? AND rn.node_key = ?
ORDER BY e.created_at, e.id`, runID, nodeKey)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var statuses []string
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			t.Fatal(err)
		}
		statuses = append(statuses, status)
	}
	return statuses
}

func httpGraph(url string) string {
	return `schema: proceed/v1
name: http-call
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: POST
      url: ` + url + `
    contract: reconcilable
    terminal: true
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`
}

func httpPool() map[executor.Kind]executor.Executor {
	return map[executor.Kind]executor.Executor{
		executor.HTTP: httpexec.New(),
	}
}

func TestHTTPNodeRunsAndRecordsEffect(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer server.Close()

	frozen := compileAndFreeze(t, st, httpGraph(server.URL))
	c := recoverController(t, st, httpPool())
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}

	if s := nodeStatus(t, st, runID, "call"); s != "succeeded" {
		t.Fatalf("node status = %q, want succeeded", s)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("server calls = %d, want 1", calls)
	}
	statuses := effectStatuses(t, st, runID, "call")
	if len(statuses) != 1 || statuses[0] != "confirmed" {
		t.Fatalf("effect statuses = %v, want [confirmed]", statuses)
	}

	var target, digest string
	var receipt sql.NullString
	if err := st.DB().QueryRow(`SELECT e.target, e.request_digest, e.receipt FROM effect e LIMIT 1`).Scan(&target, &digest, &receipt); err != nil {
		t.Fatal(err)
	}
	if target != server.URL {
		t.Fatalf("effect target = %q", target)
	}
	if len(digest) != 64 || !receipt.Valid || !strings.Contains(receipt.String, `"status_code":200`) {
		t.Fatalf("effect digest/receipt invalid: digest=%q receipt=%v", digest, receipt.String)
	}

	var artifacts int
	if err := st.DB().QueryRow("SELECT COUNT(*) FROM artifact").Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if artifacts != 1 {
		t.Fatalf("artifacts = %d, want 1", artifacts)
	}
}

func TestHTTPNodeUncertainEffectRecoversViaReconcile(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	var posts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			atomic.AddInt32(&posts, 1)
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("server does not support hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		fmt.Fprint(w, "created")
	}))
	defer server.Close()

	frozen := compileAndFreeze(t, st, httpGraph(server.URL))
	cfg := DefaultConfig()
	cfg.LeaseTTL = 40 * time.Millisecond
	c, err := New(st, cfg, httpPool())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.acquireLease(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}

	if s := nodeStatus(t, st, runID, "call"); s != "uncertain" {
		t.Fatalf("node status = %q, want uncertain", s)
	}
	if atomic.LoadInt32(&posts) != 1 {
		t.Fatalf("server posts = %d, want 1", posts)
	}
	statuses := effectStatuses(t, st, runID, "call")
	if len(statuses) != 1 || statuses[0] != "unknown" {
		t.Fatalf("effect statuses = %v, want [unknown]", statuses)
	}

	c.releaseLease(ctx)
	time.Sleep(60 * time.Millisecond)
	recovered := recoverController(t, st, httpPool())
	if err := recovered.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}

	if s := nodeStatus(t, st, runID, "call"); s != "succeeded" {
		t.Fatalf("node status after reconcile = %q, want succeeded", s)
	}
	statuses = effectStatuses(t, st, runID, "call")
	if len(statuses) != 1 || statuses[0] != "confirmed" {
		t.Fatalf("effect statuses after reconcile = %v, want [confirmed]", statuses)
	}
	if atomic.LoadInt32(&posts) != 1 {
		t.Fatalf("server posts after reconcile = %d, want 1 (no blind retry)", posts)
	}
	var reconRef sql.NullString
	if err := st.DB().QueryRow("SELECT reconciliation_ref FROM effect LIMIT 1").Scan(&reconRef); err != nil {
		t.Fatal(err)
	}
	if !reconRef.Valid || !strings.HasPrefix(reconRef.String, "reconcile:") {
		t.Fatalf("reconciliation_ref = %v, want reconcile reference", reconRef.String)
	}
}

func TestHTTPNodeTamperedTargetIsDeniedAtRuntime(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	var connections int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&connections, 1)
	}))
	defer server.Close()

	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: http-tamper
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: GET
      url: http://api.internal/thing
    contract: reconcilable
    terminal: true
    capability:
      network:
        allowlisted_hosts: [api.internal]
edges: []
`)
	var storedConfig string
	if err := st.DB().QueryRowContext(ctx,
		"SELECT config FROM graph_node WHERE graph_version_id = ? AND node_key = 'call'",
		frozen.GraphVersionID).Scan(&storedConfig); err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(storedConfig, "http://api.internal/thing", server.URL, 1)
	if tampered == storedConfig {
		t.Fatal("tampering did not change the stored config")
	}
	if _, err := st.DB().ExecContext(ctx,
		"UPDATE graph_node SET config = ? WHERE graph_version_id = ? AND node_key = 'call'",
		tampered, frozen.GraphVersionID); err != nil {
		t.Fatal(err)
	}

	c := recoverController(t, st, httpPool())
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}

	if s := nodeStatus(t, st, runID, "call"); s != "failed" {
		t.Fatalf("node status = %q, want failed", s)
	}
	if atomic.LoadInt32(&connections) != 0 {
		t.Fatalf("server connections = %d, want 0", connections)
	}
	var errorText string
	if err := st.DB().QueryRow(`
SELECT json_extract(ev.payload, '$.result.error') FROM event ev
WHERE ev.type = 'node_failed' LIMIT 1`).Scan(&errorText); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(errorText, "POLICY_DENIED") {
		t.Fatalf("node_failed error = %q, want POLICY_DENIED", errorText)
	}
}

func TestHTTPNodeCrashBetweenIntentAndReceiptMarksEffectUnknown(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	crashPool := map[executor.Kind]executor.Executor{
		executor.HTTP: executor.NewFuncExecutor(executor.HTTP, executor.Reconcilable, func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
			if _, err := req.EffectPublisher.RecordIntent(ctx, executor.EffectIntent{
				Target:        "http://api.internal/thing",
				RequestDigest: "digest",
			}); err != nil {
				return nil, err
			}
			return nil, executor.ErrUncertain
		}),
	}
	frozen := compileAndFreeze(t, st, `schema: proceed/v1
name: http-crash
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: POST
      url: http://api.internal/thing
    contract: reconcilable
    terminal: true
    capability:
      network:
        allowlisted_hosts: [api.internal]
edges: []
`)
	c := recoverController(t, st, crashPool)
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "call"); s != "uncertain" {
		t.Fatalf("node status = %q, want uncertain", s)
	}
	statuses := effectStatuses(t, st, runID, "call")
	if len(statuses) != 1 || statuses[0] != "unknown" {
		t.Fatalf("effect statuses = %v, want [unknown] after crash between intent and receipt", statuses)
	}
}

func TestHTTPNodeRetriesWithoutSecondEffectUntilResolvable(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "try again")
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()

	graph := `schema: proceed/v1
name: http-retry
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: GET
      url: ` + server.URL + `
    contract: idempotent
    terminal: true
    retry: { max_attempts: 2, backoff_ms: 0 }
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`
	frozen := compileAndFreeze(t, st, graph)
	c := recoverController(t, st, httpPool())
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "call"); s != "succeeded" {
		t.Fatalf("node status = %q, want succeeded after retry", s)
	}
	if atomic.LoadInt32(&attempts) != 2 {
		t.Fatalf("server attempts = %d, want 2", attempts)
	}
	statuses := effectStatuses(t, st, runID, "call")
	if len(statuses) != 2 || statuses[0] != "rejected" || statuses[1] != "confirmed" {
		t.Fatalf("effect statuses = %v, want [rejected confirmed]", statuses)
	}
}

// A node must not be committed as succeeded while its reconciled effect
// receipt cannot be appended durably; the recovery pass aborts and a later
// pass with a healthy store completes the node.
func TestHTTPNodeReconcileReceiptFailureDoesNotCommitSuccess(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	var posts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			atomic.AddInt32(&posts, 1)
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("server does not support hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		fmt.Fprint(w, "created")
	}))
	defer server.Close()

	frozen := compileAndFreeze(t, st, httpGraph(server.URL))
	cfg := DefaultConfig()
	cfg.LeaseTTL = 40 * time.Millisecond
	c, err := New(st, cfg, httpPool())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.acquireLease(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "call"); s != "uncertain" {
		t.Fatalf("node status = %q, want uncertain", s)
	}
	c.releaseLease(ctx)
	time.Sleep(60 * time.Millisecond)

	if _, err := st.DB().ExecContext(ctx, `
CREATE TRIGGER fail_effect_receipt BEFORE INSERT ON event
WHEN NEW.type = 'effect_receipt'
BEGIN SELECT RAISE(ABORT, 'receipt append failed'); END`); err != nil {
		t.Fatal(err)
	}
	blocked := recoverController(t, st, httpPool())
	if err := blocked.Recover(ctx, runID); err == nil {
		t.Fatal("Recover() = nil, want error when the reconciled receipt cannot be appended")
	}
	if s := nodeStatus(t, st, runID, "call"); s == "succeeded" || s == "completed" {
		t.Fatalf("node status after failed receipt append = %q, want success withheld", s)
	}

	if _, err := st.DB().ExecContext(ctx, `DROP TRIGGER fail_effect_receipt`); err != nil {
		t.Fatal(err)
	}
	blocked.releaseLease(ctx)
	healed := recoverController(t, st, httpPool())
	if err := healed.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "call"); s != "succeeded" {
		t.Fatalf("node status after healed append = %q, want succeeded", s)
	}
	statuses := effectStatuses(t, st, runID, "call")
	if len(statuses) != 1 || statuses[0] != "confirmed" {
		t.Fatalf("effect statuses = %v, want [confirmed]", statuses)
	}
	if atomic.LoadInt32(&posts) != 1 {
		t.Fatalf("server posts = %d, want 1 (no blind retry)", posts)
	}
}

// An artifact publication failure after a durable confirmed receipt must
// route the node through uncertainty, not the retry path: the external
// request already happened and must not be re-dispatched.
func TestHTTPNodeArtifactFailureDoesNotRedispatch(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	var posts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			atomic.AddInt32(&posts, 1)
		}
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()

	graph := `schema: proceed/v1
name: http-artifact-fail
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: POST
      url: ` + server.URL + `
    contract: reconcilable
    terminal: true
    retry: { max_attempts: 2, backoff_ms: 0 }
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`
	frozen := compileAndFreeze(t, st, graph)

	// Block artifact publication by occupying the artifacts directory path.
	if err := os.MkdirAll(st.DataDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(st.DataDir(), "artifacts"), []byte("blocked"), 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := New(st, DefaultConfig(), httpPool())
	if err != nil {
		t.Fatal(err)
	}
	c.cfg.LeaseTTL = 40 * time.Millisecond
	if err := c.acquireLease(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "call"); s != "uncertain" {
		t.Fatalf("node status = %q, want uncertain (artifact failure must not fail/retry)", s)
	}
	if atomic.LoadInt32(&posts) != 1 {
		t.Fatalf("server posts = %d, want 1 (no re-dispatch despite retry budget)", posts)
	}

	c.releaseLease(ctx)
	if err := os.Remove(filepath.Join(st.DataDir(), "artifacts")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	healed := recoverController(t, st, httpPool())
	if err := healed.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "call"); s != "succeeded" {
		t.Fatalf("node status after healed artifacts = %q, want succeeded", s)
	}
	if atomic.LoadInt32(&posts) != 1 {
		t.Fatalf("server posts after recovery = %d, want 1", posts)
	}
}

// A receipt-publication failure on the timeout branch must surface as an
// uncertain node, never as a retryable failure.
func TestHTTPNodeTimeoutWithFailingReceiptStaysUncertain(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	blocked := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			<-blocked
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()

	graph := `schema: proceed/v1
name: http-receipt-fail
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: POST
      url: ` + server.URL + `
    contract: reconcilable
    terminal: true
    timeout_ms: 50
    retry: { max_attempts: 2, backoff_ms: 0 }
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`
	frozen := compileAndFreeze(t, st, graph)

	if _, err := st.DB().ExecContext(ctx, `
CREATE TRIGGER fail_effect_receipt BEFORE INSERT ON event
WHEN NEW.type = 'effect_receipt'
BEGIN SELECT RAISE(ABORT, 'receipt append failed'); END`); err != nil {
		t.Fatal(err)
	}
	c, err := New(st, DefaultConfig(), httpPool())
	if err != nil {
		t.Fatal(err)
	}
	c.cfg.LeaseTTL = 40 * time.Millisecond
	if err := c.acquireLease(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	runID, err := c.Run(ctx, RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(ctx, runID); err == nil {
		t.Fatal("Drain() = nil, want error while effect receipts cannot be appended")
	}
	if s := nodeStatus(t, st, runID, "call"); s == "failed" || s == "requeued" {
		t.Fatalf("node status = %q, must not enter the retry path while receipts are undurable", s)
	}

	if _, err := st.DB().ExecContext(ctx, `DROP TRIGGER fail_effect_receipt`); err != nil {
		t.Fatal(err)
	}
	c.releaseLease(ctx)
	close(blocked)
	time.Sleep(60 * time.Millisecond)
	healed := recoverController(t, st, httpPool())
	if err := healed.Recover(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, st, runID, "call"); s != "succeeded" {
		t.Fatalf("node status after healed receipts = %q, want succeeded", s)
	}
}
