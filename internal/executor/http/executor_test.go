package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"proceed/internal/capability"
	"proceed/internal/executor"
)

type recordingEffects struct {
	mu       sync.Mutex
	intents  []executor.EffectIntent
	receipts map[string]executor.EffectReceipt
}

func newRecordingEffects() *recordingEffects {
	return &recordingEffects{receipts: map[string]executor.EffectReceipt{}}
}

func (r *recordingEffects) RecordIntent(_ context.Context, intent executor.EffectIntent) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.intents = append(r.intents, intent)
	return fmt.Sprintf("effect-%d", len(r.intents)), nil
}

func (r *recordingEffects) RecordReceipt(_ context.Context, receipt executor.EffectReceipt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.receipts[receipt.EffectID] = receipt
	return nil
}

func (r *recordingEffects) status(effectID string) (executor.EffectState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	receipt, ok := r.receipts[effectID]
	return receipt.Status, ok
}

type recordingArtifacts struct {
	mu     sync.Mutex
	inputs []executor.ArtifactInput
}

func (p *recordingArtifacts) Publish(_ context.Context, input executor.ArtifactInput) (executor.ArtifactRef, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inputs = append(p.inputs, input)
	return executor.ArtifactRef{Name: input.Name, MediaType: input.MediaType, SizeBytes: int64(len(input.Content))}, nil
}

type secretResolver map[string]string

func (s secretResolver) Resolve(_ context.Context, name string) ([]byte, error) {
	value, ok := s[name]
	if !ok {
		return nil, errors.New("missing secret")
	}
	return []byte(value), nil
}

func nodeConfig(targetURL string, hosts []string) map[string]any {
	hostList := make([]any, 0, len(hosts))
	for _, host := range hosts {
		hostList = append(hostList, host)
	}
	return map[string]any{
		"executor": map[string]any{
			"kind":   "http",
			"method": "POST",
			"url":    targetURL,
		},
		"contract": "reconcilable",
		"capability": map[string]any{
			"network": map[string]any{
				"allowlisted_hosts": hostList,
			},
		},
	}
}

func request(cfg map[string]any) *executor.Request {
	return &executor.Request{
		RunID:            "run-1",
		DefinitionDigest: "digest-1",
		NodeKey:          "call",
		AttemptNo:        1,
		OperationKey:     "op-1",
		Contract:         executor.Reconcilable,
		Config:           cfg,
	}
}

func TestExecuteAllowlistedRequestSucceeds(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer server.Close()

	effects := newRecordingEffects()
	artifacts := &recordingArtifacts{}
	adapter := New()
	result, err := adapter.Execute(context.Background(), withExtras(request(nodeConfig(server.URL, []string{"127.0.0.1"})), effects, artifacts, nil))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := result.Output["status_code"]; got != http.StatusOK {
		t.Fatalf("status_code = %v, want 200", got)
	}
	if got := result.Output["body"]; got != `{"status":"ok"}` {
		t.Fatalf("body = %v", got)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("server calls = %d, want 1", calls)
	}
	if len(effects.intents) != 1 {
		t.Fatalf("effect intents = %d, want 1", len(effects.intents))
	}
	if status, ok := effects.status("effect-1"); !ok || status != executor.EffectConfirmed {
		t.Fatalf("effect status = %v (%v), want confirmed", status, ok)
	}
	if len(artifacts.inputs) != 1 || artifacts.inputs[0].Name != "response" {
		t.Fatalf("artifacts = %+v, want one response artifact", artifacts.inputs)
	}
}

func withExtras(req *executor.Request, effects executor.EffectPublisher, artifacts executor.ArtifactPublisher, secrets executor.SecretResolver) *executor.Request {
	req.EffectPublisher = effects
	req.ArtifactPublisher = artifacts
	req.Secrets = secrets
	return req
}

func TestExecuteRejectsNonAllowlistedHostBeforeDial(t *testing.T) {
	var connections int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&connections, 1)
	}))
	defer server.Close()

	effects := newRecordingEffects()
	adapter := New()
	_, err := adapter.Execute(context.Background(), withExtras(request(nodeConfig(server.URL, []string{"api.internal"})), effects, nil, nil))
	if err == nil || !strings.Contains(err.Error(), capability.CodePolicyDenied) {
		t.Fatalf("Execute() error = %v, want policy denial", err)
	}
	if atomic.LoadInt32(&connections) != 0 {
		t.Fatalf("server connections = %d, want 0", connections)
	}
	if len(effects.intents) != 0 {
		t.Fatalf("effect intents = %d, want 0", len(effects.intents))
	}
}

func TestExecuteUncertainWhenResponseTornAfterDispatch(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
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
	}))
	defer server.Close()

	effects := newRecordingEffects()
	adapter := New()
	_, err := adapter.Execute(context.Background(), withExtras(request(nodeConfig(server.URL, []string{"127.0.0.1"})), effects, nil, nil))
	if !errors.Is(err, executor.ErrUncertain) {
		t.Fatalf("Execute() error = %v, want EFFECT_UNCERTAIN", err)
	}
	if atomic.LoadInt32(&requests) != 1 {
		t.Fatalf("server requests = %d, want 1", requests)
	}
	if status, ok := effects.status("effect-1"); !ok || status != executor.EffectUnknown {
		t.Fatalf("effect status = %v (%v), want unknown", status, ok)
	}
}

func TestExecuteIdempotentRetryReusesStableKey(t *testing.T) {
	var mu sync.Mutex
	keys := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys[r.Header.Get("Idempotency-Key")]++
		mu.Unlock()
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()

	adapter := New()
	for attempt := int64(1); attempt <= 2; attempt++ {
		req := request(nodeConfig(server.URL, []string{"127.0.0.1"}))
		req.Contract = executor.Idempotent
		req.AttemptNo = attempt
		if _, err := adapter.Execute(context.Background(), withExtras(req, newRecordingEffects(), nil, nil)); err != nil {
			t.Fatalf("Execute() attempt %d error = %v", attempt, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 1 {
		t.Fatalf("distinct idempotency keys = %d (%v), want 1", len(keys), keys)
	}
	for key, count := range keys {
		if key == "" {
			t.Fatal("idempotency key header was not sent")
		}
		if count != 2 {
			t.Fatalf("key %q used %d times, want 2", key, count)
		}
	}
}

func TestReconcileResolvesUncertainEffect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "created")
	}))
	defer server.Close()

	adapter := New()
	result, state, err := adapter.ReconcileResult(context.Background(), request(nodeConfig(server.URL, []string{"127.0.0.1"})))
	if err != nil {
		t.Fatalf("ReconcileResult() error = %v", err)
	}
	if state != executor.EffectConfirmed {
		t.Fatalf("state = %v, want confirmed", state)
	}
	if result.Output["status_code"] != http.StatusOK {
		t.Fatalf("status_code = %v, want 200", result.Output["status_code"])
	}

	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer missing.Close()

	_, state, err = adapter.ReconcileResult(context.Background(), request(nodeConfig(missing.URL, []string{"127.0.0.1"})))
	if err != nil {
		t.Fatalf("ReconcileResult() error = %v", err)
	}
	if state != executor.EffectAbsent {
		t.Fatalf("state = %v, want absent", state)
	}
}

func TestExecuteRedactsCredentialValues(t *testing.T) {
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		fmt.Fprintf(w, `{"echo":"%s"}`, receivedAuth)
	}))
	defer server.Close()

	cfg := nodeConfig(server.URL, []string{"127.0.0.1"})
	cfg["executor"].(map[string]any)["headers"] = map[string]any{
		"Authorization": "${token}",
	}
	cfg["capability"].(map[string]any)["secrets"] = []any{"token"}

	effects := newRecordingEffects()
	artifacts := &recordingArtifacts{}
	adapter := New()
	result, err := adapter.Execute(context.Background(), withExtras(request(cfg), effects, artifacts, secretResolver{"token": "tok_S3cretValue"}))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if receivedAuth != "tok_S3cretValue" {
		t.Fatalf("authorization header = %q, want resolved secret sent to target", receivedAuth)
	}
	if strings.Contains(fmt.Sprint(result.Output["body"]), "tok_S3cretValue") {
		t.Fatalf("output body leaked secret: %v", result.Output["body"])
	}
	if strings.Contains(string(artifacts.inputs[0].Content), "tok_S3cretValue") {
		t.Fatalf("artifact leaked secret: %q", artifacts.inputs[0].Content)
	}
	intent := effects.intents[0]
	if strings.Contains(intent.RequestDigest, "tok_S3cretValue") {
		t.Fatal("request digest leaked secret")
	}
	if len(intent.RequestDigest) != 64 {
		t.Fatalf("request digest = %q, want sha256 hex", intent.RequestDigest)
	}
}

func TestExecuteTimeoutLeavesEffectUnknown(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		fmt.Fprint(w, "late")
	}))
	defer server.Close()
	defer close(release)

	effects := newRecordingEffects()
	adapter := New()
	req := request(nodeConfig(server.URL, []string{"127.0.0.1"}))
	req.TimeoutMs = 50
	_, err := adapter.Execute(context.Background(), withExtras(req, effects, nil, nil))
	if !errors.Is(err, executor.ErrTimeout) {
		t.Fatalf("Execute() error = %v, want NODE_TIMEOUT", err)
	}
	if status, ok := effects.status("effect-1"); !ok || status != executor.EffectUnknown {
		t.Fatalf("effect status = %v (%v), want unknown", status, ok)
	}
}

func TestExecuteRejectsRedirectToNonAllowlistedHost(t *testing.T) {
	seen := make(chan struct{}, 1)
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case seen <- struct{}{}:
		default:
		}
		http.Redirect(w, r, "http://forbidden.invalid/elsewhere", http.StatusFound)
	}))
	defer redirector.Close()

	effects := newRecordingEffects()
	adapter := New()
	_, err := adapter.Execute(context.Background(), withExtras(request(nodeConfig(redirector.URL, []string{"127.0.0.1"})), effects, nil, nil))
	if err == nil || !strings.Contains(err.Error(), "NODE_FAILED") {
		t.Fatalf("Execute() error = %v, want definitive failure", err)
	}
	select {
	case <-seen:
	default:
		t.Fatal("redirector was never called")
	}
	if status, ok := effects.status("effect-1"); !ok || status != executor.EffectRejected {
		t.Fatalf("effect status = %v (%v), want rejected", status, ok)
	}
}

func TestAdmitRejectsInvalidConfigurations(t *testing.T) {
	adapter := New()
	cases := []struct {
		name string
		cfg  map[string]any
		want string
	}{
		{
			name: "missing capability",
			cfg: map[string]any{
				"executor": map[string]any{"kind": "http", "url": "http://api.internal/x"},
			},
			want: capability.CodePolicyDenied,
		},
		{
			name: "network none",
			cfg: map[string]any{
				"executor":   map[string]any{"kind": "http", "url": "http://api.internal/x"},
				"capability": map[string]any{"network": "none"},
			},
			want: capability.CodePolicyDenied,
		},
		{
			name: "empty host list",
			cfg: map[string]any{
				"executor": map[string]any{"kind": "http", "url": "http://api.internal/x"},
				"capability": map[string]any{
					"network": map[string]any{"allowlisted_hosts": []any{}},
				},
			},
			want: capability.CodePolicyDenied,
		},
		{
			name: "target host not allowlisted",
			cfg: map[string]any{
				"executor": map[string]any{"kind": "http", "url": "http://api.internal/x"},
				"capability": map[string]any{
					"network": map[string]any{"allowlisted_hosts": []any{"other.internal"}},
				},
			},
			want: capability.CodePolicyDenied,
		},
		{
			name: "unsupported method",
			cfg: map[string]any{
				"executor": map[string]any{"kind": "http", "method": "TRACE", "url": "http://api.internal/x"},
				"capability": map[string]any{
					"network": map[string]any{"allowlisted_hosts": []any{"api.internal"}},
				},
			},
			want: capability.CodePolicyDenied,
		},
		{
			name: "relative url",
			cfg: map[string]any{
				"executor": map[string]any{"kind": "http", "url": "/relative"},
				"capability": map[string]any{
					"network": map[string]any{"allowlisted_hosts": []any{"api.internal"}},
				},
			},
			want: capability.CodePolicyDenied,
		},
		{
			name: "non-http scheme",
			cfg: map[string]any{
				"executor": map[string]any{"kind": "http", "url": "ftp://api.internal/x"},
				"capability": map[string]any{
					"network": map[string]any{"allowlisted_hosts": []any{"api.internal"}},
				},
			},
			want: capability.CodePolicyDenied,
		},
		{
			name: "url with credentials",
			cfg: map[string]any{
				"executor": map[string]any{"kind": "http", "url": "http://user:pass@api.internal/x"},
				"capability": map[string]any{
					"network": map[string]any{"allowlisted_hosts": []any{"api.internal"}},
				},
			},
			want: capability.CodePolicyDenied,
		},
		{
			name: "partial secret reference in header",
			cfg: map[string]any{
				"executor": map[string]any{
					"kind":    "http",
					"url":     "http://api.internal/x",
					"headers": map[string]any{"Authorization": "Bearer ${token}"},
				},
				"capability": map[string]any{
					"network": map[string]any{"allowlisted_hosts": []any{"api.internal"}},
					"secrets": []any{"token"},
				},
			},
			want: capability.CodePolicyDenied,
		},
		{
			name: "undeclared secret reference in header",
			cfg: map[string]any{
				"executor": map[string]any{
					"kind":    "http",
					"url":     "http://api.internal/x",
					"headers": map[string]any{"Authorization": "${token}"},
				},
				"capability": map[string]any{
					"network": map[string]any{"allowlisted_hosts": []any{"api.internal"}},
				},
			},
			want: capability.CodePolicyDenied,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := adapter.Admit(context.Background(), request(tc.cfg))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Admit() error = %v, want %s", err, tc.want)
			}
		})
	}
}
