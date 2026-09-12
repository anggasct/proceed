package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"proceed/internal/compiler"
	"proceed/internal/executor"
	"proceed/internal/store"
)

type mapResolver map[string]string

func (m mapResolver) Resolve(ctx context.Context, name string) ([]byte, error) {
	v, ok := m[name]
	if !ok {
		return nil, errUnknownSecret
	}
	return []byte(v), nil
}

type resolveError struct{}

func (resolveError) Error() string { return "secret not found" }

var errUnknownSecret = resolveError{}

func decl(name, typ, def string, required bool) compiler.Param {
	d := compiler.Param{Name: name, Type: typ, Required: required}
	if def != "" {
		d.HasDefault = true
		d.Default = def
		d.DefaultTag = defaultTagForTest(typ)
	}
	return d
}

func defaultTagForTest(typ string) string {
	switch typ {
	case "int":
		return "!!int"
	case "float":
		return "!!float"
	case "bool":
		return "!!bool"
	default:
		return "!!str"
	}
}

func TestBindRunParamsCoercion(t *testing.T) {
	decls := []compiler.Param{
		decl("env", "string", "", true),
		decl("retries", "int", "3", false),
		decl("ratio", "float", "", false),
		decl("verbose", "bool", "false", false),
		decl("token", "secret", "", false),
	}
	cases := []struct {
		name     string
		bindings []ParamBinding
		want     string
		wantErr  string
	}{
		{name: "scalar bindings", bindings: []ParamBinding{
			{Name: "env", Value: "prod"},
			{Name: "retries", Value: "5"},
			{Name: "ratio", Value: "0.75"},
			{Name: "verbose", Value: "true"},
			{Name: "token", Value: "${GITHUB_TOKEN}"},
		}, want: `{"env":"prod","ratio":0.75,"retries":5,"verbose":true}`},
		{name: "defaults fill unbound", bindings: []ParamBinding{
			{Name: "env", Value: "staging"},
		}, want: `{"env":"staging","retries":3,"verbose":false}`},
		{name: "last wins", bindings: []ParamBinding{
			{Name: "env", Value: "a"},
			{Name: "env", Value: "b"},
		}, want: `{"env":"b","retries":3,"verbose":false}`},
		{name: "empty string for required", bindings: []ParamBinding{
			{Name: "env", Value: ""},
		}, want: `{"env":"","retries":3,"verbose":false}`},
		{name: "float accepts int form", bindings: []ParamBinding{
			{Name: "env", Value: "x"},
			{Name: "ratio", Value: "3"},
		}, want: `{"env":"x","ratio":3,"retries":3,"verbose":false}`},
		{name: "int rejects fraction", bindings: []ParamBinding{
			{Name: "env", Value: "x"},
			{Name: "retries", Value: "3.0"},
		}, wantErr: "must be an integer"},
		{name: "int rejects text", bindings: []ParamBinding{
			{Name: "env", Value: "x"},
			{Name: "retries", Value: "abc"},
		}, wantErr: "must be an integer"},
		{name: "bool strict", bindings: []ParamBinding{
			{Name: "env", Value: "x"},
			{Name: "verbose", Value: "yes"},
		}, wantErr: "must be true or false"},
		{name: "secret literal rejected", bindings: []ParamBinding{
			{Name: "env", Value: "x"},
			{Name: "token", Value: "hunter2"},
		}, wantErr: "requires a ${NAME} reference"},
		{name: "missing required", bindings: []ParamBinding{}, wantErr: `missing required param "env"`},
		{name: "undeclared name", bindings: []ParamBinding{
			{Name: "env", Value: "x"},
			{Name: "bogus", Value: "1"},
		}, wantErr: `param "bogus" is not declared`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bound, err := BindRunParams(decls, tc.bindings)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if bound.Digest != tc.want {
				t.Fatalf("digest = %s, want %s", bound.Digest, tc.want)
			}
		})
	}
}

func TestBindRunParamsSecretMarker(t *testing.T) {
	decls := []compiler.Param{
		decl("env", "string", "", true),
		decl("token", "secret", "", false),
	}
	bound, err := BindRunParams(decls, []ParamBinding{
		{Name: "env", Value: "prod"},
		{Name: "token", Value: "${GITHUB_TOKEN}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, leaked := bound.Texts["token"]; leaked {
		t.Fatal("secret must not appear in Texts")
	}
	if bound.Refs["token"] != "GITHUB_TOKEN" {
		t.Fatalf("refs = %v", bound.Refs)
	}
	if bound.Digest != `{"env":"prod"}` {
		t.Fatalf("digest = %s", bound.Digest)
	}
	for _, v := range bound.Values {
		if v.Type == "secret" && v.Value != nil {
			t.Fatal("secret value must be a presence marker only")
		}
	}
}

const paramShellGraph = `schema: proceed/v1
name: param-shell
params:
  - { name: env, type: string, required: true }
  - { name: token, type: secret }
nodes:
  - id: call
    type: task
    executor:
      kind: shell
      command: [bin/call, "{{ params.env }}"]
      x-proceed-env:
        TOKEN: "{{ params.token }}"
    contract: pure
    terminal: true
edges: []
`

func runWithParams(t *testing.T, cfg Config, pool map[executor.Kind]executor.Executor, bindings []ParamBinding) (*Controller, *store.Store, string, error) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	src := []byte(paramShellGraph)
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(context.Background(), "test.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(st, cfg, pool)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindRunParams(doc.Params, bindings)
	if err != nil {
		return c, st, "", err
	}
	runID, err := c.Run(context.Background(), RunInput{GraphVersionID: frozen.GraphVersionID, Params: bound})
	if err != nil {
		return c, st, "", err
	}
	if err := c.Drain(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	return c, st, runID, nil
}

func TestInterpolationAtAdmission(t *testing.T) {
	var mu sync.Mutex
	var commands []string
	var envs []map[string]any
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewFuncExecutor(executor.Shell, executor.Pure,
			func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
				mu.Lock()
				defer mu.Unlock()
				exec := req.Config["executor"].(map[string]any)
				commands = append(commands, strings.Join(toStrings(exec["command"].([]any)), " "))
				if env, ok := exec["x-proceed-env"].(map[string]any); ok {
					envs = append(envs, env)
				}
				return &executor.Result{}, nil
			}),
	}
	cfg := DefaultConfig()
	cfg.Secrets = mapResolver{"GITHUB_TOKEN": "secret-value"}
	_, st, runID, err := runWithParams(t, cfg, pool, []ParamBinding{
		{Name: "env", Value: "prod"},
		{Name: "token", Value: "${GITHUB_TOKEN}"},
	})
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(commands) != 1 || commands[0] != "bin/call prod" {
		t.Fatalf("commands = %v", commands)
	}
	if len(envs) != 1 || envs[0]["TOKEN"] != "secret-value" {
		t.Fatalf("envs = %v", envs)
	}

	rows, err := st.RunParams(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if rows["env"].Value.String != "prod" {
		t.Fatalf("env row = %+v", rows["env"])
	}
	if rows["token"].Value.Valid {
		t.Fatal("secret row must be presence-only")
	}

	assertNoLeak(t, st, runID, "secret-value")
}

const httpSecretParamGraph = `schema: proceed/v1
name: http-secret-param
params:
  - { name: token, type: secret, required: true }
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: GET
      url: %s/api?token={{ params.token }}
    contract: reconcilable
    terminal: true
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`

func TestHTTPSecretParamInterpolationNeverPersists(t *testing.T) {
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()

	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	src := []byte(fmt.Sprintf(httpSecretParamGraph, server.URL))
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(context.Background(), "test.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Secrets = mapResolver{"API_TOKEN": "http-secret-value"}
	c, err := New(st, cfg, httpPool())
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindRunParams(doc.Params, []ParamBinding{
		{Name: "token", Value: "${API_TOKEN}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	runID, err := c.Run(context.Background(), RunInput{GraphVersionID: frozen.GraphVersionID, Params: bound})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(context.Background(), runID); err != nil {
		t.Fatal(err)
	}

	if s := nodeStatus(t, st, runID, "call"); s != "succeeded" {
		t.Fatalf("node status = %q, want succeeded", s)
	}
	if gotQuery != "token=http-secret-value" {
		t.Fatalf("target query = %q, want the resolved secret delivered to the server", gotQuery)
	}

	var target string
	if err := st.DB().QueryRow("SELECT target FROM effect LIMIT 1").Scan(&target); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(target, "http-secret-value") || !strings.Contains(target, "[REDACTED]") {
		t.Fatalf("effect target = %q, want redacted", target)
	}

	assertNoLeak(t, st, runID, "http-secret-value")
}

func toStrings(items []any) []string {
	out := make([]string, len(items))
	for i, v := range items {
		out[i], _ = v.(string)
	}
	return out
}

func assertNoLeak(t *testing.T, st *store.Store, runID, secret string) {
	t.Helper()
	var leaks []string
	tables := []string{"event", "run_param", "node_attempt", "artifact", "effect"}
	for _, table := range tables {
		rows, err := st.DB().Query("SELECT * FROM " + table)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			for i, v := range vals {
				if s, ok := v.(string); ok && strings.Contains(s, secret) {
					leaks = append(leaks, table+"."+cols[i])
				}
			}
		}
		rows.Close()
	}
	if len(leaks) > 0 {
		t.Fatalf("secret value leaked into %v", leaks)
	}
}

func TestUnresolvableSecretFailsNode(t *testing.T) {
	executed := false
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewFuncExecutor(executor.Shell, executor.Pure,
			func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
				executed = true
				return &executor.Result{}, nil
			}),
	}
	cfg := DefaultConfig()
	cfg.Secrets = mapResolver{}
	_, st, runID, err := runWithParams(t, cfg, pool, []ParamBinding{
		{Name: "env", Value: "prod"},
		{Name: "token", Value: "${MISSING_TOKEN}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if executed {
		t.Fatal("executor must not run with an unresolvable secret")
	}
	g, err := st.RuntimeGraph(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != "failed" {
		t.Fatalf("run status = %s, want failed", g.Status)
	}
	if len(g.Nodes) != 1 || g.Nodes[0].Status != "failed" {
		t.Fatalf("node status = %+v", g.Nodes)
	}
	var reason string
	if err := st.DB().QueryRow(`
SELECT json_extract(ev.payload, '$.error') FROM event ev
WHERE ev.run_id = ? AND ev.type = 'node_failed' ORDER BY ev.sequence DESC LIMIT 1`,
		runID).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reason, "token") || strings.Contains(reason, "MISSING_TOKEN") {
		t.Fatalf("failure reason = %q", reason)
	}
}

func TestNoResolverFailsNodeClosed(t *testing.T) {
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewFuncExecutor(executor.Shell, executor.Pure,
			func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
				t.Error("executor must not run without a resolver")
				return &executor.Result{}, nil
			}),
	}
	cfg := DefaultConfig()
	_, st, runID, err := runWithParams(t, cfg, pool, []ParamBinding{
		{Name: "env", Value: "prod"},
		{Name: "token", Value: "${GITHUB_TOKEN}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	g, err := st.RuntimeGraph(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != "failed" {
		t.Fatalf("run status = %s, want failed", g.Status)
	}
}

func TestUnboundOptionalParamReferenceFailsNode(t *testing.T) {
	graph := `schema: proceed/v1
name: param-optional
params:
  - { name: env, type: string }
nodes:
  - id: call
    type: task
    executor:
      kind: shell
      command: [bin/call, "{{ params.env }}"]
    contract: pure
    terminal: true
edges: []
`
	executed := false
	pool := map[executor.Kind]executor.Executor{
		executor.Shell: executor.NewFuncExecutor(executor.Shell, executor.Pure,
			func(ctx context.Context, req *executor.Request) (*executor.Result, error) {
				executed = true
				return &executor.Result{}, nil
			}),
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	src := []byte(graph)
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(context.Background(), "test.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(st, DefaultConfig(), pool)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := c.Run(context.Background(), RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Drain(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	if executed {
		t.Fatal("executor must not receive a raw placeholder")
	}
	g, err := st.RuntimeGraph(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != "failed" {
		t.Fatalf("run status = %s, want failed", g.Status)
	}
}
