package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"proceed/internal/store"
)

func paramsE2EGraph(url string) string {
	return fmt.Sprintf(`schema: proceed/v1
name: params-e2e
params:
  - { name: env, type: string, required: true }
  - { name: retries, type: int, default: 3 }
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: GET
      url: %s/deploy?env={{ params.env }}
    contract: reconcilable
    terminal: true
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`, url)
}

func TestCLIRunParamsBindsPerRun(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")

	var mu sync.Mutex
	seen := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Query().Get("env")]++
		mu.Unlock()
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := writeGraph(t, dir, paramsE2EGraph(server.URL))

	for _, env := range []string{"staging", "prod"} {
		code, stdout, stderr := runCLI(t, "run", graph, "--param", "env="+env, "--data-dir", dataDir)
		if code != 0 {
			t.Fatalf("run %s exit = %d stdout = %q stderr = %q", env, code, stdout, stderr)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if seen["staging"] != 1 || seen["prod"] != 1 {
		t.Fatalf("target saw %v", seen)
	}

	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows, err := st.DB().Query(`
SELECT r.definition_digest, r.params_digest,
  (SELECT value FROM run_param p WHERE p.run_id = r.id AND p.name = 'env'),
  (SELECT value FROM run_param p WHERE p.run_id = r.id AND p.name = 'retries')
FROM graph_run r ORDER BY r.created_at`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type runRow struct {
		definition, params, env, retries string
	}
	var runs []runRow
	for rows.Next() {
		var r runRow
		if err := rows.Scan(&r.definition, &r.params, &r.env, &r.retries); err != nil {
			t.Fatal(err)
		}
		runs = append(runs, r)
	}
	if len(runs) != 2 {
		t.Fatalf("runs = %d", len(runs))
	}
	if runs[0].definition != runs[1].definition {
		t.Fatalf("definition digest differs: %s vs %s", runs[0].definition, runs[1].definition)
	}
	if runs[0].params == runs[1].params {
		t.Fatalf("params digest must differ per run: %s", runs[0].params)
	}
	if runs[0].env != "staging" || runs[1].env != "prod" {
		t.Fatalf("recorded env values = %s, %s", runs[0].env, runs[1].env)
	}
	if runs[0].retries != "3" || runs[1].retries != "3" {
		t.Fatalf("retries default missing: %s, %s", runs[0].retries, runs[1].retries)
	}
}

func countRuns(t *testing.T, dataDir string) int {
	t.Helper()
	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var n int
	if err := st.DB().QueryRow("SELECT COUNT(*) FROM graph_run").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCLIRunRejectsInvalidBindings(t *testing.T) {
	cases := []struct {
		name   string
		params []string
	}{
		{"undeclared param", []string{"--param", "bogus=1"}},
		{"missing required", nil},
		{"bad int", []string{"--param", "env=x", "--param", "retries=abc"}},
		{"int float form", []string{"--param", "env=x", "--param", "retries=3.0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dataDir := filepath.Join(dir, "data")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, "ok")
			}))
			defer server.Close()
			graph := writeGraph(t, dir, paramsE2EGraph(server.URL))

			args := append([]string{"run", graph, "--data-dir", dataDir}, tc.params...)
			code, _, stderr := runCLI(t, args...)
			if code != 10 {
				t.Fatalf("exit = %d stderr = %q", code, stderr)
			}
			if n := countRuns(t, dataDir); n != 0 {
				t.Fatalf("rejected binding created %d runs", n)
			}
		})
	}
}

func TestCLIRunRejectsSecretLiteral(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := writeGraph(t, dir, fmt.Sprintf(`schema: proceed/v1
name: params-secret
params:
  - { name: token, type: secret }
nodes:
  - id: call
    type: task
    executor:
      kind: http
      method: GET
      url: %s/ping
    contract: reconcilable
    terminal: true
    capability:
      network:
        allowlisted_hosts: [127.0.0.1]
edges: []
`, server.URL))

	code, _, stderr := runCLI(t, "run", graph, "--param", "token=hunter2", "--data-dir", dataDir)
	if code != 10 {
		t.Fatalf("exit = %d stderr = %q", code, stderr)
	}
	if n := countRuns(t, dataDir); n != 0 {
		t.Fatalf("secret literal created %d runs", n)
	}
}

func TestCLIRunParamlessGraphUnchanged(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	graph := writeGraph(t, dir, httpNodeGraph(server.URL, "reconcilable"))

	code, stdout, stderr := runCLI(t, "run", graph, "--data-dir", dataDir)
	if code != 0 {
		t.Fatalf("run exit = %d stdout = %q stderr = %q", code, stdout, stderr)
	}
	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var digest string
	if err := st.DB().QueryRow("SELECT params_digest FROM graph_run").Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if digest != "{}" {
		t.Fatalf("params_digest = %q", digest)
	}
}
