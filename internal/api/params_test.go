package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func paramsTestGraph(t *testing.T, dataDir, url string) string {
	t.Helper()
	graphPath := filepath.Join(dataDir, "params-graph.yaml")
	graphYAML := fmt.Sprintf(`schema: proceed/v1
name: api-params
params:
  - { name: env, type: string, default: staging }
  - { name: retries, type: int, default: 3 }
  - { name: token, type: secret }
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
	if err := os.WriteFile(graphPath, []byte(graphYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	return graphPath
}

func TestAPICreateRunBindsParams(t *testing.T) {
	cfg := testConfig(t)
	server, st, _ := testServer(t, cfg)

	var seenEnv string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenEnv = r.URL.Query().Get("env")
		fmt.Fprint(w, "ok")
	}))
	defer target.Close()

	graphPath := paramsTestGraph(t, cfg.DataDir, target.URL)
	body := `{"graph":` + jsonString(graphPath) + `,"params":{"env":"prod","retries":5}}`
	rec, payload := doJSON(t, server.Handler(), "POST", "/v1/runs", "operator-secret", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d body = %v", rec.Code, payload)
	}
	if seenEnv != "prod" {
		t.Fatalf("target saw env = %q", seenEnv)
	}
	runID := payload["run_id"].(string)
	rows, err := st.RunParams(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if rows["env"].Value.String != "prod" || rows["retries"].Value.String != "5" {
		t.Fatalf("run_param rows = %+v", rows)
	}
	if rows["retries"].Type != "int" {
		t.Fatalf("retries type = %q", rows["retries"].Type)
	}
}

func TestAPICreateRunVersionIDBindsIdentically(t *testing.T) {
	cfg := testConfig(t)
	server, st, _ := testServer(t, cfg)

	var seenEnv string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenEnv = r.URL.Query().Get("env")
		fmt.Fprint(w, "ok")
	}))
	defer target.Close()

	graphPath := paramsTestGraph(t, cfg.DataDir, target.URL)
	rec, payload := doJSON(t, server.Handler(), "POST", "/v1/runs", "operator-secret",
		`{"graph":`+jsonString(graphPath)+`}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("freeze-run status = %d body = %v", rec.Code, payload)
	}
	versionID := payload["graph_version_id"].(string)

	rec, payload = doJSON(t, server.Handler(), "POST", "/v1/runs", "operator-secret",
		`{"graph":"`+versionID+`","params":{"env":"prod"}}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("version-id run status = %d body = %v", rec.Code, payload)
	}
	if seenEnv != "prod" {
		t.Fatalf("target saw env = %q", seenEnv)
	}
	runID := payload["run_id"].(string)
	rows, err := st.RunParams(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if rows["env"].Value.String != "prod" {
		t.Fatalf("env row = %+v", rows["env"])
	}
	if rows["retries"].Value.String != "3" {
		t.Fatalf("retries default missing: %+v", rows["retries"])
	}
}

func TestAPICreateRunDefaultsWithoutParamsBody(t *testing.T) {
	cfg := testConfig(t)
	server, st, _ := testServer(t, cfg)

	var seenEnv string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenEnv = r.URL.Query().Get("env")
		fmt.Fprint(w, "ok")
	}))
	defer target.Close()

	graphPath := paramsTestGraph(t, cfg.DataDir, target.URL)
	rec, payload := doJSON(t, server.Handler(), "POST", "/v1/runs", "operator-secret",
		`{"graph":`+jsonString(graphPath)+`}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d body = %v", rec.Code, payload)
	}
	if seenEnv != "staging" {
		t.Fatalf("default env not applied, saw %q", seenEnv)
	}
	runID := payload["run_id"].(string)
	rows, err := st.RunParams(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("only defaulted non-secret params recorded: %+v", rows)
	}
}

func TestAPICreateRunInvalidBindings(t *testing.T) {
	cfg := testConfig(t)
	server, _, _ := testServer(t, cfg)

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer target.Close()
	graphPath := paramsTestGraph(t, cfg.DataDir, target.URL)

	cases := []struct {
		name string
		body string
	}{
		{"undeclared", `{"graph":` + jsonString(graphPath) + `,"params":{"bogus":"1"}}`},
		{"bad int", `{"graph":` + jsonString(graphPath) + `,"params":{"retries":"abc"}}`},
		{"int float form", `{"graph":` + jsonString(graphPath) + `,"params":{"retries":3.5}}`},
		{"secret literal", `{"graph":` + jsonString(graphPath) + `,"params":{"token":"hunter2"}}`},
		{"null value", `{"graph":` + jsonString(graphPath) + `,"params":{"env":null}}`},
		{"array value", `{"graph":` + jsonString(graphPath) + `,"params":{"env":["a"]}}`},
		{"bad secret_ref shape", `{"graph":` + jsonString(graphPath) + `,"params":{"token":{"secret_ref":"X","extra":1}}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, payload := doJSON(t, server.Handler(), "POST", "/v1/runs", "operator-secret", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body = %v", rec.Code, payload)
			}
			errBody := payload["error"].(map[string]any)
			if errBody["code"] != "GRAPH_INVALID" {
				t.Fatalf("code = %v", errBody["code"])
			}
		})
	}

	var runs int
	if err := server.deps.Store.DB().QueryRow("SELECT COUNT(*) FROM graph_run").Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("rejected bindings must not create runs, got %d", runs)
	}
}

func TestAPICreateRunSecretRefMarker(t *testing.T) {
	cfg := testConfig(t)
	server, st, _ := testServer(t, cfg)

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer target.Close()
	graphPath := paramsTestGraph(t, cfg.DataDir, target.URL)

	rec, payload := doJSON(t, server.Handler(), "POST", "/v1/runs", "operator-secret",
		`{"graph":`+jsonString(graphPath)+`,"params":{"token":{"secret_ref":"GITHUB_TOKEN"}}}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d body = %v", rec.Code, payload)
	}
	runID := payload["run_id"].(string)
	var value *string
	var typ string
	if err := server.deps.Store.DB().QueryRow(
		"SELECT type, value FROM run_param WHERE run_id = ? AND name = 'token'", runID).
		Scan(&typ, &value); err != nil {
		t.Fatal(err)
	}
	if typ != "secret" || value != nil {
		t.Fatalf("secret row = (%s, %v)", typ, value)
	}
	if _, err := st.RunParams(t.Context(), runID); err != nil {
		t.Fatal(err)
	}
}
