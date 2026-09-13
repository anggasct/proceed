package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"proceed/internal/compiler"
)

const paramsGraph = `schema: proceed/v1
name: params-store
params:
  - { name: env, type: string, required: true }
  - { name: retries, type: int, default: 3 }
  - { name: token, type: secret, default: "${GITHUB_TOKEN}" }
nodes:
  - id: call
    type: task
    executor: { kind: shell, command: [bin/call, "{{ params.env }}"] }
    contract: pure
    terminal: true
edges: []
`

func freezeParamsGraph(t *testing.T, s *Store) string {
	t.Helper()
	doc, err := compiler.Parse([]byte(paramsGraph))
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := s.FreezeDefinition(context.Background(), "params.yaml", []byte(paramsGraph), doc)
	if err != nil {
		t.Fatal(err)
	}
	return frozen.GraphVersionID
}

func TestFreezeMaterializesGraphParams(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeParamsGraph(t, s)

	decls, err := s.GraphParams(context.Background(), versionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(decls) != 3 {
		t.Fatalf("decls = %d, want 3", len(decls))
	}
	byName := map[string]compiler.Param{}
	for _, d := range decls {
		byName[d.Name] = d
	}
	if !byName["env"].Required {
		t.Fatal("env must be required")
	}
	if byName["retries"].Default != "3" || !byName["retries"].HasDefault {
		t.Fatalf("retries default = %q", byName["retries"].Default)
	}
	if byName["token"].Default != "${GITHUB_TOKEN}" {
		t.Fatalf("token default = %q", byName["token"].Default)
	}

	ref, err := s.FreezeDefinition(context.Background(), "params.yaml", []byte(paramsGraph), mustParse(t, paramsGraph))
	if err != nil {
		t.Fatal(err)
	}
	if ref.Created {
		t.Fatal("identical refreeze must not create a version")
	}
}

func mustParse(t *testing.T, src string) *compiler.Document {
	t.Helper()
	doc, err := compiler.Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func strPtr(s string) *string { return &s }

func TestCreateRunRecordsParams(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeParamsGraph(t, s)

	run, err := s.CreateRun(context.Background(), versionID, &RunParamsStart{
		Digest: `{"env":"prod","retries":3}`,
		Values: []RunParamValue{
			{Name: "env", Type: "string", Value: strPtr("prod")},
			{Name: "retries", Type: "int", Value: strPtr("3")},
			{Name: "token", Type: "secret"},
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}

	var digest string
	if err := s.db.QueryRow("SELECT params_digest FROM graph_run WHERE id = ?", run.ID).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if digest != `{"env":"prod","retries":3}` {
		t.Fatalf("params_digest = %q", digest)
	}

	rows, err := s.RunParams(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("run_param rows = %d, want 3", len(rows))
	}
	if rows["env"].Value.String != "prod" || !rows["env"].Value.Valid {
		t.Fatalf("env value = %+v", rows["env"].Value)
	}
	if rows["token"].Value.Valid {
		t.Fatal("secret row must carry no value")
	}

	var payload string
	if err := s.db.QueryRow(
		"SELECT payload FROM event WHERE run_id = ? AND type = 'run_started'", run.ID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, `"params_digest":`) {
		t.Fatalf("run_started payload = %q", payload)
	}
}

func TestCreateRunWithoutParamsKeepsLegacyShape(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeParamsGraph(t, s)

	run, err := s.CreateRun(context.Background(), versionID, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	var payload string
	if err := s.db.QueryRow(
		"SELECT payload FROM event WHERE run_id = ? AND type = 'run_started'", run.ID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(payload, "params") {
		t.Fatalf("legacy run_started payload must not mention params: %q", payload)
	}
	var digest string
	if err := s.db.QueryRow("SELECT params_digest FROM graph_run WHERE id = ?", run.ID).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if digest != "{}" {
		t.Fatalf("default params_digest = %q", digest)
	}
}

func TestRebuildProjectionsReplaysRunParams(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeParamsGraph(t, s)

	run, err := s.CreateRun(context.Background(), versionID, &RunParamsStart{
		Digest: `{"env":"staging"}`,
		Values: []RunParamValue{
			{Name: "env", Type: "string", Value: strPtr("staging")},
			{Name: "token", Type: "secret"},
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}

	report, err := s.RebuildProjections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Diverged {
		t.Fatal("projection rebuild diverged")
	}
	rows, err := s.RunParams(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows["env"].Value.String != "staging" || rows["token"].Value.Valid {
		t.Fatalf("rebuilt run_param rows = %+v", rows)
	}
	var digest string
	if err := s.db.QueryRow("SELECT params_digest FROM graph_run WHERE id = ?", run.ID).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if digest != `{"env":"staging"}` {
		t.Fatalf("rebuilt params_digest = %q", digest)
	}
}

func TestFreshStoreHasParamsDigestColumn(t *testing.T) {
	s := openTestStore(t)

	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('graph_run') WHERE name = 'params_digest'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("params_digest column missing in fresh baseline store")
	}
	var v int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != storeSchemaVersion {
		t.Fatalf("user_version = %d", v)
	}
}
