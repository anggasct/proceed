package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"proceed/internal/compiler"
)

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

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func count(t *testing.T, s *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestFreezeCanonicalFixture(t *testing.T) {
	s := openTestStore(t)
	src := readFixture(t, "../../internal/compiler/testdata/customer-research.yaml")
	doc := compileFixture(t, src)

	frozen, err := s.FreezeDefinition(context.Background(), "customer-research.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	if !frozen.Created {
		t.Error("first freeze must create")
	}
	if n := count(t, s, "SELECT COUNT(*) FROM graph"); n != 1 {
		t.Errorf("graph rows = %d, want 1", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM graph_version"); n != 1 {
		t.Errorf("graph_version rows = %d, want 1", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM graph_node"); n != 6 {
		t.Errorf("graph_node rows = %d, want 6", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM graph_edge"); n != 7 {
		t.Errorf("graph_edge rows = %d, want 7", n)
	}
	if frozen.Digest == "" || len(frozen.Digest) != 64 {
		t.Errorf("digest = %q", frozen.Digest)
	}

	var sourceSchema, compiledSchema, status string
	err = s.db.QueryRow("SELECT source_schema_version, compiled_schema_version, status FROM graph_version WHERE id = ?",
		frozen.GraphVersionID).Scan(&sourceSchema, &compiledSchema, &status)
	if err != nil {
		t.Fatal(err)
	}
	if sourceSchema != "proceed/v1" || compiledSchema != compiler.CompiledSchemaVersion || status != "frozen" {
		t.Errorf("version row = %s/%s/%s", sourceSchema, compiledSchema, status)
	}

	var gateConfig string
	err = s.db.QueryRow(`SELECT config FROM graph_node WHERE graph_version_id = ? AND node_key = 'human_approval'`,
		frozen.GraphVersionID).Scan(&gateConfig)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(gateConfig, `"executor"`) {
		t.Errorf("gate node must not carry an executor config: %s", gateConfig)
	}

	var condition sql.NullString
	var traversals sql.NullInt64
	err = s.db.QueryRow(`SELECT condition, max_traversals FROM graph_edge
		WHERE graph_version_id = ? AND from_node_key = 'verify' AND to_node_key = 'synthesize'`,
		frozen.GraphVersionID).Scan(&condition, &traversals)
	if err != nil {
		t.Fatal(err)
	}
	if !condition.Valid || condition.String != "needs_revision" {
		t.Errorf("condition = %+v", condition)
	}
	if !traversals.Valid || traversals.Int64 != 2 {
		t.Errorf("max_traversals = %+v", traversals)
	}

	var depCondition sql.NullString
	err = s.db.QueryRow(`SELECT condition FROM graph_edge
		WHERE graph_version_id = ? AND from_node_key = 'synthesize' AND to_node_key = 'verify'`,
		frozen.GraphVersionID).Scan(&depCondition)
	if err != nil {
		t.Fatal(err)
	}
	if depCondition.Valid {
		t.Errorf("depends_on edge must not carry condition: %+v", depCondition)
	}
}

func TestFreezeIdenticalDigestIsNoOp(t *testing.T) {
	s := openTestStore(t)
	src := readFixture(t, "../../internal/compiler/testdata/customer-research.yaml")
	doc := compileFixture(t, src)

	first, err := s.FreezeDefinition(context.Background(), "a.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	reformatted := strings.Replace(string(src), "schema: proceed/v1", "schema: 'proceed/v1'", 1)
	doc2 := compileFixture(t, []byte(reformatted))
	second, err := s.FreezeDefinition(context.Background(), "b.yaml", []byte(reformatted), doc2)
	if err != nil {
		t.Fatal(err)
	}
	if second.Created {
		t.Error("identical digest must be a no-op")
	}
	if second.GraphVersionID != first.GraphVersionID {
		t.Errorf("no-op must return the existing version: %s vs %s", second.GraphVersionID, first.GraphVersionID)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM graph_version"); n != 1 {
		t.Errorf("graph_version rows = %d, want 1", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM graph_node"); n != 6 {
		t.Errorf("graph_node rows = %d, want 6", n)
	}
}

func TestFreezeNewVersionUnderSameGraph(t *testing.T) {
	s := openTestStore(t)
	src := readFixture(t, "../../internal/compiler/testdata/customer-research.yaml")
	doc := compileFixture(t, src)
	if _, err := s.FreezeDefinition(context.Background(), "a.yaml", src, doc); err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(src), "when: verified }", "when: approved }", 1)
	doc2 := compileFixture(t, []byte(changed))
	second, err := s.FreezeDefinition(context.Background(), "a.yaml", []byte(changed), doc2)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Created {
		t.Error("changed content must create a new version")
	}
	if n := count(t, s, "SELECT COUNT(*) FROM graph"); n != 1 {
		t.Errorf("graph rows = %d, want 1", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM graph_version"); n != 2 {
		t.Errorf("graph_version rows = %d, want 2", n)
	}
	if n := count(t, s, "SELECT COUNT(*) FROM graph_node WHERE graph_version_id = ?", second.GraphVersionID); n != 6 {
		t.Errorf("new version nodes = %d, want 6", n)
	}
}

func TestFreezePersistsPolicies(t *testing.T) {
	s := openTestStore(t)
	src := []byte(`schema: proceed/v1
name: with-policy
nodes:
  - id: only
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
edges: []
policies:
  - name: default-retry
    kind: retry
    rule: { max_attempts: 3 }
`)
	doc := compileFixture(t, src)
	frozen, err := s.FreezeDefinition(context.Background(), "p.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	var kind, rule string
	err = s.db.QueryRow("SELECT kind, rule FROM policy WHERE graph_version_id = ?", frozen.GraphVersionID).
		Scan(&kind, &rule)
	if err != nil {
		t.Fatal(err)
	}
	if kind != "retry" || !strings.Contains(rule, `"max_attempts":3`) {
		t.Errorf("policy row = %s/%s", kind, rule)
	}
}

func TestDefinitionTablesHaveNoMutationInterface(t *testing.T) {
	typ := reflect.TypeOf(&Store{})
	banned := []string{"Update", "Delete", "Drop", "Alter"}
	for i := 0; i < typ.NumMethod(); i++ {
		name := typ.Method(i).Name
		for _, prefix := range banned {
			if strings.HasPrefix(name, prefix) {
				t.Errorf("store exposes mutation method %q on immutable definition tables", name)
			}
		}
	}
}

func TestFrozenRowsImmutableAtRuntime(t *testing.T) {
	s := openTestStore(t)
	src := readFixture(t, "../../internal/compiler/testdata/customer-research.yaml")
	doc := compileFixture(t, src)
	frozen, err := s.FreezeDefinition(context.Background(), "a.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec("DELETE FROM graph_version WHERE id = ?", frozen.GraphVersionID)
	if err == nil {
		t.Fatal("deleting a frozen version that nodes still reference must fail")
	}
	var n int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM graph_version").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("graph_version rows = %d, want 1", n)
	}
}

func readFixture(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
