package export

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/xeipuuv/gojsonschema"

	"proceed/internal/compiler"
	"proceed/internal/store"
)

func openTestStore(tb testing.TB) *store.Store {
	tb.Helper()
	st, err := store.Open(filepath.Join(tb.TempDir(), "proceed.db"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	return st
}

func fixtureRun(t *testing.T) (*store.Store, string) {
	t.Helper()
	ctx := context.Background()
	st := openTestStore(t)
	graphYAML := `schema: proceed/v1
name: export-fixture
nodes:
  - id: start
    type: task
    contract: pure
    executor: { kind: shell, command: ["echo", "start"] }
  - id: middle
    type: task
    contract: pure
    executor: { kind: shell, command: ["echo", "middle"] }
  - id: end
    type: task
    contract: pure
    terminal: true
    executor: { kind: shell, command: ["echo", "end"] }
  - id: alt
    type: task
    contract: pure
    terminal: true
    executor: { kind: shell, command: ["echo", "alt"] }
edges:
  - { from: start, to: middle, type: depends_on }
  - { from: middle, to: end, type: routes_to, when: success }
  - { from: middle, to: alt, type: routes_to, when: failure }
`
	src := []byte(graphYAML)
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := st.FreezeDefinition(ctx, "fixture.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	run, err := st.CreateRun(ctx, frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}
	db := st.DB()
	rows, err := db.QueryContext(ctx, "SELECT node_key FROM graph_node WHERE graph_version_id = ?", frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}
	var nodeKeys []string
	for rows.Next() {
		var k string
		_ = rows.Scan(&k)
		nodeKeys = append(nodeKeys, k)
	}
	rows.Close()
	// Create run_nodes for each node with deterministic statuses
	statusMap := map[string]string{
		"start":  "succeeded",
		"middle": "succeeded",
		"end":    "succeeded",
		"alt":    "skipped",
	}
	for _, k := range nodeKeys {
		status := statusMap[k]
		if status == "" {
			status = "pending"
		}
		_, err := db.ExecContext(ctx, "INSERT INTO run_node (id, run_id, node_key, status, attempt_count) VALUES (?, ?, ?, ?, 1) ON CONFLICT(run_id, node_key) DO UPDATE SET status = excluded.status", stID(), run.ID, k, status)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Mark edges as traversed: start->middle (depends_on) and middle->end (success)
	// Find edge ids
	edgeRows, err := db.QueryContext(ctx, "SELECT id, from_node_key, to_node_key FROM graph_edge WHERE graph_version_id = ?", frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}
	for edgeRows.Next() {
		var id, from, to string
		_ = edgeRows.Scan(&id, &from, &to)
		shouldTraverse := (from == "start" && to == "middle") || (from == "middle" && to == "end")
		if shouldTraverse {
			_, err := db.ExecContext(ctx, "INSERT INTO run_edge (id, run_id, edge_id, route, sequence_in_run, traversed_at) VALUES (?, ?, ?, ?, ?, ?)", stID(), run.ID, id, "success", 1, 1)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	edgeRows.Close()
	// Add an artifact for end node
	_, err = db.ExecContext(ctx, "INSERT INTO artifact (id, run_id, produced_by_node_key, name, path, content_hash, media_type, size_bytes, truncated, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)", stID(), run.ID, "end", "result", "/tmp/result.json", "abc123", "application/json", 123, 1)
	if err != nil {
		t.Fatal(err)
	}
	return st, run.ID
}

func stID() string {
	return ulid.Make().String()
}

func TestMermaidValidFlowchart(t *testing.T) {
	ctx := context.Background()
	st, runID := fixtureRun(t)
	out, err := Export(ctx, st, runID, "mermaid")
	if err != nil {
		t.Fatalf("Export mermaid: %v", err)
	}
	s := string(out)
	if !strings.HasPrefix(s, "flowchart TD") {
		t.Fatalf("mermaid must start with 'flowchart TD', got %q", s[:50])
	}
	if !strings.Contains(s, "start") || !strings.Contains(s, "middle") {
		t.Fatalf("mermaid missing nodes: %s", s)
	}
	if !strings.Contains(s, "-->") {
		t.Fatalf("mermaid missing edges: %s", s)
	}
}

func TestMermaidEscaping(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	graphID := stID()
	versionID := stID()
	digest := "test-escape-digest"
	if _, err := st.DB().ExecContext(ctx, "INSERT INTO graph (id, name) VALUES (?, ?)", graphID, "escape-test"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx, "INSERT INTO graph_version (id, graph_id, definition_digest, source_schema_version, compiled_schema_version, source_metadata, extras, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'frozen', ?)", versionID, graphID, digest, "proceed/v1", "v1", "{}", "{}", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	dangerousKeys := []string{`a"b|c[d]`, `normal`, `with-dash`, `with.dot`, `with space`, `123numeric`}
	for _, k := range dangerousKeys {
		if _, err := st.DB().ExecContext(ctx, "INSERT INTO graph_node (id, graph_version_id, node_key, type, config) VALUES (?, ?, ?, ?, ?)", stID(), versionID, k, "task", "{}"); err != nil {
			t.Fatal(err)
		}
	}
	runID := stID()
	if _, err := st.DB().ExecContext(ctx, "INSERT INTO graph_run (id, graph_version_id, definition_digest, status, created_at) VALUES (?, ?, ?, 'running', ?)", runID, versionID, digest, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	for _, k := range dangerousKeys {
		if _, err := st.DB().ExecContext(ctx, "INSERT INTO run_node (id, run_id, node_key, status, attempt_count) VALUES (?, ?, ?, ?, 1) ON CONFLICT(run_id, node_key) DO UPDATE SET status = excluded.status", stID(), runID, k, "succeeded"); err != nil {
			t.Fatal(err)
		}
	}
	out, err := Export(ctx, st, runID, "mermaid")
	if err != nil {
		t.Fatalf("Export mermaid with dangerous keys: %v", err)
	}
	s := string(out)
	if !strings.HasPrefix(s, "flowchart TD") {
		t.Fatalf("mermaid must start with 'flowchart TD', got %q", s)
	}
	if strings.Contains(s, `a"b`) {
		t.Fatalf("mermaid output contains unescaped quote: %s", s)
	}
	if strings.Contains(s, `|c[`) {
		t.Fatalf("mermaid output contains unescaped pipe/bracket: %s", s)
	}
	lines := strings.Split(s, "\n")
	for _, line := range lines {
		if strings.Contains(line, "[\"") {
			start := strings.Index(line, "[\"")
			end := strings.LastIndex(line, "\"]")
			if start == -1 || end == -1 || end <= start {
				continue
			}
			label := line[start+2 : end]
			if strings.Contains(label, "\"") {
				t.Fatalf("label contains unescaped quote: %q line %q", label, line)
			}
		}
	}
	for _, k := range dangerousKeys {
		safe := sanitizeMermaidID(k)
		if !strings.Contains(s, safe) {
			t.Fatalf("mermaid missing sanitized id %q (from %q) in %s", safe, k, s)
		}
	}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || trimmed == "flowchart TD" {
			continue
		}
		if idx := strings.Index(trimmed, "[\""); idx != -1 {
			idPart := strings.TrimSpace(trimmed[:idx])
			for _, k := range dangerousKeys {
				if k == idPart && sanitizeMermaidID(k) != k {
					t.Fatalf("mermaid contains unsanitized raw id %q as node id %q", k, idPart)
				}
			}
			if idPart != "" {
				for _, r := range idPart {
					if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_') {
						t.Fatalf("node id %q contains invalid char %q", idPart, string(r))
					}
				}
				if idPart[0] >= '0' && idPart[0] <= '9' {
					t.Fatalf("node id %q starts with digit", idPart)
				}
			}
		}
	}
	escaped := escapeMermaidLabel(`a"b|c[d]`)
	if strings.Contains(escaped, "\"") || strings.Contains(escaped, "|") || strings.Contains(escaped, "[") || strings.Contains(escaped, "]") {
		t.Fatalf("escapeMermaidLabel failed to escape: %q", escaped)
	}
}

func TestJSONValidatesSchema(t *testing.T) {
	ctx := context.Background()
	st, runID := fixtureRun(t)
	out, err := Export(ctx, st, runID, "json")
	if err != nil {
		t.Fatalf("Export json: %v", err)
	}
	var v map[string]any
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	for _, field := range []string{"run_id", "status", "graph_version_id", "definition_digest", "nodes", "edges", "artifacts"} {
		if _, ok := v[field]; !ok {
			t.Fatalf("json missing field %q: %s", field, string(out))
		}
	}
	nodes, ok := v["nodes"].([]any)
	if !ok || len(nodes) == 0 {
		t.Fatalf("nodes must be non-empty array")
	}
	edges, ok := v["edges"].([]any)
	if !ok || len(edges) == 0 {
		t.Fatalf("edges must be non-empty array")
	}
	foundTraversed := false
	for _, e := range edges {
		m, _ := e.(map[string]any)
		if m["traversed"] == true {
			foundTraversed = true
			if m["from"] == nil || m["to"] == nil {
				t.Fatalf("edge missing from/to: %+v", m)
			}
		}
	}
	if !foundTraversed {
		t.Fatalf("expected at least one traversed edge")
	}
	schemaPath := filepath.Join("schema.json")
	if _, err := os.Stat(schemaPath); err != nil {
		schemaPath = filepath.Join("internal", "query", "export", "schema.json")
		if _, err2 := os.Stat(schemaPath); err2 != nil {
			t.Fatalf("schema.json not found: %v / %v", err, err2)
		}
	}
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema invalid json: %v", err)
	}
	absSchemaPath, err := filepath.Abs(schemaPath)
	if err != nil {
		t.Fatalf("abs schema path: %v", err)
	}
	schemaLoader := gojsonschema.NewReferenceLoader("file://" + absSchemaPath)
	documentLoader := gojsonschema.NewBytesLoader(out)
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		t.Fatalf("schema validation error: %v", err)
	}
	if !result.Valid() {
		var msgs []string
		for _, e := range result.Errors() {
			msgs = append(msgs, e.String())
		}
		t.Fatalf("export json does not validate against schema.json: %s\npayload: %s", strings.Join(msgs, "; "), string(out))
	}
	invalidPayload := map[string]any{
		"run_id": "x",
	}
	invalidBytes, _ := json.Marshal(invalidPayload)
	invalidLoader := gojsonschema.NewBytesLoader(invalidBytes)
	invalidResult, err := gojsonschema.Validate(schemaLoader, invalidLoader)
	if err != nil {
		t.Fatalf("invalid payload validation error: %v", err)
	}
	if invalidResult.Valid() {
		t.Fatalf("schema validator must reject payload missing required fields")
	}
}

func TestByteIdentical(t *testing.T) {
	ctx := context.Background()
	st, runID := fixtureRun(t)
	out1, err := Export(ctx, st, runID, "json")
	if err != nil {
		t.Fatal(err)
	}
	out2, err := Export(ctx, st, runID, "json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out1, out2) {
		t.Fatalf("json export not byte-identical:\nfirst: %s\nsecond: %s", out1, out2)
	}
	m1, _ := Export(ctx, st, runID, "mermaid")
	m2, _ := Export(ctx, st, runID, "mermaid")
	if !bytes.Equal(m1, m2) {
		t.Fatalf("mermaid export not byte-identical")
	}
}

func TestReadOnlyNoWrites(t *testing.T) {
	ctx := context.Background()
	st, runID := fixtureRun(t)
	beforeDigest, err := st.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	beforeEvents, _ := countEvents(t, st, runID)
	_, err = Export(ctx, st, runID, "json")
	if err != nil {
		t.Fatal(err)
	}
	_, err = Export(ctx, st, runID, "mermaid")
	if err != nil {
		t.Fatal(err)
	}
	afterDigest, err := st.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	afterEvents, _ := countEvents(t, st, runID)
	if beforeEvents != afterEvents {
		t.Fatalf("export mutated events: before %d after %d", beforeEvents, afterEvents)
	}
	if beforeDigest != afterDigest {
		t.Fatalf("export mutated projections: digest before %q after %q", beforeDigest, afterDigest)
	}
}

func countEvents(t *testing.T, st *store.Store, runID string) (int, error) {
	t.Helper()
	var c int
	err := st.DB().QueryRowContext(context.Background(), "SELECT COUNT(*) FROM event WHERE run_id = ?", runID).Scan(&c)
	return c, err
}

func TestUnknownRunAndFormat(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	_, err := Export(ctx, st, "nonexistent-run-id", "json")
	if err == nil || !strings.Contains(err.Error(), "RUN_NOT_FOUND") {
		t.Fatalf("expected RUN_NOT_FOUND for unknown run, got %v", err)
	}
	_, err = Export(ctx, st, "any", "xml")
	if err == nil || !strings.Contains(err.Error(), "GRAPH_INVALID") {
		t.Fatalf("expected GRAPH_INVALID for unknown format, got %v", err)
	}
	// Unknown format must not have touched the store for run existence: we already validated format before run lookup,
	// so the error should be GRAPH_INVALID even if run also unknown — format check wins.
	_, err = Export(ctx, st, "nonexistent", "badformat")
	if err == nil || !strings.Contains(strings.ToUpper(err.Error()), "GRAPH_INVALID") && !strings.Contains(err.Error(), "unknown format") {
		t.Fatalf("expected format error, got %v", err)
	}
}

func TestLargeRun(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	graphYAML := "schema: proceed/v1\nname: large-run\nnodes:\n"
	for i := 0; i < 20; i++ {
		graphYAML += fmt.Sprintf("  - id: n%d\n    type: task\n    contract: pure\n    executor: { kind: shell, command: [\"echo\", \"hi\"] }\n", i)
	}
	graphYAML += "edges:\n"
	for i := 0; i < 19; i++ {
		graphYAML += fmt.Sprintf("  - { from: n%d, to: n%d, type: depends_on }\n", i, i+1)
	}
	src := []byte(graphYAML)
	doc, _ := compiler.Parse(src)
	_ = compiler.Validate(doc)
	frozen, _ := st.FreezeDefinition(ctx, "large.yaml", src, doc)
	run, _ := st.CreateRun(ctx, frozen.GraphVersionID)
	out, err := Export(ctx, st, run.ID, "json")
	if err != nil {
		t.Fatalf("large run export: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("large run export empty")
	}
	var v map[string]any
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatalf("large run json invalid: %v", err)
	}
	if nodes, ok := v["nodes"].([]any); !ok || len(nodes) != 20 {
		t.Fatalf("expected 20 nodes, got %d", len(nodes))
	}
}

func TestInFlightSnapshot(t *testing.T) {
	ctx := context.Background()
	st, runID := fixtureRun(t)
	// Export while run is still "running" should succeed and show snapshot
	out, err := Export(ctx, st, runID, "json")
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	_ = json.Unmarshal(out, &v)
	if v["run_id"] != runID {
		t.Fatalf("run_id mismatch")
	}
}

func insertExportFixtureGraph(tb testing.TB, st *store.Store, nodes []string, edges [][4]string) (string, string) {
	tb.Helper()
	ctx := context.Background()
	db := st.DB()
	graphID, versionID := stID(), stID()
	if _, err := db.ExecContext(ctx, "INSERT INTO graph (id, name) VALUES (?, ?)", graphID, "export-fixture-graph"); err != nil {
		tb.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO graph_version (id, graph_id, definition_digest, source_schema_version, compiled_schema_version, source_metadata, extras, status, created_at) VALUES (?, ?, ?, 'proceed/v1', 'v1', '{}', '{}', 'frozen', ?)", versionID, graphID, "digest-"+versionID, time.Now().UnixMilli()); err != nil {
		tb.Fatal(err)
	}
	for _, k := range nodes {
		if _, err := db.ExecContext(ctx, "INSERT INTO graph_node (id, graph_version_id, node_key, type, config) VALUES (?, ?, ?, 'task', '{}')", stID(), versionID, k); err != nil {
			tb.Fatal(err)
		}
	}
	for _, e := range edges {
		cond := e[3]
		if _, err := db.ExecContext(ctx, "INSERT INTO graph_edge (id, graph_version_id, from_node_key, to_node_key, type, condition, extras) VALUES (?, ?, ?, ?, 'routes_to', ?, '{}')", e[0], versionID, e[1], e[2], cond); err != nil {
			tb.Fatal(err)
		}
	}
	return versionID, graphID
}

func TestCycleEdgeTraversals(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	versionID, _ := insertExportFixtureGraph(t, st,
		[]string{"a", "b"},
		[][4]string{{"edge-loop", "a", "a", "success"}},
	)
	db := st.DB()
	runID := stID()
	if _, err := db.ExecContext(ctx, "INSERT INTO graph_run (id, graph_version_id, definition_digest, status, created_at) VALUES (?, ?, 'digest', 'completed', ?)", runID, versionID, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	for _, trav := range [][2]string{{"success", "1"}, {"failure", "2"}} {
		if _, err := db.ExecContext(ctx, "INSERT INTO run_edge (id, run_id, edge_id, route, sequence_in_run, traversed_at) VALUES (?, ?, 'edge-loop', ?, ?, ?)", stID(), runID, trav[0], trav[1], 1); err != nil {
			t.Fatal(err)
		}
	}

	out, err := Export(ctx, st, runID, "json")
	if err != nil {
		t.Fatalf("json export: %v", err)
	}
	var v map[string]any
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatal(err)
	}
	edges, _ := v["edges"].([]any)
	if len(edges) != 1 {
		t.Fatalf("expected 1 edge entry for a definition edge traversed twice, got %d: %s", len(edges), string(out))
	}
	e, _ := edges[0].(map[string]any)
	if e["from"] != "a" || e["to"] != "a" || e["traversed"] != true {
		t.Fatalf("unexpected edge entry: %+v", e)
	}
	if e["route"] != "success,failure" {
		t.Fatalf("route must list distinct traversals in traversal order, got %q", e["route"])
	}

	m, err := Export(ctx, st, runID, "mermaid")
	if err != nil {
		t.Fatalf("mermaid export: %v", err)
	}
	s := string(m)
	want := "  a -->|success,failure| a\n"
	if strings.Count(s, "a -->|") != 1 {
		t.Fatalf("expected exactly one edge line for the loop, got:\n%s", s)
	}
	if !strings.Contains(s, want) {
		t.Fatalf("expected edge line %q in:\n%s", want, s)
	}
}

func TestMermaidDistinctKeysNoCollision(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	versionID, _ := insertExportFixtureGraph(t, st,
		[]string{"a-b", "a_b"},
		nil,
	)
	db := st.DB()
	runID := stID()
	if _, err := db.ExecContext(ctx, "INSERT INTO graph_run (id, graph_version_id, definition_digest, status, created_at) VALUES (?, ?, 'digest', 'running', ?)", runID, versionID, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	out, err := Export(ctx, st, runID, "mermaid")
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	nodeLines := 0
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, "[\"") {
			nodeLines++
		}
	}
	if nodeLines != 2 {
		t.Fatalf("distinct keys a-b and a_b must render as 2 nodes, got %d:\n%s", nodeLines, s)
	}
	if !strings.Contains(s, "  a_b[\"") || !strings.Contains(s, "  a_b_2[\"") {
		t.Fatalf("collision must be resolved with deterministic suffixes:\n%s", s)
	}
}

func BenchmarkExportLargeRun(b *testing.B) {
	ctx := context.Background()
	st := openTestStore(b)
	const n = 5000
	nodes := make([]string, n)
	for i := range nodes {
		nodes[i] = fmt.Sprintf("n%d", i)
	}
	edges := make([][4]string, 0, n-1)
	edgeIDs := make([]string, 0, n-1)
	for i := 0; i < n-1; i++ {
		edgeIDs = append(edgeIDs, stID())
		edges = append(edges, [4]string{edgeIDs[i], nodes[i], nodes[i+1], ""})
	}
	versionID, _ := insertExportFixtureGraph(b, st, nodes, edges)
	db := st.DB()
	runID := stID()
	if _, err := db.ExecContext(ctx, "INSERT INTO graph_run (id, graph_version_id, definition_digest, status, created_at) VALUES (?, ?, 'digest', 'completed', ?)", runID, versionID, time.Now().UnixMilli()); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := Export(ctx, st, runID, "json")
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("empty export")
		}
	}
}
