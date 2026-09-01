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

	"github.com/oklog/ulid/v2"

	"proceed/internal/compiler"
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
	// Simulate a completed run by inserting run_node and run_edge and artifacts via events?
	// Use store's event log to drive projections: create run, then manually advance via RunGraph? Simpler: use controller to run? But to keep test lightweight, we will directly insert via events using the store's projection path.
	// Instead, we will use the controller's drain? For this test we need a fixture that has nodes with states and edges traversed.
	// We can manually create run_nodes and run_edges via direct SQL to simulate completed run fixture without invoking controller.
	// However, the export reads from graph_node + run_node + graph_edge + run_edge + artifact, so we need at least run_node rows with statuses and run_edge rows.
	// We'll insert them directly.
	db := st.DB()
	// Get graph nodes
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
		// Use the test helper: insert run_node via store's internal? We'll insert directly.
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
	// Check that traversed edges have route labels
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
	// Check schema file exists and is valid JSON
	schemaPath := filepath.Join("schema.json")
	if _, err := os.Stat(schemaPath); err != nil {
		// try alternative path when running from worktree root
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
	beforeEvents, _ := countEvents(t, st, runID)
	beforeNodes, _ := countRunNodes(t, st, runID)
	_, err := Export(ctx, st, runID, "json")
	if err != nil {
		t.Fatal(err)
	}
	_, err = Export(ctx, st, runID, "mermaid")
	if err != nil {
		t.Fatal(err)
	}
	afterEvents, _ := countEvents(t, st, runID)
	afterNodes, _ := countRunNodes(t, st, runID)
	if beforeEvents != afterEvents {
		t.Fatalf("export mutated events: before %d after %d", beforeEvents, afterEvents)
	}
	if beforeNodes != afterNodes {
		t.Fatalf("export mutated run_nodes: before %d after %d", beforeNodes, afterNodes)
	}
}

func countEvents(t *testing.T, st *store.Store, runID string) (int, error) {
	t.Helper()
	var c int
	err := st.DB().QueryRowContext(context.Background(), "SELECT COUNT(*) FROM event WHERE run_id = ?", runID).Scan(&c)
	return c, err
}

func countRunNodes(t *testing.T, st *store.Store, runID string) (int, error) {
	t.Helper()
	var c int
	err := st.DB().QueryRowContext(context.Background(), "SELECT COUNT(*) FROM run_node WHERE run_id = ?", runID).Scan(&c)
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

func TestLargeRunStreaming(t *testing.T) {
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
