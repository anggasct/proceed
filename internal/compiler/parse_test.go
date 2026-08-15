package compiler

import (
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func requireRule(t *testing.T, err error, rule, location string) {
	t.Helper()
	ge, ok := AsGraphInvalid(err)
	if !ok {
		t.Fatalf("expected %s error, got %v", CodeGraphInvalid, err)
	}
	for _, d := range ge.Diagnostics {
		if d.Rule == rule && (location == "" || d.Location == location) {
			return
		}
	}
	t.Fatalf("no diagnostic %s at %q in %v", rule, location, ge.Diagnostics)
}

func TestParseCanonicalFixture(t *testing.T) {
	doc, err := Parse(mustRead(t, "testdata/customer-research.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Schema != "proceed/v1" {
		t.Errorf("schema = %q", doc.Schema)
	}
	if doc.Name != "customer-research" {
		t.Errorf("name = %q", doc.Name)
	}
	if len(doc.Nodes) != 6 {
		t.Fatalf("nodes = %d, want 6", len(doc.Nodes))
	}
	if len(doc.Edges) != 7 {
		t.Fatalf("edges = %d, want 7", len(doc.Edges))
	}
	if doc.Extras != nil {
		t.Errorf("unexpected document extras: %v", doc.Extras)
	}

	byID := map[string]*Node{}
	for i := range doc.Nodes {
		byID[doc.Nodes[i].ID] = &doc.Nodes[i]
	}
	classify := byID["classify"]
	if classify.Type != "model" || classify.Executor.Kind != "agent_cli" || classify.Contract != "pure" {
		t.Errorf("classify = %+v", classify)
	}
	if !reflect.DeepEqual(classify.Executor.Args, []string{"classify"}) {
		t.Errorf("classify args = %v", classify.Executor.Args)
	}
	github := byID["github_search"]
	if github.TimeoutMs != 600000 {
		t.Errorf("github_search timeout_ms = %d", github.TimeoutMs)
	}
	if cap_ := github.Capability; cap_ == nil || cap_.Filesystem != "workspace-read" ||
		cap_.Network == nil || !reflect.DeepEqual(cap_.Network.AllowlistedHosts, []string{"api.github.com"}) {
		t.Errorf("github_search capability = %+v", github.Capability)
	}
	gate := byID["human_approval"]
	if gate.Type != "gate" || gate.Executor != nil {
		t.Errorf("human_approval = %+v", gate)
	}

	revision := doc.Edges[5]
	if revision.From != "verify" || revision.To != "synthesize" || revision.Type != "routes_to" ||
		revision.When != "needs_revision" || !revision.HasMaxTraversals || revision.MaxTraversals != 2 {
		t.Errorf("revision edge = %+v", revision)
	}
	produce := doc.Edges[2]
	if produce.Type != "produces" || produce.Artifact != "research_context" || !produce.HasArtifact {
		t.Errorf("produce edge = %+v", produce)
	}
}

func TestParseMissingSchema(t *testing.T) {
	_, err := Parse(mustRead(t, "testdata/invalid/missing-schema.yaml"))
	if err == nil {
		t.Fatal("expected error")
	}
	requireRule(t, err, RuleSchema, "schema")
	if !strings.Contains(err.Error(), "schema") {
		t.Errorf("error must name the missing field: %v", err)
	}
}

func TestParseWrongSchemaValue(t *testing.T) {
	src := "schema: proceed/v2\nname: x\nnodes: []\nedges: []\n"
	_, err := Parse([]byte(src))
	requireRule(t, err, RuleSchema, "schema")
}

func TestParseUnknownTopLevelField(t *testing.T) {
	_, err := Parse(mustRead(t, "testdata/invalid/unknown-field.yaml"))
	requireRule(t, err, RuleUnknownField, "ttl")
}

func TestParseUnknownFieldsAtEveryLevel(t *testing.T) {
	cases := []struct {
		name     string
		doc      string
		location string
	}{
		{
			name: "node",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
    bogus: 1
edges: []
`,
			location: "nodes[0].bogus",
		},
		{
			name: "edge",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
edges:
  - from: a
    to: a
    type: depends_on
    bogus: 1
`,
			location: "edges[0].bogus",
		},
		{
			name: "executor",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
    executor: { kind: shell, command: [bin/run], bogus: 1 }
edges: []
`,
			location: "nodes[0].executor.bogus",
		},
		{
			name: "executor field of other kind",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
    executor: { kind: shell, command: [bin/run], url: https://x.example }
edges: []
`,
			location: "nodes[0].executor.url",
		},
		{
			name: "unknown executor kind",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
    executor: { kind: smtp, host: mta.example }
edges: []
`,
			location: "nodes[0].executor.kind",
		},
		{
			name: "capability",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
    capability: { bogus: 1 }
edges: []
`,
			location: "nodes[0].capability.bogus",
		},
		{
			name: "network",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
    capability: { network: { allowlisted_hosts: [a.example], bogus: 1 } }
edges: []
`,
			location: "nodes[0].capability.network.bogus",
		},
		{
			name: "retry",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
    retry: { max_attempts: 2, bogus: 1 }
edges: []
`,
			location: "nodes[0].retry.bogus",
		},
		{
			name: "policy",
			doc: `schema: proceed/v1
name: d
nodes: []
edges: []
policies:
  - name: p
    kind: retry
    rule: { x: 1 }
    bogus: 1
`,
			location: "policies[0].bogus",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			if err == nil {
				t.Fatal("expected error")
			}
			requireRule(t, err, RuleUnknownField, tc.location)
		})
	}
}

func TestParseExtensionFieldsAcceptedAndPreserved(t *testing.T) {
	src := `schema: proceed/v1
name: extensions
x-tier: internal
nodes:
  - id: a
    type: task
    terminal: true
    x-owner: ops
    executor: { kind: shell, command: [bin/run], x-cmd-id: "7" }
edges:
  - from: a
    to: a
    type: depends_on
    x-note: self
`
	doc, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Extras["x-tier"].Value != "internal" {
		t.Errorf("document x-tier = %v", doc.Extras["x-tier"])
	}
	if doc.Nodes[0].Extras["x-owner"].Value != "ops" {
		t.Errorf("node x-owner = %v", doc.Nodes[0].Extras["x-owner"])
	}
	if doc.Nodes[0].Executor.Extras["x-cmd-id"].Value != "7" {
		t.Errorf("executor x-cmd-id = %v", doc.Nodes[0].Executor.Extras["x-cmd-id"])
	}
	if doc.Edges[0].Extras["x-note"].Value != "self" {
		t.Errorf("edge x-note = %v", doc.Edges[0].Extras["x-note"])
	}
}

func TestParseJSONMatchesYAML(t *testing.T) {
	yamlSrc := `schema: proceed/v1
name: equiv
nodes:
  - id: a
    type: task
    terminal: true
    executor: { kind: shell, command: [bin/run] }
    contract: pure
edges: []
`
	jsonSrc := `{"edges":[],"name":"equiv","nodes":[{"contract":"pure","executor":{"command":["bin/run"],"kind":"shell"},"id":"a","terminal":true,"type":"task"}],"schema":"proceed/v1"}`
	fromYAML, err := Parse([]byte(yamlSrc))
	if err != nil {
		t.Fatal(err)
	}
	fromJSON, err := Parse([]byte(jsonSrc))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fromYAML, fromJSON) {
		t.Errorf("yaml and json parses differ:\n%+v\n%+v", fromYAML, fromJSON)
	}
}

func TestParseTypeMismatches(t *testing.T) {
	cases := []struct {
		name string
		doc  string
	}{
		{name: "nodes not a list", doc: "schema: proceed/v1\nname: x\nnodes: 3\nedges: []\n"},
		{name: "timeout_ms not an integer", doc: "schema: proceed/v1\nname: x\nnodes:\n  - id: a\n    type: task\n    timeout_ms: fast\nedges: []\n"},
		{name: "terminal not a boolean", doc: "schema: proceed/v1\nname: x\nnodes:\n  - id: a\n    type: task\n    terminal: maybe\nedges: []\n"},
		{name: "node not a mapping", doc: "schema: proceed/v1\nname: x\nnodes:\n  - a\nedges: []\n"},
		{name: "duplicate node field", doc: "schema: proceed/v1\nname: x\nnodes:\n  - id: a\n    id: b\n    type: task\nedges: []\n"},
		{name: "empty document", doc: ""},
		{name: "syntax error", doc: "a: [unclosed\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			if err == nil {
				t.Fatal("expected error")
			}
			requireRule(t, err, RuleParse, "")
		})
	}
}

func FuzzParse(f *testing.F) {
	for _, path := range []string{
		"testdata/customer-research.yaml",
		"testdata/invalid/missing-schema.yaml",
		"testdata/invalid/unknown-field.yaml",
	} {
		b, err := os.ReadFile(path)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Add([]byte(""))
	f.Add([]byte(":"))
	f.Add([]byte("\t"))
	f.Add([]byte("a: ["))
	f.Add([]byte("&a *a"))
	f.Add([]byte("schema: 1\nname: x\nnodes: 3\nedges: []"))
	f.Add([]byte("nodes:\n  - id: a\n    type: task"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, err := Parse(data)
		if err == nil {
			return
		}
		var ge *Error
		if !errors.As(err, &ge) || ge.Code != CodeGraphInvalid {
			t.Fatalf("non-graph-invalid error: %v", err)
		}
	})
}
