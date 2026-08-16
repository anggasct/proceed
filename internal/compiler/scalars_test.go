package compiler

import "testing"

func TestValidateNonStringScalarsRejected(t *testing.T) {
	cases := []struct {
		name     string
		doc      string
		location string
	}{
		{
			name:     "numeric document name",
			doc:      "schema: proceed/v1\nname: 123\nnodes: []\nedges: []\n",
			location: "name",
		},
		{
			name: "numeric executor method",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: call
    type: tool
    executor: { kind: http, method: 123, url: "https://a.example/f" }
    contract: idempotent
    terminal: true
    capability: { network: { allowlisted_hosts: [a.example] } }
edges: []
`,
			location: "nodes[0].executor.method",
		},
		{
			name: "numeric node id",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: 42
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
edges: []
`,
			location: "nodes[0].id",
		},
		{
			name: "numeric edge endpoint",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
    terminal: true
edges:
  - { from: a, to: 7, type: depends_on }
`,
			location: "edges[0].to",
		},
		{
			name: "boolean when label",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
    terminal: true
edges:
  - { from: a, to: a, type: routes_to, when: true, max_traversals: 2 }
`,
			location: "edges[0].when",
		},
		{
			name: "numeric command token",
			doc: `schema: proceed/v1
name: d
nodes:
  - id: a
    type: task
    executor: { kind: shell, command: 123 }
    contract: pure
    terminal: true
edges: []
`,
			location: "nodes[0].executor.command",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			if err == nil {
				t.Fatal("expected error")
			}
			requireRule(t, err, RuleParse, tc.location)
		})
	}
}

func TestValidateEmptyCapabilityEnumsRejected(t *testing.T) {
	cases := []struct {
		name     string
		cap      string
		location string
	}{
		{"empty filesystem", `filesystem: ""`, "nodes[0].capability.filesystem"},
		{"empty process", `process: ""`, "nodes[0].capability.process"},
		{"empty human", `human: ""`, "nodes[0].capability.human"},
		{"empty secrets", `secrets: ""`, "nodes[0].capability.secrets"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := Parse([]byte("schema: proceed/v1\nname: caps\nnodes:\n  - id: work\n    type: task\n    executor: { kind: shell, command: [bin/do] }\n    contract: pure\n    terminal: true\n    capability: { " + tc.cap + " }\nedges: []\n"))
			if err != nil {
				t.Fatal(err)
			}
			verr := Validate(doc)
			if verr == nil {
				t.Fatalf("%s must be rejected", tc.cap)
			}
			requireRule(t, verr, RuleParse, tc.location)
		})
	}
}

func TestValidateOmittedCapabilityFieldsStillLegal(t *testing.T) {
	doc, err := Parse([]byte("schema: proceed/v1\nname: caps\nnodes:\n  - id: work\n    type: task\n    executor: { kind: shell, command: [bin/do] }\n    contract: pure\n    terminal: true\n    capability: {}\nedges: []\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(doc); err != nil {
		t.Fatalf("omitted capability fields must default to none: %v", err)
	}
}
