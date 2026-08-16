package compiler

import "testing"

func TestValidateMissingEdgesFieldRejected(t *testing.T) {
	src := `schema: proceed/v1
name: missing-edges
nodes:
  - id: only
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
`
	doc, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	verr := Validate(doc)
	if verr == nil {
		t.Fatal("missing edges field must be rejected")
	}
	requireRule(t, verr, RuleParse, "edges")
}

func TestValidateExplicitEmptyEdgesStillLegal(t *testing.T) {
	src := `schema: proceed/v1
name: empty-edges
nodes:
  - id: only
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
edges: []
`
	doc, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(doc); err != nil {
		t.Fatalf("explicit edges: [] with single terminal node must pass: %v", err)
	}
}

func TestValidateEmptyNodeIDRejected(t *testing.T) {
	src := `schema: proceed/v1
name: empty-id
nodes:
  - id: ""
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
edges: []
`
	doc, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	verr := Validate(doc)
	if verr == nil {
		t.Fatal("empty node id must be rejected")
	}
	requireRule(t, verr, RuleParse, "nodes[0].id")
}

func TestValidateIncompletePolicyRejected(t *testing.T) {
	cases := []struct {
		name     string
		policy   string
		location string
	}{
		{"missing name", `kind: retry
    rule: { max_attempts: 2 }`, "policies[0].name"},
		{"missing kind", `name: p
    rule: { max_attempts: 2 }`, "policies[0].kind"},
		{"missing rule", `name: p
    kind: retry`, "policies[0].rule"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := `schema: proceed/v1
name: policy-shape
nodes:
  - id: only
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
edges: []
policies:
  - ` + tc.policy + "\n"
			doc, err := Parse([]byte(src))
			if err != nil {
				t.Fatal(err)
			}
			verr := Validate(doc)
			if verr == nil {
				t.Fatal("incomplete policy must be rejected")
			}
			requireRule(t, verr, RuleParse, tc.location)
		})
	}
}

func TestValidateGateWithExecutorRejected(t *testing.T) {
	src := `schema: proceed/v1
name: gate-exec
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
  - id: approval
    type: gate
    executor: { kind: shell, command: [bin/never] }
    contract: pure
edges:
  - { from: work, to: approval, type: routes_to, when: done }
`
	doc, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	verr := Validate(doc)
	if verr == nil {
		t.Fatal("gate with executor must be rejected")
	}
	requireRule(t, verr, RuleParse, "nodes[1].executor")
}
