package compiler

import (
	"strings"
	"testing"
)

func validateSrc(t *testing.T, src string) (*Document, error) {
	t.Helper()
	doc, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	return doc, Validate(doc)
}

func TestValidateCanonicalFixturePasses(t *testing.T) {
	doc, err := Parse(mustRead(t, "testdata/customer-research.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(doc); err != nil {
		t.Fatalf("canonical fixture must validate: %v", err)
	}
}

func TestValidateInvalidFixtureCatalogue(t *testing.T) {
	cases := []struct {
		fixture string
		rule    string
	}{
		{"duplicate-node-id.yaml", RuleDuplicateNode},
		{"dangling-edge.yaml", RuleDanglingEdge},
		{"untyped-edge.yaml", RuleEdgeType},
		{"when-on-depends.yaml", RuleWhenPlacement},
		{"unbounded-cycle.yaml", RuleUnboundedCycle},
		{"self-verifying.yaml", RuleSelfVerifier},
		{"sink-without-terminal.yaml", RuleSinkTerminal},
		{"no-contract.yaml", RuleContractRequired},
		{"bad-node-type.yaml", RuleNodeType},
		{"traversal-on-acyclic.yaml", RuleAcyclicTraversal},
		{"artifact-on-routes.yaml", RuleArtifactEdge},
	}
	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			doc, err := Parse(mustRead(t, "testdata/invalid/"+tc.fixture))
			if err != nil {
				t.Fatal(err)
			}
			verr := Validate(doc)
			if verr == nil {
				t.Fatalf("%s must fail validation", tc.fixture)
			}
			requireRule(t, verr, tc.rule, "")
		})
	}
}

func TestValidateUnboundedCycleListsNodeIds(t *testing.T) {
	doc, err := Parse(mustRead(t, "testdata/invalid/unbounded-cycle.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	verr := Validate(doc)
	ge, ok := AsGraphInvalid(verr)
	if !ok {
		t.Fatalf("expected graph invalid, got %v", verr)
	}
	var found bool
	for _, d := range ge.Diagnostics {
		if d.Rule == RuleUnboundedCycle {
			found = true
			if !strings.Contains(d.Message, "classify") || !strings.Contains(d.Message, "verify") {
				t.Errorf("cycle diagnostic must list node ids: %s", d.Message)
			}
		}
	}
	if !found {
		t.Fatalf("no cycle diagnostic in %v", ge.Diagnostics)
	}
}

func TestValidateBoundedCyclePasses(t *testing.T) {
	src := `schema: proceed/v1
name: bounded
nodes:
  - id: classify
    type: model
    executor: { kind: agent_cli, cli: opencode, args: [classify] }
    contract: pure
  - id: verify
    type: verifier
    executor: { kind: shell, command: [scripts/verify] }
    contract: pure
  - id: human_approval
    type: gate
    terminal: true
edges:
  - { from: classify, to: verify, type: depends_on }
  - { from: verify, to: classify, type: routes_to, when: needs_revision, max_traversals: 2 }
`
	if _, err := validateSrc(t, src); err != nil {
		t.Fatal(err)
	}
}

func TestValidateBoundedSelfLoopOnNonVerifierPasses(t *testing.T) {
	src := `schema: proceed/v1
name: bounded-self-loop
nodes:
  - id: retry_step
    type: task
    executor: { kind: shell, command: [scripts/maybe] }
    contract: idempotent
    terminal: true
edges:
  - { from: retry_step, to: retry_step, type: routes_to, when: again, max_traversals: 3 }
`
	if _, err := validateSrc(t, src); err != nil {
		t.Fatal(err)
	}
}

func TestValidateUnboundedSelfLoopRejected(t *testing.T) {
	src := `schema: proceed/v1
name: unbounded-self-loop
nodes:
  - id: retry_step
    type: task
    executor: { kind: shell, command: [scripts/maybe] }
    contract: idempotent
    terminal: true
edges:
  - { from: retry_step, to: retry_step, type: routes_to, when: again }
`
	_, err := validateSrc(t, src)
	requireRule(t, err, RuleUnboundedCycle, "")
}

func TestValidateBoundedCycleWithBoundOnEitherEdge(t *testing.T) {
	src := `schema: proceed/v1
name: either-edge
nodes:
  - id: build
    type: task
    executor: { kind: shell, command: [scripts/build] }
    contract: pure
  - id: test
    type: task
    executor: { kind: shell, command: [scripts/test] }
    contract: pure
  - id: human_approval
    type: gate
    terminal: true
edges:
  - { from: build, to: test, type: routes_to, when: built, max_traversals: 2 }
  - { from: test, to: build, type: routes_to, when: failed }
  - { from: test, to: human_approval, type: routes_to, when: passed }
`
	if _, err := validateSrc(t, src); err != nil {
		t.Fatalf("cycle bounded by one edge must pass: %v", err)
	}
}

func TestValidatePartiallyBoundedSCCRejected(t *testing.T) {
	src := `schema: proceed/v1
name: two-cycles
nodes:
  - id: a
    type: task
    executor: { kind: shell, command: [scripts/a] }
    contract: pure
  - id: b
    type: task
    executor: { kind: shell, command: [scripts/b] }
    contract: pure
  - id: c
    type: task
    executor: { kind: shell, command: [scripts/c] }
    contract: pure
  - id: human_approval
    type: gate
    terminal: true
edges:
  - { from: a, to: b, type: routes_to, when: x, max_traversals: 2 }
  - { from: b, to: a, type: routes_to, when: y }
  - { from: b, to: c, type: routes_to, when: z }
  - { from: c, to: b, type: routes_to, when: w }
  - { from: c, to: human_approval, type: routes_to, when: done }
`
	_, err := validateSrc(t, src)
	requireRule(t, err, RuleUnboundedCycle, "")
}

func TestValidateMultipleSCCs(t *testing.T) {
	src := `schema: proceed/v1
name: two-components
nodes:
  - id: a
    type: task
    executor: { kind: shell, command: [scripts/a] }
    contract: pure
  - id: b
    type: task
    executor: { kind: shell, command: [scripts/b] }
    contract: pure
  - id: c
    type: task
    executor: { kind: shell, command: [scripts/c] }
    contract: pure
  - id: d
    type: task
    executor: { kind: shell, command: [scripts/d] }
    contract: pure
    terminal: true
edges:
  - { from: a, to: b, type: routes_to, when: x, max_traversals: 2 }
  - { from: b, to: a, type: routes_to, when: y }
  - { from: c, to: d, type: depends_on }
`
	if _, err := validateSrc(t, src); err != nil {
		t.Fatal(err)
	}
}

func TestValidateSinkRules(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		rule string
	}{
		{
			name: "empty edges with multiple nodes",
			doc: `schema: proceed/v1
name: multi
nodes:
  - id: a
    type: task
    executor: { kind: shell, command: [scripts/a] }
    contract: pure
    terminal: true
  - id: b
    type: task
    executor: { kind: shell, command: [scripts/b] }
    contract: pure
    terminal: true
edges: []
`,
			rule: RuleSinkTerminal,
		},
		{
			name: "gate sink without terminal is legal",
			doc: `schema: proceed/v1
name: gate-sink
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [scripts/work] }
    contract: pure
  - id: human_approval
    type: gate
edges:
  - { from: work, to: human_approval, type: routes_to, when: done }
`,
			rule: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateSrc(t, tc.doc)
			if tc.rule == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			requireRule(t, err, tc.rule, "")
		})
	}
}

func TestValidateContractAndExecutorShape(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		rule string
	}{
		{
			name: "executable node without executor",
			doc: `schema: proceed/v1
name: noexec
nodes:
  - id: only
    type: task
    contract: pure
    terminal: true
edges: []
`,
			rule: RuleContractRequired,
		},
		{
			name: "unknown contract value",
			doc: `schema: proceed/v1
name: badcontract
nodes:
  - id: only
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: sometimes
    terminal: true
edges: []
`,
			rule: RuleContractRequired,
		},
		{
			name: "router without executor is legal",
			doc: `schema: proceed/v1
name: router
nodes:
  - id: route
    type: router
  - id: only
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
edges:
  - { from: route, to: only, type: routes_to, when: go }
`,
			rule: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateSrc(t, tc.doc)
			if tc.rule == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			requireRule(t, err, tc.rule, "")
		})
	}
}

func TestValidateDiagnosticsAreSourceOrdered(t *testing.T) {
	doc, err := Parse(mustRead(t, "testdata/invalid/duplicate-node-id.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	verr := Validate(doc)
	ge, ok := AsGraphInvalid(verr)
	if !ok {
		t.Fatal(verr)
	}
	if len(ge.Diagnostics) < 1 {
		t.Fatal("expected diagnostics")
	}
	for i := 1; i < len(ge.Diagnostics); i++ {
		if ge.Diagnostics[i-1].Location > ge.Diagnostics[i].Location &&
			ge.Diagnostics[i-1].Rule == ge.Diagnostics[i].Rule {
			t.Errorf("diagnostics not ordered: %v", ge.Diagnostics)
		}
	}
}
