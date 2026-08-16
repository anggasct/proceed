package compiler

import (
	"fmt"
	"testing"
)

const (
	taskNode     = "id: work\n    type: task\n    executor: { kind: shell, command: [bin/do] }\n    contract: pure"
	verifierNode = "id: check\n    type: verifier\n    executor: { kind: shell, command: [scripts/verify] }\n    contract: pure"
	gateNode     = "id: approval\n    type: gate"
	routerNode   = "id: route\n    type: router"
)

func nodeBlock(node, suffix string) string {
	if suffix == "" {
		return node
	}
	return node + "\n    " + suffix
}

func legalityDoc(from, to, edgeType string) string {
	return fmt.Sprintf(`schema: proceed/v1
name: legality
nodes:
  - %s
  - %s
edges:
  - from: %s
    to: %s
    type: %s
`, nodeBlock(from, ""), nodeBlock(to, "terminal: true"), firstField(from), firstField(to), edgeType)
}

func firstField(node string) string {
	for _, line := range splitLines(node) {
		if len(line) > 4 && line[:4] == "id: " {
			return line[4:]
		}
	}
	return ""
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

func TestValidateEdgeLegalityMatrix(t *testing.T) {
	cases := []struct {
		name   string
		from   string
		to     string
		edge   string
		reject bool
	}{
		{"verifies from verifier", verifierNode, taskNode, "verifies", false},
		{"verifies from task", taskNode, taskNode, "verifies", true},
		{"verifies from gate", gateNode, taskNode, "verifies", true},
		{"approves from gate", gateNode, taskNode, "approves", false},
		{"approves from verifier", verifierNode, taskNode, "approves", true},
		{"routes_to from task", taskNode, gateNode, "routes_to", false},
		{"routes_to from verifier", verifierNode, gateNode, "routes_to", false},
		{"routes_to from gate", gateNode, taskNode, "routes_to", true},
		{"routes_to from router", routerNode, taskNode, "routes_to", false},
		{"depends_on executable pair", taskNode, verifierNode, "depends_on", false},
		{"depends_on to gate", taskNode, gateNode, "depends_on", true},
		{"depends_on from gate", gateNode, taskNode, "depends_on", true},
		{"depends_on to router", taskNode, routerNode, "depends_on", true},
		{"produces executable pair", taskNode, verifierNode, "produces", false},
		{"produces to gate", taskNode, gateNode, "produces", true},
		{"consumes executable pair", verifierNode, taskNode, "consumes", false},
		{"consumes from gate", gateNode, taskNode, "consumes", true},
		{"defines unrestricted pair", gateNode, taskNode, "defines", false},
		{"derived_from unrestricted pair", gateNode, taskNode, "derived_from", false},
		{"blocks unrestricted pair", gateNode, taskNode, "blocks", false},
		{"measures unrestricted pair", gateNode, taskNode, "measures", false},
		{"improves unrestricted pair", gateNode, taskNode, "improves", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := Parse([]byte(legalityDoc(tc.from, tc.to, tc.edge)))
			if err != nil {
				t.Fatal(err)
			}
			verr := Validate(doc)
			if tc.reject && verr == nil {
				t.Fatal("expected legality rejection")
			}
			if !tc.reject && verr != nil {
				t.Fatalf("expected pass: %v", verr)
			}
			if tc.reject {
				requireRule(t, verr, RuleEdgeLegality, "edges[0]")
			}
		})
	}
}
