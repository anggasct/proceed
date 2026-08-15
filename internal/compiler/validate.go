package compiler

import (
	"fmt"
	"strings"
)

const (
	RuleDuplicateNode    = "E103"
	RuleDanglingEdge     = "E104"
	RuleEdgeType         = "E105"
	RuleWhenPlacement    = "E106"
	RuleUnboundedCycle   = "E107"
	RuleSelfVerifier     = "E108"
	RuleSinkTerminal     = "E109"
	RuleContractRequired = "E110"
	RuleNodeType         = "E111"
	RuleAcyclicTraversal = "E112"
	RuleArtifactEdge     = "E113"
)

var nodeTypeSet = map[string]bool{
	"task": true, "model": true, "agent": true, "tool": true,
	"verifier": true, "router": true, "gate": true,
}

var executableTypeSet = map[string]bool{
	"task": true, "model": true, "agent": true, "tool": true,
}

var edgeTypeSet = map[string]bool{
	"defines": true, "depends_on": true, "routes_to": true, "produces": true,
	"consumes": true, "verifies": true, "derived_from": true, "blocks": true,
	"approves": true, "measures": true, "improves": true,
}

var contractSet = map[string]bool{
	"pure": true, "idempotent": true, "reconcilable": true, "non_replayable": true,
}

var policyKindSet = map[string]bool{
	"retry": true, "gate": true, "capability": true,
}

func Validate(doc *Document) error {
	v := &validator{doc: doc}
	v.documentPass()
	v.nodePass()
	v.edgePass()
	v.sinkPass()
	v.cyclePass()
	if len(v.diags) > 0 {
		return graphInvalid(v.diags...)
	}
	return nil
}

type validator struct {
	doc   *Document
	diags []Diagnostic
}

func (v *validator) errf(rule, location, format string, args ...any) {
	v.diags = append(v.diags, Diagnostic{
		Rule:     rule,
		Location: location,
		Message:  fmt.Sprintf(format, args...),
	})
}

func (v *validator) documentPass() {
	if v.doc.Name == "" {
		v.errf(RuleParse, "name", `missing required field "name"`)
	}
	if len(v.doc.Nodes) == 0 {
		v.errf(RuleParse, "nodes", "nodes must be a non-empty array")
	}
	for i := range v.doc.Policies {
		po := &v.doc.Policies[i]
		if !policyKindSet[po.Kind] {
			v.errf(RuleParse, fmt.Sprintf("policies[%d].kind", i), "policy kind must be one of retry, gate, capability")
		}
	}
}

func (v *validator) nodePass() {
	seen := map[string]bool{}
	for i := range v.doc.Nodes {
		n := &v.doc.Nodes[i]
		path := fmt.Sprintf("nodes[%d]", i)
		if !nodeTypeSet[n.Type] {
			v.errf(RuleNodeType, joinPath(path, "type"), "unknown node type %q", n.Type)
		}
		if seen[n.ID] {
			v.errf(RuleDuplicateNode, joinPath(path, "id"), "duplicate node id %q", n.ID)
		}
		seen[n.ID] = true
		if executableTypeSet[n.Type] && n.Executor == nil {
			v.errf(RuleContractRequired, joinPath(path, "executor"), "node type %q requires an executor", n.Type)
		}
		if n.Executor != nil {
			if !n.HasContract {
				v.errf(RuleContractRequired, joinPath(path, "contract"), "node with executor requires a contract")
			} else if !contractSet[n.Contract] {
				v.errf(RuleContractRequired, joinPath(path, "contract"), "unknown contract %q", n.Contract)
			}
		}
		if n.TimeoutMs < 0 {
			v.errf(RuleParse, joinPath(path, "timeout_ms"), "timeout_ms must be >= 1")
		}
		if n.Retry != nil {
			if n.Retry.MaxAttempts < 1 {
				v.errf(RuleParse, joinPath(path, "retry.max_attempts"), "retry.max_attempts must be >= 1")
			}
			if n.Retry.BackoffMs < 0 {
				v.errf(RuleParse, joinPath(path, "retry.backoff_ms"), "retry.backoff_ms must be >= 0")
			}
		}
	}
}

func (v *validator) edgePass() {
	if len(v.doc.Edges) == 0 && len(v.doc.Nodes) > 1 {
		v.errf(RuleSinkTerminal, "edges", "edges may be empty only if a single terminal node exists")
	}
	exists := map[string]bool{}
	for i := range v.doc.Nodes {
		exists[v.doc.Nodes[i].ID] = true
	}
	for i := range v.doc.Edges {
		e := &v.doc.Edges[i]
		path := fmt.Sprintf("edges[%d]", i)
		desc := e.From + " -> " + e.To
		for _, endpoint := range []struct {
			name string
			val  string
		}{{"from", e.From}, {"to", e.To}} {
			if endpoint.val == "" {
				v.errf(RuleDanglingEdge, joinPath(path, endpoint.name), "edge is missing required field %q", endpoint.name)
			} else if !exists[endpoint.val] {
				v.errf(RuleDanglingEdge, joinPath(path, endpoint.name), "edge %q targets nonexistent node %q", desc, endpoint.val)
			}
		}
		switch {
		case e.Type == "":
			v.errf(RuleEdgeType, joinPath(path, "type"), "edge %q is missing required field %q", desc, "type")
		case !edgeTypeSet[e.Type]:
			v.errf(RuleEdgeType, joinPath(path, "type"), "edge %q has unknown type %q", desc, e.Type)
		}
		if e.HasWhen && e.Type != "routes_to" {
			v.errf(RuleWhenPlacement, joinPath(path, "when"), "when is only legal on routes_to edges (edge %q)", desc)
		}
		if e.HasArtifact && e.Type != "produces" && e.Type != "consumes" {
			v.errf(RuleArtifactEdge, joinPath(path, "artifact"), "artifact is only legal on produces/consumes edges (edge %q)", desc)
		}
		if e.HasMaxTraversals && e.MaxTraversals < 1 {
			v.errf(RuleParse, joinPath(path, "max_traversals"), "max_traversals must be >= 1")
		}
		if e.From != "" && e.From == e.To && v.nodeByType(e.From, "verifier") {
			v.errf(RuleSelfVerifier, path, "verifier node %q must not have an edge routing to itself", e.From)
		}
	}
}

func (v *validator) nodeByType(id, nodeType string) bool {
	for i := range v.doc.Nodes {
		if v.doc.Nodes[i].ID == id {
			return v.doc.Nodes[i].Type == nodeType
		}
	}
	return false
}

func (v *validator) sinkPass() {
	hasOutgoing := map[string]bool{}
	for i := range v.doc.Edges {
		hasOutgoing[v.doc.Edges[i].From] = true
	}
	for i := range v.doc.Nodes {
		n := &v.doc.Nodes[i]
		if hasOutgoing[n.ID] || n.Type == "gate" {
			continue
		}
		if !n.Terminal {
			v.errf(RuleSinkTerminal, fmt.Sprintf("nodes[%d].terminal", i), "sink node %q must declare terminal: true", n.ID)
		}
	}
}

func (v *validator) cyclePass() {
	index := make(map[string]int, len(v.doc.Nodes))
	nodes := make([]string, 0, len(v.doc.Nodes))
	for i := range v.doc.Nodes {
		id := v.doc.Nodes[i].ID
		if _, ok := index[id]; !ok {
			index[id] = len(nodes)
			nodes = append(nodes, id)
		}
	}
	adj := make([][]arc, len(nodes))
	for i := range v.doc.Edges {
		e := &v.doc.Edges[i]
		from, okFrom := index[e.From]
		to, okTo := index[e.To]
		if !okFrom || !okTo {
			continue
		}
		adj[from] = append(adj[from], arc{to: to, bounded: e.HasMaxTraversals})
	}
	sccOf := iterativeSCC(len(nodes), adj)

	for i := range v.doc.Edges {
		e := &v.doc.Edges[i]
		if !e.HasMaxTraversals {
			continue
		}
		from, okFrom := index[e.From]
		to, okTo := index[e.To]
		if okFrom && okTo && sccOf[from] != sccOf[to] {
			v.errf(RuleAcyclicTraversal, fmt.Sprintf("edges[%d].max_traversals", i),
				"max_traversals on provably acyclic edge %q", e.From+" -> "+e.To)
		}
	}

	inSCC := make([][]arc, len(nodes))
	selfLoop := make([]bool, len(nodes))
	for u := range adj {
		for _, a := range adj[u] {
			if sccOf[u] == sccOf[a.to] {
				inSCC[u] = append(inSCC[u], a)
				if a.to == u {
					selfLoop[u] = true
				}
			}
		}
	}
	compMembers := map[int][]int{}
	for u, c := range sccOf {
		compMembers[c] = append(compMembers[c], u)
	}
	for _, members := range compMembers {
		cyclic := len(members) > 1
		for _, u := range members {
			if selfLoop[u] {
				cyclic = true
			}
		}
		if !cyclic {
			continue
		}
		unbounded := make([][]int, len(nodes))
		for _, u := range members {
			for _, a := range inSCC[u] {
				if !a.bounded {
					unbounded[u] = append(unbounded[u], a.to)
				}
			}
		}
		if cycle := findCycle(members, unbounded); cycle != nil {
			keys := make([]string, len(cycle))
			for i, u := range cycle {
				keys[i] = nodes[u]
			}
			v.errf(RuleUnboundedCycle, "edges",
				"cycle %s has no max_traversals bound", strings.Join(keys, " -> "))
		}
	}
}

type arc struct {
	to      int
	bounded bool
}

func iterativeSCC(n int, adj [][]arc) []int {
	const unseen = -1
	order := make([]int, n)
	low := make([]int, n)
	for i := range order {
		order[i] = unseen
	}
	onStack := make([]bool, n)
	var stack []int
	cursor := make([]int, n)
	sccOf := make([]int, n)
	next := 0
	comps := 0
	for root := 0; root < n; root++ {
		if order[root] != unseen {
			continue
		}
		call := []int{root}
		for len(call) > 0 {
			u := call[len(call)-1]
			if order[u] == unseen {
				order[u] = next
				low[u] = next
				next++
				stack = append(stack, u)
				onStack[u] = true
			}
			advanced := false
			for cursor[u] < len(adj[u]) {
				w := adj[u][cursor[u]].to
				cursor[u]++
				if order[w] == unseen {
					call = append(call, w)
					advanced = true
					break
				}
				if onStack[w] && order[w] < low[u] {
					low[u] = order[w]
				}
			}
			if advanced {
				continue
			}
			call = call[:len(call)-1]
			if len(call) > 0 {
				p := call[len(call)-1]
				if low[u] < low[p] {
					low[p] = low[u]
				}
			}
			if low[u] == order[u] {
				for {
					w := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					onStack[w] = false
					sccOf[w] = comps
					if w == u {
						break
					}
				}
				comps++
			}
		}
	}
	return sccOf
}

func findCycle(members []int, adj [][]int) []int {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make([]uint8, len(adj))
	var path []int
	var visit []int
	inPath := make([]bool, len(adj))
	pos := make([]int, len(adj))
	for _, root := range members {
		if color[root] != white {
			continue
		}
		visit = append(visit[:0], root)
		for len(visit) > 0 {
			u := visit[len(visit)-1]
			switch color[u] {
			case white:
				color[u] = gray
				path = append(path, u)
				inPath[u] = true
			case gray:
				if pos[u] >= len(adj[u]) {
					color[u] = black
					inPath[u] = false
					path = path[:len(path)-1]
					visit = visit[:len(visit)-1]
					continue
				}
				w := adj[u][pos[u]]
				pos[u]++
				if color[w] == white {
					visit = append(visit, w)
				} else if color[w] == gray && inPath[w] {
					start := len(path)
					for i := len(path) - 1; i >= 0; i-- {
						if path[i] == w {
							start = i
							break
						}
					}
					cycle := make([]int, len(path)-start)
					copy(cycle, path[start:])
					return cycle
				}
			case black:
				visit = visit[:len(visit)-1]
			}
		}
	}
	return nil
}
