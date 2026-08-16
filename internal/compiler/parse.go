package compiler

import (
	"fmt"
	"strconv"
	"strings"

	yaml "gopkg.in/yaml.v3"
)

const (
	RuleParse        = "E000"
	RuleSchema       = "E101"
	RuleUnknownField = "E102"
)

type Document struct {
	Schema   string
	Name     string
	Nodes    []Node
	Edges    []Edge
	Policies []Policy
	Extras   map[string]yaml.Node
}

type Node struct {
	ID          string
	Type        string
	Terminal    bool
	HasTerminal bool
	Executor    *Executor
	Capability  *Capability
	Contract    string
	HasContract bool
	TimeoutMs   int64
	HasTimeout  bool
	Retry       *Retry
	Extras      map[string]yaml.Node
}

type Executor struct {
	Kind        string
	Command     []string
	Workdir     string
	Method      string
	URL         string
	Headers     map[string]string
	Body        *yaml.Node
	Scope       string
	ExpiresInMs int64
	CLI         string
	Args        []string
	Extras      map[string]yaml.Node
}

type Capability struct {
	Filesystem     string
	HasFilesystem  bool
	Network        *Network
	Process        string
	HasProcess     bool
	SecretsScalar  bool
	SecretsLiteral string
	Secrets        []string
	Human          string
	HasHuman       bool
	Extras         map[string]yaml.Node
}

type Network struct {
	Mode             string
	AllowlistedHosts []string
	Extras           map[string]yaml.Node
}

type Retry struct {
	MaxAttempts     int64
	BackoffMs       int64
	RetryableErrors []string
	Extras          map[string]yaml.Node
}

type Edge struct {
	From             string
	To               string
	Type             string
	When             string
	HasWhen          bool
	MaxTraversals    int64
	HasMaxTraversals bool
	Artifact         string
	HasArtifact      bool
	Extras           map[string]yaml.Node
}

type Policy struct {
	Name   string
	Kind   string
	Rule   *yaml.Node
	Extras map[string]yaml.Node
}

var documentFields = map[string]struct{}{
	"schema": {}, "name": {}, "nodes": {}, "edges": {}, "policies": {},
}

var nodeFields = map[string]struct{}{
	"id": {}, "type": {}, "terminal": {}, "executor": {},
	"capability": {}, "contract": {}, "timeout_ms": {}, "retry": {},
}

var edgeFields = map[string]struct{}{
	"from": {}, "to": {}, "type": {}, "when": {}, "max_traversals": {}, "artifact": {},
}

var policyFields = map[string]struct{}{
	"name": {}, "kind": {}, "rule": {},
}

var retryFields = map[string]struct{}{
	"max_attempts": {}, "backoff_ms": {}, "retryable_errors": {},
}

var capabilityFields = map[string]struct{}{
	"filesystem": {}, "network": {}, "process": {}, "secrets": {}, "human": {},
}

var networkFields = map[string]struct{}{
	"allowlisted_hosts": {},
}

var executorKinds = map[string]struct{}{
	"shell": {}, "http": {}, "human_approval": {}, "agent_cli": {},
}

var executorFields = map[string]struct{}{
	"kind": {}, "command": {}, "workdir": {},
	"method": {}, "url": {}, "headers": {}, "body": {},
	"scope": {}, "expires_in_ms": {}, "cli": {}, "args": {},
}

var executorFieldKinds = map[string]string{
	"command": "shell", "workdir": "shell",
	"method": "http", "url": "http", "headers": "http", "body": "http",
	"scope": "human_approval", "expires_in_ms": "human_approval",
	"cli": "agent_cli", "args": "agent_cli",
}

func Parse(src []byte) (*Document, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(src, &root); err != nil {
		return nil, graphInvalid(Diagnostic{Rule: RuleParse, Message: err.Error()})
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return nil, graphInvalid(Diagnostic{Rule: RuleParse, Message: "empty document"})
	}
	p := &parser{}
	doc := p.document(root.Content[0])
	if len(p.diags) > 0 {
		return nil, graphInvalid(p.diags...)
	}
	return doc, nil
}

type parser struct {
	diags []Diagnostic
}

type field struct {
	name  string
	value *yaml.Node
	extra bool
}

func (p *parser) errf(rule, location, format string, args ...any) {
	p.diags = append(p.diags, Diagnostic{
		Rule:     rule,
		Location: location,
		Message:  fmt.Sprintf(format, args...),
	})
}

func (p *parser) document(n *yaml.Node) *Document {
	doc := &Document{}
	if !p.isMapping(n, "") {
		return doc
	}
	var schemaNode *yaml.Node
	for _, f := range p.fields(n, "", documentFields) {
		if f.extra {
			setExtra(&doc.Extras, f.name, f.value)
			continue
		}
		switch f.name {
		case "schema":
			schemaNode = f.value
		case "name":
			doc.Name = p.str(f.value, "name")
		case "nodes":
			doc.Nodes = p.nodes(f.value)
		case "edges":
			doc.Edges = p.edges(f.value)
		case "policies":
			doc.Policies = p.policies(f.value)
		}
	}
	switch {
	case schemaNode == nil:
		p.errf(RuleSchema, "schema", `missing required field "schema"`)
	case schemaNode.Kind != yaml.ScalarNode || schemaNode.Tag == "!!null" || schemaNode.Value != "proceed/v1":
		p.errf(RuleSchema, "schema", "schema must be proceed/v1")
	default:
		doc.Schema = schemaNode.Value
	}
	return doc
}

func (p *parser) nodes(n *yaml.Node) []Node {
	items := p.sequence(n, "nodes")
	out := make([]Node, 0, len(items))
	for i, item := range items {
		out = append(out, p.node(item, fmt.Sprintf("nodes[%d]", i)))
	}
	return out
}

func (p *parser) node(n *yaml.Node, path string) Node {
	var nd Node
	if !p.isMapping(n, path) {
		return nd
	}
	for _, f := range p.fields(n, path, nodeFields) {
		if f.extra {
			setExtra(&nd.Extras, f.name, f.value)
			continue
		}
		loc := joinPath(path, f.name)
		switch f.name {
		case "id":
			nd.ID = p.str(f.value, loc)
		case "type":
			nd.Type = p.str(f.value, loc)
		case "terminal":
			nd.Terminal, nd.HasTerminal = p.boolean(f.value, loc)
		case "executor":
			nd.Executor = p.executor(f.value, joinPath(path, "executor"))
		case "capability":
			nd.Capability = p.capability(f.value, joinPath(path, "capability"))
		case "contract":
			nd.Contract, nd.HasContract = p.strPresent(f.value, loc)
		case "timeout_ms":
			nd.TimeoutMs = p.integer(f.value, loc)
			nd.HasTimeout = true
		case "retry":
			nd.Retry = p.retry(f.value, joinPath(path, "retry"))
		}
	}
	return nd
}

func (p *parser) edges(n *yaml.Node) []Edge {
	items := p.sequence(n, "edges")
	out := make([]Edge, 0, len(items))
	for i, item := range items {
		out = append(out, p.edge(item, fmt.Sprintf("edges[%d]", i)))
	}
	return out
}

func (p *parser) edge(n *yaml.Node, path string) Edge {
	var ed Edge
	if !p.isMapping(n, path) {
		return ed
	}
	for _, f := range p.fields(n, path, edgeFields) {
		if f.extra {
			setExtra(&ed.Extras, f.name, f.value)
			continue
		}
		loc := joinPath(path, f.name)
		switch f.name {
		case "from":
			ed.From = p.str(f.value, loc)
		case "to":
			ed.To = p.str(f.value, loc)
		case "type":
			ed.Type = p.str(f.value, loc)
		case "when":
			ed.When, ed.HasWhen = p.strPresent(f.value, loc)
		case "max_traversals":
			ed.MaxTraversals = p.integer(f.value, loc)
			ed.HasMaxTraversals = true
		case "artifact":
			ed.Artifact, ed.HasArtifact = p.strPresent(f.value, loc)
		}
	}
	return ed
}

func (p *parser) policies(n *yaml.Node) []Policy {
	items := p.sequence(n, "policies")
	out := make([]Policy, 0, len(items))
	for i, item := range items {
		out = append(out, p.policy(item, fmt.Sprintf("policies[%d]", i)))
	}
	return out
}

func (p *parser) policy(n *yaml.Node, path string) Policy {
	var po Policy
	if !p.isMapping(n, path) {
		return po
	}
	for _, f := range p.fields(n, path, policyFields) {
		if f.extra {
			setExtra(&po.Extras, f.name, f.value)
			continue
		}
		loc := joinPath(path, f.name)
		switch f.name {
		case "name":
			po.Name = p.str(f.value, loc)
		case "kind":
			po.Kind = p.str(f.value, loc)
		case "rule":
			po.Rule = f.value
		}
	}
	return po
}

func (p *parser) executor(n *yaml.Node, path string) *Executor {
	e := &Executor{}
	if !p.isMapping(n, path) {
		return e
	}
	var kindNode *yaml.Node
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == "kind" {
			kindNode = n.Content[i+1]
			break
		}
	}
	switch {
	case kindNode == nil:
		p.errf(RuleUnknownField, joinPath(path, "kind"), `missing required field "kind"`)
		return e
	case kindNode.Kind != yaml.ScalarNode || kindNode.Tag != "!!str":
		p.errf(RuleParse, joinPath(path, "kind"), "expected string")
		return e
	}
	e.Kind = kindNode.Value
	if _, known := executorKinds[e.Kind]; !known {
		p.errf(RuleUnknownField, joinPath(path, "kind"), "unknown executor kind %q", e.Kind)
		return e
	}
	for _, f := range p.fields(n, path, executorFields) {
		if f.extra {
			setExtra(&e.Extras, f.name, f.value)
			continue
		}
		if f.name == "kind" {
			continue
		}
		if owner := executorFieldKinds[f.name]; owner != e.Kind {
			p.errf(RuleUnknownField, joinPath(path, f.name), "field %q is not valid for executor kind %q", f.name, e.Kind)
			continue
		}
		loc := joinPath(path, f.name)
		switch f.name {
		case "command":
			e.Command = p.command(f.value, loc)
		case "workdir":
			e.Workdir = p.str(f.value, loc)
		case "method":
			e.Method = p.str(f.value, loc)
		case "url":
			e.URL = p.str(f.value, loc)
		case "headers":
			e.Headers = p.headers(f.value, loc)
		case "body":
			e.Body = f.value
		case "scope":
			e.Scope = p.str(f.value, loc)
		case "expires_in_ms":
			e.ExpiresInMs = p.integer(f.value, loc)
		case "cli":
			e.CLI = p.str(f.value, loc)
		case "args":
			e.Args = p.strings(f.value, loc)
		}
	}
	return e
}

func (p *parser) command(n *yaml.Node, path string) []string {
	if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
		return []string{n.Value}
	}
	return p.strings(n, path)
}

func (p *parser) headers(n *yaml.Node, path string) map[string]string {
	if !p.isMapping(n, path) {
		return nil
	}
	out := make(map[string]string, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		key := n.Content[i]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			p.errf(RuleParse, path, "header names must be strings")
			continue
		}
		out[key.Value] = p.str(n.Content[i+1], joinPath(path, key.Value))
	}
	return out
}

func (p *parser) capability(n *yaml.Node, path string) *Capability {
	c := &Capability{}
	if !p.isMapping(n, path) {
		return c
	}
	for _, f := range p.fields(n, path, capabilityFields) {
		if f.extra {
			setExtra(&c.Extras, f.name, f.value)
			continue
		}
		loc := joinPath(path, f.name)
		switch f.name {
		case "filesystem":
			c.Filesystem = p.str(f.value, loc)
			c.HasFilesystem = true
		case "network":
			c.Network = p.network(f.value, loc)
		case "process":
			c.Process = p.str(f.value, loc)
			c.HasProcess = true
		case "secrets":
			if f.value.Kind == yaml.ScalarNode && f.value.Tag == "!!str" {
				c.SecretsScalar = true
				c.SecretsLiteral = f.value.Value
			} else {
				c.Secrets = p.strings(f.value, loc)
			}
		case "human":
			c.Human = p.str(f.value, loc)
			c.HasHuman = true
		}
	}
	return c
}

func (p *parser) network(n *yaml.Node, path string) *Network {
	nw := &Network{}
	if n.Kind == yaml.ScalarNode && n.Tag != "!!null" {
		nw.Mode = n.Value
		return nw
	}
	if !p.isMapping(n, path) {
		return nw
	}
	for _, f := range p.fields(n, path, networkFields) {
		if f.extra {
			setExtra(&nw.Extras, f.name, f.value)
			continue
		}
		nw.AllowlistedHosts = p.strings(f.value, joinPath(path, f.name))
	}
	return nw
}

func (p *parser) retry(n *yaml.Node, path string) *Retry {
	r := &Retry{}
	if !p.isMapping(n, path) {
		return r
	}
	for _, f := range p.fields(n, path, retryFields) {
		if f.extra {
			setExtra(&r.Extras, f.name, f.value)
			continue
		}
		loc := joinPath(path, f.name)
		switch f.name {
		case "max_attempts":
			r.MaxAttempts = p.integer(f.value, loc)
		case "backoff_ms":
			r.BackoffMs = p.integer(f.value, loc)
		case "retryable_errors":
			r.RetryableErrors = p.strings(f.value, loc)
		}
	}
	return r
}

func (p *parser) fields(n *yaml.Node, path string, allowed map[string]struct{}) []field {
	var out []field
	seen := map[string]bool{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		name := n.Content[i].Value
		value := n.Content[i+1]
		loc := joinPath(path, name)
		if seen[name] {
			p.errf(RuleParse, loc, "duplicate field %q", name)
			continue
		}
		seen[name] = true
		if strings.HasPrefix(name, "x-") {
			out = append(out, field{name: name, value: value, extra: true})
			continue
		}
		if _, ok := allowed[name]; !ok {
			p.errf(RuleUnknownField, loc, "unknown field %q", name)
			continue
		}
		out = append(out, field{name: name, value: value})
	}
	return out
}

func (p *parser) isMapping(n *yaml.Node, path string) bool {
	if n.Kind != yaml.MappingNode {
		p.errf(RuleParse, path, "expected mapping")
		return false
	}
	return true
}

func (p *parser) sequence(n *yaml.Node, path string) []*yaml.Node {
	if n.Kind != yaml.SequenceNode {
		p.errf(RuleParse, path, "expected list")
		return nil
	}
	return n.Content
}

func (p *parser) str(n *yaml.Node, path string) string {
	if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
		p.errf(RuleParse, path, "expected string")
		return ""
	}
	return n.Value
}

func (p *parser) strPresent(n *yaml.Node, path string) (string, bool) {
	if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
		p.errf(RuleParse, path, "expected string")
		return "", false
	}
	return n.Value, true
}

func (p *parser) boolean(n *yaml.Node, path string) (bool, bool) {
	if n.Kind == yaml.ScalarNode && n.Tag == "!!bool" {
		return n.Value == "true", true
	}
	p.errf(RuleParse, path, "expected boolean")
	return false, false
}

func (p *parser) integer(n *yaml.Node, path string) int64 {
	if n.Kind == yaml.ScalarNode && n.Tag == "!!int" {
		v, err := strconv.ParseInt(n.Value, 10, 64)
		if err == nil {
			return v
		}
	}
	p.errf(RuleParse, path, "expected integer")
	return 0
}

func (p *parser) strings(n *yaml.Node, path string) []string {
	items := p.sequence(n, path)
	out := make([]string, 0, len(items))
	for i, item := range items {
		out = append(out, p.str(item, fmt.Sprintf("%s[%d]", path, i)))
	}
	return out
}

func joinPath(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

func setExtra(dst *map[string]yaml.Node, name string, value *yaml.Node) {
	if *dst == nil {
		*dst = map[string]yaml.Node{}
	}
	(*dst)[name] = *value
}
