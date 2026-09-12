package compiler

import (
	"strings"
	"testing"
)

func parseAndValidate(t *testing.T, src string) error {
	t.Helper()
	doc, err := Parse([]byte(src))
	if err != nil {
		return err
	}
	return Validate(doc)
}

const paramGraphPrefix = `schema: proceed/v1
name: params-graph
nodes:
  - id: call
    type: task
    executor:
      kind: shell
      command: [bin/call, "{{ params.env }}"]
    contract: pure
    terminal: true
edges: []
`

func TestParamsDeclarationAccepted(t *testing.T) {
	src := `schema: proceed/v1
name: params-ok
params:
  - { name: env, type: string, required: true }
  - { name: retries, type: int, default: 3 }
  - { name: ratio, type: float, default: 0.5 }
  - { name: verbose, type: bool, default: false }
  - { name: token, type: secret, default: "${GITHUB_TOKEN}" }
nodes:
  - id: call
    type: task
    executor: { kind: shell, command: [bin/call] }
    contract: pure
    terminal: true
edges: []
`
	doc, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Params) != 5 {
		t.Fatalf("params = %d, want 5", len(doc.Params))
	}
	if err := Validate(doc); err != nil {
		t.Fatal(err)
	}
	if doc.Params[1].Default != "3" || !doc.Params[1].HasDefault {
		t.Fatalf("int default = %q hasDefault=%v", doc.Params[1].Default, doc.Params[1].HasDefault)
	}
	if doc.Params[2].Default != "0.5" {
		t.Fatalf("float default = %q", doc.Params[2].Default)
	}
}

func TestParamsDeclarationRejections(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want string
	}{
		{"duplicate name", "params:\n  - { name: env, type: string }\n  - { name: env, type: int }", "duplicate param name"},
		{"bad type", "params:\n  - { name: env, type: list }", "type must be one of"},
		{"bad name", "params:\n  - { name: \"env x\", type: string }", "must contain only"},
		{"required with default", "params:\n  - { name: env, type: string, required: true, default: x }", "must not declare a default"},
		{"int default string", "params:\n  - { name: retries, type: int, default: many }", "must be a int value"},
		{"int default float", "params:\n  - { name: retries, type: int, default: 3.5 }", "must be a int value"},
		{"bool default string", "params:\n  - { name: flag, type: bool, default: yes-please }", "must be a bool value"},
		{"string default int", "params:\n  - { name: env, type: string, default: 3 }", "must be a string value"},
		{"secret default literal", "params:\n  - { name: token, type: secret, default: hunter2 }", "must be a ${NAME} reference"},
		{"unknown field", "params:\n  - { name: env, type: string, label: x }", "unknown field"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := "schema: proceed/v1\nname: params-bad\n" + tc.src + "\nnodes:\n  - id: call\n    type: task\n    executor: { kind: shell, command: [bin/call] }\n    contract: pure\n    terminal: true\nedges: []\n"
			err := parseAndValidate(t, src)
			if err == nil {
				t.Fatalf("expected rejection")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestParamRefsAllowedSurfaces(t *testing.T) {
	src := `schema: proceed/v1
name: refs-ok
params:
  - { name: env, type: string }
  - { name: token, type: secret }
nodes:
  - id: call
    type: task
    executor:
      kind: shell
      command: ["bin/call", "{{ params.env }}"]
      x-proceed-env:
        TOKEN: "{{ params.token }}"
    contract: pure
  - id: fetch
    type: task
    executor:
      kind: http
      method: POST
      url: http://example.test/api/{{ params.env }}
      body: >
        {"env": "{{ params.env }}", "nested": {"env": "{{ params.env }}"}}
    contract: idempotent
    terminal: true
    capability:
      secrets: [token]
      network:
        allowlisted_hosts: [example.test]
edges:
  - { from: call, to: fetch, type: depends_on }
`
	if err := parseAndValidate(t, src); err != nil {
		t.Fatal(err)
	}
}

func TestParamRefsForbiddenSurfaces(t *testing.T) {
	cases := []struct {
		name string
		node string
	}{
		{"shell workdir", `kind: shell
      command: [bin/call]
      workdir: "{{ params.env }}"`},
		{"env key", `kind: shell
      command: [bin/call]
      x-proceed-env:
        "{{ params.env }}": value`},
		{"http header", `kind: http
      method: GET
      url: http://example.test/
      headers:
        X-Env: "${token}"
        X-Extra: "{{ params.env }}"`},
		{"agent args", `kind: agent_cli
      cli: codemodel
      args: ["{{ params.env }}"]`},
		{"approval scope", `kind: human_approval
      scope: "{{ params.env }}"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := "schema: proceed/v1\nname: refs-bad\nparams:\n  - { name: env, type: string }\n  - { name: token, type: secret }\nnodes:\n  - id: call\n    type: task\n    executor:\n      " + tc.node + "\n    contract: pure\n    terminal: true\n    capability:\n      secrets: [token]\n      network:\n        allowlisted_hosts: [example.test]\nedges: []\n"
			err := parseAndValidate(t, src)
			if err == nil {
				t.Fatalf("expected rejection")
			}
			if !strings.Contains(err.Error(), paramAllowlistHint) {
				t.Fatalf("error %q does not contain allowlist hint", err.Error())
			}
		})
	}
}

func TestParamRefUndeclaredInCommand(t *testing.T) {
	src := strings.Replace(paramGraphPrefix, "{{ params.env }}", "{{ params.bogus }}", 1)
	err := parseAndValidate(t, src)
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), `references undeclared param "bogus"`) {
		t.Fatalf("error = %v", err)
	}
}

func TestParamRefInEdgeConditionRejected(t *testing.T) {
	src := `schema: proceed/v1
name: refs-edge
params:
  - { name: env, type: string }
nodes:
  - id: a
    type: task
    executor: { kind: shell, command: [bin/a] }
    contract: pure
  - id: b
    type: task
    executor: { kind: shell, command: [bin/b] }
    contract: pure
    terminal: true
edges:
  - { from: a, to: b, type: routes_to, when: "output == '{{ params.env }}'" }
`
	err := parseAndValidate(t, src)
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), paramAllowlistHint) {
		t.Fatalf("error = %v", err)
	}
}

func TestCanonicalJSONFloatInParams(t *testing.T) {
	src := `schema: proceed/v1
name: float-param
params:
  - { name: ratio, type: float, default: 0.5 }
nodes:
  - id: call
    type: task
    executor: { kind: shell, command: [bin/call] }
    contract: pure
    terminal: true
edges: []
`
	if _, err := CanonicalJSON([]byte(src)); err != nil {
		t.Fatal(err)
	}
	noParams := strings.Replace(src, "params:\n  - { name: ratio, type: float, default: 0.5 }\n", "", 1)
	if _, err := CanonicalJSON([]byte(noParams)); err != nil {
		t.Fatal(err)
	}
}

func TestCanonicalJSONFloatOutsideParamsStillRejected(t *testing.T) {
	src := `schema: proceed/v1
name: float-bad
x-meta: { weight: 0.5 }
nodes:
  - id: call
    type: task
    executor: { kind: shell, command: [bin/call] }
    contract: pure
    timeout_ms: 1.5
    terminal: true
edges: []
`
	if _, err := CanonicalJSON([]byte(src)); err == nil {
		t.Fatal("expected float rejection outside params")
	}
}

func TestDigestStableWithoutParams(t *testing.T) {
	src := `schema: proceed/v1
name: digest-stable
nodes:
  - id: call
    type: task
    executor: { kind: shell, command: [bin/call] }
    contract: pure
    terminal: true
edges: []
`
	canonical, err := CanonicalJSON([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	digest := DefinitionDigest(canonical)
	canonical2, err := CanonicalJSON([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if digest != DefinitionDigest(canonical2) {
		t.Fatal("digest of identical sources differs")
	}
	withParams := strings.Replace(src, "nodes:", "params:\n  - { name: env, type: string }\nnodes:", 1)
	canonical3, err := CanonicalJSON([]byte(withParams))
	if err != nil {
		t.Fatal(err)
	}
	if digest == DefinitionDigest(canonical3) {
		t.Fatal("declaration must change the digest")
	}
}

func TestReplaceParamRefs(t *testing.T) {
	out, err := ReplaceParamRefs("a {{params.x}} b {{ params.y }} c", func(name string) (string, error) {
		return "<" + name + ">", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if out != "a <x> b <y> c" {
		t.Fatalf("out = %q", out)
	}
	if _, err := ReplaceParamRefs("{{ params.x }}", func(name string) (string, error) {
		return "", errorForTest()
	}); err == nil {
		t.Fatal("expected error")
	}
}

func errorForTest() error { return &Error{Code: CodeGraphInvalid} }
