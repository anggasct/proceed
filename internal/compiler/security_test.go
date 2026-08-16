package compiler

import (
	"fmt"
	"strings"
	"testing"
)

func TestValidateExecutorRequiredFields(t *testing.T) {
	cases := []struct {
		name     string
		executor string
		location string
	}{
		{"shell without command", `{ kind: shell }`, "nodes[0].executor.command"},
		{"http without method", `{ kind: http, url: https://a.example }`, "nodes[0].executor.method"},
		{"http without url", `{ kind: http, method: GET }`, "nodes[0].executor.url"},
		{"human_approval without scope", `{ kind: human_approval }`, "nodes[0].executor.scope"},
		{"agent_cli without cli", `{ kind: agent_cli, args: [x] }`, "nodes[0].executor.cli"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := Parse([]byte("schema: proceed/v1\nname: e\nnodes:\n  - id: work\n    type: task\n    executor: " + tc.executor + "\n    contract: pure\n    terminal: true\nedges: []\n"))
			if err != nil {
				t.Fatal(err)
			}
			verr := Validate(doc)
			if verr == nil {
				t.Fatal("expected error")
			}
			requireRule(t, verr, RuleParse, tc.location)
		})
	}
}

func TestValidateHTTPHeaderValuesMustBeReferences(t *testing.T) {
	literal := `schema: proceed/v1
name: headers
nodes:
  - id: call
    type: tool
    executor: { kind: http, method: GET, url: https://a.example, headers: { Authorization: "Bearer sk-live-abcdef" } }
    contract: idempotent
    terminal: true
edges: []
`
	doc, err := Parse([]byte(literal))
	if err != nil {
		t.Fatal(err)
	}
	verr := Validate(doc)
	if verr == nil {
		t.Fatal("literal header value must be rejected")
	}
	requireRule(t, verr, RuleParse, "nodes[0].executor.headers.Authorization")

	reference := `schema: proceed/v1
name: headers
nodes:
  - id: call
    type: tool
    executor: { kind: http, method: GET, url: https://a.example, headers: { Authorization: "${api_token}" } }
    contract: idempotent
    terminal: true
    capability:
      network: { allowlisted_hosts: [a.example] }
      secrets: [api_token]
edges: []
`
	doc, err = Parse([]byte(reference))
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(doc); err != nil {
		t.Fatalf("declared reference header value must pass: %v", err)
	}

	undeclared := `schema: proceed/v1
name: headers
nodes:
  - id: call
    type: tool
    executor: { kind: http, method: GET, url: https://a.example, headers: { Authorization: "${api_token}" } }
    contract: idempotent
    terminal: true
    capability: { network: { allowlisted_hosts: [a.example] } }
edges: []
`
	doc, err = Parse([]byte(undeclared))
	if err != nil {
		t.Fatal(err)
	}
	verr = Validate(doc)
	if verr == nil {
		t.Fatal("undeclared secret reference must be rejected")
	}
	requireRule(t, verr, RuleParse, "nodes[0].executor.headers.Authorization")
}

func TestValidateHTTPURLUserinfoRejected(t *testing.T) {
	for _, u := range []string{
		"https://user:literal-secret@allowed.example/fetch",
		"https://user@allowed.example/fetch",
	} {
		doc, err := Parse([]byte(fmt.Sprintf(`schema: proceed/v1
name: userinfo
nodes:
  - id: call
    type: tool
    executor: { kind: http, method: GET, url: "%s" }
    contract: idempotent
    terminal: true
    capability: { network: { allowlisted_hosts: [allowed.example] } }
edges: []
`, u)))
		if err != nil {
			t.Fatal(err)
		}
		verr := Validate(doc)
		if verr == nil {
			t.Fatalf("url %s must be rejected", u)
		}
		requireRule(t, verr, RuleParse, "nodes[0].executor.url")
	}
}

func TestValidateHTTPHostAllowlist(t *testing.T) {
	const docTemplate = `schema: proceed/v1
name: allowlist
nodes:
  - id: call
    type: tool
    executor: { kind: http, method: GET, url: %s }
    contract: idempotent
    terminal: true
    capability: { network: { allowlisted_hosts: [allowed.example] } }
edges: []
`
	cases := []struct {
		name   string
		url    string
		reject bool
	}{
		{name: "host outside allowlist", url: "https://evil.example/fetch", reject: true},
		{name: "host inside allowlist", url: "https://allowed.example/fetch", reject: false},
		{name: "host with port inside allowlist", url: "https://allowed.example:8443/fetch", reject: false},
		{name: "host with port outside allowlist", url: "https://evil.example:8443/fetch", reject: true},
		{name: "case-insensitive host match", url: "https://ALLOWED.Example/fetch", reject: false},
		{name: "scheme not http", url: "ftp://allowed.example/fetch", reject: true},
		{name: "relative url", url: "/relative/path", reject: true},
		{name: "empty hostname", url: "https://:443/fetch", reject: true},
		{name: "ip literal host", url: "https://10.0.0.1:443/fetch", reject: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := Parse([]byte(fmt.Sprintf(docTemplate, tc.url)))
			if err != nil {
				t.Fatal(err)
			}
			verr := Validate(doc)
			if tc.reject && verr == nil {
				t.Fatalf("url %s must be rejected", tc.url)
			}
			if !tc.reject && verr != nil {
				t.Fatalf("url %s must pass: %v", tc.url, verr)
			}
			if tc.reject {
				requireRule(t, verr, RuleParse, "nodes[0].executor.url")
			}
		})
	}
}

func TestValidateAllowlistEntriesMustBeValidHosts(t *testing.T) {
	cases := []struct {
		name   string
		hosts  string
		reject bool
	}{
		{name: "empty entry", hosts: `[""]`, reject: true},
		{name: "hostname with spaces", hosts: `["not a host"]`, reject: true},
		{name: "hostname with scheme", hosts: `["https://a.example"]`, reject: true},
		{name: "leading hyphen label", hosts: `["-bad.example"]`, reject: true},
		{name: "valid hostname", hosts: `["api.github.com"]`, reject: false},
		{name: "valid ip literal", hosts: `["10.0.0.1"]`, reject: false},
		{name: "trailing dot hostname", hosts: `["a.example."]`, reject: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host := strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(tc.hosts, `["`), `"]`), `"`)
			urlValue := "https://" + strings.TrimSuffix(host, ".") + "/f"
			src := fmt.Sprintf(`schema: proceed/v1
name: hosts
nodes:
  - id: call
    type: tool
    executor: { kind: http, method: GET, url: "%s" }
    contract: idempotent
    terminal: true
    capability: { network: { allowlisted_hosts: %s } }
edges: []
`, urlValue, tc.hosts)
			doc, err := Parse([]byte(src))
			if err != nil {
				t.Fatal(err)
			}
			verr := Validate(doc)
			if tc.reject && verr == nil {
				t.Fatalf("hosts %s must be rejected", tc.hosts)
			}
			if !tc.reject && verr != nil {
				t.Fatalf("hosts %s must pass: %v", tc.hosts, verr)
			}
			if tc.reject {
				requireRule(t, verr, RuleParse, "nodes[0].capability.network.allowlisted_hosts[0]")
			}
		})
	}
}

func TestValidateHTTPHostWithoutCapabilityRejected(t *testing.T) {
	src := `schema: proceed/v1
name: no-cap
nodes:
  - id: call
    type: tool
    executor: { kind: http, method: GET, url: https://a.example }
    contract: idempotent
    terminal: true
edges: []
`
	doc, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	verr := Validate(doc)
	if verr == nil {
		t.Fatal("http node without allowlist capability must be rejected")
	}
	requireRule(t, verr, RuleParse, "nodes[0].executor.url")
}

func TestValidateCapabilityEnums(t *testing.T) {
	cases := []struct {
		name     string
		cap      string
		location string
	}{
		{"filesystem enum", `{ filesystem: arbitrary-access }`, "nodes[0].capability.filesystem"},
		{"process enum", `{ process: arbitrary-command }`, "nodes[0].capability.process"},
		{"human enum", `{ human: everyone }`, "nodes[0].capability.human"},
		{"scalar secrets", `{ secrets: github-token }`, "nodes[0].capability.secrets"},
		{"network scalar mode", `{ network: allow-all }`, "nodes[0].capability.network"},
		{"network object without hosts", `{ network: { allowlisted_hosts: [] } }`, "nodes[0].capability.network.allowlisted_hosts"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := "schema: proceed/v1\nname: caps\nnodes:\n  - id: work\n    type: task\n    executor: { kind: shell, command: [bin/do] }\n    contract: pure\n    terminal: true\n    capability: " + tc.cap + "\nedges: []\n"
			doc, err := Parse([]byte(src))
			if err != nil {
				t.Fatal(err)
			}
			verr := Validate(doc)
			if verr == nil {
				t.Fatal("expected error")
			}
			requireRule(t, verr, RuleParse, tc.location)
		})
	}
}

func TestValidateCapabilityAcceptedForms(t *testing.T) {
	src := `schema: proceed/v1
name: caps-ok
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
    capability:
      filesystem: workspace-read
      process: declared-command
      human: approval-scope
      secrets: [github-token, openai_key.1]
      network: { allowlisted_hosts: [api.github.com] }
edges: []
`
	doc, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if err := Validate(doc); err != nil {
		t.Fatal(err)
	}
}

func TestValidateSecretListEntriesMustBeNames(t *testing.T) {
	src := `schema: proceed/v1
name: caps-bad-secret
nodes:
  - id: work
    type: task
    executor: { kind: shell, command: [bin/do] }
    contract: pure
    terminal: true
    capability: { secrets: ["sk-live-abcdef with spaces"] }
edges: []
`
	doc, err := Parse([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	verr := Validate(doc)
	if verr == nil {
		t.Fatal("non-name secret entry must be rejected")
	}
	requireRule(t, verr, RuleParse, "nodes[0].capability.secrets[0]")
}

func TestValidateTimeoutPresence(t *testing.T) {
	for _, value := range []string{"0", "-5"} {
		src := "schema: proceed/v1\nname: t\nnodes:\n  - id: work\n    type: task\n    executor: { kind: shell, command: [bin/do] }\n    contract: pure\n    terminal: true\n    timeout_ms: " + value + "\nedges: []\n"
		doc, err := Parse([]byte(src))
		if err != nil {
			t.Fatal(err)
		}
		verr := Validate(doc)
		if verr == nil {
			t.Fatalf("timeout_ms=%s must be rejected", value)
		}
		requireRule(t, verr, RuleParse, "nodes[0].timeout_ms")
	}
}
