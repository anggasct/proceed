package store

import (
	"context"
	"strings"
	"testing"
)

const extensionsGraph = `schema: proceed/v1
name: extensions-e2e
x-tier: internal
nodes:
  - id: work
    type: task
    x-owner: ops
    executor: { kind: shell, command: [bin/do], x-cmd-id: "7" }
    contract: pure
    terminal: true
    capability:
      x-reviewer: bot
      network:
        allowlisted_hosts: [a.example]
        x-proxy: direct
    retry: { max_attempts: 2, x-backoff-policy: exp }
edges:
  - from: work
    to: work
    type: routes_to
    when: again
    max_traversals: 2
    x-note: self
policies:
  - name: p
    kind: retry
    rule: { max_attempts: 1 }
    x-source: manual
`

func TestFreezePersistsExtensionsAtEveryLevel(t *testing.T) {
	s := openTestStore(t)
	src := []byte(extensionsGraph)
	doc := compileFixture(t, src)
	frozen, err := s.FreezeDefinition(context.Background(), "e.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}

	var versionExtras string
	if err := s.db.QueryRow("SELECT extras FROM graph_version WHERE id = ?", frozen.GraphVersionID).
		Scan(&versionExtras); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(versionExtras, `"x-tier":"internal"`) {
		t.Errorf("graph_version.extras = %s", versionExtras)
	}

	var edgeExtras string
	err = s.db.QueryRow(`SELECT extras FROM graph_edge WHERE graph_version_id = ? AND from_node_key = 'work'`,
		frozen.GraphVersionID).Scan(&edgeExtras)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(edgeExtras, `"x-note":"self"`) {
		t.Errorf("graph_edge.extras = %s", edgeExtras)
	}

	var policyExtras string
	if err := s.db.QueryRow("SELECT extras FROM policy WHERE graph_version_id = ?", frozen.GraphVersionID).
		Scan(&policyExtras); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(policyExtras, `"x-source":"manual"`) {
		t.Errorf("policy.extras = %s", policyExtras)
	}

	var config string
	if err := s.db.QueryRow(`SELECT config FROM graph_node WHERE graph_version_id = ? AND node_key = 'work'`,
		frozen.GraphVersionID).Scan(&config); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"x-owner":"ops"`,
		`"x-cmd-id":"7"`,
		`"x-reviewer":"bot"`,
		`"x-proxy":"direct"`,
		`"x-backoff-policy":"exp"`,
	} {
		if !strings.Contains(config, want) {
			t.Errorf("node config missing %s: %s", want, config)
		}
	}
}
