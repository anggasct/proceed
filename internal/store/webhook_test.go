package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"proceed/internal/compiler"
)

const triggerGraph = `schema: proceed/v1
name: trigger-store
params:
  - { name: env, type: string, default: staging }
nodes:
  - id: call
    type: task
    executor: { kind: http, method: GET, url: "http://example.test/ping" }
    contract: pure
    terminal: true
    capability:
      network:
        allowlisted_hosts: [example.test]
edges: []
`

func freezeTriggerGraph(t *testing.T, s *Store) string {
	t.Helper()
	doc, err := compiler.Parse([]byte(triggerGraph))
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := s.FreezeDefinition(context.Background(), "trigger.yaml", []byte(triggerGraph), doc)
	if err != nil {
		t.Fatal(err)
	}
	return frozen.GraphVersionID
}

func TestWebhookTriggerCRUD(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeTriggerGraph(t, s)

	if err := s.AddWebhookTrigger(context.Background(), "deploy", versionID); err != nil {
		t.Fatal(err)
	}
	err = s.AddWebhookTrigger(context.Background(), "deploy", versionID)
	if !IsCode(err, CodeGraphInvalid) || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("duplicate add error = %v", err)
	}
	err = s.AddWebhookTrigger(context.Background(), "other", "01NOPE")
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("unknown version error = %v", err)
	}

	triggers, err := s.ListWebhookTriggers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(triggers) != 1 || triggers[0].Name != "deploy" {
		t.Fatalf("triggers = %+v", triggers)
	}
	if triggers[0].GraphVersionID != versionID || triggers[0].DefinitionDigest == "" {
		t.Fatalf("trigger row = %+v", triggers[0])
	}

	got, err := s.WebhookTrigger(context.Background(), "deploy")
	if err != nil || got == nil || got.GraphVersionID != versionID {
		t.Fatalf("lookup = %+v %v", got, err)
	}
	missing, err := s.WebhookTrigger(context.Background(), "nope")
	if err != nil || missing != nil {
		t.Fatalf("missing lookup = %+v %v", missing, err)
	}

	removed, err := s.RemoveWebhookTrigger(context.Background(), "deploy")
	if err != nil || !removed {
		t.Fatalf("remove = %v %v", removed, err)
	}
	removed, err = s.RemoveWebhookTrigger(context.Background(), "deploy")
	if err != nil || removed {
		t.Fatalf("second remove = %v %v", removed, err)
	}
}

func TestCreateRunRecordsTriggerName(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	versionID := freezeTriggerGraph(t, s)

	run, err := s.CreateRun(context.Background(), versionID, nil, "deploy")
	if err != nil {
		t.Fatal(err)
	}
	var triggerName string
	if err := s.db.QueryRow("SELECT trigger_name FROM graph_run WHERE id = ?", run.ID).Scan(&triggerName); err != nil {
		t.Fatal(err)
	}
	if triggerName != "deploy" {
		t.Fatalf("trigger_name = %q", triggerName)
	}

	plain, err := s.CreateRun(context.Background(), versionID, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	var none *string
	if err := s.db.QueryRow("SELECT trigger_name FROM graph_run WHERE id = ?", plain.ID).Scan(&none); err != nil {
		t.Fatal(err)
	}
	if none != nil {
		t.Fatalf("plain run trigger_name = %v", none)
	}

	report, err := s.RebuildProjections(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Diverged {
		t.Fatal("projection rebuild diverged")
	}
	if err := s.db.QueryRow("SELECT trigger_name FROM graph_run WHERE id = ?", run.ID).Scan(&triggerName); err != nil {
		t.Fatal(err)
	}
	if triggerName != "deploy" {
		t.Fatalf("rebuilt trigger_name = %q", triggerName)
	}
}

func TestFreshStoreHasTriggerNameColumn(t *testing.T) {
	s := openTestStore(t)

	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('graph_run') WHERE name = 'trigger_name'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("trigger_name column missing in fresh baseline store")
	}
}
