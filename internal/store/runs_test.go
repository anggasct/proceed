package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func seedListedRun(t *testing.T, s *Store, graphVersionID, status string, createdAt int64) string {
	t.Helper()
	var digest string
	if err := s.db.QueryRow("SELECT definition_digest FROM graph_version WHERE id = ?",
		graphVersionID).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("run-%s-%d", status, createdAt)
	_, err := s.db.Exec(`INSERT INTO graph_run (id, graph_version_id, definition_digest, status, created_at)
VALUES (?, ?, ?, ?, ?)`, id, graphVersionID, digest, status, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestListRunsNewestFirst(t *testing.T) {
	s := openTestStore(t)
	src := readFixture(t, "../../internal/compiler/testdata/customer-research.yaml")
	doc := compileFixture(t, src)
	frozen, err := s.FreezeDefinition(context.Background(), "customer-research.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	seedListedRun(t, s, frozen.GraphVersionID, "completed", now-2000)
	seedListedRun(t, s, frozen.GraphVersionID, "failed", now-1000)
	seedListedRun(t, s, frozen.GraphVersionID, "running", now)

	summaries, err := s.ListRuns(context.Background(), "", 50)
	if err != nil {
		t.Fatalf("ListRuns() error = %v", err)
	}
	if len(summaries) != 3 {
		t.Fatalf("len = %d, want 3", len(summaries))
	}
	wantOrder := []string{"running", "failed", "completed"}
	for i, want := range wantOrder {
		if summaries[i].Status != want {
			t.Fatalf("summaries[%d].Status = %q, want %q", i, summaries[i].Status, want)
		}
	}
	if summaries[0].GraphName == "" {
		t.Fatal("GraphName is empty")
	}
}

func TestListRunsFiltersByStatus(t *testing.T) {
	s := openTestStore(t)
	src := readFixture(t, "../../internal/compiler/testdata/customer-research.yaml")
	doc := compileFixture(t, src)
	frozen, err := s.FreezeDefinition(context.Background(), "customer-research.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	seedListedRun(t, s, frozen.GraphVersionID, "completed", now-1000)
	seedListedRun(t, s, frozen.GraphVersionID, "failed", now)

	summaries, err := s.ListRuns(context.Background(), "failed", 50)
	if err != nil {
		t.Fatalf("ListRuns() error = %v", err)
	}
	if len(summaries) != 1 || summaries[0].Status != "failed" {
		t.Fatalf("summaries = %+v, want one failed run", summaries)
	}
}

func TestListRunsRejectsBadInput(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.ListRuns(context.Background(), "waiting", 50); ErrorCode(err) != "GRAPH_INVALID" {
		t.Fatalf("status error = %v, want GRAPH_INVALID", err)
	}
	if _, err := s.ListRuns(context.Background(), "", 0); ErrorCode(err) != "GRAPH_INVALID" {
		t.Fatalf("limit error = %v, want GRAPH_INVALID", err)
	}
	if _, err := s.ListRuns(context.Background(), "", 501); ErrorCode(err) != "GRAPH_INVALID" {
		t.Fatalf("limit error = %v, want GRAPH_INVALID", err)
	}
}
