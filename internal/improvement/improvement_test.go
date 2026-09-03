package improvement_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/compiler"
	"proceed/internal/controller"
	"proceed/internal/executor"
	"proceed/internal/improvement"
	"proceed/internal/store"
)

func openTestStore(tb testing.TB) *store.Store {
	tb.Helper()
	st, err := store.Open(filepath.Join(tb.TempDir(), "proceed.db"))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	return st
}

func seedGraphAndVersion(t *testing.T, st *store.Store, name string) (string, string, string) {
	t.Helper()
	yamlDoc := `schema: proceed/v1
name: ` + name + `
nodes:
  - id: step1
    type: task
    contract: pure
    terminal: true
    executor: { kind: shell, command: ["echo", "hello"] }
edges: []
`
	doc, err := compiler.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("failed to parse graph: %v", err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatalf("failed to validate graph: %v", err)
	}
	frozen, err := st.FreezeDefinition(context.Background(), name+".yaml", []byte(yamlDoc), doc)
	if err != nil {
		t.Fatalf("failed to freeze definition: %v", err)
	}
	return frozen.GraphID, frozen.GraphVersionID, frozen.Digest
}

func mustCreateRun(t *testing.T, st *store.Store, versionID string) string {
	t.Helper()
	run, err := st.CreateRun(context.Background(), versionID)
	if err != nil {
		t.Fatalf("failed to create run: %v", err)
	}
	return run.ID
}

func appendTerminalEvent(t *testing.T, st *store.Store, runID, eventType, detail string) string {
	t.Helper()
	eventID := ulid.Make().String()
	now := time.Now().UnixMilli()
	payload := `{}`
	if detail != "" {
		payload = `{"detail":"` + detail + `"}`
	}
	_, err := st.Append(context.Background(), store.Event{
		EventID:       eventID,
		RunID:         runID,
		Sequence:      2,
		SchemaVersion: "proceed/v1",
		Type:          eventType,
		OccurredAt:    now,
		RecordedAt:    now,
		ActorType:     "controller",
		ActorID:       "controller-test",
		Payload:       payload,
	})
	if err != nil {
		t.Fatalf("failed to append terminal event %s: %v", eventType, err)
	}
	return eventID
}

func createGrantApproval(t *testing.T, st *store.Store, runID, versionID string) string {
	t.Helper()
	return createApproval(t, st, runID, versionID, time.Now().UnixMilli()+60000)
}

func createApproval(t *testing.T, st *store.Store, runID, versionID string, expiresAt int64) string {
	t.Helper()
	approvalID := ulid.Make().String()
	nodeID := ulid.Make().String()
	nodeKey := "gate-" + approvalID[len(approvalID)-8:]
	now := time.Now().UnixMilli()

	_, err := st.DB().Exec(`
INSERT INTO run_node (id, run_id, node_key, status, attempt_count, started_at)
VALUES (?, ?, ?, 'waiting', 1, ?)`, nodeID, runID, nodeKey, now)
	if err != nil {
		t.Fatalf("failed to create run_node: %v", err)
	}

	_, err = st.DB().Exec(`
INSERT INTO approval (id, run_id, run_node_id, graph_version_id, requested_action, evidence_references,
                      required_scope, expires_at, decision, decided_by, decided_at, created_at)
VALUES (?, ?, ?, ?, 'deploy', '[]', 'approve', ?, 'grant', 'operator', ?, ?)`,
		approvalID, runID, nodeID, versionID, expiresAt, now, now)
	if err != nil {
		t.Fatalf("failed to create approval: %v", err)
	}
	return approvalID
}

// Verifies that a terminal run (completed, failed, cancelled, abandoned) produces
// exactly one outcome row + anchor pinning the run's definition digest.
func TestTerminalRunProducesOneOutcomeAndAnchor(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	_, versionID, digest := seedGraphAndVersion(t, st, "terminal-run-graph")

	cases := []struct {
		name      string
		eventType string
		expected  string
		detail    string
	}{
		{"completed run", "run_completed", "completed", ""},
		{"failed run", "run_failed", "failed", "attempt limit reached"},
		{"cancelled run", "run_cancelled", "cancelled", "operator cancelled"},
		{"abandoned run", "run_abandoned", "abandoned", "controller lease timeout"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runID := mustCreateRun(t, st, versionID)
			appendTerminalEvent(t, st, runID, tc.eventType, tc.detail)

			outcome, err := svc.GetOutcome(ctx, runID)
			if err != nil {
				t.Fatalf("failed to get outcome: %v", err)
			}
			if outcome == nil {
				t.Fatalf("expected outcome row for run %s, got nil", runID)
			}
			if outcome.Result != tc.expected {
				t.Errorf("outcome result = %q, want %q", outcome.Result, tc.expected)
			}
			if tc.detail != "" && outcome.Detail != tc.detail {
				t.Errorf("outcome detail = %q, want %q", outcome.Detail, tc.detail)
			}

			// Verify anchor pinning the definition digest
			anchor, err := svc.GetAnchor(ctx, outcome.AnchorID)
			if err != nil {
				t.Fatalf("failed to get anchor: %v", err)
			}
			if anchor == nil {
				t.Fatalf("expected anchor row for anchor %s, got nil", outcome.AnchorID)
			}
			if anchor.GraphVersionID != versionID {
				t.Errorf("anchor version = %q, want %q", anchor.GraphVersionID, versionID)
			}
			if anchor.DefinitionDigest != digest {
				t.Errorf("anchor digest = %q, want %q", anchor.DefinitionDigest, digest)
			}

			// Exactly one outcome row check
			var outcomeCount int
			err = st.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM outcome WHERE run_id = ?", runID).Scan(&outcomeCount)
			if err != nil {
				t.Fatalf("failed to count outcomes: %v", err)
			}
			if outcomeCount != 1 {
				t.Errorf("outcome count = %d, want 1", outcomeCount)
			}
		})
	}
}

// Verifies that controller abandonment produces an outcome and anchor.
func TestControllerAbandonRunProducesOutcomeAndAnchor(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	_, versionID, digest := seedGraphAndVersion(t, st, "abandon-test-graph")
	runID := mustCreateRun(t, st, versionID)

	ctrl, err := controller.New(st, controller.DefaultConfig(), map[executor.Kind]executor.Executor{})
	if err != nil {
		t.Fatalf("failed to initialize controller: %v", err)
	}

	detailMsg := "operator decided to abandon run"
	if err := ctrl.AbandonRun(ctx, runID, detailMsg); err != nil {
		t.Fatalf("failed to abandon run: %v", err)
	}

	outcome, err := svc.GetOutcome(ctx, runID)
	if err != nil {
		t.Fatalf("failed to get outcome: %v", err)
	}
	if outcome == nil {
		t.Fatal("expected outcome row, got nil")
	}
	if outcome.Result != "abandoned" {
		t.Errorf("outcome result = %q, want abandoned", outcome.Result)
	}
	if outcome.Detail != detailMsg {
		t.Errorf("outcome detail = %q, want %q", outcome.Detail, detailMsg)
	}

	anchor, err := svc.GetAnchor(ctx, outcome.AnchorID)
	if err != nil {
		t.Fatalf("failed to get anchor: %v", err)
	}
	if anchor == nil || anchor.DefinitionDigest != digest {
		t.Fatalf("anchor digest mismatch: got %+v, want digest %s", anchor, digest)
	}
}

// Verifies that metrics derived from outcomes (pass rate, latency from timings)
// join to anchors and versions correctly.
func TestDerivedMetricsJoinToAnchorsAndVersions(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	_, versionID, digest := seedGraphAndVersion(t, st, "metrics-graph")

	// 2 completed runs and 1 failed run
	run1 := mustCreateRun(t, st, versionID)
	appendTerminalEvent(t, st, run1, "run_completed", "")
	run2 := mustCreateRun(t, st, versionID)
	appendTerminalEvent(t, st, run2, "run_completed", "")
	run3 := mustCreateRun(t, st, versionID)
	appendTerminalEvent(t, st, run3, "run_failed", "timeout")

	outcome1, err := svc.GetOutcome(ctx, run1)
	if err != nil || outcome1 == nil {
		t.Fatalf("failed to get outcome1: %v", err)
	}

	derived, err := svc.DeriveMetrics(ctx, outcome1.AnchorID)
	if err != nil {
		t.Fatalf("failed to derive metrics: %v", err)
	}
	if len(derived) == 0 {
		t.Fatal("expected derived metrics, got none")
	}

	metrics, err := svc.ListMetricsForAnchor(ctx, outcome1.AnchorID)
	if err != nil {
		t.Fatalf("failed to list metrics for anchor: %v", err)
	}
	if len(metrics) == 0 {
		t.Fatal("expected persisted metrics for anchor, got 0")
	}

	foundPassRate := false
	for _, m := range metrics {
		if m.Name == "pass_rate" {
			foundPassRate = true
			expectedPassRate := 2.0 / 3.0
			if m.Value < expectedPassRate-0.01 || m.Value > expectedPassRate+0.01 {
				t.Errorf("pass_rate = %f, want %f", m.Value, expectedPassRate)
			}
			if m.Unit != "ratio" {
				t.Errorf("pass_rate unit = %q, want ratio", m.Unit)
			}

			// Verify query join to anchor and graph_version
			var joinedVersionID, joinedDigest string
			err = st.DB().QueryRowContext(ctx, `
SELECT gv.id, gv.definition_digest
FROM metric m
JOIN anchor a ON a.id = m.anchor_id
JOIN graph_version gv ON gv.id = a.graph_version_id
WHERE m.id = ?`, m.ID).Scan(&joinedVersionID, &joinedDigest)
			if err != nil {
				t.Fatalf("failed to join metric -> anchor -> version: %v", err)
			}
			if joinedVersionID != versionID || joinedDigest != digest {
				t.Errorf("joined version=%s, digest=%s; want %s, %s", joinedVersionID, joinedDigest, versionID, digest)
			}
		}
	}
	if !foundPassRate {
		t.Error("expected pass_rate metric in derived list")
	}
}

// Verifies that anchorless metric insertion is rejected by the store.
func TestAnchorlessMetricInsertionRejected(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	// 1. Metric with empty AnchorID
	_, err := svc.RecordMetric(ctx, improvement.Metric{
		AnchorID: "",
		Name:     "unanchored_metric",
		Value:    42.0,
		Unit:     "count",
	})
	if err == nil {
		t.Fatal("expected error for empty AnchorID, got nil")
	}

	// 2. Metric with non-existent AnchorID
	nonExistentAnchorID := ulid.Make().String()
	_, err = svc.RecordMetric(ctx, improvement.Metric{
		AnchorID: nonExistentAnchorID,
		Name:     "orphan_metric",
		Value:    10.0,
		Unit:     "score",
	})
	if err == nil {
		t.Fatal("expected error for non-existent AnchorID, got nil")
	}

	// 3. Direct SQL insert into metric table without anchor (foreign key violation)
	_, err = st.DB().ExecContext(ctx, `
INSERT INTO metric (id, anchor_id, name, value, unit, recorded_at)
VALUES (?, ?, 'sql_orphan', 1.0, 'unit', ?)`, ulid.Make().String(), nonExistentAnchorID, time.Now().UnixMilli())
	if err == nil {
		t.Fatal("expected database constraint failure on foreign key anchor_id, got nil")
	}
}

// Verifies proposal lifecycle: draft -> proposed -> approved requires approval reference;
// rejection records rationale.
func TestProposalLifecycle(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	_, versionID, _ := seedGraphAndVersion(t, st, "proposal-graph")
	runID := mustCreateRun(t, st, versionID)
	appendTerminalEvent(t, st, runID, "run_completed", "")

	// 1. Create proposal (initial status draft)
	p, err := svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID,
		Rationale:            "reduce timeout for step1 to 5s",
		ProposedChange:       `{"step1": {"timeout_ms": 5000}}`,
	})
	if err != nil {
		t.Fatalf("failed to create proposal: %v", err)
	}
	if p.Status != improvement.ProposalStatusDraft {
		t.Errorf("initial status = %q, want draft", p.Status)
	}

	// 2. Draft -> Proposed
	p, err = svc.SubmitProposal(ctx, p.ID)
	if err != nil {
		t.Fatalf("failed to submit proposal: %v", err)
	}
	if p.Status != improvement.ProposalStatusProposed {
		t.Errorf("status = %q, want proposed", p.Status)
	}

	// 3. Attempt to approve without approval reference -> MUST fail
	_, err = svc.ApproveProposal(ctx, p.ID, "")
	if err == nil {
		t.Fatal("expected error when approving without approval reference, got nil")
	}

	// 4. Attempt to approve with non-existent approval ID -> MUST fail
	_, err = svc.ApproveProposal(ctx, p.ID, ulid.Make().String())
	if err == nil {
		t.Fatal("expected error when approving with invalid approval ID, got nil")
	}

	// 5. Database constraint verification: CHECK (status <> 'approved' OR approval_id IS NOT NULL)
	_, err = st.DB().ExecContext(ctx, `
UPDATE policy_change_proposal SET status = 'approved', approval_id = NULL WHERE id = ?`, p.ID)
	if err == nil {
		t.Fatal("expected database CHECK constraint failure when status=approved without approval_id, got nil")
	}

	// 6. Approve with valid grant approval reference -> succeeds
	approvalID := createGrantApproval(t, st, runID, versionID)
	approvedP, err := svc.ApproveProposal(ctx, p.ID, approvalID)
	if err != nil {
		t.Fatalf("failed to approve proposal: %v", err)
	}
	if approvedP.Status != improvement.ProposalStatusApproved {
		t.Errorf("status = %q, want approved", approvedP.Status)
	}
	if approvedP.ApprovalID == nil || *approvedP.ApprovalID != approvalID {
		t.Errorf("approval_id = %v, want %s", approvedP.ApprovalID, approvalID)
	}
	if approvedP.DecidedAt == nil {
		t.Error("expected decided_at to be recorded on approved proposal")
	}

	// 7. Test Rejection with rationale
	originalRationale := "increase retry limit to 10"
	p2, err := svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID,
		Rationale:            originalRationale,
		ProposedChange:       `{"step1": {"max_attempts": 10}}`,
	})
	if err != nil {
		t.Fatalf("failed to create proposal 2: %v", err)
	}
	p2, err = svc.SubmitProposal(ctx, p2.ID)
	if err != nil {
		t.Fatalf("failed to submit proposal 2: %v", err)
	}

	// Rejection without rationale -> MUST fail
	_, err = svc.RejectProposal(ctx, p2.ID, "")
	if err == nil {
		t.Fatal("expected error when rejecting without rationale, got nil")
	}

	// Rejection with rationale -> succeeds and preserves the proposer's rationale
	rejectRationale := "retry limit exceeds cluster policy threshold of 5"
	rejectedP, err := svc.RejectProposal(ctx, p2.ID, rejectRationale)
	if err != nil {
		t.Fatalf("failed to reject proposal: %v", err)
	}
	if rejectedP.Status != improvement.ProposalStatusRejected {
		t.Errorf("status = %q, want rejected", rejectedP.Status)
	}
	if rejectedP.Rationale != originalRationale {
		t.Errorf("rationale = %q, want original %q preserved", rejectedP.Rationale, originalRationale)
	}
	if rejectedP.RejectionReason == nil || *rejectedP.RejectionReason != rejectRationale {
		t.Errorf("rejection_reason = %v, want %q", rejectedP.RejectionReason, rejectRationale)
	}
	if rejectedP.DecidedAt == nil {
		t.Error("expected decided_at to be set on rejected proposal")
	}

	// Rejection reason is retrievable from the stored row
	reloaded, err := svc.GetProposal(ctx, p2.ID)
	if err != nil {
		t.Fatalf("failed to reload rejected proposal: %v", err)
	}
	if reloaded == nil {
		t.Fatal("expected rejected proposal row, got nil")
	}
	if reloaded.Rationale != originalRationale {
		t.Errorf("persisted rationale = %q, want original %q preserved", reloaded.Rationale, originalRationale)
	}
	if reloaded.RejectionReason == nil || *reloaded.RejectionReason != rejectRationale {
		t.Errorf("persisted rejection_reason = %v, want %q", reloaded.RejectionReason, rejectRationale)
	}
}

// Verifies that proposal_metric association renders the evidence chain
// (proposal -> metrics -> anchors -> versions) in the query join.
func TestEvidenceChainQueryJoin(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	_, versionID, digest := seedGraphAndVersion(t, st, "evidence-graph")
	runID := mustCreateRun(t, st, versionID)
	appendTerminalEvent(t, st, runID, "run_completed", "")

	outcome, err := svc.GetOutcome(ctx, runID)
	if err != nil || outcome == nil {
		t.Fatalf("failed to get outcome: %v", err)
	}

	m1, err := svc.RecordMetric(ctx, improvement.Metric{
		AnchorID: outcome.AnchorID,
		Name:     "latency_p50",
		Value:    124.5,
		Unit:     "ms",
	})
	if err != nil {
		t.Fatalf("failed to record metric 1: %v", err)
	}

	m2, err := svc.RecordMetric(ctx, improvement.Metric{
		AnchorID: outcome.AnchorID,
		Name:     "error_rate",
		Value:    0.02,
		Unit:     "ratio",
	})
	if err != nil {
		t.Fatalf("failed to record metric 2: %v", err)
	}

	p, err := svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID,
		Rationale:            "latency spike requires edge condition adjustment",
		ProposedChange:       `{"step1": {"cache": true}}`,
	})
	if err != nil {
		t.Fatalf("failed to create proposal: %v", err)
	}

	// Associate metrics with proposal
	err = svc.AssociateMetrics(ctx, p.ID, []string{m1.ID, m2.ID})
	if err != nil {
		t.Fatalf("failed to associate metrics: %v", err)
	}

	// Render evidence chain
	chain, err := svc.GetEvidenceChain(ctx, p.ID)
	if err != nil {
		t.Fatalf("failed to get evidence chain: %v", err)
	}
	if chain == nil {
		t.Fatal("expected evidence chain, got nil")
	}

	if chain.Proposal.ID != p.ID {
		t.Errorf("proposal ID = %q, want %q", chain.Proposal.ID, p.ID)
	}
	if chain.Proposal.TargetDigest != digest {
		t.Errorf("proposal target digest = %q, want %q", chain.Proposal.TargetDigest, digest)
	}

	if len(chain.Evidence) != 2 {
		t.Fatalf("expected 2 evidence items, got %d", len(chain.Evidence))
	}

	for _, ev := range chain.Evidence {
		if ev.Anchor.ID != outcome.AnchorID {
			t.Errorf("evidence anchor ID = %q, want %q", ev.Anchor.ID, outcome.AnchorID)
		}
		if ev.Anchor.GraphVersionID != versionID {
			t.Errorf("evidence version ID = %q, want %q", ev.Anchor.GraphVersionID, versionID)
		}
		if ev.Digest != digest {
			t.Errorf("evidence definition digest = %q, want %q", ev.Digest, digest)
		}
	}
}

// Verifies that a proposal targeting a superseded version records the supersession explicitly,
// with no silent retargeting permitted.
func TestProposalTargetingSupersededVersion(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	_, versionID1, _ := seedGraphAndVersion(t, st, "superseded-graph-v1")

	// Create an initial proposal while version is frozen
	priorP, err := svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID1,
		Rationale:            "initial baseline proposal",
		ProposedChange:       `{"timeout_ms": 10000}`,
	})
	if err != nil {
		t.Fatalf("failed to create baseline proposal: %v", err)
	}

	// Mark version 1 as superseded
	_, err = st.DB().ExecContext(ctx, `UPDATE graph_version SET status = 'superseded' WHERE id = ?`, versionID1)
	if err != nil {
		t.Fatalf("failed to set version status superseded: %v", err)
	}

	// 1. Attempt proposal targeting superseded version WITHOUT supersedes_proposal_id -> MUST fail
	_, err = svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID1,
		Rationale:            "patching an old version",
		ProposedChange:       `{"patch": true}`,
		SupersedesProposalID: "", // empty -> silent retarget denied
	})
	if err == nil {
		t.Fatal("expected error when targeting superseded version without explicit supersedes_proposal_id, got nil")
	}

	// 2. Target superseded version WITH explicit supersedes_proposal_id -> succeeds
	p, err := svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID1,
		Rationale:            "explicit revision of proposal " + priorP.ID,
		ProposedChange:       `{"patch": true}`,
		SupersedesProposalID: priorP.ID,
	})
	if err != nil {
		t.Fatalf("failed to create proposal with explicit supersession: %v", err)
	}
	if p.SupersedesProposalID == nil || *p.SupersedesProposalID != priorP.ID {
		t.Errorf("supersedes_proposal_id = %v, want %q", p.SupersedesProposalID, priorP.ID)
	}
}

// Verifies that an approval reference is only valid when it is a grant bound to
// the proposal's target graph version and has not expired.
func TestApproveProposalRequiresMatchingUnexpiredApproval(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	_, versionID1, _ := seedGraphAndVersion(t, st, "approval-bind-graph-v1")
	_, versionID2, _ := seedGraphAndVersion(t, st, "approval-bind-graph-v2")

	p, err := svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID1,
		Rationale:            "tune gate timeout",
		ProposedChange:       `{"step1": {"timeout_ms": 3000}}`,
	})
	if err != nil {
		t.Fatalf("failed to create proposal: %v", err)
	}
	if _, err := svc.SubmitProposal(ctx, p.ID); err != nil {
		t.Fatalf("failed to submit proposal: %v", err)
	}

	// 1. Grant approval bound to a different graph version -> MUST fail
	runID2 := mustCreateRun(t, st, versionID2)
	foreignApprovalID := createGrantApproval(t, st, runID2, versionID2)
	_, err = svc.ApproveProposal(ctx, p.ID, foreignApprovalID)
	if err == nil {
		t.Fatal("expected error when approving with approval bound to a different graph version, got nil")
	}

	// 2. Expired grant approval bound to the target version -> MUST fail
	runID1 := mustCreateRun(t, st, versionID1)
	expiredApprovalID := createApproval(t, st, runID1, versionID1, time.Now().UnixMilli()-1000)
	_, err = svc.ApproveProposal(ctx, p.ID, expiredApprovalID)
	if err == nil {
		t.Fatal("expected error when approving with an expired approval, got nil")
	}

	// Proposal must remain proposed with no approval recorded
	reloaded, err := svc.GetProposal(ctx, p.ID)
	if err != nil {
		t.Fatalf("failed to reload proposal: %v", err)
	}
	if reloaded.Status != improvement.ProposalStatusProposed {
		t.Errorf("status = %q, want proposed", reloaded.Status)
	}
	if reloaded.ApprovalID != nil {
		t.Errorf("approval_id = %v, want nil after failed approvals", reloaded.ApprovalID)
	}

	// 3. Unexpired grant approval bound to the target version -> succeeds
	validApprovalID := createGrantApproval(t, st, runID1, versionID1)
	if _, err := svc.ApproveProposal(ctx, p.ID, validApprovalID); err != nil {
		t.Fatalf("failed to approve proposal with matching unexpired approval: %v", err)
	}
}

// Verifies that concurrent approval decisions produce exactly one winner: the
// losing decision returns an error and the stored row records only the winning
// approval reference.
func TestApproveProposalConcurrentDecisionSingleWinner(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	_, versionID, _ := seedGraphAndVersion(t, st, "concurrent-approval-graph")
	runID := mustCreateRun(t, st, versionID)

	p, err := svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID,
		Rationale:            "raise batch size",
		ProposedChange:       `{"step1": {"batch_size": 64}}`,
	})
	if err != nil {
		t.Fatalf("failed to create proposal: %v", err)
	}
	if _, err := svc.SubmitProposal(ctx, p.ID); err != nil {
		t.Fatalf("failed to submit proposal: %v", err)
	}

	approvalA := createGrantApproval(t, st, runID, versionID)
	approvalB := createGrantApproval(t, st, runID, versionID)

	start := make(chan struct{})
	results := make(chan error, 2)
	winner := make(chan string, 2)
	for _, approvalID := range []string{approvalA, approvalB} {
		go func(id string) {
			<-start
			_, err := svc.ApproveProposal(ctx, p.ID, id)
			results <- err
			if err == nil {
				winner <- id
			}
		}(approvalID)
	}
	close(start)

	var winnerID string
	for i := 0; i < 2; i++ {
		if err := <-results; err == nil {
			if winnerID != "" {
				t.Fatal("expected exactly one successful approval, got two")
			}
			winnerID = <-winner
		}
	}
	if winnerID == "" {
		t.Fatal("expected one successful approval, got none")
	}

	reloaded, err := svc.GetProposal(ctx, p.ID)
	if err != nil {
		t.Fatalf("failed to reload proposal: %v", err)
	}
	if reloaded.Status != improvement.ProposalStatusApproved {
		t.Errorf("status = %q, want approved", reloaded.Status)
	}
	if reloaded.ApprovalID == nil || *reloaded.ApprovalID != winnerID {
		t.Errorf("approval_id = %v, want winning approval %s", reloaded.ApprovalID, winnerID)
	}
}

// Verifies supersede guards: the superseding proposal must exist, and only
// draft/proposed proposals can be superseded.
func TestSupersedeProposalGuards(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	_, versionID, _ := seedGraphAndVersion(t, st, "supersede-guard-graph")
	runID := mustCreateRun(t, st, versionID)

	proposedP, err := svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID,
		Rationale:            "baseline",
		ProposedChange:       `{"a": 1}`,
	})
	if err != nil {
		t.Fatalf("failed to create proposal: %v", err)
	}
	replacementP, err := svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID,
		Rationale:            "replacement",
		ProposedChange:       `{"a": 2}`,
	})
	if err != nil {
		t.Fatalf("failed to create replacement proposal: %v", err)
	}

	// 1. Empty superseding proposal ID -> MUST fail
	if _, err := svc.SupersedeProposal(ctx, proposedP.ID, ""); err == nil {
		t.Fatal("expected error when superseding without a superseding proposal, got nil")
	}

	// 2. Non-existent superseding proposal ID -> MUST fail
	if _, err := svc.SupersedeProposal(ctx, proposedP.ID, ulid.Make().String()); err == nil {
		t.Fatal("expected error when superseding proposal does not exist, got nil")
	}

	// 3. Approved proposal -> MUST fail (a recorded human decision is never erased)
	approvedP, err := svc.CreateProposal(ctx, improvement.CreateProposalRequest{
		TargetGraphVersionID: versionID,
		Rationale:            "approved change",
		ProposedChange:       `{"a": 3}`,
	})
	if err != nil {
		t.Fatalf("failed to create approved candidate: %v", err)
	}
	if _, err := svc.SubmitProposal(ctx, approvedP.ID); err != nil {
		t.Fatalf("failed to submit approved candidate: %v", err)
	}
	approvalID := createGrantApproval(t, st, runID, versionID)
	if _, err := svc.ApproveProposal(ctx, approvedP.ID, approvalID); err != nil {
		t.Fatalf("failed to approve candidate: %v", err)
	}
	if _, err := svc.SupersedeProposal(ctx, approvedP.ID, replacementP.ID); err == nil {
		t.Fatal("expected error when superseding an approved proposal, got nil")
	}

	// 4. Proposed proposal -> succeeds
	supersededP, err := svc.SupersedeProposal(ctx, proposedP.ID, replacementP.ID)
	if err != nil {
		t.Fatalf("failed to supersede proposed proposal: %v", err)
	}
	if supersededP.Status != improvement.ProposalStatusSuperseded {
		t.Errorf("status = %q, want superseded", supersededP.Status)
	}
	if supersededP.DecidedAt == nil {
		t.Error("expected decided_at to be set on superseded proposal")
	}

	// 5. Already-superseded proposal -> idempotent, no error
	again, err := svc.SupersedeProposal(ctx, proposedP.ID, replacementP.ID)
	if err != nil {
		t.Fatalf("expected idempotent supersede, got error: %v", err)
	}
	if again.Status != improvement.ProposalStatusSuperseded {
		t.Errorf("status = %q, want superseded", again.Status)
	}
}

// Verifies that metrics with malformed JSON dimensions are rejected at the
// write boundary instead of breaking overview serialization later.
func TestRecordMetricRejectsInvalidDimensionsJSON(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)
	ctx := context.Background()

	_, versionID, _ := seedGraphAndVersion(t, st, "metric-dims-graph")
	runID := mustCreateRun(t, st, versionID)
	appendTerminalEvent(t, st, runID, "run_completed", "")

	outcome, err := svc.GetOutcome(ctx, runID)
	if err != nil || outcome == nil {
		t.Fatalf("failed to get outcome: %v", err)
	}

	_, err = svc.RecordMetric(ctx, improvement.Metric{
		AnchorID:   outcome.AnchorID,
		Name:       "bad_dims",
		Value:      1.0,
		Unit:       "count",
		Dimensions: json.RawMessage(`{not json`),
	})
	if err == nil {
		t.Fatal("expected error for malformed JSON dimensions, got nil")
	}

	valid, err := svc.RecordMetric(ctx, improvement.Metric{
		AnchorID:   outcome.AnchorID,
		Name:       "good_dims",
		Value:      2.0,
		Unit:       "count",
		Dimensions: json.RawMessage(`{"route":"fast"}`),
	})
	if err != nil {
		t.Fatalf("failed to record metric with valid dimensions: %v", err)
	}

	metrics, err := svc.ListMetricsForAnchor(ctx, outcome.AnchorID)
	if err != nil {
		t.Fatalf("failed to list metrics: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("metric count = %d, want 1 (only the valid metric persisted)", len(metrics))
	}
	if string(metrics[0].Dimensions) != `{"route":"fast"}` {
		t.Errorf("dimensions = %s, want %s", metrics[0].Dimensions, `{"route":"fast"}`)
	}
	if _, err := json.Marshal(improvement.Overview{Versions: []improvement.VersionSummary{{Metrics: metrics}}}); err != nil {
		t.Fatalf("overview serialization must succeed for valid dimensions: %v", err)
	}
	_ = valid
}

// Verifies that GetOverview propagates query failures instead of returning a
// silently incomplete overview.
func TestGetOverviewPropagatesQueryErrors(t *testing.T) {
	st := openTestStore(t)
	svc := improvement.New(st)

	seedGraphAndVersion(t, st, "overview-error-graph")
	st.Close()

	overview, err := svc.GetOverview(context.Background(), "overview-error-graph")
	if err == nil {
		t.Fatal("expected error from GetOverview against a failed store, got nil")
	}
	if overview != nil {
		t.Errorf("overview = %+v, want nil on error", overview)
	}
}
