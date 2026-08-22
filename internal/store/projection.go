package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
)

type Run struct {
	ID               string
	GraphVersionID   string
	DefinitionDigest string
}

func (s *Store) CreateRun(ctx context.Context, graphVersionID string) (Run, error) {
	now := time.Now().UnixMilli()
	run := Run{ID: ulid.Make().String(), GraphVersionID: graphVersionID}
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx,
			"SELECT definition_digest FROM graph_version WHERE id = ?", graphVersionID).
			Scan(&run.DefinitionDigest); err != nil {
			if err == sql.ErrNoRows {
				return storeErr(CodeGraphInvalid, "graph version %s does not exist", graphVersionID)
			}
			return err
		}
		payload, err := json.Marshal(runStartedPayload{
			GraphVersionID:   graphVersionID,
			DefinitionDigest: run.DefinitionDigest,
		})
		if err != nil {
			return err
		}
		ev := Event{
			EventID:       ulid.Make().String(),
			RunID:         run.ID,
			Sequence:      1,
			SchemaVersion: eventSchemaVersion,
			Type:          "run_started",
			OccurredAt:    now,
			RecordedAt:    now,
			ActorType:     "controller",
			ActorID:       "controller",
			PayloadDigest: payloadDigest(string(payload)),
			Payload:       string(payload),
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO graph_run (id, graph_version_id, definition_digest, status, created_at)
VALUES (?, ?, ?, 'running', ?)`,
			run.ID, run.GraphVersionID, run.DefinitionDigest, now); err != nil {
			return err
		}
		return appendEventTx(ctx, tx, &ev)
	})
	if err != nil {
		return Run{}, err
	}
	return run, nil
}

type runStartedPayload struct {
	GraphVersionID   string `json:"graph_version_id"`
	DefinitionDigest string `json:"definition_digest"`
}

type nodeStartedPayload struct {
	NodeKey            string `json:"node_key"`
	AttemptNo          int64  `json:"attempt_no"`
	Executor           string `json:"executor"`
	SideEffectContract string `json:"side_effect_contract"`
	OperationKey       string `json:"operation_key"`
	LeaseToken         string `json:"lease_token,omitempty"`
	LeaseExpiresAt     int64  `json:"lease_expires_at,omitempty"`
}

type nodeTerminalPayload struct {
	NodeKey            string          `json:"node_key"`
	AttemptNo          int64           `json:"attempt_no"`
	Executor           string          `json:"executor"`
	SideEffectContract string          `json:"side_effect_contract"`
	OperationKey       string          `json:"operation_key"`
	Result             json.RawMessage `json:"result"`
}

type edgeTraversedPayload struct {
	EdgeID        string `json:"edge_id"`
	Route         string `json:"route"`
	SequenceInRun int64  `json:"sequence_in_run"`
}

type artifactPublishedPayload struct {
	NodeKey     string `json:"node_key"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	ContentHash string `json:"content_hash"`
	MediaType   string `json:"media_type"`
	SizeBytes   int64  `json:"size_bytes"`
	Truncated   bool   `json:"truncated"`
}

type evaluationFailedPayload struct {
	ArtifactID         string `json:"artifact_id"`
	EvaluatedByNodeKey string `json:"evaluated_by_node_key"`
	EvidenceRef        string `json:"evidence_ref"`
}

type effectIntentPayload struct {
	NodeAttemptID string `json:"node_attempt_id"`
	OperationKey  string `json:"operation_key"`
	Target        string `json:"target"`
	RequestDigest string `json:"request_digest"`
}

type effectReceiptPayload struct {
	EffectID          string          `json:"effect_id"`
	Status            string          `json:"status"`
	Receipt           json.RawMessage `json:"receipt"`
	ReconciliationRef string          `json:"reconciliation_ref"`
}

type approvalRequestedPayload struct {
	NodeKey            string          `json:"node_key"`
	RequestedAction    json.RawMessage `json:"requested_action"`
	EvidenceReferences json.RawMessage `json:"evidence_references"`
	RequiredScope      string          `json:"required_scope"`
	ExpiresAt          int64           `json:"expires_at"`
}

type approvalDecidedPayload struct {
	ApprovalID string `json:"approval_id"`
	DecidedBy  string `json:"decided_by"`
}

type causalLinkPayload struct {
	TargetNodeKey string `json:"target_node_key"`
	Attribution   string `json:"attribution"`
	SourceKind    string `json:"source_kind"`
	SourceID      string `json:"source_id"`
	CitationType  string `json:"citation_type"`
	CitationID    string `json:"citation_id"`
	GroupKey      string `json:"group_key"`
}

type decisionRecordedPayload struct {
	NodeKey           string              `json:"node_key"`
	Kind              string              `json:"kind"`
	CandidateEdges    []string            `json:"candidate_edges"`
	SelectedEdgeID    string              `json:"selected_edge_id"`
	Rejection         string              `json:"rejection"`
	PredicateSnapshot json.RawMessage     `json:"predicate_snapshot"`
	InputReferences   json.RawMessage     `json:"input_references"`
	PolicyVersion     string              `json:"policy_version"`
	CausalLinks       []causalLinkPayload `json:"causal_links"`
}

type runTerminalPayload struct {
	Detail string `json:"detail"`
}

func decodePayload(ev *Event, dst any) error {
	if err := json.Unmarshal([]byte(ev.Payload), dst); err != nil {
		return storeErr(CodeGraphInvalid, "event %s payload does not decode for type %s: %v",
			ev.EventID, ev.Type, err)
	}
	return nil
}

func rawOr(v json.RawMessage, def string) string {
	if len(v) == 0 {
		return def
	}
	return string(v)
}

func nullableOr(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func runNodeID(ctx context.Context, tx *sql.Tx, runID, nodeKey string) (string, error) {
	var id string
	err := tx.QueryRowContext(ctx,
		"SELECT id FROM run_node WHERE run_id = ? AND node_key = ?", runID, nodeKey).Scan(&id)
	if err == sql.ErrNoRows {
		return "", storeErr(CodeGraphInvalid, "run %s has no node %q", runID, nodeKey)
	}
	return id, err
}

func ensureRunNode(ctx context.Context, tx *sql.Tx, runID, nodeKey string) (string, error) {
	id, err := runNodeID(ctx, tx, runID, nodeKey)
	if err == nil {
		return id, nil
	}
	if se, ok := AsStoreError(err); !ok || se.Code != CodeGraphInvalid {
		return "", err
	}
	id = derivedNodeID(runID, nodeKey)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO run_node (id, run_id, node_key, status, attempt_count)
VALUES (?, ?, ?, 'pending', 0) ON CONFLICT(run_id, node_key) DO NOTHING`,
		id, runID, nodeKey); err != nil {
		return "", err
	}
	return runNodeID(ctx, tx, runID, nodeKey)
}

func derivedNodeID(runID, nodeKey string) string {
	sum := sha256.Sum256([]byte(runID + "\x00" + nodeKey))
	return hex.EncodeToString(sum[:])[:26]
}

func applyProjections(ctx context.Context, tx *sql.Tx, ev *Event) error {
	handler, ok := projectedHandlers[ev.Type]
	if !ok {
		return nil
	}
	return handler(ctx, tx, ev)
}

func onRunStarted(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p runStartedPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	digest := p.DefinitionDigest
	if digest == "" {
		if err := tx.QueryRowContext(ctx,
			"SELECT definition_digest FROM graph_run WHERE id = ?", ev.RunID).Scan(&digest); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO graph_run (id, graph_version_id, definition_digest, status, created_at)
VALUES (?, ?, ?, 'running', ?) ON CONFLICT(id) DO NOTHING`,
		ev.RunID, p.GraphVersionID, digest, ev.OccurredAt)
	return err
}

func onNodeStarted(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p nodeStartedPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	if p.NodeKey == "" {
		return storeErr(CodeGraphInvalid, "event %s node_started requires node_key", ev.EventID)
	}
	attemptNo := p.AttemptNo
	if attemptNo <= 0 {
		attemptNo = 1
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO run_node (id, run_id, node_key, status, attempt_count, started_at)
VALUES (?, ?, ?, 'running', ?, ?)
ON CONFLICT(run_id, node_key) DO UPDATE SET
  status = 'running',
  attempt_count = MAX(run_node.attempt_count, excluded.attempt_count),
  started_at = COALESCE(run_node.started_at, excluded.started_at)`,
		derivedNodeID(ev.RunID, p.NodeKey), ev.RunID, p.NodeKey, attemptNo, ev.OccurredAt); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE graph_run SET started_at = ? WHERE id = ? AND started_at IS NULL`,
		ev.OccurredAt, ev.RunID); err != nil {
		return err
	}
	if p.OperationKey == "" {
		return nil
	}
	nodeID, err := runNodeID(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO node_attempt (id, run_node_id, attempt_no, operation_key, executor,
                          side_effect_contract, lease_token, lease_expires_at, status, started_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running', ?)
ON CONFLICT(run_node_id, attempt_no) DO UPDATE SET
  lease_token = COALESCE(excluded.lease_token, node_attempt.lease_token),
  lease_expires_at = COALESCE(excluded.lease_expires_at, node_attempt.lease_expires_at),
  status = 'running', started_at = COALESCE(node_attempt.started_at, excluded.started_at)`,
		ev.EventID, nodeID, attemptNo, p.OperationKey, p.Executor, p.SideEffectContract,
		nullableOr(p.LeaseToken), nullableInt(p.LeaseExpiresAt), ev.OccurredAt)
	return err
}

func nullableInt(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func onNodeTerminal(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p nodeTerminalPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	nodeID, err := ensureRunNode(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	attemptStatus := map[string]string{
		"node_finished":  "succeeded",
		"node_failed":    "failed",
		"node_uncertain": "uncertain",
	}[ev.Type]
	if p.AttemptNo > 0 {
		var attempts int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM node_attempt WHERE run_node_id = ? AND attempt_no = ?",
			nodeID, p.AttemptNo).Scan(&attempts); err != nil {
			return err
		}
		if attempts == 0 && p.OperationKey != "" {
			if p.Executor == "" || p.SideEffectContract == "" {
				return storeErr(CodeGraphInvalid, "event %s cannot create a rejected attempt without executor metadata", ev.EventID)
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO node_attempt (id, run_node_id, attempt_no, operation_key, executor,
                          side_effect_contract, status, result, finished_at)
VALUES (?, ?, ?, ?, ?, ?, 'failed', ?, ?)`,
				ev.EventID, nodeID, p.AttemptNo, p.OperationKey, p.Executor, p.SideEffectContract,
				nullableOr(rawOr(p.Result, "")), finishedAt(ev, ev.Type)); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE node_attempt SET status = ?, finished_at = COALESCE(?, finished_at), result = COALESCE(?, result)
WHERE run_node_id = ? AND attempt_no = ? AND status <> 'cancelled'`,
			attemptStatus, finishedAt(ev, ev.Type), nullableOr(rawOr(p.Result, "")), nodeID, p.AttemptNo); err != nil {
			return err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
UPDATE node_attempt SET status = ?, finished_at = COALESCE(?, finished_at), result = COALESCE(?, result)
WHERE run_node_id = ? AND attempt_no = (SELECT MAX(attempt_no) FROM node_attempt WHERE run_node_id = ?)
  AND status <> 'cancelled'`,
			attemptStatus, finishedAt(ev, ev.Type), nullableOr(rawOr(p.Result, "")), nodeID, nodeID); err != nil {
			return err
		}
	}
	nodeStatus := map[string]string{
		"node_finished":  "succeeded",
		"node_failed":    "failed",
		"node_uncertain": "uncertain",
	}[ev.Type]
	_, err = tx.ExecContext(ctx, `
UPDATE run_node SET status = ?, attempt_count = MAX(attempt_count, ?), finished_at = COALESCE(?, finished_at)
WHERE id = ? AND status NOT IN ('cancel_requested', 'cancelled')`,
		nodeStatus, p.AttemptNo, finishedAt(ev, ev.Type), nodeID)
	return err
}

func onNodeCancelRequested(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p nodeTerminalPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	nodeID, err := runNodeID(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
UPDATE run_node SET status = 'cancel_requested'
WHERE id = ? AND status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')`, nodeID)
	return err
}

func onNodeSkipped(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p nodeTerminalPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	nodeID, err := ensureRunNode(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
UPDATE run_node SET status = 'skipped', finished_at = ?
WHERE id = ? AND status NOT IN ('cancel_requested', 'cancelled')`, ev.OccurredAt, nodeID)
	return err
}

func onNodeCancelled(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p nodeTerminalPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	nodeID, err := ensureRunNode(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	if p.AttemptNo > 0 {
		if _, err := tx.ExecContext(ctx, `
UPDATE node_attempt SET status = 'cancelled', finished_at = COALESCE(?, finished_at)
WHERE run_node_id = ? AND attempt_no = ? AND status NOT IN ('succeeded', 'failed', 'cancelled')`,
			ev.OccurredAt, nodeID, p.AttemptNo); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `
UPDATE run_node SET status = 'cancelled', finished_at = ?
WHERE id = ? AND status NOT IN ('succeeded', 'failed', 'skipped', 'cancelled')`, ev.OccurredAt, nodeID)
	return err
}

func onNodeRequeued(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p struct {
		NodeKey       string `json:"node_key"`
		AttemptNo     int64  `json:"attempt_no"`
		ResumeAttempt bool   `json:"resume_attempt"`
	}
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	nodeID, err := ensureRunNode(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	if p.ResumeAttempt && p.AttemptNo > 0 {
		if _, err := tx.ExecContext(ctx, `
UPDATE run_node SET status = 'eligible', finished_at = NULL,
  attempt_count = ? - 1
				WHERE id = ? AND status NOT IN ('cancel_requested', 'cancelled')`, p.AttemptNo, nodeID); err != nil {
			return err
		}
		return nil
	}
	_, err = tx.ExecContext(ctx, "UPDATE run_node SET status = 'eligible', finished_at = NULL WHERE id = ? AND status NOT IN ('cancel_requested', 'cancelled')", nodeID)
	return err
}

func onNodeWaiting(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p nodeTerminalPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	nodeID, err := ensureRunNode(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE run_node SET status = 'waiting' WHERE id = ? AND status NOT IN ('cancel_requested', 'cancelled')", nodeID)
	return err
}

func onNodeReconciling(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p nodeTerminalPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	nodeID, err := ensureRunNode(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
UPDATE run_node SET status = 'reconciling'
WHERE id = ? AND status NOT IN ('cancel_requested', 'cancelled')`, nodeID)
	return err
}

func onNodeAttemptFailed(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p nodeTerminalPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	nodeID, err := runNodeID(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE node_attempt SET status = 'failed', finished_at = ?, result = COALESCE(?, result)
WHERE run_node_id = ? AND attempt_no = ?`,
		ev.OccurredAt, nullableOr(rawOr(p.Result, "")), nodeID, p.AttemptNo); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"UPDATE run_node SET status = 'eligible' WHERE id = ? AND status IN ('running','leased')", nodeID)
	return err
}

func onEdgeTraversed(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p edgeTraversedPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO run_edge (id, run_id, edge_id, route, sequence_in_run, traversed_at)
VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		ev.EventID, ev.RunID, p.EdgeID, nullableOr(p.Route), p.SequenceInRun, ev.OccurredAt)
	return err
}

func onArtifactPublished(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p artifactPublishedPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	truncated := 0
	if p.Truncated {
		truncated = 1
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO artifact (id, run_id, produced_by_node_key, name, path, content_hash,
                      media_type, size_bytes, truncated, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		ev.EventID, ev.RunID, p.NodeKey, p.Name, p.Path, p.ContentHash,
		p.MediaType, p.SizeBytes, truncated, ev.OccurredAt)
	return err
}

func onEvaluationFailed(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p evaluationFailedPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO evaluation (id, artifact_id, evaluated_by_node_key, run_id, verdict, evidence_ref, evaluated_at)
VALUES (?, ?, ?, ?, 'failed', ?, ?) ON CONFLICT(id) DO NOTHING`,
		ev.EventID, p.ArtifactID, p.EvaluatedByNodeKey, ev.RunID, nullableOr(p.EvidenceRef), ev.OccurredAt)
	return err
}

func onEffectIntent(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p effectIntentPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO effect (id, node_attempt_id, operation_key, target, status, request_digest,
                   created_at, updated_at)
VALUES (?, ?, ?, ?, 'pending', ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		ev.EventID, p.NodeAttemptID, p.OperationKey, p.Target, p.RequestDigest, ev.OccurredAt, ev.OccurredAt)
	return err
}

func onEffectReceipt(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p effectReceiptPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
UPDATE effect SET status = ?, receipt = COALESCE(?, receipt),
                  reconciliation_ref = COALESCE(?, reconciliation_ref), updated_at = ?
WHERE id = ?`,
		p.Status, nullableOr(rawOr(p.Receipt, "")), nullableOr(p.ReconciliationRef), ev.OccurredAt, p.EffectID)
	return err
}

func onApprovalRequested(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p approvalRequestedPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	nodeID, err := runNodeID(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	var versionID string
	if err := tx.QueryRowContext(ctx,
		"SELECT graph_version_id FROM graph_run WHERE id = ?", ev.RunID).Scan(&versionID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO approval (id, run_id, run_node_id, graph_version_id, requested_action,
                      evidence_references, required_scope, expires_at, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		ev.EventID, ev.RunID, nodeID, versionID, rawOr(p.RequestedAction, "{}"),
		rawOr(p.EvidenceReferences, "[]"), p.RequiredScope, p.ExpiresAt, ev.OccurredAt)
	return err
}

func onApprovalDecided(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p approvalDecidedPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	decision := "deny"
	if ev.Type == "approval_granted" {
		decision = "grant"
	}
	_, err := tx.ExecContext(ctx, `
UPDATE approval SET decision = ?, decided_by = ?, decided_at = ?, decision_idempotency_key = ?
WHERE id = ?`,
		decision, p.DecidedBy, ev.OccurredAt, ev.EventID, p.ApprovalID)
	return err
}

func onDecisionRecorded(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p decisionRecordedPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	nodeID, err := runNodeID(ctx, tx, ev.RunID, p.NodeKey)
	if err != nil {
		return err
	}
	candidates, err := json.Marshal(p.CandidateEdges)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO decision (id, run_id, run_node_id, kind, candidate_edges, selected_edge_id,
                      rejection, predicate_snapshot, input_references, policy_version, decided_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		ev.EventID, ev.RunID, nodeID, p.Kind, string(candidates), nullableOr(p.SelectedEdgeID),
		nullableOr(p.Rejection), rawOr(p.PredicateSnapshot, "{}"), rawOr(p.InputReferences, "[]"),
		p.PolicyVersion, ev.OccurredAt); err != nil {
		return err
	}
	for i, link := range p.CausalLinks {
		targetID, err := runNodeID(ctx, tx, ev.RunID, link.TargetNodeKey)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO causal_link (id, decision_id, target_run_node_id, attribution, source_kind,
                         source_id, citation_type, citation_id, group_key)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
			fmt.Sprintf("%s#%d", ev.EventID, i), ev.EventID, targetID, link.Attribution,
			link.SourceKind, link.SourceID, nullableOr(link.CitationType),
			nullableOr(link.CitationID), nullableOr(link.GroupKey)); err != nil {
			return err
		}
	}
	return nil
}

func onRunTerminal(ctx context.Context, tx *sql.Tx, ev *Event) error {
	var p runTerminalPayload
	if err := decodePayload(ev, &p); err != nil {
		return err
	}
	result := map[string]string{
		"run_completed": "completed",
		"run_failed":    "failed",
		"run_cancelled": "cancelled",
	}[ev.Type]
	var versionID string
	if err := tx.QueryRowContext(ctx,
		"SELECT graph_version_id FROM graph_run WHERE id = ?", ev.RunID).Scan(&versionID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE graph_run SET status = ?, finished_at = ? WHERE id = ?`,
		result, ev.OccurredAt, ev.RunID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO anchor (id, graph_version_id, created_at)
VALUES (?, ?, ?) ON CONFLICT(id) DO NOTHING`, ev.EventID, versionID, ev.OccurredAt); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO outcome (id, run_id, anchor_id, result, detail, recorded_at)
VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		ev.EventID, ev.RunID, ev.EventID, result, nullableOr(p.Detail), ev.OccurredAt)
	return err
}

func finishedAt(ev *Event, typ string) any {
	switch typ {
	case "node_finished", "node_failed":
		return ev.OccurredAt
	default:
		return nil
	}
}

var wipeOrder = []string{
	"causal_link", "decision", "evaluation", "effect", "artifact", "outcome", "anchor",
	"approval", "node_attempt", "run_edge", "run_node", "graph_run",
}

var digestTables = []string{
	"graph_run", "run_node", "run_edge", "node_attempt", "artifact", "evaluation",
	"effect", "decision", "causal_link", "approval", "outcome", "anchor",
}

type RebuildReport struct {
	Before   string
	After    string
	Diverged bool
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func projectionDigestTx(ctx context.Context, q queryer) (string, error) {
	h := sha256.New()
	for _, table := range digestTables {
		rows, err := q.QueryContext(ctx, "SELECT * FROM "+table+" ORDER BY id")
		if err != nil {
			return "", err
		}
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			return "", err
		}
		h.Write([]byte(table))
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return "", err
			}
			for _, v := range vals {
				h.Write([]byte{0x1f})
				h.Write([]byte(formatValue(v)))
			}
			h.Write([]byte{0x1e})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return "", err
		}
		rows.Close()
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func formatValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "\x00"
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func (s *Store) ProjectionDigest(ctx context.Context) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	digest, err := projectionDigestTx(ctx, tx)
	if err != nil {
		return "", err
	}
	return digest, nil
}

func (s *Store) RebuildProjections(ctx context.Context) (RebuildReport, error) {
	var report RebuildReport
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		r, err := rebuildProjectionsTx(ctx, tx)
		if err == nil {
			report = r
		}
		return err
	})
	if err != nil {
		return RebuildReport{}, err
	}
	return report, nil
}

func rebuildProjectionsTx(ctx context.Context, tx *sql.Tx) (RebuildReport, error) {
	if _, err := tx.ExecContext(ctx, "PRAGMA defer_foreign_keys = 1"); err != nil {
		return RebuildReport{}, err
	}
	before, err := projectionDigestTx(ctx, tx)
	if err != nil {
		return RebuildReport{}, err
	}
	for _, table := range wipeOrder {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return RebuildReport{}, err
		}
	}
	rows, err := tx.QueryContext(ctx, `
SELECT event_id, run_id, sequence, schema_version, type, occurred_at, recorded_at,
       actor_type, actor_id, causation_id, correlation_id, idempotency_key,
       payload_digest, payload
FROM event ORDER BY run_id, sequence`)
	if err != nil {
		return RebuildReport{}, err
	}
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			rows.Close()
			return RebuildReport{}, err
		}
		if err := applyProjections(ctx, tx, &ev); err != nil {
			rows.Close()
			return RebuildReport{}, err
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return RebuildReport{}, err
	}
	rows.Close()
	after, err := projectionDigestTx(ctx, tx)
	if err != nil {
		return RebuildReport{}, err
	}
	return RebuildReport{Before: before, After: after, Diverged: before != after}, nil
}

type projectionHandler func(ctx context.Context, tx *sql.Tx, ev *Event) error

var projectedHandlers = map[string]projectionHandler{
	"run_started":           onRunStarted,
	"node_started":          onNodeStarted,
	"node_finished":         onNodeTerminal,
	"node_failed":           onNodeTerminal,
	"node_uncertain":        onNodeTerminal,
	"node_cancel_requested": onNodeCancelRequested,
	"node_skipped":          onNodeSkipped,
	"node_cancelled":        onNodeCancelled,
	"node_requeued":         onNodeRequeued,
	"node_waiting":          onNodeWaiting,
	"node_reconciling":      onNodeReconciling,
	"node_attempt_failed":   onNodeAttemptFailed,
	"edge_traversed":        onEdgeTraversed,
	"artifact_published":    onArtifactPublished,
	"evaluation_failed":     onEvaluationFailed,
	"effect_intent":         onEffectIntent,
	"effect_receipt":        onEffectReceipt,
	"approval_requested":    onApprovalRequested,
	"approval_granted":      onApprovalDecided,
	"approval_denied":       onApprovalDecided,
	"decision_recorded":     onDecisionRecorded,
	"run_completed":         onRunTerminal,
	"run_failed":            onRunTerminal,
	"run_cancelled":         onRunTerminal,
}
