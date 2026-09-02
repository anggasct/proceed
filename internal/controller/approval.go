package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/executor"
	"proceed/internal/store"
)

const (
	ApprovalDecided        = "APPROVAL_DECIDED"
	ApprovalAlreadyDecided = "APPROVAL_ALREADY_DECIDED"
	ApprovalExpired        = "APPROVAL_EXPIRED"
	ApprovalConflict       = "APPROVAL_CONFLICT"

	maxApprovalFieldLen = 512
	maxActorLen         = 256
	maxReasonLen        = 1024
)

type ApprovalDecisionRequest struct {
	ApprovalID     string
	RunID          string
	Decision       string
	Actor          string
	IdempotencyKey string
	Reason         string
}

type ApprovalDecisionResult struct {
	Code       string
	HTTPStatus int
	Message    string
	ApprovalID string
	RunID      string
	NodeKey    string
	Decision   string
	Actor      string
}

func (r *ApprovalDecisionResult) Accepted() bool {
	return r != nil && (r.Code == ApprovalDecided || r.Code == ApprovalAlreadyDecided)
}

// RegisterApprovalGate persists the approval request and flips the node to
// waiting in one transaction. A pending approval for the run node short
// circuits registration, so re-dispatch after recovery never creates a
// second row; a re-entered gate (previous approval decided) opens a new one.
func (c *Controller) RegisterApprovalGate(ctx context.Context, runID string, n runnableNode, gateScope string, expiresInMs int64) error {
	nowMs := time.Now().UnixMilli()
	if expiresInMs <= 0 {
		expiresInMs = 7 * 24 * 60 * 60 * 1000
	}
	expiresAt := nowMs + expiresInMs

	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		var runStatus, graphVersionID string
		if err := tx.QueryRowContext(ctx,
			"SELECT status, graph_version_id FROM graph_run WHERE id = ?", runID).
			Scan(&runStatus, &graphVersionID); err != nil {
			return err
		}
		if runStatus != "running" {
			return store.NewCodeError(store.CodeStoreConflict,
				"run %s is %s, cannot register approval gate", runID, runStatus)
		}

		var nodeID string
		var attemptCount int64
		err := tx.QueryRowContext(ctx,
			"SELECT id, attempt_count FROM run_node WHERE run_id = ? AND node_key = ?", runID, n.NodeKey).
			Scan(&nodeID, &attemptCount)
		if err == sql.ErrNoRows {
			return store.NewCodeError(store.CodeGraphInvalid,
				"run %s has no node %q", runID, n.NodeKey)
		}
		if err != nil {
			return err
		}
		if isTerminalNodeStatus(approvalNodeStatus(ctx, tx, nodeID)) {
			return store.NewCodeError(store.CodeStoreConflict,
				"node %s is terminal, cannot register approval gate", n.NodeKey)
		}

		var pending string
		err = tx.QueryRowContext(ctx,
			"SELECT id FROM approval WHERE run_node_id = ? AND decision IS NULL LIMIT 1", nodeID).Scan(&pending)
		if err == nil {
			return nil
		}
		if err != sql.ErrNoRows {
			return err
		}

		evidence, err := approvalEvidenceRefs(ctx, tx, runID, graphVersionID, n.NodeKey)
		if err != nil {
			return err
		}
		if evidence == nil {
			evidence = []string{}
		}
		requestedAction := payloadJSON(map[string]any{
			"node_key": n.NodeKey,
			"action":   string(executor.HumanApproval),
			"scope":    gateScope,
		})

		requestedID := ulid.Make().String()
		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       requestedID,
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "approval_requested",
			OccurredAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload: payloadJSON(map[string]any{
				"node_key":            n.NodeKey,
				"requested_action":    json.RawMessage(requestedAction),
				"evidence_references": evidence,
				"required_scope":      gateScope,
				"expires_at":          expiresAt,
			}),
		}); err != nil {
			return err
		}

		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "node_waiting",
			OccurredAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			CausationID:   requestedID,
			Payload:       payloadJSON(map[string]any{"node_key": n.NodeKey}),
		}); err != nil {
			return err
		}
		return nil
	})
}

func approvalNodeStatus(ctx context.Context, tx *sql.Tx, nodeID string) string {
	var status string
	_ = tx.QueryRowContext(ctx, "SELECT status FROM run_node WHERE id = ?", nodeID).Scan(&status)
	return status
}

func approvalEvidenceRefs(ctx context.Context, tx *sql.Tx, runID, graphVersionID, nodeKey string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT a.id FROM artifact a
JOIN graph_edge ge
  ON ge.graph_version_id = ? AND ge.to_node_key = ?
 AND ge.type IN ('depends_on','produces','consumes','routes_to')
 AND a.produced_by_node_key = ge.from_node_key
WHERE a.run_id = ?
ORDER BY a.id
LIMIT 64`, graphVersionID, nodeKey, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var refs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		refs = append(refs, "artifact:"+id)
	}
	return refs, rows.Err()
}

// DecideApproval applies one grant/deny decision. The client's idempotency
// key decides exactly once: the first accepted decision event wins, any
// replay returns the original outcome, and a different key on a decided or
// used key conflicts.
func (c *Controller) DecideApproval(ctx context.Context, req ApprovalDecisionRequest) (*ApprovalDecisionResult, error) {
	if req.ApprovalID == "" || len(req.ApprovalID) > maxApprovalFieldLen {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "approval id is required")
	}
	if req.Decision != "grant" && req.Decision != "deny" {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "decision must be grant or deny")
	}
	if req.Actor == "" || len(req.Actor) > maxActorLen || hasControlChars(req.Actor) || isSensitiveValue(req.Actor) {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "actor must be a bounded non-empty identity")
	}
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > maxApprovalFieldLen || hasControlChars(req.IdempotencyKey) || isSensitiveValue(req.IdempotencyKey) {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "decision_idempotency_key must be a bounded non-empty opaque string")
	}
	if len(req.Reason) > maxReasonLen || hasControlChars(req.Reason) {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "reason must be a bounded single-line note")
	}

	nowMs := time.Now().UnixMilli()
	result := &ApprovalDecisionResult{
		Code:       ApprovalDecided,
		HTTPStatus: http.StatusAccepted,
		ApprovalID: req.ApprovalID,
		Decision:   req.Decision,
		Actor:      req.Actor,
	}

	err := c.store.WithTx(ctx, func(tx *sql.Tx) error {
		var runID, runNodeID, graphVersionID string
		var expiresAt int64
		var decision sql.NullString
		err := tx.QueryRowContext(ctx, `
SELECT run_id, run_node_id, graph_version_id, expires_at, decision
FROM approval WHERE id = ?`, req.ApprovalID).
			Scan(&runID, &runNodeID, &graphVersionID, &expiresAt, &decision)
		if err == sql.ErrNoRows {
			*result = ApprovalDecisionResult{
				Code:       "RUN_NOT_FOUND",
				HTTPStatus: http.StatusNotFound,
				Message:    fmt.Sprintf("approval %s not found", req.ApprovalID),
			}
			return nil
		}
		if err != nil {
			return err
		}
		result.RunID = runID
		if req.RunID != "" && req.RunID != runID {
			*result = ApprovalDecisionResult{
				Code:       "RUN_NOT_FOUND",
				HTTPStatus: http.StatusNotFound,
				Message:    fmt.Sprintf("approval %s does not belong to run %s", req.ApprovalID, req.RunID),
			}
			return nil
		}
		nodeKey := c.nodeKeyForID(ctx, runID, runNodeID)
		result.NodeKey = nodeKey

		var existingEventID, existingType, existingPayload string
		err = tx.QueryRowContext(ctx,
			"SELECT event_id, type, payload FROM event WHERE idempotency_key = ?", req.IdempotencyKey).
			Scan(&existingEventID, &existingType, &existingPayload)
		if err == nil {
			var payload struct {
				ApprovalID string `json:"approval_id"`
				DecidedBy  string `json:"decided_by"`
			}
			_ = json.Unmarshal([]byte(existingPayload), &payload)
			if (existingType == "approval_granted" || existingType == "approval_denied") && payload.ApprovalID == req.ApprovalID {
				original := "grant"
				if existingType == "approval_denied" {
					original = "deny"
				}
				*result = ApprovalDecisionResult{
					Code:       ApprovalAlreadyDecided,
					HTTPStatus: http.StatusOK,
					Message:    "idempotent duplicate; original decision preserved",
					ApprovalID: req.ApprovalID,
					RunID:      runID,
					NodeKey:    nodeKey,
					Decision:   original,
					Actor:      payload.DecidedBy,
				}
				return nil
			}
			result.Code = ApprovalConflict
			result.HTTPStatus = http.StatusConflict
			result.Message = "decision_idempotency_key already recorded for another decision"
			return nil
		}
		if err != sql.ErrNoRows {
			return err
		}

		if decision.Valid {
			result.Code = ApprovalConflict
			result.HTTPStatus = http.StatusConflict
			result.Message = fmt.Sprintf("approval %s is already decided (%s by %s)",
				req.ApprovalID, decision.String, nodeDecidedBy(ctx, tx, req.ApprovalID))
			return nil
		}

		var expiryEvent int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM event WHERE idempotency_key = ?", "approval_expired:"+req.ApprovalID).
			Scan(&expiryEvent); err != nil {
			return err
		}
		if expiryEvent > 0 || expiresAt <= nowMs {
			*result = ApprovalDecisionResult{
				Code:       ApprovalExpired,
				HTTPStatus: http.StatusAccepted,
				Message:    "approval already expired; decision not applied",
				ApprovalID: req.ApprovalID,
				RunID:      runID,
				NodeKey:    nodeKey,
			}
			return nil
		}

		var runGraphVersionID, runDigest, runStatus string
		if err := tx.QueryRowContext(ctx,
			"SELECT graph_version_id, definition_digest, status FROM graph_run WHERE id = ?", runID).
			Scan(&runGraphVersionID, &runDigest, &runStatus); err != nil {
			return err
		}
		if runGraphVersionID != graphVersionID || runDigest != approvalVersionDigest(ctx, tx, graphVersionID) {
			result.Code = "POLICY_DENIED"
			result.HTTPStatus = http.StatusConflict
			result.Message = "approval version does not match the run's current binding"
			return nil
		}
		if runStatus != "running" {
			result.Code = ApprovalConflict
			result.HTTPStatus = http.StatusConflict
			result.Message = fmt.Sprintf("run %s is %s; decision cannot revive it", runID, runStatus)
			return nil
		}

		var attemptCount int64
		if err := tx.QueryRowContext(ctx,
			"SELECT attempt_count FROM run_node WHERE id = ?", runNodeID).Scan(&attemptCount); err != nil {
			return err
		}

		decisionType := "approval_granted"
		if req.Decision == "deny" {
			decisionType = "approval_denied"
		}
		decisionPayload := map[string]any{
			"approval_id":              req.ApprovalID,
			"node_key":                 nodeKey,
			"decided_by":               req.Actor,
			"decision_idempotency_key": req.IdempotencyKey,
		}
		if req.Reason != "" {
			decisionPayload["reason"] = req.Reason
		}
		decisionEventID := ulid.Make().String()
		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:        decisionEventID,
			RunID:          runID,
			SchemaVersion:  "proceed/v1",
			Type:           decisionType,
			OccurredAt:     nowMs,
			ActorType:      "human",
			ActorID:        req.Actor,
			CausationID:    req.ApprovalID,
			CorrelationID:  nodeKey,
			IdempotencyKey: req.IdempotencyKey,
			Payload:        payloadJSON(decisionPayload),
		}); err != nil {
			return err
		}

		output := map[string]any{"decision": req.Decision, "actor": req.Actor}
		if req.Reason != "" {
			output["reason"] = req.Reason
		}
		route := "success"
		if req.Decision == "deny" {
			route = denyRoute(ctx, tx, graphVersionID, nodeKey)
		}

		if route != "" {
			if _, err := c.appendWithin(ctx, tx, &store.Event{
				EventID:       ulid.Make().String(),
				RunID:         runID,
				SchemaVersion: "proceed/v1",
				Type:          "node_finished",
				OccurredAt:    nowMs,
				ActorType:     "controller",
				ActorID:       c.cfg.OwnerID,
				CausationID:   decisionEventID,
				CorrelationID: nodeKey,
				Payload: payloadJSON(map[string]any{
					"node_key":   nodeKey,
					"attempt_no": attemptCount,
					"result":     output,
					"route":      route,
				}),
			}); err != nil {
				return err
			}
			if err := c.routeFromTx(ctx, tx, runID, graphVersionID, runDigest,
				runnableNode{NodeKey: nodeKey, AttemptNo: attemptCount},
				&executor.Result{Route: route, Output: output}, true, nowMs); err != nil {
				return err
			}
			return nil
		}

		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       ulid.Make().String(),
			RunID:         runID,
			SchemaVersion: "proceed/v1",
			Type:          "node_failed",
			OccurredAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			CausationID:   decisionEventID,
			CorrelationID: nodeKey,
			Payload: payloadJSON(map[string]any{
				"node_key":   nodeKey,
				"attempt_no": attemptCount,
				"error":      "APPROVAL_DENIED",
				"result":     output,
			}),
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// denyRoute resolves the alternate route a denial takes when the definition
// declares one; an empty result means terminate the node as failed.
func denyRoute(ctx context.Context, tx *sql.Tx, graphVersionID, nodeKey string) string {
	var cond string
	_ = tx.QueryRowContext(ctx, `
SELECT condition FROM graph_edge
WHERE graph_version_id = ? AND from_node_key = ? AND type = 'routes_to'
  AND condition IN ('denied', 'failure') LIMIT 1`,
		graphVersionID, nodeKey).Scan(&cond)
	return cond
}

func approvalVersionDigest(ctx context.Context, tx *sql.Tx, graphVersionID string) string {
	var digest string
	_ = tx.QueryRowContext(ctx,
		"SELECT definition_digest FROM graph_version WHERE id = ?", graphVersionID).Scan(&digest)
	return digest
}

func nodeDecidedBy(ctx context.Context, tx *sql.Tx, approvalID string) string {
	var decidedBy string
	_ = tx.QueryRowContext(ctx,
		"SELECT decided_by FROM approval WHERE id = ?", approvalID).Scan(&decidedBy)
	return decidedBy
}

// ExpireApprovals transitions approvals whose expiry passed without a
// decision. The deterministic idempotency key keeps a re-scan from emitting
// a second expiry event for the same approval.
func (c *Controller) ExpireApprovals(ctx context.Context, nowMs int64) error {
	currentTime := time.Now().UnixMilli()
	if nowMs <= 0 {
		nowMs = currentTime
	}
	expired, err := c.store.ListExpiredApprovals(ctx, nowMs)
	if err != nil {
		return err
	}
	occurredAt := nowMs
	if occurredAt > currentTime {
		occurredAt = currentTime
	}
	for _, a := range expired {
		err := c.store.WithTx(ctx, func(tx *sql.Tx) error {
			var decision sql.NullString
			if err := tx.QueryRowContext(ctx,
				"SELECT decision FROM approval WHERE id = ?", a.ID).Scan(&decision); err != nil {
				return err
			}
			if decision.Valid {
				return nil
			}
			var runStatus string
			if err := tx.QueryRowContext(ctx,
				"SELECT status FROM graph_run WHERE id = ?", a.RunID).Scan(&runStatus); err != nil {
				return err
			}
			if runStatus != "running" {
				return nil
			}

			nodeKey := c.nodeKeyForID(ctx, a.RunID, a.RunNodeID)
			expiryEventID := ulid.Make().String()
			if _, err := c.appendWithin(ctx, tx, &store.Event{
				EventID:        expiryEventID,
				RunID:          a.RunID,
				SchemaVersion:  "proceed/v1",
				Type:           "approval_expired",
				OccurredAt:     occurredAt,
				ActorType:      "controller",
				ActorID:        c.cfg.OwnerID,
				IdempotencyKey: "approval_expired:" + a.ID,
				Payload: payloadJSON(map[string]any{
					"approval_id": a.ID,
					"node_key":    nodeKey,
				}),
			}); err != nil {
				if store.ErrorCode(err) == store.CodeStoreConflict {
					return nil
				}
				return err
			}

			var attemptCount int64
			_ = tx.QueryRowContext(ctx,
				"SELECT attempt_count FROM run_node WHERE id = ?", a.RunNodeID).Scan(&attemptCount)

			var expiryRoute string
			_ = tx.QueryRowContext(ctx, `
SELECT condition FROM graph_edge
WHERE graph_version_id = ? AND from_node_key = ? AND type = 'routes_to'
  AND condition IN ('expired', 'timeout') LIMIT 1`,
				a.GraphVersionID, nodeKey).Scan(&expiryRoute)

			if expiryRoute != "" {
				if _, err := c.appendWithin(ctx, tx, &store.Event{
					EventID:       ulid.Make().String(),
					RunID:         a.RunID,
					SchemaVersion: "proceed/v1",
					Type:          "node_finished",
					OccurredAt:    occurredAt,
					ActorType:     "controller",
					ActorID:       c.cfg.OwnerID,
					CausationID:   expiryEventID,
					Payload: payloadJSON(map[string]any{
						"node_key":   nodeKey,
						"attempt_no": attemptCount,
						"route":      expiryRoute,
					}),
				}); err != nil {
					return err
				}
				return c.routeFromTx(ctx, tx, a.RunID, a.GraphVersionID, c.runDigest(ctx, a.RunID),
					runnableNode{NodeKey: nodeKey, AttemptNo: attemptCount},
					&executor.Result{Route: expiryRoute}, true, occurredAt)
			}

			if _, err := c.appendWithin(ctx, tx, &store.Event{
				EventID:       ulid.Make().String(),
				RunID:         a.RunID,
				SchemaVersion: "proceed/v1",
				Type:          "node_failed",
				OccurredAt:    occurredAt,
				ActorType:     "controller",
				ActorID:       c.cfg.OwnerID,
				CausationID:   expiryEventID,
				Payload: payloadJSON(map[string]any{
					"node_key":   nodeKey,
					"attempt_no": attemptCount,
					"error":      "APPROVAL_EXPIRED",
				}),
			}); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) runDigest(ctx context.Context, runID string) string {
	var digest string
	_ = c.store.DB().QueryRowContext(ctx,
		"SELECT definition_digest FROM graph_run WHERE id = ?", runID).Scan(&digest)
	return digest
}
