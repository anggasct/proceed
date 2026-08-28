package controller

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/executor"
	"proceed/internal/store"
)

type ExternalWaitRequest struct {
	RunID             string
	NodeKey           string
	EventType         string
	CorrelationKey    string
	ExpectedCondition string
	ExpiresAt         int64
	WaitID            string
}

type CompleteWaitRequest struct {
	WaitID          string          `json:"wait_id"`
	ProviderEventID string          `json:"provider_event_id"`
	EventType       string          `json:"event_type"`
	Source          string          `json:"source"`
	CorrelationKey  string          `json:"correlation_key"`
	OccurredAt      int64           `json:"occurred_at"`
	Status          string          `json:"status"`
	PayloadDigest   string          `json:"payload_digest"`
	Payload         json.RawMessage `json:"payload,omitempty"`
}

type CompletionResult struct {
	Code       string         `json:"code"`
	HTTPStatus int            `json:"-"`
	Message    string         `json:"message,omitempty"`
	WaitID     string         `json:"wait_id,omitempty"`
	RunID      string         `json:"run_id,omitempty"`
	NodeKey    string         `json:"node_key,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
}

func (c *Controller) RegisterExternalWait(ctx context.Context, req ExternalWaitRequest) (*store.ExternalWait, error) {
	if req.RunID == "" || req.NodeKey == "" || req.EventType == "" || req.CorrelationKey == "" {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "run_id, node_key, event_type, and correlation_key are required")
	}
	run, err := c.loadRun(ctx, req.RunID)
	if err != nil {
		return nil, err
	}
	if run.status != "running" {
		return nil, store.NewCodeError(store.CodeStoreConflict, "run %s is %s, cannot register external wait", req.RunID, run.status)
	}

	if req.WaitID == "" {
		req.WaitID = ulid.Make().String()
	}
	if req.ExpectedCondition == "" {
		req.ExpectedCondition = "{}"
	}

	nowMs := time.Now().UnixMilli()
	err = c.store.WithTx(ctx, func(tx *sql.Tx) error {
		var existingCount int
		err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM external_wait WHERE event_type = ? AND correlation_key = ? AND status = 'pending'",
			req.EventType, req.CorrelationKey).Scan(&existingCount)
		if err != nil {
			return err
		}
		if existingCount > 0 {
			return store.NewCodeError(store.CodeStoreConflict,
				"pending external wait already exists for event_type %q and correlation_key %q",
				req.EventType, req.CorrelationKey)
		}

		var nodeID string
		err = tx.QueryRowContext(ctx, "SELECT id FROM run_node WHERE run_id = ? AND node_key = ?", req.RunID, req.NodeKey).Scan(&nodeID)
		if err == sql.ErrNoRows {
			nodeID = ulid.Make().String()
			if _, err := tx.ExecContext(ctx, `
INSERT INTO run_node (id, run_id, node_key, status, attempt_count)
VALUES (?, ?, ?, 'waiting', 0) ON CONFLICT(run_id, node_key) DO UPDATE SET status = 'waiting'`,
				nodeID, req.RunID, req.NodeKey); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

		waitPayload := map[string]any{
			"wait_id":            req.WaitID,
			"node_key":           req.NodeKey,
			"event_type":         req.EventType,
			"correlation_key":    req.CorrelationKey,
			"expected_condition": json.RawMessage(req.ExpectedCondition),
		}
		if req.ExpiresAt > 0 {
			waitPayload["expires_at"] = req.ExpiresAt
		}

		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       ulid.Make().String(),
			RunID:         req.RunID,
			SchemaVersion: "proceed/v1",
			Type:          "external_wait_requested",
			OccurredAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload:       payloadJSON(waitPayload),
		}); err != nil {
			return err
		}

		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       ulid.Make().String(),
			RunID:         req.RunID,
			SchemaVersion: "proceed/v1",
			Type:          "node_waiting",
			OccurredAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			Payload:       payloadJSON(map[string]any{"node_key": req.NodeKey}),
		}); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return c.store.GetExternalWait(ctx, req.WaitID)
}

func (c *Controller) CompleteExternalWait(ctx context.Context, req CompleteWaitRequest) (*CompletionResult, error) {
	if req.WaitID == "" {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "wait_id is required")
	}
	if req.ProviderEventID == "" || req.EventType == "" || req.Source == "" || req.CorrelationKey == "" || req.Status == "" || req.PayloadDigest == "" {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "provider_event_id, event_type, source, correlation_key, status, and payload_digest are required")
	}

	canonicalPayload := "{}"
	if len(req.Payload) > 0 {
		var pv any
		if err := json.Unmarshal(req.Payload, &pv); err != nil {
			return nil, store.NewCodeError(store.CodeGraphInvalid, "payload must be valid JSON: %v", err)
		}
		b, _ := json.Marshal(pv)
		canonicalPayload = string(b)
	}
	computedDigest := hexDigest(canonicalPayload)
	normDigest := normalizeDigest(req.PayloadDigest)
	if normDigest != computedDigest && normDigest != "sha256:"+computedDigest {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "payload_digest %s does not match payload (computed sha256:%s)", req.PayloadDigest, computedDigest)
	}

	// 1. Check idempotency: has this provider_event_id already been recorded?
	var existingEventID, existingType string
	err := c.store.DB().QueryRowContext(ctx,
		"SELECT event_id, type FROM event WHERE idempotency_key = ?", req.ProviderEventID).
		Scan(&existingEventID, &existingType)
	if err == nil {
		wait, err := c.store.GetExternalWait(ctx, req.WaitID)
		if err != nil {
			return nil, err
		}
		if wait != nil && wait.Status == "completed" {
			nodeKey := c.nodeKeyForID(ctx, wait.RunID, wait.RunNodeID)
			return &CompletionResult{
				Code:       "WAIT_ALREADY_COMPLETED",
				HTTPStatus: http.StatusOK,
				WaitID:     req.WaitID,
				RunID:      wait.RunID,
				NodeKey:    nodeKey,
				Message:    "idempotent duplicate; original resolution preserved",
			}, nil
		}
		return &CompletionResult{
			Code:       "WAIT_REJECTED",
			HTTPStatus: http.StatusAccepted,
			WaitID:     req.WaitID,
			Message:    "event already processed",
		}, nil
	} else if err != sql.ErrNoRows {
		return nil, err
	}

	// 2. Lookup wait
	wait, err := c.store.GetExternalWait(ctx, req.WaitID)
	if err != nil {
		return nil, err
	}
	if wait == nil {
		return &CompletionResult{
			Code:       "WAIT_NOT_FOUND",
			HTTPStatus: http.StatusNotFound,
			WaitID:     req.WaitID,
			Message:    fmt.Sprintf("wait %s not found", req.WaitID),
		}, nil
	}

	nodeKey := c.nodeKeyForID(ctx, wait.RunID, wait.RunNodeID)

	// 3. Validate state & correlation
	nowMs := time.Now().UnixMilli()
	if wait.Status != "pending" {
		_ = c.recordRejectedEvent(ctx, wait.RunID, req, fmt.Sprintf("wait is already %s", wait.Status), nowMs)
		return &CompletionResult{
			Code:       "WAIT_CONFLICT",
			HTTPStatus: http.StatusConflict,
			WaitID:     req.WaitID,
			RunID:      wait.RunID,
			NodeKey:    nodeKey,
			Message:    fmt.Sprintf("wait %s is already %s", req.WaitID, wait.Status),
		}, nil
	}

	if wait.EventType != req.EventType || wait.CorrelationKey != req.CorrelationKey {
		_ = c.recordRejectedEvent(ctx, wait.RunID, req, "mismatched event_type or correlation_key", nowMs)
		return &CompletionResult{
			Code:       "WAIT_CONFLICT",
			HTTPStatus: http.StatusConflict,
			WaitID:     req.WaitID,
			RunID:      wait.RunID,
			NodeKey:    nodeKey,
			Message:    fmt.Sprintf("mismatched correlation or event type for wait %s", req.WaitID),
		}, nil
	}

	run, err := c.loadRun(ctx, wait.RunID)
	if err != nil {
		return nil, err
	}
	if run.status != "running" {
		_ = c.recordRejectedEvent(ctx, wait.RunID, req, fmt.Sprintf("run is %s", run.status), nowMs)
		return &CompletionResult{
			Code:       "WAIT_CONFLICT",
			HTTPStatus: http.StatusConflict,
			WaitID:     req.WaitID,
			RunID:      wait.RunID,
			NodeKey:    nodeKey,
			Message:    fmt.Sprintf("run %s is not running (%s)", wait.RunID, run.status),
		}, nil
	}

	// 4. Accept completion atomically
	receivedEventID := ulid.Make().String()
	completedEventID := ulid.Make().String()
	occurredAt := req.OccurredAt
	if occurredAt <= 0 || occurredAt > nowMs {
		occurredAt = nowMs
	}

	err = c.store.WithTx(ctx, func(tx *sql.Tx) error {
		// Append external_event_received
		recvPayload := map[string]any{
			"wait_id":           req.WaitID,
			"provider_event_id": req.ProviderEventID,
			"event_type":        req.EventType,
			"source":            req.Source,
			"correlation_key":   req.CorrelationKey,
			"occurred_at":       occurredAt,
			"status":            req.Status,
			"payload_digest":    req.PayloadDigest,
		}
		if len(req.Payload) > 0 {
			recvPayload["payload"] = json.RawMessage(canonicalPayload)
		}

		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:        receivedEventID,
			RunID:          wait.RunID,
			SchemaVersion:  "proceed/v1",
			Type:           "external_event_received",
			OccurredAt:     occurredAt,
			RecordedAt:     nowMs,
			ActorType:      "executor",
			ActorID:        req.Source,
			CorrelationID:  req.CorrelationKey,
			IdempotencyKey: req.ProviderEventID,
			Payload:        payloadJSON(recvPayload),
		}); err != nil {
			return err
		}

		// Append external_wait_completed
		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       completedEventID,
			RunID:         wait.RunID,
			SchemaVersion: "proceed/v1",
			Type:          "external_wait_completed",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			CausationID:   receivedEventID,
			CorrelationID: req.CorrelationKey,
			Payload: payloadJSON(map[string]any{
				"wait_id":           req.WaitID,
				"node_key":          nodeKey,
				"received_event_id": receivedEventID,
				"status":            "completed",
				"payload_digest":    req.PayloadDigest,
			}),
		}); err != nil {
			return err
		}

		// Resume the waiting node and traverse outgoing edges
		var attemptCount int64
		_ = tx.QueryRowContext(ctx,
			"SELECT attempt_count FROM run_node WHERE id = ?", wait.RunNodeID).Scan(&attemptCount)

		output := map[string]any{}
		if len(req.Payload) > 0 {
			_ = json.Unmarshal([]byte(canonicalPayload), &output)
		}

		result := &executor.Result{
			Route:  req.Status,
			Output: output,
		}

		// Append node_finished
		if _, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:       ulid.Make().String(),
			RunID:         wait.RunID,
			SchemaVersion: "proceed/v1",
			Type:          "node_finished",
			OccurredAt:    nowMs,
			RecordedAt:    nowMs,
			ActorType:     "controller",
			ActorID:       c.cfg.OwnerID,
			CausationID:   receivedEventID,
			CorrelationID: req.CorrelationKey,
			Payload: payloadJSON(map[string]any{
				"node_key":   nodeKey,
				"attempt_no": attemptCount,
				"result":     result.Output,
				"route":      result.Route,
			}),
		}); err != nil {
			return err
		}

		// Route edges from the completed node
		return c.routeFromTx(ctx, tx, wait.RunID, wait.GraphVersionID, wait.DefinitionDigest,
			runnableNode{NodeKey: nodeKey, AttemptNo: attemptCount}, result, true, nowMs)
	})
	if err != nil {
		return nil, err
	}

	return &CompletionResult{
		Code:       "WAIT_COMPLETED",
		HTTPStatus: http.StatusAccepted,
		WaitID:     req.WaitID,
		RunID:      wait.RunID,
		NodeKey:    nodeKey,
		Message:    "event accepted and wait resolved",
	}, nil
}

func (c *Controller) ExpireExternalWaits(ctx context.Context, nowMs int64) error {
	currentTime := time.Now().UnixMilli()
	if nowMs <= 0 {
		nowMs = currentTime
	}
	expired, err := c.store.ListExpiredExternalWaits(ctx, nowMs)
	if err != nil {
		return err
	}
	occurredAt := nowMs
	if occurredAt > currentTime {
		occurredAt = currentTime
	}
	for _, w := range expired {
		err := c.store.WithTx(ctx, func(tx *sql.Tx) error {
			var status string
			if err := tx.QueryRowContext(ctx,
				"SELECT status FROM external_wait WHERE id = ?", w.ID).Scan(&status); err != nil {
				return err
			}
			if status != "pending" {
				return nil
			}

			nodeKey := c.nodeKeyForID(ctx, w.RunID, w.RunNodeID)

			if _, err := c.appendWithin(ctx, tx, &store.Event{
				EventID:       ulid.Make().String(),
				RunID:         w.RunID,
				SchemaVersion: "proceed/v1",
				Type:          "external_wait_expired",
				OccurredAt:    occurredAt,
				ActorType:     "controller",
				ActorID:       c.cfg.OwnerID,
				Payload: payloadJSON(map[string]any{
					"wait_id":  w.ID,
					"node_key": nodeKey,
				}),
			}); err != nil {
				return err
			}

			// Check if there is a timeout/expired route
			var hasTimeoutRoute bool
			var count int
			_ = tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM graph_edge
WHERE graph_version_id = ? AND from_node_key = ? AND type = 'routes_to' AND condition IN ('timeout', 'expired')`,
				w.GraphVersionID, nodeKey).Scan(&count)
			hasTimeoutRoute = count > 0

			var attemptCount int64
			_ = tx.QueryRowContext(ctx,
				"SELECT attempt_count FROM run_node WHERE id = ?", w.RunNodeID).Scan(&attemptCount)

			if hasTimeoutRoute {
				result := &executor.Result{Route: "timeout"}
				if _, err := c.appendWithin(ctx, tx, &store.Event{
					EventID:       ulid.Make().String(),
					RunID:         w.RunID,
					SchemaVersion: "proceed/v1",
					Type:          "node_finished",
					OccurredAt:    nowMs,
					ActorType:     "controller",
					ActorID:       c.cfg.OwnerID,
					Payload: payloadJSON(map[string]any{
						"node_key":   nodeKey,
						"attempt_no": attemptCount,
						"route":      "timeout",
					}),
				}); err != nil {
					return err
				}
				return c.routeFromTx(ctx, tx, w.RunID, w.GraphVersionID, w.DefinitionDigest,
					runnableNode{NodeKey: nodeKey, AttemptNo: attemptCount}, result, true, nowMs)
			}

			// Otherwise fail the node with timeout
			if _, err := c.appendWithin(ctx, tx, &store.Event{
				EventID:       ulid.Make().String(),
				RunID:         w.RunID,
				SchemaVersion: "proceed/v1",
				Type:          "node_failed",
				OccurredAt:    nowMs,
				ActorType:     "controller",
				ActorID:       c.cfg.OwnerID,
				Payload: payloadJSON(map[string]any{
					"node_key":   nodeKey,
					"attempt_no": attemptCount,
					"error":      "external wait expired",
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

func (c *Controller) recordRejectedEvent(ctx context.Context, runID string, req CompleteWaitRequest, reason string, nowMs int64) error {
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := c.appendWithin(ctx, tx, &store.Event{
			EventID:        ulid.Make().String(),
			RunID:          runID,
			SchemaVersion:  "proceed/v1",
			Type:           "external_event_rejected",
			OccurredAt:     nowMs,
			RecordedAt:     nowMs,
			ActorType:      "controller",
			ActorID:        c.cfg.OwnerID,
			CorrelationID:  req.CorrelationKey,
			IdempotencyKey: req.ProviderEventID,
			Payload: payloadJSON(map[string]any{
				"wait_id":           req.WaitID,
				"provider_event_id": req.ProviderEventID,
				"reason":            reason,
				"event_type":        req.EventType,
				"correlation_key":   req.CorrelationKey,
			}),
		})
		if err != nil && store.ErrorCode(err) == store.CodeStoreConflict {
			return nil
		}
		return err
	})
}

func (c *Controller) handleGateNode(ctx context.Context, runID, graphVersionID, digest string, n runnableNode) error {
	var cfg map[string]any
	if err := json.Unmarshal([]byte(n.Config), &cfg); err != nil {
		return c.waitingNode(ctx, runID, n.NodeKey)
	}

	eventType, _ := cfg["event_type"].(string)
	correlationKey, _ := cfg["correlation_key"].(string)
	if eventType == "" || correlationKey == "" {
		return c.waitingNode(ctx, runID, n.NodeKey)
	}

	var expectedCondStr string
	if ec, ok := cfg["expected_condition"]; ok {
		b, _ := json.Marshal(ec)
		expectedCondStr = string(b)
	}

	var expiresAt int64
	if v, ok := cfg["expires_in_ms"].(float64); ok && int64(v) > 0 {
		expiresAt = time.Now().UnixMilli() + int64(v)
	} else if v, ok := cfg["timeout_ms"].(float64); ok && int64(v) > 0 {
		expiresAt = time.Now().UnixMilli() + int64(v)
	}

	waitID, _ := cfg["wait_id"].(string)

	_, err := c.RegisterExternalWait(ctx, ExternalWaitRequest{
		RunID:             runID,
		NodeKey:           n.NodeKey,
		EventType:         eventType,
		CorrelationKey:    correlationKey,
		ExpectedCondition: expectedCondStr,
		ExpiresAt:         expiresAt,
		WaitID:            waitID,
	})
	return err
}

func (c *Controller) nodeKeyForID(ctx context.Context, runID, runNodeID string) string {
	var key string
	_ = c.store.DB().QueryRowContext(ctx,
		"SELECT node_key FROM run_node WHERE id = ?", runNodeID).Scan(&key)
	return key
}

func hexDigest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func normalizeDigest(s string) string {
	return strings.TrimPrefix(s, "sha256:")
}
