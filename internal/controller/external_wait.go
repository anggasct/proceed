package controller

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
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

// Accepted reports whether the outcome resolved the wait. Every other typed
// outcome leaves the delivery retryable at the provider boundary.
func (r *CompletionResult) Accepted() bool {
	return r != nil && (r.Code == "WAIT_COMPLETED" || r.Code == "WAIT_ALREADY_COMPLETED")
}

func isTerminalNodeStatus(status string) bool {
	switch status {
	case "succeeded", "failed", "skipped", "cancelled":
		return true
	}
	return false
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
	var condShape map[string]any
	if err := json.Unmarshal([]byte(req.ExpectedCondition), &condShape); err != nil || condShape == nil {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "expected_condition must be a JSON object")
	}

	nowMs := time.Now().UnixMilli()
	err = c.store.WithTx(ctx, func(tx *sql.Tx) error {
		var runStatus string
		if err := tx.QueryRowContext(ctx,
			"SELECT status FROM graph_run WHERE id = ?", req.RunID).Scan(&runStatus); err != nil {
			return err
		}
		if runStatus != "running" {
			return store.NewCodeError(store.CodeStoreConflict,
				"run %s is %s, cannot register external wait", req.RunID, runStatus)
		}

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

		var nodeID, nodeStatus string
		err = tx.QueryRowContext(ctx,
			"SELECT id, status FROM run_node WHERE run_id = ? AND node_key = ?", req.RunID, req.NodeKey).
			Scan(&nodeID, &nodeStatus)
		switch {
		case err == sql.ErrNoRows:
			nodeID = ulid.Make().String()
			if _, err := tx.ExecContext(ctx, `
INSERT INTO run_node (id, run_id, node_key, status, attempt_count)
VALUES (?, ?, ?, 'waiting', 0) ON CONFLICT(run_id, node_key) DO NOTHING`,
				nodeID, req.RunID, req.NodeKey); err != nil {
				return err
			}
		case err != nil:
			return err
		case isTerminalNodeStatus(nodeStatus):
			return store.NewCodeError(store.CodeStoreConflict,
				"node %s is %s, cannot register external wait", req.NodeKey, nodeStatus)
		}

		var pendingForNode int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM external_wait WHERE run_node_id = ? AND status = 'pending' AND id <> ?",
			nodeID, req.WaitID).Scan(&pendingForNode); err != nil {
			return err
		}
		if pendingForNode > 0 {
			return store.NewCodeError(store.CodeStoreConflict,
				"node %s already has a pending external wait", req.NodeKey)
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

// Credential-shaped values are rejected even inside allowlisted fields: a
// normalized provider field carrying a verifiable secret form is not provably
// safe to persist.
var sensitiveValuePatterns = []*regexp.Regexp{
	regexp.MustCompile(`-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----`),
	regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{16,}`),
	regexp.MustCompile(`github_pat_[A-Za-z0-9_]{20,}`),
	regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{20,}`),
	regexp.MustCompile(`sk-(ant-)?[A-Za-z0-9]{40,}`),
	regexp.MustCompile(`(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}`),
	regexp.MustCompile(`xox[baprs]-[A-Za-z0-9-]{10,}`),
	regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
	regexp.MustCompile(`AIza[0-9A-Za-z_-]{30,}`),
	regexp.MustCompile(`npm_[A-Za-z0-9]{30,}`),
}

const (
	maxPayloadStringLen     = 256
	maxPayloadURLLen        = 2048
	maxPayloadKeyLen        = 128
	maxRequiredCheckEntries = 64
	maxPayloadNumber        = 1e15
)

// The completion payload admits only these normalized provider fields. Any
// other field is rejected before hashing or persistence: its safety cannot be
// proven and it is not part of the normalized contract.
var allowedConclusionValues = map[string]bool{
	"success":         true,
	"failure":         true,
	"neutral":         true,
	"skipped":         true,
	"cancelled":       true,
	"timed_out":       true,
	"action_required": true,
	"startup_failure": true,
	"stale":           true,
}

func isSensitiveValue(v string) bool {
	for _, p := range sensitiveValuePatterns {
		if p.MatchString(v) {
			return true
		}
	}
	return false
}

func hasControlChars(s string) bool {
	return strings.ContainsFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f })
}

func normalizedStringField(field string, v any, maxLen int) error {
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("field %q must be a string", field)
	}
	if len(s) == 0 || len(s) > maxLen || hasControlChars(s) {
		return fmt.Errorf("field %q is not a bounded normalized string", field)
	}
	if isSensitiveValue(s) {
		return fmt.Errorf("field %q carries a credential-shaped value", field)
	}
	return nil
}

func normalizedNumberField(field string, v any) error {
	n, ok := v.(float64)
	if !ok || n != math.Trunc(n) || n < 0 || n > maxPayloadNumber {
		return fmt.Errorf("field %q must be a non-negative integer", field)
	}
	return nil
}

func validateNormalizedPayload(v any) error {
	root, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("payload must be a JSON object of normalized provider fields")
	}
	if len(root) > maxRequiredCheckEntries {
		return fmt.Errorf("payload exceeds %d fields", maxRequiredCheckEntries)
	}
	for k, val := range root {
		switch k {
		case "repository", "head_sha", "check_name":
			if err := normalizedStringField(k, val, maxPayloadStringLen); err != nil {
				return err
			}
		case "url":
			s, ok := val.(string)
			if !ok {
				return fmt.Errorf("field %q must be a string", k)
			}
			if len(s) > maxPayloadURLLen || hasControlChars(s) || strings.ContainsAny(s, " \t@") ||
				!(strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")) {
				return fmt.Errorf("field %q is not a bounded normalized URL", k)
			}
		case "pull_request", "check_run_id", "started_at", "completed_at":
			if err := normalizedNumberField(k, val); err != nil {
				return err
			}
		case "conclusion":
			s, ok := val.(string)
			if !ok || !allowedConclusionValues[s] {
				return fmt.Errorf("field %q must be a known provider conclusion", k)
			}
		case "required_checks":
			m, ok := val.(map[string]any)
			if !ok {
				return fmt.Errorf("field %q must be an object", k)
			}
			if len(m) > maxRequiredCheckEntries {
				return fmt.Errorf("field %q exceeds %d entries", k, maxRequiredCheckEntries)
			}
			for name, conclusion := range m {
				if len(name) == 0 || len(name) > maxPayloadKeyLen || hasControlChars(name) {
					return fmt.Errorf("field %q has a non-normalized check name", k)
				}
				s, ok := conclusion.(string)
				if !ok || !allowedConclusionValues[s] {
					return fmt.Errorf("field %q has a non-conclusion value for check %q", k, name)
				}
			}
		default:
			return fmt.Errorf("field %q is not part of the normalized payload contract", k)
		}
	}
	return nil
}

func (c *Controller) CompleteExternalWait(ctx context.Context, req CompleteWaitRequest) (*CompletionResult, error) {
	if req.WaitID == "" {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "wait_id is required")
	}
	if req.ProviderEventID == "" || req.EventType == "" || req.Source == "" || req.CorrelationKey == "" || req.Status == "" || req.PayloadDigest == "" {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "provider_event_id, event_type, source, correlation_key, status, and payload_digest are required")
	}
	for field, value := range map[string]string{
		"provider_event_id": req.ProviderEventID,
		"event_type":        req.EventType,
		"source":            req.Source,
		"correlation_key":   req.CorrelationKey,
		"status":            req.Status,
		"payload_digest":    req.PayloadDigest,
	} {
		if len(value) > 512 || hasControlChars(value) {
			return nil, store.NewCodeError(store.CodeGraphInvalid, "%s must be a bounded single-line string", field)
		}
	}

	// 1. Payload must be a proven-safe allowlisted normalized object before
	// hashing or persistence; anything unprovable is rejected outright.
	var rawParsed any
	if len(req.Payload) > 0 {
		if err := json.Unmarshal(req.Payload, &rawParsed); err != nil {
			return nil, store.NewCodeError(store.CodeGraphInvalid, "payload must be valid JSON: %v", err)
		}
	} else {
		rawParsed = map[string]any{}
	}
	if err := validateNormalizedPayload(rawParsed); err != nil {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "payload rejected: %v", err)
	}

	rawCanonicalBytes, _ := json.Marshal(rawParsed)
	rawDigest := hexDigest(string(rawCanonicalBytes))

	normDigest := normalizeDigest(req.PayloadDigest)
	if normDigest != rawDigest && normDigest != "sha256:"+rawDigest {
		return nil, store.NewCodeError(store.CodeGraphInvalid, "payload_digest %s does not match payload (computed sha256:%s)", req.PayloadDigest, rawDigest)
	}

	persistedPayloadBytes := rawCanonicalBytes
	persistedDigest := "sha256:" + rawDigest

	var result CompletionResult
	nowMs := time.Now().UnixMilli()
	occurredAt := req.OccurredAt
	if occurredAt <= 0 || occurredAt > nowMs {
		occurredAt = nowMs
	}

	err := c.store.WithTx(ctx, func(tx *sql.Tx) error {
		// A. Check idempotency inside transaction
		var existingEventID, existingType string
		err := tx.QueryRowContext(ctx,
			"SELECT event_id, type FROM event WHERE idempotency_key = ?", req.ProviderEventID).
			Scan(&existingEventID, &existingType)
		if err == nil {
			var waitStatus, runID, runNodeID string
			err := tx.QueryRowContext(ctx,
				"SELECT status, run_id, run_node_id FROM external_wait WHERE id = ?", req.WaitID).
				Scan(&waitStatus, &runID, &runNodeID)
			if err == nil && waitStatus == "completed" {
				var nodeKey string
				_ = tx.QueryRowContext(ctx, "SELECT node_key FROM run_node WHERE id = ?", runNodeID).Scan(&nodeKey)
				result = CompletionResult{
					Code:       "WAIT_ALREADY_COMPLETED",
					HTTPStatus: http.StatusOK,
					WaitID:     req.WaitID,
					RunID:      runID,
					NodeKey:    nodeKey,
					Message:    "idempotent duplicate; original resolution preserved",
				}
				return nil
			}
			result = CompletionResult{
				Code:       "WAIT_REJECTED",
				HTTPStatus: http.StatusAccepted,
				WaitID:     req.WaitID,
				Message:    "event already processed",
			}
			return nil
		} else if err != sql.ErrNoRows {
			return err
		}

		// B. Lookup external_wait inside transaction
		var wait struct {
			RunID            string
			RunNodeID        string
			GraphVersionID   string
			DefinitionDigest string
			EventType        string
			CorrelationKey   string
			ExpectedCond     string
			Status           string
		}
		err = tx.QueryRowContext(ctx, `
SELECT run_id, run_node_id, graph_version_id, definition_digest, event_type, correlation_key, expected_condition, status
FROM external_wait WHERE id = ?`, req.WaitID).
			Scan(&wait.RunID, &wait.RunNodeID, &wait.GraphVersionID, &wait.DefinitionDigest,
				&wait.EventType, &wait.CorrelationKey, &wait.ExpectedCond, &wait.Status)
		if err == sql.ErrNoRows {
			result = CompletionResult{
				Code:       "WAIT_NOT_FOUND",
				HTTPStatus: http.StatusNotFound,
				WaitID:     req.WaitID,
				Message:    fmt.Sprintf("wait %s not found", req.WaitID),
			}
			return nil
		} else if err != nil {
			return err
		}

		var nodeKey string
		_ = tx.QueryRowContext(ctx, "SELECT node_key FROM run_node WHERE id = ?", wait.RunNodeID).Scan(&nodeKey)

		// C. Validate pending status and matching correlation
		if wait.Status != "pending" {
			_ = c.recordRejectedEventTx(ctx, tx, wait.RunID, req, fmt.Sprintf("wait is already %s", wait.Status), nowMs)
			result = CompletionResult{
				Code:       "WAIT_CONFLICT",
				HTTPStatus: http.StatusConflict,
				WaitID:     req.WaitID,
				RunID:      wait.RunID,
				NodeKey:    nodeKey,
				Message:    fmt.Sprintf("wait %s is already %s", req.WaitID, wait.Status),
			}
			return nil
		}

		if wait.EventType != req.EventType || wait.CorrelationKey != req.CorrelationKey {
			_ = c.recordRejectedEventTx(ctx, tx, wait.RunID, req, "mismatched event_type or correlation_key", nowMs)
			result = CompletionResult{
				Code:       "WAIT_CONFLICT",
				HTTPStatus: http.StatusConflict,
				WaitID:     req.WaitID,
				RunID:      wait.RunID,
				NodeKey:    nodeKey,
				Message:    fmt.Sprintf("mismatched correlation or event type for wait %s", req.WaitID),
			}
			return nil
		}

		var runStatus string
		err = tx.QueryRowContext(ctx, "SELECT status FROM graph_run WHERE id = ?", wait.RunID).Scan(&runStatus)
		if err != nil {
			return err
		}
		if runStatus != "running" {
			_ = c.recordRejectedEventTx(ctx, tx, wait.RunID, req, fmt.Sprintf("run is %s", runStatus), nowMs)
			result = CompletionResult{
				Code:       "WAIT_CONFLICT",
				HTTPStatus: http.StatusConflict,
				WaitID:     req.WaitID,
				RunID:      wait.RunID,
				NodeKey:    nodeKey,
				Message:    fmt.Sprintf("run %s is not running (%s)", wait.RunID, runStatus),
			}
			return nil
		}

		// D. Non-terminal provider states leave the wait pending without
		// consuming the provider event id, so the terminal delivery can still win.
		if isNonTerminalProviderStatus(req.Status) {
			_ = c.recordObservedEventTx(ctx, tx, wait.RunID, req,
				fmt.Sprintf("provider status %q is non-terminal", req.Status), nowMs)
			result = CompletionResult{
				Code:       "WAIT_REJECTED",
				HTTPStatus: http.StatusAccepted,
				WaitID:     req.WaitID,
				RunID:      wait.RunID,
				NodeKey:    nodeKey,
				Message:    fmt.Sprintf("provider status %q is non-terminal; wait remains pending", req.Status),
			}
			return nil
		}

		// E. Evaluate the declared expected condition before accepting.
		satisfied, hasPredicate := evaluateExpectedCondition(wait.ExpectedCond, req.Status, rawParsed)
		route := req.Status
		if hasPredicate {
			if satisfied {
				route = "success"
			} else if req.Status == "success" {
				_ = c.recordObservedEventTx(ctx, tx, wait.RunID, req,
					"completion does not satisfy the declared expected condition", nowMs)
				result = CompletionResult{
					Code:       "WAIT_REJECTED",
					HTTPStatus: http.StatusAccepted,
					WaitID:     req.WaitID,
					RunID:      wait.RunID,
					NodeKey:    nodeKey,
					Message:    fmt.Sprintf("completion does not satisfy the expected condition of wait %s", req.WaitID),
				}
				return nil
			}
		}

		// F. Accept completion atomically
		receivedEventID := ulid.Make().String()
		completedEventID := ulid.Make().String()

		// Append external_event_received
		recvPayload := map[string]any{
			"wait_id":           req.WaitID,
			"provider_event_id": req.ProviderEventID,
			"event_type":        req.EventType,
			"source":            req.Source,
			"correlation_key":   req.CorrelationKey,
			"occurred_at":       occurredAt,
			"status":            req.Status,
			"payload_digest":    persistedDigest,
			"payload":           json.RawMessage(persistedPayloadBytes),
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
				"payload_digest":    persistedDigest,
			}),
		}); err != nil {
			return err
		}

		// Resume the waiting node and traverse outgoing edges
		var attemptCount int64
		_ = tx.QueryRowContext(ctx,
			"SELECT attempt_count FROM run_node WHERE id = ?", wait.RunNodeID).Scan(&attemptCount)

		var outputMap map[string]any
		if m, ok := rawParsed.(map[string]any); ok {
			outputMap = m
		} else {
			outputMap = map[string]any{"payload": rawParsed}
		}

		nodeResult := &executor.Result{
			Route:  route,
			Output: outputMap,
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
				"result":     nodeResult.Output,
				"route":      nodeResult.Route,
			}),
		}); err != nil {
			return err
		}

		// Route edges from the completed node
		if err := c.routeFromTx(ctx, tx, wait.RunID, wait.GraphVersionID, wait.DefinitionDigest,
			runnableNode{NodeKey: nodeKey, AttemptNo: attemptCount}, nodeResult, true, nowMs); err != nil {
			return err
		}

		result = CompletionResult{
			Code:       "WAIT_COMPLETED",
			HTTPStatus: http.StatusAccepted,
			WaitID:     req.WaitID,
			RunID:      wait.RunID,
			NodeKey:    nodeKey,
			Message:    "event accepted and wait resolved",
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func isNonTerminalProviderStatus(status string) bool {
	switch strings.ToLower(status) {
	case "queued", "pending", "in_progress", "running", "started", "waiting":
		return true
	}
	return false
}

func evaluateExpectedCondition(condJSON, status string, payload any) (satisfied, hasPredicate bool) {
	var cond map[string]any
	if err := json.Unmarshal([]byte(condJSON), &cond); err != nil || len(cond) == 0 {
		return false, false
	}
	for k, want := range cond {
		actual, ok := conditionLookup(k, status, payload)
		if !ok || !conditionEqual(actual, want) {
			return false, true
		}
	}
	return true, true
}

func conditionLookup(key, status string, payload any) (any, bool) {
	if key == "status" {
		return status, true
	}
	cur := payload
	for _, part := range strings.Split(key, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		v, exists := m[part]
		if !exists {
			return nil, false
		}
		cur = v
	}
	return cur, true
}

func conditionEqual(actual, want any) bool {
	switch w := want.(type) {
	case string:
		s, ok := actual.(string)
		return ok && s == w
	case float64:
		n, ok := actual.(float64)
		return ok && n == w
	case bool:
		b, ok := actual.(bool)
		return ok && b == w
	case nil:
		return actual == nil
	}
	return false
}

func (c *Controller) recordRejectedEventTx(ctx context.Context, tx *sql.Tx, runID string, req CompleteWaitRequest, reason string, nowMs int64) error {
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
}

func (c *Controller) recordObservedEventTx(ctx context.Context, tx *sql.Tx, runID string, req CompleteWaitRequest, reason string, nowMs int64) error {
	_, err := c.appendWithin(ctx, tx, &store.Event{
		EventID:       ulid.Make().String(),
		RunID:         runID,
		SchemaVersion: "proceed/v1",
		Type:          "external_event_rejected",
		OccurredAt:    nowMs,
		RecordedAt:    nowMs,
		ActorType:     "controller",
		ActorID:       c.cfg.OwnerID,
		CorrelationID: req.CorrelationKey,
		Payload: payloadJSON(map[string]any{
			"wait_id":           req.WaitID,
			"provider_event_id": req.ProviderEventID,
			"reason":            reason,
			"event_type":        req.EventType,
			"correlation_key":   req.CorrelationKey,
			"status":            req.Status,
		}),
	})
	return err
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
		return c.recordRejectedEventTx(ctx, tx, runID, req, reason, nowMs)
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
