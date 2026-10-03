package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"unicode/utf8"
)

const failureMessageCap = 512

// The exit-code class table lives in cmd/proceed, which the store cannot import, so the
// persisted vocabulary is restated here: derivation must classify stored text without
// inventing a class, and an unlisted prefix has to degrade to a null code.
var failureClasses = map[string]bool{
	"GRAPH_INVALID":     true,
	"POLICY_DENIED":     true,
	"RUN_NOT_FOUND":     true,
	"NODE_TIMEOUT":      true,
	"NODE_FAILED":       true,
	"EFFECT_UNCERTAIN":  true,
	"APPROVAL_REQUIRED": true,
	"APPROVAL_EXPIRED":  true,
	"RUN_CANCELLED":     true,
	"STORE_BUSY":        true,
	"STORE_CONFLICT":    true,
}

type NodeFailure struct {
	Code    *string `json:"code"`
	Message string  `json:"message"`
}

type terminalEventPayload struct {
	NodeKey   string          `json:"node_key"`
	AttemptNo int64           `json:"attempt_no"`
	Error     json.RawMessage `json:"error"`
	Reason    json.RawMessage `json:"reason"`
}

type nodeCause struct {
	attemptNo int64
	text      string
}

func (s *Store) terminalEventCauses(ctx context.Context, runID string) (map[string]nodeCause, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT type, payload FROM event
WHERE run_id = ? AND type IN ('node_failed', 'node_uncertain')
ORDER BY sequence`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	causes := map[string]nodeCause{}
	for rows.Next() {
		var typ, payload string
		if err := rows.Scan(&typ, &payload); err != nil {
			return nil, err
		}
		var p terminalEventPayload
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			continue
		}
		if p.NodeKey == "" || p.AttemptNo <= 0 {
			continue
		}
		causes[p.NodeKey] = nodeCause{attemptNo: p.AttemptNo, text: terminalEventText(typ, p)}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return causes, nil
}

func terminalEventText(typ string, p terminalEventPayload) string {
	field := p.Error
	if typ == "node_uncertain" {
		field = p.Reason
	}
	if len(field) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(field, &text); err != nil {
		return string(field)
	}
	return text
}

func (s *Store) attemptResultCauses(ctx context.Context, runID string) (map[string]nodeCause, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT rn.node_key, na.result FROM node_attempt na
JOIN run_node rn ON rn.id = na.run_node_id
WHERE rn.run_id = ? AND na.status IN ('failed', 'uncertain')
  AND na.attempt_no = (SELECT MAX(x.attempt_no) FROM node_attempt x WHERE x.run_node_id = na.run_node_id)
  AND na.attempt_no = rn.attempt_count`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	causes := map[string]nodeCause{}
	for rows.Next() {
		var nodeKey string
		var result sql.NullString
		if err := rows.Scan(&nodeKey, &result); err != nil {
			return nil, err
		}
		text := attemptResultError(result)
		if text == "" {
			continue
		}
		causes[nodeKey] = nodeCause{attemptNo: -1, text: text}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return causes, nil
}

func attemptResultError(result sql.NullString) string {
	if !result.Valid || strings.TrimSpace(result.String) == "" {
		return ""
	}
	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(result.String), &payload); err != nil {
		return ""
	}
	return payload.Error
}

func resolveNodeFailure(nodeKey string, attemptCount int64, eventCauses, resultCauses map[string]nodeCause) *NodeFailure {
	text := ""
	if cause, ok := eventCauses[nodeKey]; ok && cause.attemptNo == attemptCount {
		text = cause.text
	}
	if strings.TrimSpace(text) == "" {
		text = ""
		if cause, ok := resultCauses[nodeKey]; ok {
			text = cause.text
		}
	}
	if text == "" {
		return nil
	}
	return newNodeFailure(text)
}

func newNodeFailure(text string) *NodeFailure {
	failure := &NodeFailure{Message: truncateFailureMessage(text)}
	if code, message, ok := strings.Cut(text, ": "); ok && failureClasses[code] {
		failure.Code = &code
		failure.Message = truncateFailureMessage(message)
	}
	return failure
}

func truncateFailureMessage(text string) string {
	if len(text) <= failureMessageCap {
		return text
	}
	cut := failureMessageCap
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut]
}
