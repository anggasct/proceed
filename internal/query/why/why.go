package why

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"proceed/internal/store"
)

type Explanation struct {
	Recorded  Recorded  `json:"recorded"`
	Inference Inference `json:"inference"`
}

type Recorded struct {
	RunID          string             `json:"run_id"`
	GraphVersionID string             `json:"graph_version_id"`
	NodeKey        string             `json:"node_key"`
	NodeStatus     string             `json:"node_status"`
	AttemptCount   int64              `json:"attempt_count"`
	CandidateEdges []string           `json:"candidate_edges"`
	Decisions      []RecordedDecision `json:"decisions"`
	CausalLinks    []RecordedLink     `json:"causal_links"`
	Evidence       Evidence           `json:"evidence"`
	Source         string             `json:"source"`
}

type RecordedDecision struct {
	ID                string          `json:"id"`
	Kind              string          `json:"kind"`
	CandidateEdges    []string        `json:"candidate_edges"`
	SelectedEdgeID    string          `json:"selected_edge_id,omitempty"`
	Rejection         string          `json:"rejection,omitempty"`
	PredicateSnapshot json.RawMessage `json:"predicate_snapshot"`
	InputReferences   []string        `json:"input_references"`
	PolicyVersion     string          `json:"policy_version"`
	DecidedAt         int64           `json:"decided_at"`
}

type RecordedLink struct {
	DecisionID    string `json:"decision_id"`
	DecidedByNode string `json:"decided_by_node"`
	Attribution   string `json:"attribution"`
	SourceKind    string `json:"source_kind"`
	SourceID      string `json:"source_id"`
	CitationType  string `json:"citation_type,omitempty"`
	CitationID    string `json:"citation_id,omitempty"`
	GroupKey      string `json:"group_key,omitempty"`
	TargetNodeKey string `json:"target_node_key"`
}

type Evidence struct {
	Artifacts   []ArtifactEvidence   `json:"artifacts"`
	Evaluations []EvaluationEvidence `json:"evaluations"`
	Approvals   []ApprovalEvidence   `json:"approvals"`
	Events      []EventEvidence      `json:"events"`
}

type ArtifactEvidence struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	ContentHash string `json:"content_hash"`
	MediaType   string `json:"media_type"`
	SizeBytes   int64  `json:"size_bytes"`
	Truncated   bool   `json:"truncated"`
}

type EvaluationEvidence struct {
	ID                 string `json:"id"`
	Verdict            string `json:"verdict"`
	EvaluatedByNodeKey string `json:"evaluated_by_node_key"`
	EvidenceRef        string `json:"evidence_ref,omitempty"`
}

type ApprovalEvidence struct {
	ID              string `json:"id"`
	RequestedAction string `json:"requested_action"`
	RequiredScope   string `json:"required_scope"`
	Decision        string `json:"decision,omitempty"`
	DecidedBy       string `json:"decided_by,omitempty"`
	ExpiresAt       int64  `json:"expires_at"`
}

type EventEvidence struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	OccurredAt int64  `json:"occurred_at"`
}

type Inference struct {
	Summary      string        `json:"summary"`
	SelectedEdge string        `json:"selected_edge,omitempty"`
	Attributions []Attribution `json:"attributions"`
	Pending      bool          `json:"pending"`
}

type Attribution struct {
	TargetNodeKey string `json:"target_node_key"`
	Strength      string `json:"strength"`
	GroupKey      string `json:"group_key,omitempty"`
	GroupMembers  int    `json:"group_members,omitempty"`
	Source        string `json:"source"`
}

type Query struct {
	st *store.Store
}

func New(st *store.Store) *Query {
	return &Query{st: st}
}

func (q *Query) Explain(ctx context.Context, runID, nodeKey string) (*Explanation, error) {
	current, err := q.projectionCurrent(ctx, runID)
	if err != nil {
		return nil, err
	}
	var rec *Recorded
	if current {
		rec, err = q.load(ctx, runID, nodeKey)
	} else {
		rec, err = q.loadFromEvents(ctx, runID, nodeKey)
	}
	if err != nil {
		return nil, err
	}
	return &Explanation{Recorded: *rec, Inference: render(*rec)}, nil
}

// projectionCurrent reports whether the projection tables are present and
// hold every decision event the immutable stream recorded for the run; a
// missing graph_run row or lagging projections route the explanation
// through event replay.
func (q *Query) projectionCurrent(ctx context.Context, runID string) (bool, error) {
	var runExists int
	if err := q.st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM graph_run WHERE id = ?", runID).Scan(&runExists); err != nil {
		return false, err
	}
	if runExists == 0 {
		var runEvents int
		if err := q.st.DB().QueryRowContext(ctx,
			"SELECT COUNT(*) FROM event WHERE run_id = ? AND type = 'run_started'", runID).Scan(&runEvents); err != nil {
			return false, err
		}
		if runEvents > 0 {
			return false, nil
		}
		return false, store.NewCodeError("RUN_NOT_FOUND", "run %s does not exist", runID)
	}
	var projected int
	if err := q.st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM decision WHERE run_id = ?", runID).Scan(&projected); err != nil {
		return false, err
	}
	var recorded int
	if err := q.st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM event WHERE run_id = ? AND type = 'decision_recorded'", runID).Scan(&recorded); err != nil {
		return false, err
	}
	if projected != recorded {
		return false, nil
	}

	rows, err := q.st.DB().QueryContext(ctx, `
SELECT type, payload FROM event
WHERE run_id = ? AND type IN (
  'decision_recorded', 'edge_traversed', 'artifact_published', 'evaluation_failed',
  'approval_requested', 'node_started', 'node_finished', 'node_failed',
  'node_skipped', 'node_uncertain', 'node_cancelled', 'node_attempt_failed',
  'node_requeued', 'node_waiting', 'node_reconciling', 'node_cancel_requested'
) ORDER BY sequence`, runID)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	expected := map[string]int{}
	nodeKeys := map[string]bool{}
	causalLinks := 0
	for rows.Next() {
		var typ, payload string
		if err := rows.Scan(&typ, &payload); err != nil {
			return false, err
		}
		switch typ {
		case "decision_recorded":
			var p decisionEventPayload
			if err := json.Unmarshal([]byte(payload), &p); err != nil {
				return false, err
			}
			expected["decision"]++
			causalLinks += len(p.CausalLinks)
			if p.NodeKey != "" {
				nodeKeys[p.NodeKey] = true
			}
			for _, link := range p.CausalLinks {
				if link.TargetNodeKey != "" {
					nodeKeys[link.TargetNodeKey] = true
				}
			}
		case "edge_traversed":
			expected["run_edge"]++
		case "artifact_published":
			var p struct {
				NodeKey string `json:"node_key"`
			}
			if err := json.Unmarshal([]byte(payload), &p); err != nil {
				return false, err
			}
			expected["artifact"]++
			if p.NodeKey != "" {
				nodeKeys[p.NodeKey] = true
			}
		case "evaluation_failed":
			expected["evaluation"]++
		case "approval_requested":
			var p struct {
				NodeKey string `json:"node_key"`
			}
			if err := json.Unmarshal([]byte(payload), &p); err != nil {
				return false, err
			}
			expected["approval"]++
			if p.NodeKey != "" {
				nodeKeys[p.NodeKey] = true
			}
		default:
			var p struct {
				NodeKey string `json:"node_key"`
			}
			if err := json.Unmarshal([]byte(payload), &p); err != nil {
				return false, err
			}
			if p.NodeKey != "" {
				nodeKeys[p.NodeKey] = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	counts := map[string]string{
		"run_edge":   "SELECT COUNT(*) FROM run_edge WHERE run_id = ?",
		"artifact":   "SELECT COUNT(*) FROM artifact WHERE run_id = ?",
		"evaluation": "SELECT COUNT(*) FROM evaluation WHERE run_id = ?",
		"approval":   "SELECT COUNT(*) FROM approval WHERE run_id = ?",
	}
	for table, query := range counts {
		var actual int
		if err := q.st.DB().QueryRowContext(ctx, query, runID).Scan(&actual); err != nil {
			return false, err
		}
		if actual != expected[table] {
			return false, nil
		}
	}
	var actualLinks int
	if err := q.st.DB().QueryRowContext(ctx, `
SELECT COUNT(*) FROM causal_link cl
JOIN decision d ON d.id = cl.decision_id
WHERE d.run_id = ?`, runID).Scan(&actualLinks); err != nil {
		return false, err
	}
	if actualLinks != causalLinks {
		return false, nil
	}
	for nodeKey := range nodeKeys {
		var present int
		if err := q.st.DB().QueryRowContext(ctx,
			"SELECT COUNT(*) FROM run_node WHERE run_id = ? AND node_key = ?", runID, nodeKey).Scan(&present); err != nil {
			return false, err
		}
		if present == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (q *Query) load(ctx context.Context, runID, nodeKey string) (*Recorded, error) {
	var graphVersionID string
	err := q.st.DB().QueryRowContext(ctx,
		"SELECT graph_version_id FROM graph_run WHERE id = ?", runID).Scan(&graphVersionID)
	if err == sql.ErrNoRows {
		return nil, store.NewCodeError("RUN_NOT_FOUND", "run %s does not exist", runID)
	}
	if err != nil {
		return nil, err
	}

	var defined int
	if err := q.st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM graph_node WHERE graph_version_id = ? AND node_key = ?",
		graphVersionID, nodeKey).Scan(&defined); err != nil {
		return nil, err
	}
	if defined == 0 {
		return nil, store.NewCodeError("NODE_NOT_FOUND",
			"run %s has no node %q", runID, nodeKey)
	}

	var nodeID string
	var nodeStatus string
	var attemptCount int64
	err = q.st.DB().QueryRowContext(ctx, `
SELECT COALESCE(rn.id, ''), COALESCE(rn.status, 'pending'), COALESCE(rn.attempt_count, 0)
FROM graph_node gn
LEFT JOIN run_node rn ON rn.run_id = ? AND rn.node_key = gn.node_key
WHERE gn.graph_version_id = ? AND gn.node_key = ?`, runID, graphVersionID, nodeKey).
		Scan(&nodeID, &nodeStatus, &attemptCount)
	if err != nil {
		return nil, err
	}

	rec := &Recorded{
		RunID:          runID,
		GraphVersionID: graphVersionID,
		NodeKey:        nodeKey,
		NodeStatus:     nodeStatus,
		AttemptCount:   attemptCount,
		CandidateEdges: []string{},
		Source:         "projection",
	}

	if rec.CandidateEdges, err = q.definitionCandidates(ctx, graphVersionID, nodeKey); err != nil {
		return nil, err
	}
	if err := q.loadDecisions(ctx, rec, nodeID); err != nil {
		return nil, err
	}
	if err := q.loadCausalLinks(ctx, rec, nodeID); err != nil {
		return nil, err
	}
	if err := q.loadEvidence(ctx, rec, runID, nodeKey, nodeID); err != nil {
		return nil, err
	}
	return rec, nil
}

func (q *Query) definitionCandidates(ctx context.Context, graphVersionID, nodeKey string) ([]string, error) {
	rows, err := q.st.DB().QueryContext(ctx, `
SELECT id FROM graph_edge
WHERE graph_version_id = ? AND to_node_key = ?
  AND type IN ('depends_on', 'routes_to', 'produces', 'consumes')
ORDER BY id`, graphVersionID, nodeKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// loadFromEvents reconstructs the explanation purely from the immutable
// event stream when the projections are missing or stale.
func (q *Query) loadFromEvents(ctx context.Context, runID, nodeKey string) (*Recorded, error) {
	rows, err := q.st.DB().QueryContext(ctx, `
SELECT event_id, sequence, payload FROM event
WHERE run_id = ? AND type = 'run_started'
ORDER BY sequence LIMIT 1`, runID)
	if err != nil {
		return nil, err
	}
	var graphVersionID, payload string
	for rows.Next() {
		if err := rows.Scan(&graphVersionID, &payload, &payload); err != nil {
			rows.Close()
			return nil, err
		}
		var started struct {
			GraphVersionID string `json:"graph_version_id"`
		}
		if err := json.Unmarshal([]byte(payload), &started); err != nil {
			rows.Close()
			return nil, err
		}
		graphVersionID = started.GraphVersionID
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if graphVersionID == "" {
		return nil, store.NewCodeError("RUN_NOT_FOUND", "run %s does not exist", runID)
	}

	var defined int
	if err := q.st.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM graph_node WHERE graph_version_id = ? AND node_key = ?",
		graphVersionID, nodeKey).Scan(&defined); err != nil {
		return nil, err
	}
	if defined == 0 {
		return nil, store.NewCodeError("NODE_NOT_FOUND", "run %s has no node %q", runID, nodeKey)
	}

	rec := &Recorded{
		RunID:          runID,
		GraphVersionID: graphVersionID,
		NodeKey:        nodeKey,
		NodeStatus:     "pending",
		Source:         "events",
		CandidateEdges: []string{},
	}
	if rec.CandidateEdges, err = q.definitionCandidates(ctx, graphVersionID, nodeKey); err != nil {
		return nil, err
	}

	type decisionEvent struct {
		id         string
		payload    decisionEventPayload
		occurredAt int64
	}
	var decisionEvents []decisionEvent
	decRows, err := q.st.DB().QueryContext(ctx, `
SELECT event_id, payload, occurred_at FROM event
WHERE run_id = ? AND type = 'decision_recorded'
ORDER BY sequence`, runID)
	if err != nil {
		return nil, err
	}
	for decRows.Next() {
		var eventID string
		var payload string
		var occurredAt int64
		if err := decRows.Scan(&eventID, &payload, &occurredAt); err != nil {
			decRows.Close()
			return nil, err
		}
		var p decisionEventPayload
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			decRows.Close()
			return nil, err
		}
		targetsNode := false
		for _, link := range p.CausalLinks {
			if link.TargetNodeKey == nodeKey {
				targetsNode = true
			}
		}
		if p.NodeKey == nodeKey || targetsNode {
			decisionEvents = append(decisionEvents, decisionEvent{id: eventID, payload: p, occurredAt: occurredAt})
			rec.CandidateEdges = mergeUnique(rec.CandidateEdges, p.CandidateEdges)
		}
	}
	decRows.Close()
	if err := decRows.Err(); err != nil {
		return nil, err
	}
	for _, event := range decisionEvents {
		p := event.payload
		rec.Decisions = append(rec.Decisions, RecordedDecision{
			ID:                event.id,
			Kind:              p.Kind,
			CandidateEdges:    orEmpty(p.CandidateEdges),
			SelectedEdgeID:    p.SelectedEdgeID,
			Rejection:         p.Rejection,
			PredicateSnapshot: p.PredicateSnapshot,
			InputReferences:   orEmpty(p.InputReferences),
			PolicyVersion:     p.PolicyVersion,
			DecidedAt:         event.occurredAt,
		})
	}

	type finishState struct {
		status string
		count  int64
	}
	nodeStates := map[string]finishState{}
	stateRows, err := q.st.DB().QueryContext(ctx, `
SELECT type, payload FROM event
WHERE run_id = ? AND type IN ('node_started','node_finished','node_failed','node_skipped','node_uncertain','node_cancelled',
                              'node_attempt_failed','node_requeued','node_waiting','node_reconciling','node_cancel_requested')
ORDER BY sequence`, runID)
	if err != nil {
		return nil, err
	}
	for stateRows.Next() {
		var typ, payload string
		if err := stateRows.Scan(&typ, &payload); err != nil {
			stateRows.Close()
			return nil, err
		}
		var p struct {
			NodeKey   string `json:"node_key"`
			AttemptNo int64  `json:"attempt_no"`
		}
		_ = json.Unmarshal([]byte(payload), &p)
		if p.NodeKey != nodeKey {
			continue
		}
		state := nodeStates[p.NodeKey]
		switch typ {
		case "node_started":
			state.status = "running"
			attemptNo := p.AttemptNo
			if attemptNo <= 0 {
				attemptNo = 1
			}
			if attemptNo > state.count {
				state.count = attemptNo
			}
		case "node_finished":
			state.status = "succeeded"
		case "node_failed":
			state.status = "failed"
		case "node_skipped":
			state.status = "skipped"
		case "node_uncertain":
			state.status = "uncertain"
		case "node_cancelled":
			state.status = "cancelled"
		case "node_attempt_failed", "node_requeued":
			state.status = "eligible"
		case "node_waiting":
			state.status = "waiting"
		case "node_reconciling":
			state.status = "reconciling"
		case "node_cancel_requested":
			state.status = "cancel_requested"
		}
		nodeStates[p.NodeKey] = state
	}
	stateRows.Close()
	if s, ok := nodeStates[nodeKey]; ok {
		rec.NodeStatus = s.status
		rec.AttemptCount = s.count
	}

	linkRows, err := q.st.DB().QueryContext(ctx, `
SELECT event_id, payload FROM event
WHERE run_id = ? AND type = 'decision_recorded'
ORDER BY sequence`, runID)
	if err != nil {
		return nil, err
	}
	defer linkRows.Close()
	for linkRows.Next() {
		var eventID string
		var payload string
		if err := linkRows.Scan(&eventID, &payload); err != nil {
			return nil, err
		}
		var p decisionEventPayload
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			return nil, err
		}
		for _, link := range p.CausalLinks {
			if link.TargetNodeKey != nodeKey {
				continue
			}
			rec.CausalLinks = append(rec.CausalLinks, RecordedLink{
				DecisionID:    eventID,
				Attribution:   link.Attribution,
				SourceKind:    link.SourceKind,
				SourceID:      link.SourceID,
				CitationType:  link.CitationType,
				CitationID:    link.CitationID,
				GroupKey:      link.GroupKey,
				TargetNodeKey: link.TargetNodeKey,
				DecidedByNode: p.NodeKey,
			})
		}
	}

	if err := q.loadEvidenceByReferences(ctx, rec, runID, collectEvidenceReferences(rec)); err != nil {
		return nil, err
	}
	if err := q.loadEvidenceFromEvents(ctx, rec, runID, nodeKey); err != nil {
		return nil, err
	}
	return rec, nil
}

type decisionEventPayload struct {
	NodeKey           string             `json:"node_key"`
	Kind              string             `json:"kind"`
	CandidateEdges    []string           `json:"candidate_edges"`
	SelectedEdgeID    string             `json:"selected_edge_id"`
	Rejection         string             `json:"rejection"`
	PredicateSnapshot json.RawMessage    `json:"predicate_snapshot"`
	InputReferences   []string           `json:"input_references"`
	PolicyVersion     string             `json:"policy_version"`
	CausalLinks       []linkEventPayload `json:"causal_links"`
}

type linkEventPayload struct {
	TargetNodeKey string `json:"target_node_key"`
	Attribution   string `json:"attribution"`
	SourceKind    string `json:"source_kind"`
	SourceID      string `json:"source_id"`
	CitationType  string `json:"citation_type"`
	CitationID    string `json:"citation_id"`
	GroupKey      string `json:"group_key"`
}

func (q *Query) loadDecisions(ctx context.Context, rec *Recorded, nodeID string) error {
	rows, err := q.st.DB().QueryContext(ctx, `
SELECT d.id, d.kind, d.candidate_edges, COALESCE(d.selected_edge_id, ''),
       COALESCE(d.rejection, ''), d.predicate_snapshot, d.input_references, d.policy_version, d.decided_at
FROM decision d
WHERE d.run_node_id = ?
   OR d.id IN (SELECT cl.decision_id FROM causal_link cl WHERE cl.target_run_node_id = ?)
ORDER BY d.decided_at, d.id`, nodeID, nodeID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var d RecordedDecision
		var candidates, snapshot, inputs string
		if err := rows.Scan(&d.ID, &d.Kind, &candidates, &d.SelectedEdgeID, &d.Rejection,
			&snapshot, &inputs, &d.PolicyVersion, &d.DecidedAt); err != nil {
			return err
		}
		_ = json.Unmarshal([]byte(candidates), &d.CandidateEdges)
		_ = json.Unmarshal([]byte(inputs), &d.InputReferences)
		d.CandidateEdges = orEmpty(d.CandidateEdges)
		d.InputReferences = orEmpty(d.InputReferences)
		d.PredicateSnapshot = json.RawMessage(snapshot)
		rec.Decisions = append(rec.Decisions, d)
		rec.CandidateEdges = mergeUnique(rec.CandidateEdges, d.CandidateEdges)
	}
	return rows.Err()
}

func (q *Query) loadCausalLinks(ctx context.Context, rec *Recorded, nodeID string) error {
	rows, err := q.st.DB().QueryContext(ctx, `
SELECT cl.decision_id, dn.node_key, cl.attribution, cl.source_kind, cl.source_id,
       COALESCE(cl.citation_type, ''), COALESCE(cl.citation_id, ''), COALESCE(cl.group_key, ''),
       tn.node_key
FROM causal_link cl
JOIN run_node tn ON tn.id = cl.target_run_node_id
JOIN decision d ON d.id = cl.decision_id
JOIN run_node dn ON dn.id = d.run_node_id
WHERE cl.target_run_node_id = ?
ORDER BY d.decided_at, cl.id`, nodeID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var l RecordedLink
		if err := rows.Scan(&l.DecisionID, &l.DecidedByNode, &l.Attribution, &l.SourceKind, &l.SourceID,
			&l.CitationType, &l.CitationID, &l.GroupKey, &l.TargetNodeKey); err != nil {
			return err
		}
		rec.CausalLinks = append(rec.CausalLinks, l)
	}
	return rows.Err()
}

type evidenceReferences struct {
	artifacts   map[string]bool
	evaluations map[string]bool
	approvals   map[string]bool
	events      map[string]bool
}

func newEvidenceReferences() evidenceReferences {
	return evidenceReferences{
		artifacts:   map[string]bool{},
		evaluations: map[string]bool{},
		approvals:   map[string]bool{},
		events:      map[string]bool{},
	}
}

func (r *evidenceReferences) add(kind, id string) {
	if id == "" {
		return
	}
	switch kind {
	case "artifact":
		r.artifacts[id] = true
	case "evaluation":
		r.evaluations[id] = true
	case "approval":
		r.approvals[id] = true
	case "event":
		r.events[id] = true
	}
}

func collectEvidenceReferences(rec *Recorded) evidenceReferences {
	refs := newEvidenceReferences()
	for _, decision := range rec.Decisions {
		for _, input := range decision.InputReferences {
			kind, id, ok := strings.Cut(input, ":")
			if ok {
				refs.add(kind, id)
			}
		}
	}
	for _, link := range rec.CausalLinks {
		refs.add(link.CitationType, link.CitationID)
	}
	return refs
}

func (q *Query) loadEvidence(ctx context.Context, rec *Recorded, runID, nodeKey, nodeID string) error {
	refs := collectEvidenceReferences(rec)
	if err := q.loadEvidenceByReferences(ctx, rec, runID, refs); err != nil {
		return err
	}
	ownRows, err := q.st.DB().QueryContext(ctx, `
SELECT id, name, content_hash, media_type, size_bytes, truncated
FROM artifact WHERE run_id = ? AND produced_by_node_key = ? ORDER BY id`, runID, nodeKey)
	if err != nil {
		return err
	}
	defer ownRows.Close()
	for ownRows.Next() {
		var a ArtifactEvidence
		var truncated int
		if err := ownRows.Scan(&a.ID, &a.Name, &a.ContentHash, &a.MediaType, &a.SizeBytes, &truncated); err != nil {
			return err
		}
		a.Truncated = truncated == 1
		appendArtifactEvidence(rec, a)
	}
	if err := ownRows.Err(); err != nil {
		return err
	}
	ownEvalRows, err := q.st.DB().QueryContext(ctx, `
SELECT e.id, e.verdict, e.evaluated_by_node_key, COALESCE(e.evidence_ref, '')
FROM evaluation e
JOIN artifact a ON a.id = e.artifact_id
WHERE e.run_id = ? AND a.produced_by_node_key = ? ORDER BY e.id`, runID, nodeKey)
	if err != nil {
		return err
	}
	defer ownEvalRows.Close()
	for ownEvalRows.Next() {
		var e EvaluationEvidence
		if err := ownEvalRows.Scan(&e.ID, &e.Verdict, &e.EvaluatedByNodeKey, &e.EvidenceRef); err != nil {
			return err
		}
		appendEvaluationEvidence(rec, e)
	}
	if err := ownEvalRows.Err(); err != nil {
		return err
	}

	approvalRows, err := q.st.DB().QueryContext(ctx, `
SELECT id, requested_action, required_scope, COALESCE(decision, ''), COALESCE(decided_by, ''), expires_at
FROM approval WHERE run_id = ? AND run_node_id = ? ORDER BY created_at, id`, runID, nodeID)
	if err != nil {
		return err
	}
	defer approvalRows.Close()
	for approvalRows.Next() {
		var a ApprovalEvidence
		if err := approvalRows.Scan(&a.ID, &a.RequestedAction, &a.RequiredScope,
			&a.Decision, &a.DecidedBy, &a.ExpiresAt); err != nil {
			return err
		}
		appendApprovalEvidence(rec, a)
	}
	return approvalRows.Err()
}

// loadEvidenceByReferences resolves the artifact ids the decision actually
// cited, so a downstream node's explanation includes the upstream evidence
// that made it eligible.
func idClause(ids map[string]bool) (string, []any) {
	keys := make([]string, 0, len(ids))
	for id := range ids {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	placeholders := make([]string, len(keys))
	args := make([]any, len(keys))
	for i, key := range keys {
		placeholders[i] = "?"
		args[i] = key
	}
	return strings.Join(placeholders, ","), args
}

func appendArtifactEvidence(rec *Recorded, evidence ArtifactEvidence) {
	for _, existing := range rec.Evidence.Artifacts {
		if existing.ID == evidence.ID {
			return
		}
	}
	rec.Evidence.Artifacts = append(rec.Evidence.Artifacts, evidence)
}

func appendEvaluationEvidence(rec *Recorded, evidence EvaluationEvidence) {
	for _, existing := range rec.Evidence.Evaluations {
		if existing.ID == evidence.ID {
			return
		}
	}
	rec.Evidence.Evaluations = append(rec.Evidence.Evaluations, evidence)
}

func appendApprovalEvidence(rec *Recorded, evidence ApprovalEvidence) {
	for _, existing := range rec.Evidence.Approvals {
		if existing.ID == evidence.ID {
			return
		}
	}
	rec.Evidence.Approvals = append(rec.Evidence.Approvals, evidence)
}

func appendEventEvidence(rec *Recorded, evidence EventEvidence) {
	for _, existing := range rec.Evidence.Events {
		if existing.ID == evidence.ID {
			return
		}
	}
	rec.Evidence.Events = append(rec.Evidence.Events, evidence)
}

func (q *Query) loadEvaluationReferences(ctx context.Context, rec *Recorded, runID string, refs evidenceReferences) error {
	if len(refs.evaluations) == 0 {
		return nil
	}
	clause, ids := idClause(refs.evaluations)
	args := []any{runID}
	args = append(args, ids...)
	rows, err := q.st.DB().QueryContext(ctx, `
SELECT id, artifact_id, verdict, evaluated_by_node_key, COALESCE(evidence_ref, '')
FROM evaluation WHERE run_id = ? AND id IN (`+clause+
`) ORDER BY id`, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var evidence EvaluationEvidence
		var artifactID string
		if err := rows.Scan(&evidence.ID, &artifactID, &evidence.Verdict,
			&evidence.EvaluatedByNodeKey, &evidence.EvidenceRef); err != nil {
			rows.Close()
			return err
		}
		appendEvaluationEvidence(rec, evidence)
		refs.artifacts[artifactID] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	return nil
}

func (q *Query) loadApprovalReferences(ctx context.Context, rec *Recorded, runID string, refs evidenceReferences) error {
	if len(refs.approvals) == 0 {
		return nil
	}
	clause, ids := idClause(refs.approvals)
	args := []any{runID}
	args = append(args, ids...)
	rows, err := q.st.DB().QueryContext(ctx, `
SELECT id, requested_action, required_scope, COALESCE(decision, ''), COALESCE(decided_by, ''), expires_at
FROM approval WHERE run_id = ? AND id IN (`+clause+
`) ORDER BY id`, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var evidence ApprovalEvidence
		if err := rows.Scan(&evidence.ID, &evidence.RequestedAction, &evidence.RequiredScope,
			&evidence.Decision, &evidence.DecidedBy, &evidence.ExpiresAt); err != nil {
			rows.Close()
			return err
		}
		appendApprovalEvidence(rec, evidence)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	return nil
}

func (q *Query) loadEventReferences(ctx context.Context, rec *Recorded, runID string, refs evidenceReferences) error {
	if len(refs.events) == 0 {
		return nil
	}
	clause, ids := idClause(refs.events)
	args := []any{runID}
	args = append(args, ids...)
	rows, err := q.st.DB().QueryContext(ctx, `
SELECT event_id, type, occurred_at FROM event
WHERE run_id = ? AND event_id IN (`+clause+
`) ORDER BY sequence`, args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var evidence EventEvidence
		if err := rows.Scan(&evidence.ID, &evidence.Type, &evidence.OccurredAt); err != nil {
			rows.Close()
			return err
		}
		appendEventEvidence(rec, evidence)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	return nil
}

func (q *Query) loadEvidenceByReferences(ctx context.Context, rec *Recorded, runID string, refs evidenceReferences) error {
	if err := q.loadEvaluationReferences(ctx, rec, runID, refs); err != nil {
		return err
	}
	if err := q.loadApprovalReferences(ctx, rec, runID, refs); err != nil {
		return err
	}
	if err := q.loadEventReferences(ctx, rec, runID, refs); err != nil {
		return err
	}
	ids := map[string]bool{}
	for id := range refs.artifacts {
		ids[id] = true
	}
	if len(ids) == 0 {
		return nil
	}
	placeholders := make([]string, 0, len(ids))
	args := make([]any, 0, len(ids)+1)
	args = append(args, runID)
	for id := range ids {
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}
	artRows, err := q.st.DB().QueryContext(ctx, `
SELECT id, name, content_hash, media_type, size_bytes, truncated
FROM artifact WHERE run_id = ? AND id IN (`+strings.Join(placeholders, ",")+`) ORDER BY id`, args...)
	if err != nil {
		return err
	}
	defer artRows.Close()
	for artRows.Next() {
		var a ArtifactEvidence
		var truncated int
		if err := artRows.Scan(&a.ID, &a.Name, &a.ContentHash, &a.MediaType, &a.SizeBytes, &truncated); err != nil {
			return err
		}
		a.Truncated = truncated == 1
		appendArtifactEvidence(rec, a)
	}
	if err := artRows.Err(); err != nil {
		return err
	}

	evalRows, err := q.st.DB().QueryContext(ctx, `
SELECT e.id, e.verdict, e.evaluated_by_node_key, COALESCE(e.evidence_ref, '')
FROM evaluation e
JOIN artifact a ON a.id = e.artifact_id
WHERE a.run_id = ? AND a.id IN (`+strings.Join(placeholders, ",")+`) ORDER BY e.id`, args...)
	if err != nil {
		return err
	}
	defer evalRows.Close()
	for evalRows.Next() {
		var e EvaluationEvidence
		if err := evalRows.Scan(&e.ID, &e.Verdict, &e.EvaluatedByNodeKey, &e.EvidenceRef); err != nil {
			return err
		}
		appendEvaluationEvidence(rec, e)
	}
	return evalRows.Err()
}

func (q *Query) loadEvidenceFromEvents(ctx context.Context, rec *Recorded, runID, nodeKey string) error {
	type eventEvidence struct {
		id       string
		typ      string
		occurred int64
		payload  string
	}
	rows, err := q.st.DB().QueryContext(ctx, `
SELECT event_id, type, occurred_at, payload FROM event
WHERE run_id = ?
ORDER BY sequence`, runID)
	if err != nil {
		return err
	}
	var events []eventEvidence
	for rows.Next() {
		var ev eventEvidence
		if err := rows.Scan(&ev.id, &ev.typ, &ev.occurred, &ev.payload); err != nil {
			rows.Close()
			return err
		}
		events = append(events, ev)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	refs := collectEvidenceReferences(rec)
	wantedArtifacts := refs.artifacts
	wantedEvaluations := refs.evaluations
	wantedApprovals := refs.approvals
	for _, ev := range events {
		if ev.typ != "evaluation_failed" || !wantedEvaluations[ev.id] {
			continue
		}
		var p struct {
			ArtifactID string `json:"artifact_id"`
		}
		if err := json.Unmarshal([]byte(ev.payload), &p); err != nil {
			return err
		}
		wantedArtifacts[p.ArtifactID] = true
	}
	artifactNodes := map[string]string{}
	for _, ev := range events {
		if ev.typ != "artifact_published" {
			continue
		}
		var p struct {
			NodeKey     string `json:"node_key"`
			Name        string `json:"name"`
			ContentHash string `json:"content_hash"`
			MediaType   string `json:"media_type"`
			SizeBytes   int64  `json:"size_bytes"`
			Truncated   bool   `json:"truncated"`
		}
		if err := json.Unmarshal([]byte(ev.payload), &p); err != nil {
			return err
		}
		artifactNodes[ev.id] = p.NodeKey
		if p.NodeKey != nodeKey && !wantedArtifacts[ev.id] {
			continue
		}
		found := false
		for _, artifact := range rec.Evidence.Artifacts {
			if artifact.ID == ev.id {
				found = true
				break
			}
		}
		if !found {
			rec.Evidence.Artifacts = append(rec.Evidence.Artifacts, ArtifactEvidence{
				ID: ev.id, Name: p.Name, ContentHash: p.ContentHash, MediaType: p.MediaType,
				SizeBytes: p.SizeBytes, Truncated: p.Truncated,
			})
		}
	}
	for _, ev := range events {
		if ev.typ != "evaluation_failed" {
			continue
		}
		var p struct {
			ArtifactID         string `json:"artifact_id"`
			EvaluatedByNodeKey string `json:"evaluated_by_node_key"`
			EvidenceRef        string `json:"evidence_ref"`
		}
		if err := json.Unmarshal([]byte(ev.payload), &p); err != nil {
			return err
		}
		if !wantedEvaluations[ev.id] && !wantedArtifacts[p.ArtifactID] && artifactNodes[p.ArtifactID] != nodeKey {
			continue
		}
		found := false
		for _, evaluation := range rec.Evidence.Evaluations {
			if evaluation.ID == ev.id {
				found = true
				break
			}
		}
		if !found {
			rec.Evidence.Evaluations = append(rec.Evidence.Evaluations, EvaluationEvidence{
				ID: ev.id, Verdict: "failed", EvaluatedByNodeKey: p.EvaluatedByNodeKey, EvidenceRef: p.EvidenceRef,
			})
		}
	}
	approvalIndexes := map[string]int{}
	for i, approval := range rec.Evidence.Approvals {
		approvalIndexes[approval.ID] = i
	}
	for _, ev := range events {
		switch ev.typ {
		case "approval_requested":
			var p struct {
				NodeKey         string          `json:"node_key"`
				RequestedAction json.RawMessage `json:"requested_action"`
				RequiredScope   string          `json:"required_scope"`
				ExpiresAt       int64           `json:"expires_at"`
			}
			if err := json.Unmarshal([]byte(ev.payload), &p); err != nil {
				return err
			}
			if p.NodeKey != nodeKey && !wantedApprovals[ev.id] {
				continue
			}
			if _, ok := approvalIndexes[ev.id]; !ok {
				action := string(p.RequestedAction)
				if action == "" {
					action = "{}"
				}
				rec.Evidence.Approvals = append(rec.Evidence.Approvals, ApprovalEvidence{
					ID: ev.id, RequestedAction: action, RequiredScope: p.RequiredScope, ExpiresAt: p.ExpiresAt,
				})
				approvalIndexes[ev.id] = len(rec.Evidence.Approvals) - 1
			}
		case "approval_granted", "approval_denied":
			var p struct {
				ApprovalID string `json:"approval_id"`
				DecidedBy  string `json:"decided_by"`
			}
			if err := json.Unmarshal([]byte(ev.payload), &p); err != nil {
				return err
			}
			if i, ok := approvalIndexes[p.ApprovalID]; ok {
				rec.Evidence.Approvals[i].Decision = map[string]string{
					"approval_granted": "grant", "approval_denied": "deny",
				}[ev.typ]
				rec.Evidence.Approvals[i].DecidedBy = p.DecidedBy
			}
		}
	}
	for _, ev := range events {
		if refs.events[ev.id] {
			appendEventEvidence(rec, EventEvidence{ID: ev.id, Type: ev.typ, OccurredAt: ev.occurred})
		}
	}
	return nil
}

func orEmpty(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

func mergeUnique(base, add []string) []string {
	seen := map[string]bool{}
	for _, v := range base {
		seen[v] = true
	}
	for _, v := range add {
		if !seen[v] {
			seen[v] = true
			base = append(base, v)
		}
	}
	return base
}

func render(rec Recorded) Inference {
	inference := Inference{Attributions: []Attribution{}}
	if len(rec.Decisions) == 0 && len(rec.CausalLinks) == 0 && (rec.NodeStatus == "pending" || rec.NodeStatus == "eligible") {
		inference.Pending = true
		inference.Summary = fmt.Sprintf("node %s has no recorded decision yet (%d candidate transitions)", rec.NodeKey, len(rec.CandidateEdges))
		return inference
	}

	groups := map[string][]RecordedLink{}
	for _, link := range rec.CausalLinks {
		if link.GroupKey != "" && link.Attribution == "contributing" {
			groups[link.GroupKey] = append(groups[link.GroupKey], link)
		}
	}
	for key := range groups {
		sort.Slice(groups[key], func(i, j int) bool { return groups[key][i].SourceID < groups[key][j].SourceID })
	}
	groupKeys := make([]string, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)

	for _, key := range groupKeys {
		members := groups[key]
		sources := ""
		for i, m := range members {
			if i > 0 {
				sources += ", "
			}
			sources += m.SourceID
		}
		inference.Attributions = append(inference.Attributions, Attribution{
			TargetNodeKey: members[0].TargetNodeKey,
			Strength:      "sufficient",
			GroupKey:      key,
			GroupMembers:  len(members),
			Source:        "group(" + sources + ")",
		})
	}
	inGroup := map[string]bool{}
	for _, key := range groupKeys {
		for _, m := range groups[key] {
			inGroup[m.DecisionID+"#"+m.SourceID] = true
		}
	}
	for _, link := range rec.CausalLinks {
		if inGroup[link.DecisionID+"#"+link.SourceID] {
			continue
		}
		inference.Attributions = append(inference.Attributions, Attribution{
			TargetNodeKey: link.TargetNodeKey,
			Strength:      link.Attribution,
			GroupKey:      link.GroupKey,
			Source:        link.SourceKind + ":" + link.SourceID,
		})
	}

	inference.Summary = fmt.Sprintf("node %s became eligible through %d recorded causal sources", rec.NodeKey, len(rec.CausalLinks))
	for i := len(rec.Decisions) - 1; i >= 0; i-- {
		d := rec.Decisions[i]
		if d.SelectedEdgeID != "" {
			inference.Summary = fmt.Sprintf("node %s ran via selected edge %s", rec.NodeKey, d.SelectedEdgeID)
			break
		}
		if d.Rejection != "" {
			inference.Summary = fmt.Sprintf("node %s did not run: %s", rec.NodeKey, d.Rejection)
			break
		}
	}
	for i := len(rec.Decisions) - 1; i >= 0; i-- {
		if rec.Decisions[i].SelectedEdgeID != "" {
			inference.SelectedEdge = rec.Decisions[i].SelectedEdgeID
			break
		}
	}
	return inference
}
