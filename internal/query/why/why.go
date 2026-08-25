package why

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

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
	rec, err := q.load(ctx, runID, nodeKey)
	if err != nil {
		return nil, err
	}
	return &Explanation{Recorded: *rec, Inference: render(*rec)}, nil
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
WHERE graph_version_id = ? AND to_node_key = ? AND type IN ('depends_on', 'routes_to')
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

func (q *Query) loadDecisions(ctx context.Context, rec *Recorded, nodeID string) error {
	rows, err := q.st.DB().QueryContext(ctx, `
SELECT d.id, d.kind, d.candidate_edges, COALESCE(d.selected_edge_id, ''),
       COALESCE(d.rejection, ''), d.predicate_snapshot, d.input_references, d.policy_version, d.decided_at
FROM decision d
WHERE d.run_node_id = ?
ORDER BY d.decided_at, d.id`, nodeID)
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

func (q *Query) loadEvidence(ctx context.Context, rec *Recorded, runID, nodeKey, nodeID string) error {
	artRows, err := q.st.DB().QueryContext(ctx, `
SELECT id, name, content_hash, media_type, size_bytes, truncated
FROM artifact WHERE run_id = ? AND produced_by_node_key = ? ORDER BY id`, runID, nodeKey)
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
		rec.Evidence.Artifacts = append(rec.Evidence.Artifacts, a)
	}
	if err := artRows.Err(); err != nil {
		return err
	}

	evalRows, err := q.st.DB().QueryContext(ctx, `
SELECT e.id, e.verdict, e.evaluated_by_node_key, COALESCE(e.evidence_ref, '')
FROM evaluation e
JOIN artifact a ON a.id = e.artifact_id
WHERE e.run_id = ? AND a.produced_by_node_key = ? ORDER BY e.id`, runID, nodeKey)
	if err != nil {
		return err
	}
	defer evalRows.Close()
	for evalRows.Next() {
		var e EvaluationEvidence
		if err := evalRows.Scan(&e.ID, &e.Verdict, &e.EvaluatedByNodeKey, &e.EvidenceRef); err != nil {
			return err
		}
		rec.Evidence.Evaluations = append(rec.Evidence.Evaluations, e)
	}
	if err := evalRows.Err(); err != nil {
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
		rec.Evidence.Approvals = append(rec.Evidence.Approvals, a)
	}
	return approvalRows.Err()
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
	if len(rec.Decisions) == 0 && len(rec.CausalLinks) == 0 {
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
