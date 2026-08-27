package controller

import (
	"context"
	"database/sql"

	"github.com/oklog/ulid/v2"

	"proceed/internal/store"
)

type causalLink struct {
	TargetNodeKey string `json:"target_node_key"`
	Attribution   string `json:"attribution"`
	SourceKind    string `json:"source_kind"`
	SourceID      string `json:"source_id"`
	CitationType  string `json:"citation_type,omitempty"`
	CitationID    string `json:"citation_id,omitempty"`
	GroupKey      string `json:"group_key,omitempty"`
}

type decisionPayload struct {
	NodeKey           string       `json:"node_key"`
	Kind              string       `json:"kind"`
	CandidateEdges    []string     `json:"candidate_edges"`
	SelectedEdgeID    string       `json:"selected_edge_id"`
	Rejection         string       `json:"rejection"`
	PredicateSnapshot any          `json:"predicate_snapshot"`
	InputReferences   []string     `json:"input_references"`
	PolicyVersion     string       `json:"policy_version"`
	CausalLinks       []causalLink `json:"causal_links"`
}

func (c *Controller) appendDecisionEvent(ctx context.Context, tx *sql.Tx, runID string, nowMs int64, eventID string, payload decisionPayload) error {
	if payload.CandidateEdges == nil {
		payload.CandidateEdges = []string{}
	}
	if payload.InputReferences == nil {
		payload.InputReferences = []string{}
	}
	if payload.PredicateSnapshot == nil {
		payload.PredicateSnapshot = map[string]any{}
	}
	_, err := c.appendWithin(ctx, tx, &store.Event{
		EventID:       eventID,
		RunID:         runID,
		SchemaVersion: "proceed/v1",
		Type:          "decision_recorded",
		OccurredAt:    nowMs,
		ActorType:     "controller",
		ActorID:       c.cfg.OwnerID,
		Payload:       payloadJSON(payload),
	})
	return err
}

type edgeInfo struct {
	ID   string
	To   string
	Type string
	Cond sql.NullString
}

// recordRoutingDecision persists the route selection made by a finishing
// node: candidate routes_to edges, one decision row per selected edge (the
// schema binds a single edge id per row), the predicate snapshot, and
// blocked_by links for targets the rejection fail-closed. Links live in
// the event payload so a projection rebuild reproduces them, and every
// link source id is the decision event id that produced it.
func (c *Controller) recordRoutingDecision(ctx context.Context, tx *sql.Tx, runID, graphVersionID, nodeKey, digest, route string, routeKnown bool, edges []edgeInfo, nowMs int64) error {
	var candidates []edgeInfo
	var selected []edgeInfo
	for _, e := range edges {
		if e.Type != "routes_to" {
			continue
		}
		candidates = append(candidates, e)
		if routeKnown && (!e.Cond.Valid || e.Cond.String == "" || route == e.Cond.String) {
			selected = append(selected, e)
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	inputRefs, err := c.nodeArtifactRefs(ctx, tx, runID, nodeKey)
	if err != nil {
		return err
	}
	candidateIDs := edgeIDs(candidates)

	if len(selected) == 0 {
		rejection := "no candidate condition matched the recorded route"
		eventID := ulid.Make().String()
		var links []causalLink
		blockedTargets := map[string]bool{}
		for _, e := range candidates {
			if !blockedTargets[e.To] && c.onlyIncomingRoutesFromNode(ctx, tx, graphVersionID, e.To, nodeKey) {
				blockedTargets[e.To] = true
				links = append(links, causalLink{
					TargetNodeKey: e.To,
					Attribution:   "blocked_by",
					SourceKind:    "decision",
					SourceID:      eventID,
				})
			}
		}
		return c.appendDecisionEvent(ctx, tx, runID, nowMs, eventID, decisionPayload{
			NodeKey:           nodeKey,
			Kind:              "routing",
			CandidateEdges:    candidateIDs,
			Rejection:         rejection,
			PredicateSnapshot: map[string]any{"route": route, "match": "router produced no route"},
			InputReferences:   inputRefs,
			PolicyVersion:     digest,
			CausalLinks:       links,
		})
	}

	selectedIDs := map[string]bool{}
	selectedTargets := map[string]bool{}
	for _, e := range selected {
		selectedIDs[e.ID] = true
		selectedTargets[e.To] = true
	}
	var blockedLinks []causalLink
	blockedTargets := map[string]bool{}
	for _, e := range candidates {
		if !selectedIDs[e.ID] && !selectedTargets[e.To] && !blockedTargets[e.To] && c.onlyIncomingRoutesFromNode(ctx, tx, graphVersionID, e.To, nodeKey) {
			blockedTargets[e.To] = true
			blockedLinks = append(blockedLinks, causalLink{
				TargetNodeKey: e.To,
				Attribution:   "blocked_by",
				SourceKind:    "decision",
			})
		}
	}

	for i, e := range selected {
		eventID := ulid.Make().String()
		conditional := e.Cond.Valid && e.Cond.String != ""
		attribution := "contributing"
		basis := "unconditional edge; selection did not depend on the route value"
		if conditional {
			attribution = "necessary"
			basis = "recorded route value matched the edge condition; a different recorded route value would not have selected this edge"
		}
		links := []causalLink{{
			TargetNodeKey: e.To,
			Attribution:   attribution,
			SourceKind:    "decision",
			SourceID:      eventID,
		}}
		if i == 0 {
			for j := range blockedLinks {
				blockedLinks[j].SourceID = eventID
			}
			links = append(links, blockedLinks...)
		}
		if err := c.appendDecisionEvent(ctx, tx, runID, nowMs, eventID, decisionPayload{
			NodeKey:        nodeKey,
			Kind:           "routing",
			CandidateEdges: candidateIDs,
			SelectedEdgeID: e.ID,
			PredicateSnapshot: map[string]any{
				"route":                route,
				"match":                "route value equals edge condition",
				"counterfactual_basis": basis,
			},
			InputReferences: inputRefs,
			PolicyVersion:   digest,
			CausalLinks:     links,
		}); err != nil {
			return err
		}
	}
	return nil
}

func edgeIDs(edges []edgeInfo) []string {
	ids := make([]string, 0, len(edges))
	for _, e := range edges {
		ids = append(ids, e.ID)
	}
	return ids
}

type dependency struct {
	edgeID string
	from   string
}

// recordEligibilityDecisions persists why each newly-eligible downstream
// node became eligible: the satisfied release-edge sources as a conjunctive
// group, with per-source attribution bounded by recorded evidence.
func (c *Controller) recordEligibilityDecisions(ctx context.Context, tx *sql.Tx, runID, graphVersionID, digest string, edges []edgeInfo, nowMs int64) error {
	seen := map[string]bool{}
	for _, e := range edges {
		if (e.Type != "depends_on" && e.Type != "produces" && e.Type != "consumes") || seen[e.To] {
			continue
		}
		seen[e.To] = true
		if err := c.recordOneEligibility(ctx, tx, runID, graphVersionID, digest, e.To, nowMs); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) recordOneEligibility(ctx context.Context, tx *sql.Tx, runID, graphVersionID, digest, nodeKey string, nowMs int64) error {
	depRows, err := tx.QueryContext(ctx, `
SELECT ge.id, ge.from_node_key FROM graph_edge ge
WHERE ge.graph_version_id = ? AND ge.to_node_key = ? AND ge.type IN ('depends_on', 'produces', 'consumes')
ORDER BY ge.id`, graphVersionID, nodeKey)
	if err != nil {
		return err
	}
	var deps []dependency
	for depRows.Next() {
		var d dependency
		if err := depRows.Scan(&d.edgeID, &d.from); err != nil {
			depRows.Close()
			return err
		}
		deps = append(deps, d)
	}
	depRows.Close()
	if err := depRows.Err(); err != nil {
		return err
	}
	if len(deps) == 0 {
		return nil
	}

	statuses := map[string]string{}
	for _, d := range deps {
		var srcStatus string
		if err := tx.QueryRowContext(ctx,
			"SELECT status FROM run_node WHERE run_id = ? AND node_key = ?", runID, d.from).Scan(&srcStatus); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}
		statuses[d.from] = srcStatus
		if srcStatus != "succeeded" && srcStatus != "skipped" {
			// The conjunctive group is not satisfied yet; record only once
			// the node actually became eligible, with full context.
			return nil
		}
	}

	var progressed int
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM decision WHERE run_node_id = (SELECT id FROM run_node WHERE run_id = ? AND node_key = ?)",
		runID, nodeKey).Scan(&progressed); err != nil && err != sql.ErrNoRows {
		return err
	}
	if progressed > 0 {
		return nil
	}

	var inputRefs []string
	var links []causalLink
	groupKey := ""
	if len(deps) > 1 {
		groupKey = "conj:" + nodeKey
	}
	sole := len(deps) == 1
	for _, d := range deps {
		attribution := "contributing"
		switch {
		case statuses[d.from] != "succeeded":
			attribution = "unknown"
		case !c.nodeProducedEvidence(ctx, tx, runID, d.from):
			attribution = "unknown"
		case sole:
			attribution = "necessary"
		}
		finishEventID, err := c.latestNodeFinishedEvent(ctx, tx, runID, d.from)
		if err != nil {
			return err
		}
		links = append(links, causalLink{
			TargetNodeKey: nodeKey,
			Attribution:   attribution,
			SourceKind:    "event",
			SourceID:      finishEventID,
			GroupKey:      groupKey,
		})
		artifactRefs, err := c.nodeArtifactRefs(ctx, tx, runID, d.from)
		if err != nil {
			return err
		}
		inputRefs = append(inputRefs, artifactRefs...)
	}

	snapshot := map[string]any{
		"all_dependencies_succeeded": true,
		"required":                   len(deps),
	}
	if sole {
		snapshot["counterfactual_basis"] = "sole required dependency; without it the node would not be eligible"
	} else {
		snapshot["counterfactual_basis"] = "conjunctive group; no single member is individually necessary"
	}
	return c.appendDecisionEvent(ctx, tx, runID, nowMs, ulid.Make().String(), decisionPayload{
		NodeKey:           nodeKey,
		Kind:              "routing",
		CandidateEdges:    dependencyEdgeIDs(deps),
		PredicateSnapshot: snapshot,
		InputReferences:   inputRefs,
		PolicyVersion:     digest,
		CausalLinks:       links,
	})
}

// latestNodeFinishedEvent resolves the durable finish event backing a
// dependency source, so a causal link always cites a resolvable event id —
// including after retries produced several finish events.
func (c *Controller) latestNodeFinishedEvent(ctx context.Context, tx *sql.Tx, runID, nodeKey string) (string, error) {
	var eventID string
	err := tx.QueryRowContext(ctx, `
	SELECT event_id FROM event
	WHERE run_id = ? AND type IN ('node_finished', 'node_skipped') AND json_extract(payload, '$.node_key') = ?
	ORDER BY sequence DESC LIMIT 1`, runID, nodeKey).Scan(&eventID)
	if err == sql.ErrNoRows {
		return "node:" + nodeKey, nil
	}
	return eventID, err
}

func dependencyEdgeIDs(deps []dependency) []string {
	ids := make([]string, 0, len(deps))
	for _, d := range deps {
		ids = append(ids, d.edgeID)
	}
	return ids
}

func (c *Controller) nodeProducedEvidence(ctx context.Context, tx *sql.Tx, runID, nodeKey string) bool {
	var artifacts int
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM artifact WHERE run_id = ? AND produced_by_node_key = ?", runID, nodeKey).Scan(&artifacts); err != nil {
		return false
	}
	if artifacts > 0 {
		return true
	}
	var evaluations int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM evaluation e
JOIN artifact a ON a.id = e.artifact_id
WHERE e.run_id = ? AND a.produced_by_node_key = ?`, runID, nodeKey).Scan(&evaluations); err != nil {
		return false
	}
	return evaluations > 0
}

func (c *Controller) nodeArtifactRefs(ctx context.Context, tx *sql.Tx, runID, nodeKey string) ([]string, error) {
	rows, err := tx.QueryContext(ctx,
		"SELECT id FROM artifact WHERE run_id = ? AND produced_by_node_key = ? ORDER BY id", runID, nodeKey)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, "artifact:"+id)
	}
	rows.Close()
	return ids, rows.Err()
}
