package store

import (
	"context"
	"database/sql"
)

type RunGraphNode struct {
	NodeKey      string `json:"node_key"`
	Status       string `json:"status"`
	AttemptCount int64  `json:"attempt_count"`
}

type RunGraphEdge struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Type      string `json:"type"`
	Traversed bool   `json:"traversed"`
}

type RunGraph struct {
	RunID            string         `json:"run_id"`
	Status           string         `json:"status"`
	GraphVersionID   string         `json:"graph_version_id"`
	DefinitionDigest string         `json:"definition_digest"`
	Nodes            []RunGraphNode `json:"nodes"`
	Edges            []RunGraphEdge `json:"edges"`
}

func (s *Store) RuntimeGraph(ctx context.Context, runID string) (*RunGraph, error) {
	var g RunGraph
	err := s.db.QueryRowContext(ctx,
		"SELECT id, graph_version_id, definition_digest, status FROM graph_run WHERE id = ?",
		runID).Scan(&g.RunID, &g.GraphVersionID, &g.DefinitionDigest, &g.Status)
	if err == sql.ErrNoRows {
		return nil, NewCodeError("RUN_NOT_FOUND", "run %s does not exist", runID)
	}
	if err != nil {
		return nil, err
	}

	nodeRows, err := s.db.QueryContext(ctx, `
SELECT COALESCE(rn.node_key, gn.node_key), COALESCE(rn.status, 'pending'), COALESCE(rn.attempt_count, 0)
FROM graph_node gn
LEFT JOIN run_node rn ON rn.node_key = gn.node_key AND rn.run_id = ?
WHERE gn.graph_version_id = ?
ORDER BY gn.node_key`, runID, g.GraphVersionID)
	if err != nil {
		return nil, err
	}
	defer nodeRows.Close()
	for nodeRows.Next() {
		var n RunGraphNode
		if err := nodeRows.Scan(&n.NodeKey, &n.Status, &n.AttemptCount); err != nil {
			return nil, err
		}
		g.Nodes = append(g.Nodes, n)
	}
	if err := nodeRows.Err(); err != nil {
		return nil, err
	}

	edgeRows, err := s.db.QueryContext(ctx, `
SELECT ge.from_node_key, ge.to_node_key, ge.type,
       EXISTS(SELECT 1 FROM run_edge re WHERE re.run_id = ? AND re.edge_id = ge.id)
FROM graph_edge ge
WHERE ge.graph_version_id = ?
ORDER BY ge.from_node_key, ge.to_node_key`, runID, g.GraphVersionID)
	if err != nil {
		return nil, err
	}
	defer edgeRows.Close()
	for edgeRows.Next() {
		var e RunGraphEdge
		if err := edgeRows.Scan(&e.From, &e.To, &e.Type, &e.Traversed); err != nil {
			return nil, err
		}
		g.Edges = append(g.Edges, e)
	}
	if err := edgeRows.Err(); err != nil {
		return nil, err
	}
	return &g, nil
}
