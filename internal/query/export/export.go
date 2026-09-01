package export

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"proceed/internal/store"
)

type Format string

const (
	FormatMermaid Format = "mermaid"
	FormatJSON    Format = "json"
)

func ValidateFormat(format string) error {
	switch Format(strings.ToLower(format)) {
	case FormatMermaid, FormatJSON:
		return nil
	default:
		return fmt.Errorf("unknown format %q: must be mermaid or json", format)
	}
}

func Export(ctx context.Context, s *store.Store, runID, format string) ([]byte, error) {
	if err := ValidateFormat(format); err != nil {
		return nil, store.NewCodeError("GRAPH_INVALID", "%v", err)
	}
	data, err := fetchExportData(ctx, s, runID)
	if err != nil {
		return nil, err
	}
	switch Format(strings.ToLower(format)) {
	case FormatMermaid:
		return []byte(renderMermaid(data)), nil
	case FormatJSON:
		return renderJSON(data)
	default:
		return nil, store.NewCodeError("GRAPH_INVALID", "unknown format %q", format)
	}
}

type exportData struct {
	RunID            string
	Status           string
	GraphVersionID   string
	DefinitionDigest string
	Nodes            []nodeWithType
	Edges            []edgeWithRoute
	Artifacts        []artifactRef
}

type nodeWithType struct {
	NodeKey      string
	Status       string
	AttemptCount int64
	Type         string
}

type edgeWithRoute struct {
	From      string
	To        string
	Type      string
	Condition string
	Route     string
	Traversed bool
}

type artifactRef struct {
	NodeKey     string `json:"produced_by_node_key"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	ContentHash string `json:"content_hash"`
	MediaType   string `json:"media_type"`
	SizeBytes   int64  `json:"size_bytes"`
	Truncated   bool   `json:"truncated"`
}

func fetchExportData(ctx context.Context, s *store.Store, runID string) (*exportData, error) {
	tx, err := s.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		tx, err = s.DB().BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
	}
	defer func() { _ = tx.Rollback() }()

	var g exportData
	err = tx.QueryRowContext(ctx,
		"SELECT id, graph_version_id, definition_digest, status FROM graph_run WHERE id = ?",
		runID).Scan(&g.RunID, &g.GraphVersionID, &g.DefinitionDigest, &g.Status)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, store.NewCodeError("RUN_NOT_FOUND", "run %s does not exist", runID)
		}
		return nil, err
	}

	nodeRows, err := tx.QueryContext(ctx, `
SELECT COALESCE(rn.node_key, gn.node_key), COALESCE(rn.status, 'pending'), COALESCE(rn.attempt_count, 0), gn.type
FROM graph_node gn
LEFT JOIN run_node rn ON rn.node_key = gn.node_key AND rn.run_id = ?
WHERE gn.graph_version_id = ?
ORDER BY gn.node_key`, runID, g.GraphVersionID)
	if err != nil {
		return nil, err
	}
	defer nodeRows.Close()
	for nodeRows.Next() {
		var n nodeWithType
		if err := nodeRows.Scan(&n.NodeKey, &n.Status, &n.AttemptCount, &n.Type); err != nil {
			return nil, err
		}
		g.Nodes = append(g.Nodes, n)
	}
	if err := nodeRows.Err(); err != nil {
		return nil, err
	}

	edgeRows, err := tx.QueryContext(ctx, `
SELECT ge.from_node_key, ge.to_node_key, ge.type, COALESCE(ge.condition, ''), COALESCE(re.route, ''), EXISTS(SELECT 1 FROM run_edge re2 WHERE re2.run_id = ? AND re2.edge_id = ge.id)
FROM graph_edge ge
LEFT JOIN run_edge re ON re.edge_id = ge.id AND re.run_id = ?
WHERE ge.graph_version_id = ?
ORDER BY ge.from_node_key, ge.to_node_key`, runID, runID, g.GraphVersionID)
	if err != nil {
		return nil, err
	}
	defer edgeRows.Close()
	for edgeRows.Next() {
		var e edgeWithRoute
		var traversed int
		if err := edgeRows.Scan(&e.From, &e.To, &e.Type, &e.Condition, &e.Route, &traversed); err != nil {
			return nil, err
		}
		e.Traversed = traversed != 0
		g.Edges = append(g.Edges, e)
	}
	if err := edgeRows.Err(); err != nil {
		return nil, err
	}

	artRows, err := tx.QueryContext(ctx, `
SELECT produced_by_node_key, name, path, content_hash, media_type, size_bytes, truncated
FROM artifact WHERE run_id = ? ORDER BY produced_by_node_key, name`, runID)
	if err != nil {
		return nil, err
	}
	defer artRows.Close()
	for artRows.Next() {
		var a artifactRef
		var truncated int
		if err := artRows.Scan(&a.NodeKey, &a.Name, &a.Path, &a.ContentHash, &a.MediaType, &a.SizeBytes, &truncated); err != nil {
			return nil, err
		}
		a.Truncated = truncated != 0
		g.Artifacts = append(g.Artifacts, a)
	}
	if err := artRows.Err(); err != nil {
		return nil, err
	}

	sort.Slice(g.Nodes, func(i, j int) bool { return g.Nodes[i].NodeKey < g.Nodes[j].NodeKey })
	sort.Slice(g.Edges, func(i, j int) bool {
		if g.Edges[i].From == g.Edges[j].From {
			return g.Edges[i].To < g.Edges[j].To
		}
		return g.Edges[i].From < g.Edges[j].From
	})
	sort.Slice(g.Artifacts, func(i, j int) bool {
		if g.Artifacts[i].NodeKey == g.Artifacts[j].NodeKey {
			return g.Artifacts[i].Name < g.Artifacts[j].Name
		}
		return g.Artifacts[i].NodeKey < g.Artifacts[j].NodeKey
	})

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &g, nil
}

func renderMermaid(data *exportData) string {
	var b strings.Builder
	b.WriteString("flowchart TD\n")
	for _, n := range data.Nodes {
		safeKey := sanitizeMermaidID(n.NodeKey)
		escapedKey := escapeMermaidLabel(n.NodeKey)
		escapedStatus := escapeMermaidLabel(n.Status)
		label := fmt.Sprintf("%s<br/>%s", escapedKey, escapedStatus)
		if n.AttemptCount > 0 {
			label = fmt.Sprintf("%s<br/>%s (attempt %d)", escapedKey, escapedStatus, n.AttemptCount)
		}
		b.WriteString(fmt.Sprintf("  %s[\"%s\"]\n", safeKey, label))
	}
	for _, e := range data.Edges {
		from := sanitizeMermaidID(e.From)
		to := sanitizeMermaidID(e.To)
		routeLabel := e.Condition
		if e.Route != "" {
			routeLabel = e.Route
		}
		routeLabel = escapeMermaidLabel(routeLabel)
		if routeLabel != "" && e.Traversed {
			b.WriteString(fmt.Sprintf("  %s -->|%s| %s\n", from, routeLabel, to))
		} else if routeLabel != "" {
			b.WriteString(fmt.Sprintf("  %s -.->|%s| %s\n", from, routeLabel, to))
		} else if e.Traversed {
			b.WriteString(fmt.Sprintf("  %s --> %s\n", from, to))
		} else {
			b.WriteString(fmt.Sprintf("  %s -.-> %s\n", from, to))
		}
	}
	return b.String()
}

func escapeMermaidLabel(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString("#quot;")
		case '|':
			b.WriteString("_")
		case '[':
			b.WriteString("(")
		case ']':
			b.WriteString(")")
		case '{':
			b.WriteString("(")
		case '}':
			b.WriteString(")")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '#':
			b.WriteString("#35;")
		case '&':
			b.WriteString("&amp;")
		case '\n', '\r':
			b.WriteRune(' ')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func sanitizeMermaidID(id string) string {
	var b strings.Builder
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	s := b.String()
	if s == "" {
		return "_"
	}
	if s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}
	return s
}

type jsonExport struct {
	RunID            string        `json:"run_id"`
	Status           string        `json:"status"`
	GraphVersionID   string        `json:"graph_version_id"`
	DefinitionDigest string        `json:"definition_digest"`
	Nodes            []jsonNode    `json:"nodes"`
	Edges            []jsonEdge    `json:"edges"`
	Artifacts        []artifactRef `json:"artifacts"`
}

type jsonNode struct {
	NodeKey      string `json:"node_key"`
	Status       string `json:"status"`
	AttemptCount int64  `json:"attempt_count"`
	Type         string `json:"type"`
}

type jsonEdge struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Type      string `json:"type"`
	Condition string `json:"condition,omitempty"`
	Route     string `json:"route,omitempty"`
	Traversed bool   `json:"traversed"`
}

func renderJSON(data *exportData) ([]byte, error) {
	out := jsonExport{
		RunID:            data.RunID,
		Status:           data.Status,
		GraphVersionID:   data.GraphVersionID,
		DefinitionDigest: data.DefinitionDigest,
		Artifacts:        data.Artifacts,
	}
	if out.Artifacts == nil {
		out.Artifacts = []artifactRef{}
	}
	for _, n := range data.Nodes {
		out.Nodes = append(out.Nodes, jsonNode{
			NodeKey:      n.NodeKey,
			Status:       n.Status,
			AttemptCount: n.AttemptCount,
			Type:         n.Type,
		})
	}
	for _, e := range data.Edges {
		out.Edges = append(out.Edges, jsonEdge{
			From:      e.From,
			To:        e.To,
			Type:      e.Type,
			Condition: e.Condition,
			Route:     e.Route,
			Traversed: e.Traversed,
		})
	}
	if out.Nodes == nil {
		out.Nodes = []jsonNode{}
	}
	if out.Edges == nil {
		out.Edges = []jsonEdge{}
	}
	return json.Marshal(out)
}
