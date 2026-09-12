package store

import (
	"context"
)

const dashboardSectionLimit = 10

type DashboardApproval struct {
	ID      string
	RunID   string
	NodeKey string
	Scope   string
}

type DashboardWait struct {
	ID             string
	RunID          string
	NodeKey        string
	EventType      string
	CorrelationKey string
}

type DashboardFailedNode struct {
	RunID   string
	NodeKey string
}

type DashboardSnapshot struct {
	Runs             []RunSummary
	TotalRuns        int
	Approvals        []DashboardApproval
	TotalApprovals   int
	Waits            []DashboardWait
	TotalWaits       int
	FailedRuns       []RunSummary
	TotalFailedRuns  int
	FailedNodes      []DashboardFailedNode
	TotalFailedNodes int
}

func (s *Store) DashboardSnapshot(ctx context.Context) (*DashboardSnapshot, error) {
	snap := &DashboardSnapshot{}
	var err error
	if snap.Runs, err = s.ListRuns(ctx, "", dashboardSectionLimit); err != nil {
		return nil, err
	}
	if snap.TotalRuns, err = s.countDashboardRows(ctx, `SELECT COUNT(*) FROM graph_run`); err != nil {
		return nil, err
	}
	if snap.Approvals, err = s.listDashboardApprovals(ctx); err != nil {
		return nil, err
	}
	if snap.TotalApprovals, err = s.countDashboardRows(ctx, `SELECT COUNT(*) FROM approval WHERE decision IS NULL`); err != nil {
		return nil, err
	}
	if snap.Waits, err = s.listDashboardWaits(ctx); err != nil {
		return nil, err
	}
	if snap.TotalWaits, err = s.countDashboardRows(ctx, `SELECT COUNT(*) FROM external_wait WHERE status = 'pending'`); err != nil {
		return nil, err
	}
	if snap.FailedRuns, err = s.ListRuns(ctx, "failed", dashboardSectionLimit); err != nil {
		return nil, err
	}
	if snap.TotalFailedRuns, err = s.countDashboardRows(ctx, `SELECT COUNT(*) FROM graph_run WHERE status = 'failed'`); err != nil {
		return nil, err
	}
	if snap.FailedNodes, err = s.listDashboardFailedNodes(ctx); err != nil {
		return nil, err
	}
	if snap.TotalFailedNodes, err = s.countDashboardRows(ctx, `SELECT COUNT(*) FROM run_node WHERE status = 'failed'`); err != nil {
		return nil, err
	}
	return snap, nil
}

func (s *Store) countDashboardRows(ctx context.Context, query string) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *Store) listDashboardApprovals(ctx context.Context) ([]DashboardApproval, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT a.id, a.run_id, rn.node_key, a.required_scope
FROM approval a
JOIN run_node rn ON rn.id = a.run_node_id
WHERE a.decision IS NULL
ORDER BY a.created_at, a.id
LIMIT ?`, dashboardSectionLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []DashboardApproval
	for rows.Next() {
		var a DashboardApproval
		if err := rows.Scan(&a.ID, &a.RunID, &a.NodeKey, &a.Scope); err != nil {
			return nil, err
		}
		list = append(list, a)
	}
	return list, rows.Err()
}

func (s *Store) listDashboardWaits(ctx context.Context) ([]DashboardWait, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT w.id, w.run_id, rn.node_key, w.event_type, w.correlation_key
FROM external_wait w
JOIN run_node rn ON rn.id = w.run_node_id
WHERE w.status = 'pending'
ORDER BY w.created_at, w.id
LIMIT ?`, dashboardSectionLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []DashboardWait
	for rows.Next() {
		var w DashboardWait
		if err := rows.Scan(&w.ID, &w.RunID, &w.NodeKey, &w.EventType, &w.CorrelationKey); err != nil {
			return nil, err
		}
		list = append(list, w)
	}
	return list, rows.Err()
}

func (s *Store) listDashboardFailedNodes(ctx context.Context) ([]DashboardFailedNode, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT rn.run_id, rn.node_key
FROM run_node rn
JOIN graph_run gr ON gr.id = rn.run_id
WHERE rn.status = 'failed'
ORDER BY gr.created_at DESC, gr.id DESC, rn.node_key
LIMIT ?`, dashboardSectionLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []DashboardFailedNode
	for rows.Next() {
		var n DashboardFailedNode
		if err := rows.Scan(&n.RunID, &n.NodeKey); err != nil {
			return nil, err
		}
		list = append(list, n)
	}
	return list, rows.Err()
}
