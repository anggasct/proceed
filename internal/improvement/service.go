package improvement

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/store"
)

type Service struct {
	st *store.Store
}

func New(st *store.Store) *Service {
	return &Service{st: st}
}

func (s *Service) GetOutcome(ctx context.Context, runID string) (*Outcome, error) {
	row := s.st.DB().QueryRowContext(ctx, `
SELECT id, run_id, anchor_id, result, COALESCE(detail, ''), recorded_at
FROM outcome WHERE run_id = ?`, runID)
	var o Outcome
	if err := row.Scan(&o.ID, &o.RunID, &o.AnchorID, &o.Result, &o.Detail, &o.RecordedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &o, nil
}

func (s *Service) GetAnchor(ctx context.Context, anchorID string) (*Anchor, error) {
	row := s.st.DB().QueryRowContext(ctx, `
SELECT a.id, a.graph_version_id, gv.definition_digest, a.created_at
FROM anchor a
JOIN graph_version gv ON gv.id = a.graph_version_id
WHERE a.id = ?`, anchorID)
	var a Anchor
	if err := row.Scan(&a.ID, &a.GraphVersionID, &a.DefinitionDigest, &a.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &a, nil
}

func (s *Service) ListOutcomesForVersion(ctx context.Context, graphVersionID string) ([]Outcome, error) {
	rows, err := s.st.DB().QueryContext(ctx, `
SELECT o.id, o.run_id, o.anchor_id, o.result, COALESCE(o.detail, ''), o.recorded_at
FROM outcome o
JOIN anchor a ON a.id = o.anchor_id
WHERE a.graph_version_id = ?
ORDER BY o.recorded_at ASC`, graphVersionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []Outcome
	for rows.Next() {
		var o Outcome
		if err := rows.Scan(&o.ID, &o.RunID, &o.AnchorID, &o.Result, &o.Detail, &o.RecordedAt); err != nil {
			return nil, err
		}
		list = append(list, o)
	}
	return list, rows.Err()
}

func (s *Service) RecordMetric(ctx context.Context, m Metric) (*Metric, error) {
	if strings.TrimSpace(m.AnchorID) == "" {
		return nil, errors.New("anchor_id is required: anchorless metrics are rejected")
	}
	if strings.TrimSpace(m.Name) == "" {
		return nil, errors.New("metric name is required")
	}
	if strings.TrimSpace(m.Unit) == "" {
		return nil, errors.New("metric unit is required")
	}
	if m.ID == "" {
		m.ID = ulid.Make().String()
	}
	if m.RecordedAt == 0 {
		m.RecordedAt = time.Now().UnixMilli()
	}
	var dimStr any = nil
	if len(m.Dimensions) > 0 && string(m.Dimensions) != "null" {
		if !json.Valid(m.Dimensions) {
			return nil, errors.New("metric dimensions must be well-formed JSON")
		}
		dimStr = string(m.Dimensions)
	}

	err := s.st.WithTx(ctx, func(tx *sql.Tx) error {
		var exists int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM anchor WHERE id = ?", m.AnchorID).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return errors.New("anchor does not exist: anchorless metrics are rejected")
		}
		_, err := tx.ExecContext(ctx, `
INSERT INTO metric (id, anchor_id, name, value, unit, dimensions, recorded_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`, m.ID, m.AnchorID, m.Name, m.Value, m.Unit, dimStr, m.RecordedAt)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *Service) ListMetricsForAnchor(ctx context.Context, anchorID string) ([]Metric, error) {
	rows, err := s.st.DB().QueryContext(ctx, `
SELECT id, anchor_id, name, value, unit, COALESCE(dimensions, ''), recorded_at
FROM metric
WHERE anchor_id = ?
ORDER BY recorded_at ASC, name ASC`, anchorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []Metric
	for rows.Next() {
		var m Metric
		var dims string
		if err := rows.Scan(&m.ID, &m.AnchorID, &m.Name, &m.Value, &m.Unit, &dims, &m.RecordedAt); err != nil {
			return nil, err
		}
		if dims != "" {
			m.Dimensions = json.RawMessage(dims)
		}
		list = append(list, m)
	}
	return list, rows.Err()
}

func (s *Service) DeriveMetrics(ctx context.Context, anchorID string) ([]Metric, error) {
	anchor, err := s.GetAnchor(ctx, anchorID)
	if err != nil {
		return nil, err
	}
	if anchor == nil {
		return nil, fmt.Errorf("anchor %s not found", anchorID)
	}

	var runID, result string
	var startedAt, finishedAt sql.NullInt64
	err = s.st.DB().QueryRowContext(ctx, `
SELECT o.run_id, o.result, r.started_at, r.finished_at
FROM outcome o
JOIN graph_run r ON r.id = o.run_id
WHERE o.anchor_id = ?`, anchorID).Scan(&runID, &result, &startedAt, &finishedAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	var derived []Metric
	now := time.Now().UnixMilli()

	// 1. Pass rate for the version
	var totalRuns, completedRuns int
	err = s.st.DB().QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(SUM(CASE WHEN o.result = 'completed' THEN 1 ELSE 0 END), 0)
FROM outcome o
JOIN anchor a ON a.id = o.anchor_id
WHERE a.graph_version_id = ?`, anchor.GraphVersionID).Scan(&totalRuns, &completedRuns)
	if err == nil && totalRuns > 0 {
		passRate := float64(completedRuns) / float64(totalRuns)
		m, err := s.RecordMetric(ctx, Metric{
			AnchorID:   anchorID,
			Name:       "pass_rate",
			Value:      passRate,
			Unit:       "ratio",
			RecordedAt: now,
		})
		if err != nil {
			return nil, err
		}
		derived = append(derived, *m)
	}

	// 2. Latency from run timings
	if startedAt.Valid && finishedAt.Valid && finishedAt.Int64 >= startedAt.Int64 {
		duration := float64(finishedAt.Int64 - startedAt.Int64)
		m, err := s.RecordMetric(ctx, Metric{
			AnchorID:   anchorID,
			Name:       "latency_ms",
			Value:      duration,
			Unit:       "ms",
			RecordedAt: now,
		})
		if err != nil {
			return nil, err
		}
		derived = append(derived, *m)
	}

	// 3. Attempt latency from attempt timings if available
	if runID != "" {
		var avgAttemptLatency sql.NullFloat64
		err = s.st.DB().QueryRowContext(ctx, `
SELECT AVG(na.finished_at - na.started_at)
FROM node_attempt na
JOIN run_node rn ON rn.id = na.run_node_id
WHERE rn.run_id = ? AND na.finished_at IS NOT NULL AND na.started_at IS NOT NULL`, runID).Scan(&avgAttemptLatency)
		if err == nil && avgAttemptLatency.Valid {
			m, err := s.RecordMetric(ctx, Metric{
				AnchorID:   anchorID,
				Name:       "attempt_latency_ms",
				Value:      avgAttemptLatency.Float64,
				Unit:       "ms",
				RecordedAt: now,
			})
			if err != nil {
				return nil, err
			}
			derived = append(derived, *m)
		}
	}

	return derived, nil
}

func (s *Service) CreateProposal(ctx context.Context, req CreateProposalRequest) (*PolicyChangeProposal, error) {
	if strings.TrimSpace(req.TargetGraphVersionID) == "" {
		return nil, errors.New("target_graph_version_id is required")
	}
	if strings.TrimSpace(req.Rationale) == "" {
		return nil, errors.New("rationale is required")
	}
	if strings.TrimSpace(req.ProposedChange) == "" {
		return nil, errors.New("proposed_change is required")
	}

	status := ProposalStatusDraft
	if req.Status == ProposalStatusProposed {
		status = ProposalStatusProposed
	} else if req.Status != "" && req.Status != ProposalStatusDraft {
		return nil, fmt.Errorf("invalid initial proposal status %q", req.Status)
	}

	proposalID := ulid.Make().String()
	now := time.Now().UnixMilli()

	var supersedesRef any = nil
	if strings.TrimSpace(req.SupersedesProposalID) != "" {
		supersedesRef = strings.TrimSpace(req.SupersedesProposalID)
	}

	var created *PolicyChangeProposal
	err := s.st.WithTx(ctx, func(tx *sql.Tx) error {
		var versionStatus, digest string
		err := tx.QueryRowContext(ctx, `
SELECT status, definition_digest FROM graph_version WHERE id = ?`, req.TargetGraphVersionID).
			Scan(&versionStatus, &digest)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("target graph version %s does not exist", req.TargetGraphVersionID)
		}
		if err != nil {
			return err
		}

		if versionStatus == "superseded" && strings.TrimSpace(req.SupersedesProposalID) == "" {
			return errors.New("proposal targeting a superseded version must explicitly specify supersedes_proposal_id")
		}

		if _, err := tx.ExecContext(ctx, `
INSERT INTO policy_change_proposal (id, target_graph_version_id, status, rationale, proposed_change, supersedes_proposal_id, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
			proposalID, req.TargetGraphVersionID, string(status), req.Rationale, req.ProposedChange, supersedesRef, now); err != nil {
			return err
		}

		var supersedesPtr *string
		if supersedesRef != nil {
			sVal := supersedesRef.(string)
			supersedesPtr = &sVal
		}

		created = &PolicyChangeProposal{
			ID:                   proposalID,
			TargetGraphVersionID: req.TargetGraphVersionID,
			TargetDigest:         digest,
			Status:               status,
			Rationale:            req.Rationale,
			ProposedChange:       req.ProposedChange,
			SupersedesProposalID: supersedesPtr,
			CreatedAt:            now,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

func (s *Service) GetProposal(ctx context.Context, id string) (*PolicyChangeProposal, error) {
	return queryProposal(ctx, s.st.DB(), id)
}

type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func queryProposal(ctx context.Context, q rowQuerier, id string) (*PolicyChangeProposal, error) {
	row := q.QueryRowContext(ctx, `
SELECT p.id, p.target_graph_version_id, gv.definition_digest, p.status, p.rationale, p.rejection_reason,
       p.proposed_change, p.approval_id, p.supersedes_proposal_id, p.created_at, p.decided_at
FROM policy_change_proposal p
JOIN graph_version gv ON gv.id = p.target_graph_version_id
WHERE p.id = ?`, id)

	var p PolicyChangeProposal
	var status string
	var appID, supID, rejection sql.NullString
	var decAt sql.NullInt64

	if err := row.Scan(&p.ID, &p.TargetGraphVersionID, &p.TargetDigest, &status, &p.Rationale, &rejection,
		&p.ProposedChange, &appID, &supID, &p.CreatedAt, &decAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	p.Status = ProposalStatus(status)
	if appID.Valid {
		p.ApprovalID = &appID.String
	}
	if supID.Valid {
		p.SupersedesProposalID = &supID.String
	}
	if rejection.Valid {
		p.RejectionReason = &rejection.String
	}
	if decAt.Valid {
		p.DecidedAt = &decAt.Int64
	}
	return &p, nil
}

func (s *Service) SubmitProposal(ctx context.Context, proposalID string) (*PolicyChangeProposal, error) {
	p, err := s.GetProposal(ctx, proposalID)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("proposal %s not found", proposalID)
	}
	if p.Status != ProposalStatusDraft {
		return nil, fmt.Errorf("cannot submit proposal with status %s; must be in draft", p.Status)
	}

	res, err := s.st.DB().ExecContext(ctx, `
UPDATE policy_change_proposal SET status = 'proposed' WHERE id = ? AND status = 'draft'`, proposalID)
	if err != nil {
		return nil, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected == 0 {
		return nil, fmt.Errorf("proposal %s was not updated", proposalID)
	}
	p.Status = ProposalStatusProposed
	return p, nil
}

func (s *Service) ApproveProposal(ctx context.Context, proposalID, approvalID string) (*PolicyChangeProposal, error) {
	if strings.TrimSpace(approvalID) == "" {
		return nil, errors.New("approval reference is required to approve proposal")
	}

	now := time.Now().UnixMilli()
	var approved *PolicyChangeProposal
	err := s.st.WithTx(ctx, func(tx *sql.Tx) error {
		p, err := queryProposal(ctx, tx, proposalID)
		if err != nil {
			return err
		}
		if p == nil {
			return fmt.Errorf("proposal %s not found", proposalID)
		}
		if p.Status != ProposalStatusProposed {
			return fmt.Errorf("cannot approve proposal with status %s; must be in proposed", p.Status)
		}

		var decision sql.NullString
		var approvalVersion string
		var expiresAt int64
		err = tx.QueryRowContext(ctx, `SELECT decision, graph_version_id, expires_at FROM approval WHERE id = ?`, approvalID).
			Scan(&decision, &approvalVersion, &expiresAt)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("referenced approval %s does not exist", approvalID)
		}
		if err != nil {
			return err
		}
		if !decision.Valid || decision.String != "grant" {
			return fmt.Errorf("referenced approval %s decision is not grant (got %v)", approvalID, decision)
		}
		if approvalVersion != p.TargetGraphVersionID {
			return fmt.Errorf("referenced approval %s is bound to graph version %s, not the proposal's target version %s", approvalID, approvalVersion, p.TargetGraphVersionID)
		}
		if expiresAt <= now {
			return fmt.Errorf("referenced approval %s expired at %d", approvalID, expiresAt)
		}

		res, err := tx.ExecContext(ctx, `
UPDATE policy_change_proposal
SET status = 'approved', approval_id = ?, decided_at = ?
WHERE id = ? AND status = 'proposed'`, approvalID, now, proposalID)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("proposal %s was not updated; it is no longer in proposed", proposalID)
		}

		p.Status = ProposalStatusApproved
		p.ApprovalID = &approvalID
		p.DecidedAt = &now
		approved = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return approved, nil
}

func (s *Service) RejectProposal(ctx context.Context, proposalID, rationale string) (*PolicyChangeProposal, error) {
	if strings.TrimSpace(rationale) == "" {
		return nil, errors.New("rejection rationale is required")
	}

	now := time.Now().UnixMilli()
	var rejected *PolicyChangeProposal
	err := s.st.WithTx(ctx, func(tx *sql.Tx) error {
		p, err := queryProposal(ctx, tx, proposalID)
		if err != nil {
			return err
		}
		if p == nil {
			return fmt.Errorf("proposal %s not found", proposalID)
		}
		if p.Status != ProposalStatusProposed {
			return fmt.Errorf("cannot reject proposal with status %s; must be in proposed", p.Status)
		}

		res, err := tx.ExecContext(ctx, `
UPDATE policy_change_proposal
SET status = 'rejected', rejection_reason = ?, decided_at = ?
WHERE id = ? AND status = 'proposed'`, rationale, now, proposalID)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("proposal %s was not updated; it is no longer in proposed", proposalID)
		}

		p.Status = ProposalStatusRejected
		p.RejectionReason = &rationale
		p.DecidedAt = &now
		rejected = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rejected, nil
}

func (s *Service) SupersedeProposal(ctx context.Context, proposalID string, supersedingProposalID string) (*PolicyChangeProposal, error) {
	if strings.TrimSpace(supersedingProposalID) == "" {
		return nil, errors.New("superseding_proposal_id is required to supersede a proposal")
	}
	if supersedingProposalID == proposalID {
		return nil, fmt.Errorf("proposal %s cannot supersede itself", proposalID)
	}

	var superseded *PolicyChangeProposal
	err := s.st.WithTx(ctx, func(tx *sql.Tx) error {
		p, err := queryProposal(ctx, tx, proposalID)
		if err != nil {
			return err
		}
		if p == nil {
			return fmt.Errorf("proposal %s not found", proposalID)
		}
		if p.Status == ProposalStatusSuperseded {
			superseded = p
			return nil
		}
		if p.Status != ProposalStatusDraft && p.Status != ProposalStatusProposed {
			return fmt.Errorf("cannot supersede proposal with status %s; only draft or proposed proposals can be superseded", p.Status)
		}

		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM policy_change_proposal WHERE id = ?`, supersedingProposalID).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return fmt.Errorf("superseding proposal %s does not exist", supersedingProposalID)
		}

		now := time.Now().UnixMilli()
		res, err := tx.ExecContext(ctx, `
UPDATE policy_change_proposal
SET status = 'superseded', decided_at = ?
WHERE id = ? AND status IN ('draft','proposed')`, now, proposalID)
		if err != nil {
			return err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("proposal %s was not updated; it can no longer be superseded", proposalID)
		}

		p.Status = ProposalStatusSuperseded
		p.DecidedAt = &now
		superseded = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return superseded, nil
}

func (s *Service) AssociateMetrics(ctx context.Context, proposalID string, metricIDs []string) error {
	if len(metricIDs) == 0 {
		return nil
	}

	p, err := s.GetProposal(ctx, proposalID)
	if err != nil {
		return err
	}
	if p == nil {
		return fmt.Errorf("proposal %s not found", proposalID)
	}

	return s.st.WithTx(ctx, func(tx *sql.Tx) error {
		for _, mID := range metricIDs {
			var exists int
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM metric WHERE id = ?", mID).Scan(&exists); err != nil {
				return err
			}
			if exists == 0 {
				return fmt.Errorf("metric %s does not exist", mID)
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO proposal_metric (proposal_id, metric_id)
VALUES (?, ?) ON CONFLICT(proposal_id, metric_id) DO NOTHING`, proposalID, mID); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) GetEvidenceChain(ctx context.Context, proposalID string) (*EvidenceChain, error) {
	p, err := s.GetProposal(ctx, proposalID)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("proposal %s not found", proposalID)
	}

	rows, err := s.st.DB().QueryContext(ctx, `
SELECT m.id, m.anchor_id, m.name, m.value, m.unit, COALESCE(m.dimensions, ''), m.recorded_at,
       a.id, a.graph_version_id, a.created_at, gv.definition_digest
FROM proposal_metric pm
JOIN metric m ON m.id = pm.metric_id
JOIN anchor a ON a.id = m.anchor_id
JOIN graph_version gv ON gv.id = a.graph_version_id
WHERE pm.proposal_id = ?
ORDER BY m.recorded_at ASC, m.name ASC`, proposalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var evidence []EvidenceMetric
	for rows.Next() {
		var em EvidenceMetric
		var dims string
		if err := rows.Scan(
			&em.Metric.ID, &em.Metric.AnchorID, &em.Metric.Name, &em.Metric.Value, &em.Metric.Unit, &dims, &em.Metric.RecordedAt,
			&em.Anchor.ID, &em.Anchor.GraphVersionID, &em.Anchor.CreatedAt, &em.Digest,
		); err != nil {
			return nil, err
		}
		if dims != "" {
			em.Metric.Dimensions = json.RawMessage(dims)
		}
		em.Anchor.DefinitionDigest = em.Digest
		evidence = append(evidence, em)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &EvidenceChain{
		Proposal: *p,
		Evidence: evidence,
	}, nil
}

func (s *Service) GetOverview(ctx context.Context, graphIDOrName string) (*Overview, error) {
	var graphID, graphName string
	err := s.st.DB().QueryRowContext(ctx, `
SELECT id, name FROM graph WHERE id = ? OR name = ? COLLATE NOCASE`, graphIDOrName, graphIDOrName).
		Scan(&graphID, &graphName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.NewCodeError("GRAPH_INVALID", "graph %q not found", graphIDOrName)
	}
	if err != nil {
		return nil, err
	}

	vRows, err := s.st.DB().QueryContext(ctx, `
SELECT id, definition_digest, status, created_at
FROM graph_version
WHERE graph_id = ?
ORDER BY created_at ASC`, graphID)
	if err != nil {
		return nil, err
	}
	defer vRows.Close()

	var versions []VersionSummary
	var versionIDs []string
	for vRows.Next() {
		var v VersionSummary
		if err := vRows.Scan(&v.ID, &v.DefinitionDigest, &v.Status, &v.CreatedAt); err != nil {
			return nil, err
		}
		versions = append(versions, v)
		versionIDs = append(versionIDs, v.ID)
	}
	if err := vRows.Err(); err != nil {
		return nil, err
	}
	vRows.Close()

	for i := range versions {
		outcomes, err := s.ListOutcomesForVersion(ctx, versions[i].ID)
		if err != nil {
			return nil, err
		}
		versions[i].Outcomes = outcomes

		mRows, err := s.st.DB().QueryContext(ctx, `
SELECT m.id, m.anchor_id, m.name, m.value, m.unit, COALESCE(m.dimensions, ''), m.recorded_at
FROM metric m
JOIN anchor a ON a.id = m.anchor_id
WHERE a.graph_version_id = ?
ORDER BY m.recorded_at ASC, m.name ASC`, versions[i].ID)
		if err != nil {
			return nil, err
		}
		var metrics []Metric
		for mRows.Next() {
			var m Metric
			var dims string
			if err := mRows.Scan(&m.ID, &m.AnchorID, &m.Name, &m.Value, &m.Unit, &dims, &m.RecordedAt); err != nil {
				mRows.Close()
				return nil, err
			}
			if dims != "" {
				m.Dimensions = json.RawMessage(dims)
			}
			metrics = append(metrics, m)
		}
		if err := mRows.Err(); err != nil {
			mRows.Close()
			return nil, err
		}
		mRows.Close()
		versions[i].Metrics = metrics
	}

	var proposals []EvidenceChain
	if len(versionIDs) > 0 {
		placeholders := make([]string, len(versionIDs))
		args := make([]any, len(versionIDs))
		for i, vid := range versionIDs {
			placeholders[i] = "?"
			args[i] = vid
		}
		pRows, err := s.st.DB().QueryContext(ctx, fmt.Sprintf(`
SELECT id FROM policy_change_proposal
WHERE target_graph_version_id IN (%s)
ORDER BY created_at ASC`, strings.Join(placeholders, ",")), args...)
		if err != nil {
			return nil, err
		}
		var pIDs []string
		for pRows.Next() {
			var pid string
			if err := pRows.Scan(&pid); err != nil {
				pRows.Close()
				return nil, err
			}
			pIDs = append(pIDs, pid)
		}
		if err := pRows.Err(); err != nil {
			pRows.Close()
			return nil, err
		}
		pRows.Close()

		for _, pid := range pIDs {
			chain, err := s.GetEvidenceChain(ctx, pid)
			if err != nil {
				return nil, err
			}
			if chain != nil {
				proposals = append(proposals, *chain)
			}
		}
	}

	return &Overview{
		GraphID:   graphID,
		GraphName: graphName,
		Versions:  versions,
		Proposals: proposals,
	}, nil
}
