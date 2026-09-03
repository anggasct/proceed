package improvement

import (
	"encoding/json"
)

type ProposalStatus string

const (
	ProposalStatusDraft      ProposalStatus = "draft"
	ProposalStatusProposed   ProposalStatus = "proposed"
	ProposalStatusApproved   ProposalStatus = "approved"
	ProposalStatusRejected   ProposalStatus = "rejected"
	ProposalStatusSuperseded ProposalStatus = "superseded"
)

type Outcome struct {
	ID         string `json:"id"`
	RunID      string `json:"run_id"`
	AnchorID   string `json:"anchor_id"`
	Result     string `json:"result"`
	Detail     string `json:"detail,omitempty"`
	RecordedAt int64  `json:"recorded_at"`
}

type Anchor struct {
	ID               string `json:"id"`
	GraphVersionID   string `json:"graph_version_id"`
	DefinitionDigest string `json:"definition_digest,omitempty"`
	CreatedAt        int64  `json:"created_at"`
}

type Metric struct {
	ID         string          `json:"id"`
	AnchorID   string          `json:"anchor_id"`
	Name       string          `json:"name"`
	Value      float64         `json:"value"`
	Unit       string          `json:"unit"`
	Dimensions json.RawMessage `json:"dimensions,omitempty"`
	RecordedAt int64           `json:"recorded_at"`
}

type PolicyChangeProposal struct {
	ID                   string         `json:"id"`
	TargetGraphVersionID string         `json:"target_graph_version_id"`
	TargetDigest         string         `json:"target_digest,omitempty"`
	Status               ProposalStatus `json:"status"`
	Rationale            string         `json:"rationale"`
	RejectionReason      *string        `json:"rejection_reason,omitempty"`
	ProposedChange       string         `json:"proposed_change"`
	ApprovalID           *string        `json:"approval_id,omitempty"`
	SupersedesProposalID *string        `json:"supersedes_proposal_id,omitempty"`
	CreatedAt            int64          `json:"created_at"`
	DecidedAt            *int64         `json:"decided_at,omitempty"`
}

type EvidenceMetric struct {
	Metric Metric `json:"metric"`
	Anchor Anchor `json:"anchor"`
	Digest string `json:"definition_digest"`
}

type EvidenceChain struct {
	Proposal PolicyChangeProposal `json:"proposal"`
	Evidence []EvidenceMetric     `json:"evidence"`
}

type CreateProposalRequest struct {
	TargetGraphVersionID string         `json:"target_graph_version_id"`
	Rationale            string         `json:"rationale"`
	ProposedChange       string         `json:"proposed_change"`
	SupersedesProposalID string         `json:"supersedes_proposal_id,omitempty"`
	Status               ProposalStatus `json:"status,omitempty"`
}

type Overview struct {
	GraphID   string           `json:"graph_id"`
	GraphName string           `json:"graph_name"`
	Versions  []VersionSummary `json:"versions"`
	Proposals []EvidenceChain  `json:"proposals"`
}

type VersionSummary struct {
	ID               string    `json:"id"`
	DefinitionDigest string    `json:"definition_digest"`
	Status           string    `json:"status"`
	CreatedAt        int64     `json:"created_at"`
	Outcomes         []Outcome `json:"outcomes,omitempty"`
	Metrics          []Metric  `json:"metrics,omitempty"`
}
