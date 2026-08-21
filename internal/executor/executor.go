package executor

import (
	"context"
	"errors"

	"proceed/internal/capability"
)

type Contract string

const (
	Pure          Contract = "pure"
	Idempotent    Contract = "idempotent"
	Reconcilable  Contract = "reconcilable"
	NonReplayable Contract = "non_replayable"
)

type Kind string

const (
	Shell         Kind = "shell"
	HTTP          Kind = "http"
	HumanApproval Kind = "human_approval"
	AgentCLI      Kind = "agent_cli"
)

type Request struct {
	RunID             string
	GraphVersionID    string
	DefinitionDigest  string
	NodeKey           string
	AttemptNo         int64
	OperationKey      string
	Config            map[string]any
	DeclaredCommand   []string
	TimeoutMs         int64
	Cancellation      <-chan struct{}
	Capability        capability.Profile
	WorkspaceRoot     string
	Inputs            []ArtifactRef
	Secrets           SecretResolver
	ArtifactPublisher ArtifactPublisher
}

type Result struct {
	Output    map[string]any
	Route     string
	Artifacts []ArtifactRef
	Effects   []Effect
}

type ArtifactInput struct {
	Name      string
	MediaType string
	Content   []byte
	Truncated bool
}

type ArtifactRef struct {
	ID          string
	Name        string
	Path        string
	ContentHash string
	MediaType   string
	SizeBytes   int64
	Truncated   bool
}

type SecretResolver interface {
	Resolve(ctx context.Context, name string) ([]byte, error)
}

type ArtifactPublisher interface {
	Publish(ctx context.Context, input ArtifactInput) (ArtifactRef, error)
}

type Effect struct {
	Target       string
	RequestHash  string
	ReconcileRef string
}

var (
	ErrTimeout         = errors.New("NODE_TIMEOUT")
	ErrCancelled       = errors.New("RUN_CANCELLED")
	ErrUncertain       = errors.New("EFFECT_UNCERTAIN")
	ErrNotReconcilable = errors.New("EFFECT_UNCERTAIN")
)

type Executor interface {
	Kind() Kind
	Execute(ctx context.Context, req *Request) (*Result, error)
}

type Admitter interface {
	Admit(ctx context.Context, req *Request) error
}

type Reconciler interface {
	Reconcile(ctx context.Context, req *Request) (EffectState, error)
}

type ResultReconciler interface {
	ReconcileResult(ctx context.Context, req *Request) (*Result, EffectState, error)
}

type EffectState string

const (
	EffectConfirmed EffectState = "confirmed"
	EffectAbsent    EffectState = "absent"
	EffectUnknown   EffectState = "unknown"
)
