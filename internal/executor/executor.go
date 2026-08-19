package executor

import (
	"context"
	"errors"
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
	RunID            string
	GraphVersionID   string
	DefinitionDigest string
	NodeKey          string
	AttemptNo        int64
	OperationKey     string
	Config           map[string]any
	TimeoutMs        int64
	Cancellation     chan struct{}
}

type Result struct {
	Output  map[string]any
	Route   string
	Effects []Effect
}

type Effect struct {
	Target       string
	RequestHash  string
	ReconcileRef string
}

var (
	ErrTimeout         = errors.New("executor timeout")
	ErrCancelled       = errors.New("executor cancelled")
	ErrUncertain       = errors.New("executor effect uncertain")
	ErrNotReconcilable = errors.New("executor does not support reconcile")
)

type Executor interface {
	Kind() Kind
	Execute(ctx context.Context, req *Request) (*Result, error)
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
