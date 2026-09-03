package approval

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"proceed/internal/executor"
)

const DefaultExpiryMs = int64(7 * 24 * time.Hour / time.Millisecond)

const maxScopeLen = 256

type Gate struct {
	Scope       string
	ExpiresInMs int64
}

type Executor struct{}

func New() *Executor { return &Executor{} }

func (e *Executor) Kind() executor.Kind { return executor.HumanApproval }

func (e *Executor) Admit(ctx context.Context, req *executor.Request) error {
	_, err := ParseGate(req.Config)
	return err
}

// Execute never performs the approval itself: the gate wait is durable
// state owned by the controller, so dispatch reports the wait and exits
// instead of blocking a lease.
func (e *Executor) Execute(ctx context.Context, req *executor.Request) (*executor.Result, error) {
	if err := executor.AbortIfCancelled(ctx, req.Cancellation); err != nil {
		return nil, err
	}
	return nil, executor.ErrWaitRequested
}

func ParseGate(cfg map[string]any) (Gate, error) {
	raw, ok := cfg["executor"].(map[string]any)
	if !ok {
		return Gate{}, fmt.Errorf("node has no executor config")
	}
	scope, _ := raw["scope"].(string)
	if scope == "" {
		return Gate{}, fmt.Errorf("human_approval executor requires scope")
	}
	if len(scope) > maxScopeLen || strings.ContainsFunc(scope, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return Gate{}, fmt.Errorf("human_approval scope must be a bounded single-line string")
	}
	var expires int64
	if v, ok := raw["expires_in_ms"].(float64); ok {
		if v < 0 || v != math.Trunc(v) || v > math.MaxInt64 {
			return Gate{}, fmt.Errorf("human_approval expires_in_ms must be a non-negative integer")
		}
		expires = int64(v)
	}
	return Gate{Scope: scope, ExpiresInMs: expires}, nil
}
