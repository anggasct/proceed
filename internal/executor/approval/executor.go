package approval

import (
	"context"
	"time"

	"proceed/internal/executor"
)

const DefaultExpiryMs = int64(7 * 24 * time.Hour / time.Millisecond)

type Executor struct{}

func New() *Executor { return &Executor{} }

func (e *Executor) Kind() executor.Kind { return executor.HumanApproval }

func (e *Executor) Admit(ctx context.Context, req *executor.Request) error {
	_, err := executor.ParseGate(req.Config)
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
