package executor

import (
	"context"
	"errors"
)

// AbortIfCancelled classifies an already-cancelled request before any
// admission or dispatch work runs, so a cancellation racing the start of
// an execution reports the stable cancellation outcome instead of a raw
// context error that would be misfiled as a node failure.
func AbortIfCancelled(ctx context.Context, cancellation <-chan struct{}) error {
	if cancellation != nil {
		select {
		case <-cancellation:
			return ErrCancelled
		default:
		}
	}
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrTimeout
		}
		return ErrCancelled
	}
	return nil
}
