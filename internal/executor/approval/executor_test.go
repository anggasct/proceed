package approval

import (
	"context"
	"errors"
	"testing"

	"proceed/internal/executor"
)

func TestKind(t *testing.T) {
	if got := (New()).Kind(); got != executor.HumanApproval {
		t.Errorf("Kind() = %q, want %q", got, executor.HumanApproval)
	}
}

func gateConfig(scope string, expires any) map[string]any {
	exec := map[string]any{"kind": "human_approval", "scope": scope}
	if expires != nil {
		exec["expires_in_ms"] = expires
	}
	return map[string]any{"executor": exec}
}

func TestAdmitRejectsInvalidConfig(t *testing.T) {
	ex := New()
	if err := ex.Admit(context.Background(), &executor.Request{Config: gateConfig("deploy-prod", nil)}); err != nil {
		t.Errorf("Admit(valid) = %v, want nil", err)
	}
	if err := ex.Admit(context.Background(), &executor.Request{Config: map[string]any{}}); err == nil {
		t.Errorf("Admit(invalid) = nil, want error")
	}
}

func TestExecuteReportsWaitWithoutBlocking(t *testing.T) {
	ex := New()
	result, err := ex.Execute(context.Background(), &executor.Request{
		Config:       gateConfig("deploy-prod", nil),
		Cancellation: make(chan struct{}),
	})
	if result != nil {
		t.Errorf("result = %+v, want nil", result)
	}
	if !errors.Is(err, executor.ErrWaitRequested) {
		t.Errorf("err = %v, want ErrWaitRequested", err)
	}
}

func TestExecuteHonorsCancellation(t *testing.T) {
	ex := New()
	cancelled := make(chan struct{})
	close(cancelled)
	result, err := ex.Execute(context.Background(), &executor.Request{
		Config:       gateConfig("deploy-prod", nil),
		Cancellation: cancelled,
	})
	if result != nil {
		t.Errorf("result = %+v, want nil", result)
	}
	if !errors.Is(err, executor.ErrCancelled) {
		t.Errorf("err = %v, want ErrCancelled", err)
	}
}
