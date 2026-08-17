package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FuncExecutor struct {
	name     Kind
	contract Contract
	fn       func(ctx context.Context, req *Request) (*Result, error)
}

func NewFuncExecutor(name Kind, contract Contract, fn func(ctx context.Context, req *Request) (*Result, error)) *FuncExecutor {
	return &FuncExecutor{name: name, contract: contract, fn: fn}
}

func (f *FuncExecutor) Kind() Kind { return f.name }

func (f *FuncExecutor) Contract() Contract { return f.contract }

func (f *FuncExecutor) Execute(ctx context.Context, req *Request) (*Result, error) {
	return f.fn(ctx, req)
}

type AppendFileExecutor struct {
	mu   sync.Mutex
	seen map[string]int
}

func NewAppendFileExecutor() *AppendFileExecutor {
	return &AppendFileExecutor{seen: map[string]int{}}
}

func (a *AppendFileExecutor) Kind() Kind { return "shell" }

func (a *AppendFileExecutor) Contract() Contract { return Idempotent }

func (a *AppendFileExecutor) Execute(ctx context.Context, req *Request) (*Result, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen[req.OperationKey]++
	return &Result{Output: map[string]any{"appends": a.seen[req.OperationKey]}}, nil
}

func (a *AppendFileExecutor) Calls(opKey string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.seen[opKey]
}

type NonReplayableExecutor struct {
	mu      sync.Mutex
	charged map[string]bool
}

func NewNonReplayableExecutor() *NonReplayableExecutor {
	return &NonReplayableExecutor{charged: map[string]bool{}}
}

func (n *NonReplayableExecutor) Kind() Kind { return "http" }

func (n *NonReplayableExecutor) Contract() Contract { return NonReplayable }

func (n *NonReplayableExecutor) Execute(ctx context.Context, req *Request) (*Result, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.charged[req.OperationKey] = true
	return &Result{}, nil
}

func (n *NonReplayableExecutor) Charged(opKey string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.charged[opKey]
}

type ReconcilableExecutor struct {
	mu       sync.Mutex
	executed map[string]bool
	receipts map[string]EffectState
}

func NewReconcilableExecutor() *ReconcilableExecutor {
	return &ReconcilableExecutor{executed: map[string]bool{}, receipts: map[string]EffectState{}}
}

func (r *ReconcilableExecutor) Kind() Kind { return "http" }

func (r *ReconcilableExecutor) Contract() Contract { return Reconcilable }

func (r *ReconcilableExecutor) Execute(ctx context.Context, req *Request) (*Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executed[req.OperationKey] = true
	r.receipts[req.OperationKey] = EffectConfirmed
	return &Result{}, nil
}

func (r *ReconcilableExecutor) Reconcile(ctx context.Context, req *Request) (EffectState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.receipts[req.OperationKey]
	if !ok {
		return EffectAbsent, nil
	}
	return state, nil
}

func (r *ReconcilableExecutor) Executed(opKey string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.executed[opKey]
}

var _ Executor = (*FuncExecutor)(nil)
var _ Executor = (*AppendFileExecutor)(nil)
var _ Executor = (*NonReplayableExecutor)(nil)
var _ Executor = (*ReconcilableExecutor)(nil)
var _ Reconciler = (*ReconcilableExecutor)(nil)

func WriteMarker(dir string) func(context.Context, *Request) (*Result, error) {
	return func(ctx context.Context, req *Request) (*Result, error) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		name := fmt.Sprintf("%s.attempt%d", req.NodeKey, req.AttemptNo)
		if err := os.WriteFile(filepath.Join(dir, name), []byte(req.OperationKey), 0o644); err != nil {
			return nil, err
		}
		return &Result{}, nil
	}
}

func ErrOnce(fn func(context.Context, *Request) (*Result, error)) func(context.Context, *Request) (*Result, error) {
	var once sync.Once
	var cached error
	return func(ctx context.Context, req *Request) (*Result, error) {
		once.Do(func() {
			cached = errors.New("injected failure")
		})
		if cached != nil {
			return nil, cached
		}
		return fn(ctx, req)
	}
}
