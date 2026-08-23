//go:build linux

package shell

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"proceed/internal/executor"
)

func TestExecuteCancellationKillsProcessGroup(t *testing.T) {
	adapter := &Executor{Launcher: Launcher{Path: fakeBubblewrap(t)}}
	workspace := t.TempDir()
	pidFile := filepath.Join(workspace, "child.pid")
	command := fmt.Sprintf("sleep 5 & child=$!; printf '%%s' \"$child\" > %q; wait", pidFile)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := adapter.Execute(ctx, &executor.Request{
			Config: map[string]any{
				"executor": map[string]any{
					"kind":    "shell",
					"command": []any{"/bin/sh", "-c", command},
				},
			},
			Capability:    testProfile(),
			WorkspaceRoot: workspace,
		})
		done <- err
	}()

	childPID := waitForPIDFile(t, pidFile)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, executor.ErrCancelled) {
			t.Fatalf("Execute() error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not stop after cancellation")
	}
	waitForProcessExit(t, childPID)
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("child PID was not written to %s", path)
	return 0
}

func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil {
			t.Fatalf("checking child process %d: %v", pid, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("child process %d survived cancellation", pid)
}
