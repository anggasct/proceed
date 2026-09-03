package agent

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"proceed/internal/capability"
	"proceed/internal/executor"
	"proceed/internal/executor/shell"
)

func TestExecuteRunsRegisteredCLI(t *testing.T) {
	publisher := &recordingPublisher{}
	cli := writeCLI(t, "#!/bin/sh\nprintf 'agent-output'")
	adapter := &Executor{Launcher: shell.Launcher{Path: fakeBubblewrap(t)}, Allowlist: map[string]string{"helper": cli}}
	result, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{"kind": "agent_cli", "cli": "helper"},
		},
		Capability:        testProfile(),
		WorkspaceRoot:     t.TempDir(),
		ArtifactPublisher: publisher,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := result.Output["stdout"]; got != "agent-output" {
		t.Fatalf("stdout = %v, want agent-output", got)
	}
	if got := result.Output["exit_code"]; got != 0 {
		t.Fatalf("exit_code = %v, want 0", got)
	}
	if len(publisher.inputs) != 2 || string(publisher.inputs[0].Content) != "agent-output" {
		t.Fatalf("published output = %+v", publisher.inputs)
	}
}

func TestExecuteSpawnsExactArgv(t *testing.T) {
	cli := writeCLI(t, "#!/bin/sh\nprintf '%s\\n' \"$0\" \"$@\"")
	adapter := &Executor{Launcher: shell.Launcher{Path: fakeBubblewrap(t)}, Allowlist: map[string]string{"helper": cli}}
	result, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{"kind": "agent_cli", "cli": "helper", "args": []any{"do", "the-thing"}},
		},
		Capability:    testProfile(),
		WorkspaceRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := strings.Join([]string{cli, "do", "the-thing"}, "\n") + "\n"
	if got := result.Output["stdout"]; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}

func TestAdmitRejectsUnlistedCLIBeforeSpawn(t *testing.T) {
	launcher := &recordingLauncher{}
	adapter := &Executor{Launcher: launcher, Allowlist: map[string]string{}}
	err := adapter.Admit(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{"kind": "agent_cli", "cli": "evil"},
		},
		Capability:    testProfile(),
		WorkspaceRoot: t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), capability.CodePolicyDenied) {
		t.Fatalf("Admit() error = %v, want policy denial", err)
	}
	if launcher.calls != 0 {
		t.Fatalf("launcher calls = %d, want 0 (nothing spawned)", launcher.calls)
	}
}

func TestAdmitRejectsMissingBinary(t *testing.T) {
	adapter := &Executor{Launcher: shell.Launcher{Path: fakeBubblewrap(t)}, Allowlist: map[string]string{"ghost": "/nonexistent/agent-cli"}}
	_, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{"kind": "agent_cli", "cli": "ghost"},
		},
		Capability:    testProfile(),
		WorkspaceRoot: t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "NODE_FAILED") {
		t.Fatalf("Execute() error = %v, want node failure", err)
	}
}

func TestAdmitRejectsNonExecutableBinary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-executable")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	adapter := &Executor{Launcher: shell.Launcher{Path: fakeBubblewrap(t)}, Allowlist: map[string]string{"plain": path}}
	_, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{"kind": "agent_cli", "cli": "plain"},
		},
		Capability:    testProfile(),
		WorkspaceRoot: t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "NODE_FAILED") {
		t.Fatalf("Execute() error = %v, want node failure", err)
	}
}

func TestExecuteBoundsAndPublishesOutput(t *testing.T) {
	publisher := &recordingPublisher{}
	cli := writeCLI(t, "#!/bin/sh\nprintf 0123456789")
	adapter := &Executor{Launcher: shell.Launcher{Path: fakeBubblewrap(t)}, Allowlist: map[string]string{"helper": cli}, OutputLimit: 4}
	result, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{"kind": "agent_cli", "cli": "helper"},
		},
		Capability:        testProfile(),
		WorkspaceRoot:     t.TempDir(),
		ArtifactPublisher: publisher,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := result.Output["stdout"]; got != "0123" {
		t.Fatalf("stdout = %v, want 0123", got)
	}
	if got := result.Output["truncated"]; got != true {
		t.Fatalf("truncated = %v, want true", got)
	}
}

func TestExecuteRedactsResolvedSecrets(t *testing.T) {
	publisher := &recordingPublisher{}
	cli := writeCLI(t, "#!/bin/sh\nprintf '%s' \"$TOKEN\"")
	adapter := &Executor{Launcher: shell.Launcher{Path: fakeBubblewrap(t)}, Allowlist: map[string]string{"helper": cli}}
	result, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{
				"kind": "agent_cli",
				"cli":  "helper",
				"x-proceed-env": map[string]any{
					"TOKEN": "${token}",
				},
			},
		},
		Capability: func() capability.Profile {
			p := testProfile()
			p.Secrets = []string{"token"}
			return p
		}(),
		WorkspaceRoot:     t.TempDir(),
		Secrets:           resolver{"token": "secret-value"},
		ArtifactPublisher: publisher,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if strings.Contains(result.Output["stdout"].(string), "secret-value") {
		t.Fatalf("stdout leaked secret: %q", result.Output["stdout"])
	}
	if got := result.Output["stdout"]; got != "[REDACTED]" {
		t.Fatalf("stdout = %v, want redacted output", got)
	}
}

func TestExecuteRejectsUndeclaredSecretBeforeStart(t *testing.T) {
	cli := writeCLI(t, "#!/bin/sh\nprintf hi")
	adapter := &Executor{Launcher: shell.Launcher{Path: fakeBubblewrap(t)}, Allowlist: map[string]string{"helper": cli}}
	_, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{
				"kind": "agent_cli",
				"cli":  "helper",
				"x-proceed-env": map[string]any{
					"TOKEN": "${token}",
				},
			},
		},
		Capability:    testProfile(),
		WorkspaceRoot: t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), capability.CodePolicyDenied) {
		t.Fatalf("Execute() error = %v, want policy denial", err)
	}
}

func TestExecuteTimeoutKillsProcessGroup(t *testing.T) {
	workspace := t.TempDir()
	pidFile := filepath.Join(workspace, "child.pid")
	cli := writeCLI(t, "#!/bin/sh\nsleep 30 & child=$!; printf '%s' \"$child\" > \"$1\"; wait")
	adapter := &Executor{Launcher: shell.Launcher{Path: fakeBubblewrap(t)}, Allowlist: map[string]string{"helper": cli}}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := adapter.Execute(ctx, &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{"kind": "agent_cli", "cli": "helper", "args": []any{pidFile}},
		},
		Capability:    testProfile(),
		WorkspaceRoot: workspace,
	})
	if !errors.Is(err, executor.ErrTimeout) {
		t.Fatalf("Execute() error = %v, want timeout", err)
	}
	childPID := waitForPIDFile(t, pidFile)
	waitForProcessExit(t, childPID)
}

func TestExecuteCancellationKillsProcessGroup(t *testing.T) {
	workspace := t.TempDir()
	pidFile := filepath.Join(workspace, "child.pid")
	cli := writeCLI(t, "#!/bin/sh\nsleep 30 & child=$!; printf '%s' \"$child\" > \"$1\"; wait")
	adapter := &Executor{Launcher: shell.Launcher{Path: fakeBubblewrap(t)}, Allowlist: map[string]string{"helper": cli}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := adapter.Execute(ctx, &executor.Request{
			Config: map[string]any{
				"executor": map[string]any{"kind": "agent_cli", "cli": "helper", "args": []any{pidFile}},
			},
			Capability:    testProfile(),
			WorkspaceRoot: workspace,
		})
		done <- err
	}()
	waitForPIDFile(t, pidFile)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, executor.ErrCancelled) {
			t.Fatalf("Execute() error = %v, want cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Execute() did not stop after cancellation")
	}
}

func TestNativeSandboxRunsRegisteredCLI(t *testing.T) {
	if os.Getenv("PROCEED_NATIVE_SANDBOX") != "1" {
		t.Skip("native sandbox test disabled")
	}
	adapter := New(map[string]string{"sh": "/bin/sh"})
	result, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{"kind": "agent_cli", "cli": "sh", "args": []any{"-c", "printf native-agent-ok"}},
		},
		Capability:    testProfile(),
		WorkspaceRoot: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := result.Output["stdout"]; got != "native-agent-ok" {
		t.Fatalf("stdout = %q, want native-agent-ok", got)
	}
}

func testProfile() capability.Profile {
	return capability.Profile{
		Filesystem: capability.Filesystem{Mode: capability.FilesystemNone},
		Process:    capability.ProcessDeclaredCommand,
		Network:    capability.NetworkNone,
	}
}

type recordingPublisher struct {
	inputs []executor.ArtifactInput
}

func (p *recordingPublisher) Publish(_ context.Context, input executor.ArtifactInput) (executor.ArtifactRef, error) {
	input.Content = append([]byte(nil), input.Content...)
	p.inputs = append(p.inputs, input)
	return executor.ArtifactRef{Name: input.Name, MediaType: input.MediaType, SizeBytes: int64(len(input.Content))}, nil
}

type resolver map[string]string

func (r resolver) Resolve(_ context.Context, name string) ([]byte, error) {
	value, ok := r[name]
	if !ok {
		return nil, errors.New("missing secret")
	}
	return []byte(value), nil
}

type recordingLauncher struct {
	calls int
}

func (l *recordingLauncher) Check(_ capability.Profile, _ string) error { return nil }

func (l *recordingLauncher) Command(_ capability.Profile, _, _ string, _ []string, _ map[string]string) (*exec.Cmd, error) {
	l.calls++
	return nil, errors.New("must not spawn")
}

func writeCLI(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent-cli")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func fakeBubblewrap(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bwrap")
	script := `#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    --setenv) export "$2=$3"; shift 3 ;;
    --) shift; exec "$@" ;;
    *) shift ;;
  esac
done
exit 127
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for child pid file")
	return 0
}

func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var status syscall.WaitStatus
		wpid, _ := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
		if wpid == pid {
			return
		}
		if err := syscall.Kill(pid, 0); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d still running after kill", pid)
}
