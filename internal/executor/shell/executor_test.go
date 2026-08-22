package shell

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"proceed/internal/capability"
	"proceed/internal/executor"
)

func TestExecuteBoundsAndPublishesOutput(t *testing.T) {
	publisher := &recordingPublisher{}
	workspace := t.TempDir()
	adapter := &Executor{Launcher: Launcher{Path: fakeBubblewrap(t)}, OutputLimit: 4}
	result, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{
				"kind":    "shell",
				"command": []any{"/bin/sh", "-c", "printf 0123456789"},
			},
		},
		Capability:        testProfile(),
		WorkspaceRoot:     workspace,
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
	if len(publisher.inputs) != 2 || string(publisher.inputs[0].Content) != "0123" {
		t.Fatalf("published output = %+v", publisher.inputs)
	}
}

func TestExecuteRedactsResolvedSecrets(t *testing.T) {
	publisher := &recordingPublisher{}
	adapter := &Executor{Launcher: Launcher{Path: fakeBubblewrap(t)}}
	result, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{
				"kind":    "shell",
				"command": []any{"/bin/sh", "-c", "printf '%s' \"$TOKEN\""},
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
	if got := string(publisher.inputs[0].Content); got != "[REDACTED]" {
		t.Fatalf("published stdout = %q, want redacted output", got)
	}
}

func TestExecuteRedactsSecretsBeforeOutputLimit(t *testing.T) {
	secret := "secret-value-longer-than-limit"
	publisher := &recordingPublisher{}
	adapter := &Executor{Launcher: Launcher{Path: fakeBubblewrap(t)}, OutputLimit: 12}
	result, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{
				"kind":    "shell",
				"command": []any{"/bin/sh", "-c", "printf 'prefix-%s' \"$TOKEN\""},
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
		Secrets:           resolver{"token": secret},
		ArtifactPublisher: publisher,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	stdout := result.Output["stdout"].(string)
	if strings.Contains(stdout, secret) || strings.Contains(string(publisher.inputs[0].Content), secret) {
		t.Fatalf("secret leaked across output boundary: stdout=%q artifact=%q", stdout, publisher.inputs[0].Content)
	}
	if result.Output["truncated"] != true {
		t.Fatalf("truncated = %v, want true", result.Output["truncated"])
	}
}

func TestExecuteRejectsUndeclaredSecretBeforeStart(t *testing.T) {
	adapter := &Executor{Launcher: Launcher{Path: fakeBubblewrap(t)}}
	_, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{
				"kind":    "shell",
				"command": []any{"/bin/true"},
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

func TestExecuteRejectsCommandMismatchBeforeStart(t *testing.T) {
	adapter := &Executor{Launcher: Launcher{Path: fakeBubblewrap(t)}}
	_, err := adapter.Execute(context.Background(), &executor.Request{
		Config: map[string]any{
			"executor": map[string]any{
				"kind":    "shell",
				"command": []any{"/bin/false"},
			},
		},
		DeclaredCommand: []string{"/bin/true"},
		Capability:      testProfile(),
		WorkspaceRoot:   t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), capability.CodePolicyDenied) {
		t.Fatalf("Execute() error = %v, want policy denial", err)
	}
}

func TestExecuteCancellationKillsProcessGroup(t *testing.T) {
	adapter := &Executor{Launcher: Launcher{Path: fakeBubblewrap(t)}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := adapter.Execute(ctx, &executor.Request{
			Config: map[string]any{
				"executor": map[string]any{
					"kind":    "shell",
					"command": []any{"/bin/sh", "-c", "sleep 5"},
				},
			},
			Capability:    testProfile(),
			WorkspaceRoot: t.TempDir(),
		})
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, executor.ErrCancelled) {
			t.Fatalf("Execute() error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not stop after cancellation")
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
