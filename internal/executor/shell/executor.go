package shell

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"proceed/internal/capability"
	"proceed/internal/executor"
)

const DefaultOutputLimit = 1 << 20

type Executor struct {
	Launcher    Launcher
	OutputLimit int
}

func New() *Executor {
	return &Executor{Launcher: NewLauncher(""), OutputLimit: DefaultOutputLimit}
}

func (e *Executor) Kind() executor.Kind { return executor.Shell }

func (e *Executor) Admit(ctx context.Context, req *executor.Request) error {
	if req == nil {
		return &capability.Error{Message: "executor request is required"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	config, err := parseConfig(req.Config)
	if err != nil {
		return err
	}
	if len(req.DeclaredCommand) == 0 {
		req.DeclaredCommand = append([]string(nil), config.command...)
	} else if !sameCommand(req.DeclaredCommand, config.command) {
		return &capability.Error{Message: "command differs from declared argv"}
	}
	if err := validateEnvironment(config.env); err != nil {
		return err
	}
	if err := validateEnvironmentReferences(req.Capability, config.env); err != nil {
		return err
	}
	if err := req.Capability.Validate(req.WorkspaceRoot); err != nil {
		return err
	}
	err = e.Launcher.Check(req.Capability, req.WorkspaceRoot)
	if errors.Is(err, ErrSandboxUnavailable) {
		return &capability.Error{Message: "sandbox is unavailable"}
	}
	return err
}

func (e *Executor) Execute(ctx context.Context, req *executor.Request) (*executor.Result, error) {
	if req == nil {
		return nil, &capability.Error{Message: "executor request is required"}
	}
	if err := e.Admit(ctx, req); err != nil {
		return nil, err
	}
	config, err := parseConfig(req.Config)
	if err != nil {
		return nil, err
	}
	env, redactions, err := resolveEnvironment(ctx, req, config.env)
	if err != nil {
		return nil, err
	}
	cmd, err := e.Launcher.Command(req.Capability, req.WorkspaceRoot, config.workdir, config.command, env)
	if errors.Is(err, ErrSandboxUnavailable) {
		return nil, &capability.Error{Message: "sandbox is unavailable"}
	}
	if err != nil {
		return nil, err
	}

	limit := e.OutputLimit
	if limit <= 0 {
		limit = DefaultOutputLimit
	}
	captureLimit := redactionCaptureLimit(limit, redactions)
	stdout := &limitedBuffer{limit: captureLimit}
	stderr := &limitedBuffer{limit: captureLimit}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, &FailureError{reason: "command could not start"}
	}

	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	var runErr error
	select {
	case runErr = <-wait:
	case <-ctx.Done():
		_ = terminateProcessGroup(cmd)
		<-wait
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, executor.ErrTimeout
		}
		return nil, executor.ErrCancelled
	case <-req.Cancellation:
		_ = terminateProcessGroup(cmd)
		<-wait
		return nil, executor.ErrCancelled
	}

	stdoutBytes, stdoutTruncated := redactAndBound(stdout.Bytes(), redactions, limit, stdout.Truncated())
	stderrBytes, stderrTruncated := redactAndBound(stderr.Bytes(), redactions, limit, stderr.Truncated())
	result := &executor.Result{
		Output: map[string]any{
			"stdout":    string(stdoutBytes),
			"stderr":    string(stderrBytes),
			"exit_code": 0,
			"truncated": stdoutTruncated || stderrTruncated,
		},
	}
	refs, err := publishOutput(ctx, req.ArtifactPublisher, stdoutBytes, stderrBytes, stdoutTruncated, stderrTruncated)
	if err != nil {
		return nil, err
	}
	result.Artifacts = refs
	if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			result.Output["exit_code"] = exitErr.ExitCode()
			return result, &FailureError{exitCode: exitErr.ExitCode()}
		}
		result.Output["exit_code"] = -1
		return result, &FailureError{reason: "command execution failed"}
	}
	return result, nil
}

func sameCommand(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

type shellConfig struct {
	command []string
	workdir string
	env     map[string]string
}

func parseConfig(config map[string]any) (shellConfig, error) {
	raw, ok := config["executor"].(map[string]any)
	if !ok {
		return shellConfig{}, &capability.Error{Message: "shell executor config is required"}
	}
	command, err := parseCommand(raw["command"])
	if err != nil {
		return shellConfig{}, err
	}
	workdir, _ := raw["workdir"].(string)
	env, err := parseEnvironment(raw["x-proceed-env"])
	if err != nil {
		return shellConfig{}, err
	}
	return shellConfig{command: command, workdir: workdir, env: env}, nil
}

func parseCommand(raw any) ([]string, error) {
	if command, ok := raw.(string); ok && command != "" {
		return []string{command}, nil
	}
	values, ok := raw.([]any)
	if !ok || len(values) == 0 {
		return nil, &capability.Error{Message: "shell command is required"}
	}
	command := make([]string, 0, len(values))
	for _, value := range values {
		part, ok := value.(string)
		if !ok || part == "" {
			return nil, &capability.Error{Message: "shell command arguments must be non-empty strings"}
		}
		command = append(command, part)
	}
	return command, nil
}

func parseEnvironment(raw any) (map[string]string, error) {
	if raw == nil {
		return nil, nil
	}
	values, ok := raw.(map[string]any)
	if !ok {
		return nil, &capability.Error{Message: "shell environment must be an object"}
	}
	env := make(map[string]string, len(values))
	for name, value := range values {
		ref, ok := value.(string)
		if !ok || !strings.HasPrefix(ref, "${") || !strings.HasSuffix(ref, "}") {
			return nil, &capability.Error{Message: "shell environment values must be secret references"}
		}
		env[name] = ref
	}
	return env, nil
}

func resolveEnvironment(ctx context.Context, req *executor.Request, refs map[string]string) (map[string]string, [][]byte, error) {
	if len(refs) == 0 {
		return nil, nil, nil
	}
	if err := validateEnvironmentReferences(req.Capability, refs); err != nil {
		return nil, nil, err
	}
	if req.Secrets == nil {
		return nil, nil, &capability.Error{Message: "secret resolver is required"}
	}
	env := make(map[string]string, len(refs))
	redactions := make([][]byte, 0, len(refs))
	for envName, ref := range refs {
		name, ok := capability.NormalizeSecretReference(ref)
		if !ok {
			return nil, nil, &capability.Error{Message: "secret reference is invalid"}
		}
		secret, err := req.Secrets.Resolve(ctx, name)
		if err != nil {
			return nil, nil, &capability.Error{Message: "secret resolution failed"}
		}
		env[envName] = string(secret)
		if len(secret) > 0 {
			redactions = append(redactions, append([]byte(nil), secret...))
		}
	}
	return env, redactions, nil
}

func validateEnvironmentReferences(profile capability.Profile, refs map[string]string) error {
	for _, ref := range refs {
		if !profile.AllowsSecret(ref) {
			return &capability.Error{Message: "secret reference is not declared"}
		}
	}
	return nil
}

func publishOutput(ctx context.Context, publisher executor.ArtifactPublisher, stdout, stderr []byte, stdoutTruncated, stderrTruncated bool) ([]executor.ArtifactRef, error) {
	if publisher == nil {
		return nil, nil
	}
	outputs := []struct {
		name      string
		content   []byte
		truncated bool
	}{
		{name: "stdout", content: stdout, truncated: stdoutTruncated},
		{name: "stderr", content: stderr, truncated: stderrTruncated},
	}
	refs := make([]executor.ArtifactRef, 0, len(outputs))
	for _, output := range outputs {
		ref, err := publisher.Publish(ctx, executor.ArtifactInput{
			Name:      output.name,
			MediaType: "text/plain",
			Content:   output.content,
			Truncated: output.truncated,
		})
		if err != nil {
			return nil, &FailureError{reason: "artifact publication failed"}
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

type limitedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.buf.Len()
	if remaining <= 0 {
		b.truncated = true
		return len(p), nil
	}
	if len(p) > remaining {
		_, _ = b.buf.Write(p[:remaining])
		b.truncated = true
		return len(p), nil
	}
	_, _ = b.buf.Write(p)
	return len(p), nil
}

func (b *limitedBuffer) Bytes() []byte { return b.buf.Bytes() }

func (b *limitedBuffer) Truncated() bool { return b.truncated }

func redactionCaptureLimit(limit int, secrets [][]byte) int {
	maxSecret := 0
	for _, secret := range secrets {
		if len(secret) > maxSecret {
			maxSecret = len(secret)
		}
	}
	if maxSecret <= 1 {
		return limit
	}
	maxInt := int(^uint(0) >> 1)
	extra := maxSecret - 1
	if extra > maxInt-limit {
		return maxInt
	}
	return limit + extra
}

func redactAndBound(value []byte, secrets [][]byte, limit int, capturedLimit bool) ([]byte, bool) {
	redacted := redact(value, secrets)
	truncated := capturedLimit || len(value) > limit || len(redacted) > limit
	if len(redacted) > limit {
		redacted = redacted[:limit]
	}
	return redacted, truncated
}

func redact(value []byte, secrets [][]byte) []byte {
	for _, secret := range secrets {
		if len(secret) > 0 {
			value = bytes.ReplaceAll(value, secret, []byte("[REDACTED]"))
		}
	}
	return value
}

type FailureError struct {
	exitCode int
	reason   string
}

var _ executor.Executor = (*Executor)(nil)
var _ executor.Admitter = (*Executor)(nil)

func (e *FailureError) Error() string {
	if e.reason != "" {
		return "NODE_FAILED: " + e.reason
	}
	return fmt.Sprintf("NODE_FAILED: command exited with status %d", e.exitCode)
}
