package shell

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"proceed/internal/capability"
)

func TestLauncherBuildsReadonlyNetworkDeniedCommand(t *testing.T) {
	workspace := t.TempDir()
	p := capability.Profile{
		Filesystem: capability.Filesystem{Mode: capability.FilesystemWorkspaceRead},
		Process:    capability.ProcessDeclaredCommand,
		Network:    capability.NetworkNone,
	}
	cmd, err := (Launcher{Path: "/missing/bwrap"}).Command(p, workspace, "", []string{"/bin/true"}, nil)
	if !errors.Is(err, ErrSandboxUnavailable) {
		t.Fatalf("Command() error = %v, want ErrSandboxUnavailable", err)
	}
	if cmd != nil {
		t.Fatal("Command() returned a command for an unavailable sandbox")
	}
}

func TestLauncherBuildsDeclaredPathSandbox(t *testing.T) {
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "output"), 0o755); err != nil {
		t.Fatal(err)
	}
	p := capability.Profile{
		Filesystem: capability.Filesystem{
			Mode:  capability.FilesystemDeclaredPaths,
			Paths: []string{"output"},
		},
		Process: capability.ProcessDeclaredCommand,
		Network: capability.NetworkNone,
	}
	cmd, err := (Launcher{Path: "/bin/true"}).Command(p, workspace, "", []string{"/bin/true"}, nil)
	if err != nil {
		t.Fatalf("Command() error = %v", err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{"--unshare-net", "--unshare-pid", "--unshare-ipc", "--unshare-uts", "--bind", filepath.Join(workspace, "output"), "/workspace/output"} {
		if !strings.Contains(args, want) {
			t.Errorf("sandbox args missing %q: %s", want, args)
		}
	}
	if strings.Contains(args, "--ro-bind "+workspace+" /workspace") {
		t.Fatalf("declared-paths sandbox exposes the entire workspace: %s", args)
	}
}

func TestNativeSandbox(t *testing.T) {
	if os.Getenv("PROCEED_NATIVE_SANDBOX") != "1" {
		t.Skip("native sandbox test disabled")
	}
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "approved"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "secret.txt"), []byte("hidden"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := capability.Profile{
		Filesystem: capability.Filesystem{Mode: capability.FilesystemDeclaredPaths, Paths: []string{"approved"}},
		Process:    capability.ProcessDeclaredCommand,
		Network:    capability.NetworkNone,
	}
	cmd, err := NewLauncher("").Command(p, workspace, "", []string{"/bin/sh", "-c", `printf allowed > /workspace/approved/allowed; if cat /workspace/secret.txt >/dev/null 2>&1; then exit 1; fi; if printf blocked > /tmp/escape 2>/dev/null; then exit 1; fi; printf native-sandbox-ok`}, nil)
	if err != nil {
		t.Fatalf("Command() error = %v", err)
	}
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("native sandbox command error = %v", err)
	}
	if string(output) != "native-sandbox-ok" {
		t.Fatalf("native sandbox output = %q", output)
	}
	content, err := os.ReadFile(filepath.Join(workspace, "approved", "allowed"))
	if err != nil {
		t.Fatalf("workspace output missing: %v", err)
	}
	if string(content) != "allowed" {
		t.Fatalf("workspace output = %q", content)
	}
}

func TestLauncherRejectsWorkspaceEscape(t *testing.T) {
	p := capability.Profile{
		Filesystem: capability.Filesystem{
			Mode:  capability.FilesystemDeclaredPaths,
			Paths: []string{"../escape"},
		},
		Process: capability.ProcessDeclaredCommand,
		Network: capability.NetworkNone,
	}
	_, err := (Launcher{Path: "/missing/bwrap"}).Command(p, t.TempDir(), "", []string{"/bin/true"}, nil)
	if err == nil || !strings.Contains(err.Error(), capability.CodePolicyDenied) {
		t.Fatalf("Command() error = %v, want policy denial", err)
	}
}

func TestLauncherRejectsSymlinkEscape(t *testing.T) {
	workspace := t.TempDir()
	external := t.TempDir()
	if err := os.Symlink(external, filepath.Join(workspace, "link")); err != nil {
		t.Fatal(err)
	}
	p := capability.Profile{
		Filesystem: capability.Filesystem{
			Mode:  capability.FilesystemDeclaredPaths,
			Paths: []string{"link"},
		},
		Process: capability.ProcessDeclaredCommand,
		Network: capability.NetworkNone,
	}
	_, err := (Launcher{Path: "/bin/true"}).Command(p, workspace, "", []string{"/bin/true"}, nil)
	if err == nil || !strings.Contains(err.Error(), capability.CodePolicyDenied) {
		t.Fatalf("Command() error = %v, want policy denial", err)
	}
}

func TestLauncherRejectsAbsoluteWorkdir(t *testing.T) {
	p := capability.Profile{
		Filesystem: capability.Filesystem{Mode: capability.FilesystemNone},
		Process:    capability.ProcessDeclaredCommand,
		Network:    capability.NetworkNone,
	}
	_, err := (Launcher{Path: "/missing/bwrap"}).Command(p, t.TempDir(), "/tmp", []string{"/bin/true"}, nil)
	if err == nil || !strings.Contains(err.Error(), capability.CodePolicyDenied) {
		t.Fatalf("Command() error = %v, want policy denial", err)
	}
}

func TestLauncherRejectsInvalidEnvironmentName(t *testing.T) {
	p := capability.Profile{
		Filesystem: capability.Filesystem{Mode: capability.FilesystemNone},
		Process:    capability.ProcessDeclaredCommand,
		Network:    capability.NetworkNone,
	}
	_, err := (Launcher{Path: "/missing/bwrap"}).Command(p, t.TempDir(), "", []string{"/bin/true"}, map[string]string{"BAD=NAME": "value"})
	if err == nil || !strings.Contains(err.Error(), capability.CodePolicyDenied) {
		t.Fatalf("Command() error = %v, want policy denial", err)
	}
}
