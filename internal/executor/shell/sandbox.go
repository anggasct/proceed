package shell

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"proceed/internal/capability"
)

var ErrSandboxUnavailable = errors.New("bubblewrap sandbox is unavailable")

type Launcher struct {
	Path string
}

func NewLauncher(path string) Launcher {
	if path == "" {
		path = "bwrap"
	}
	return Launcher{Path: path}
}

func (l Launcher) Check(profile capability.Profile, workspace string) error {
	cmd, err := l.Command(profile, workspace, "", []string{"/bin/true"}, nil)
	if err != nil {
		return err
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return ErrSandboxUnavailable
	}
	return nil
}

func (l Launcher) Command(profile capability.Profile, workspace, workdir string, argv []string, env map[string]string) (*exec.Cmd, error) {
	if err := profile.Validate(workspace); err != nil {
		return nil, err
	}
	if len(argv) == 0 {
		return nil, &capability.Error{Message: "shell command is required"}
	}
	workspace, err := filepath.Abs(workspace)
	if err != nil {
		return nil, &capability.Error{Message: "workspace path is invalid"}
	}
	info, err := os.Stat(workspace)
	if err != nil || !info.IsDir() {
		return nil, &capability.Error{Message: "workspace must be an existing directory"}
	}
	workdir, err = relativeWorkdir(workdir)
	if err != nil {
		return nil, err
	}
	if err := validateEnvironment(env); err != nil {
		return nil, err
	}
	launcherPath := l.Path
	if launcherPath == "" {
		launcherPath = "bwrap"
	}
	path, err := exec.LookPath(launcherPath)
	if err != nil {
		return nil, ErrSandboxUnavailable
	}

	args := []string{
		"--die-with-parent",
		"--new-session",
		"--unshare-net",
		"--unshare-pid",
		"--unshare-ipc",
		"--unshare-uts",
		"--proc", "/proc",
		"--dev", "/dev",
		"--dir", "/workspace",
		"--chmod", "0555", "/workspace",
		"--dir", "/tmp",
		"--chmod", "0555", "/tmp",
	}
	for _, dir := range []string{"/usr", "/bin", "/sbin", "/lib", "/lib64"} {
		if _, err := os.Stat(dir); err == nil {
			args = append(args, "--ro-bind", dir, dir)
		}
	}
	args, err = appendFilesystemArgs(args, profile, workspace)
	if err != nil {
		return nil, err
	}
	args = append(args, "--chdir", sandboxWorkdir(workdir), "--clearenv")
	args = append(args, "--setenv", "PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	for name, value := range env {
		args = append(args, "--setenv", name, value)
	}
	args = append(args, "--")
	args = append(args, argv...)

	cmd := exec.Command(path, args...)
	if err := configureCommand(cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

func appendFilesystemArgs(args []string, profile capability.Profile, workspace string) ([]string, error) {
	switch profile.Filesystem.Mode {
	case capability.FilesystemWorkspaceRead:
		return append(args, "--ro-bind", workspace, "/workspace"), nil
	case capability.FilesystemWorkspaceWrite:
		return append(args, "--bind", workspace, "/workspace"), nil
	case capability.FilesystemDeclaredPaths:
		resolvedWorkspace, err := filepath.EvalSymlinks(workspace)
		if err != nil {
			return nil, &capability.Error{Message: "workspace path cannot be resolved"}
		}
		for _, path := range profile.Filesystem.Paths {
			hostPath := filepath.Join(workspace, path)
			info, err := os.Stat(hostPath)
			if err != nil {
				return nil, &capability.Error{Message: "approved workspace path does not exist"}
			}
			resolvedPath, err := filepath.EvalSymlinks(hostPath)
			if err != nil || !withinPath(resolvedWorkspace, resolvedPath) {
				return nil, &capability.Error{Message: "approved workspace path escapes the workspace"}
			}
			args = appendSandboxParentDirs(args, path)
			if info.IsDir() {
				target := filepath.Join("/workspace", path)
				args = append(args, "--dir", target, "--chmod", "0555", target)
			}
			args = append(args, "--bind", hostPath, filepath.Join("/workspace", path))
		}
	}
	return args, nil
}

func appendSandboxParentDirs(args []string, path string) []string {
	target := filepath.Join("/workspace", path)
	var parents []string
	for parent := filepath.Dir(target); parent != "/workspace"; parent = filepath.Dir(parent) {
		parents = append(parents, parent)
	}
	for i := len(parents) - 1; i >= 0; i-- {
		args = append(args, "--dir", parents[i], "--chmod", "0555", parents[i])
	}
	return args
}

func withinPath(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func relativeWorkdir(workdir string) (string, error) {
	if workdir == "" {
		return ".", nil
	}
	if filepath.IsAbs(workdir) {
		return "", &capability.Error{Message: "shell workdir must be relative to the workspace"}
	}
	clean := filepath.Clean(workdir)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", &capability.Error{Message: "shell workdir cannot escape the workspace"}
	}
	return clean, nil
}

func sandboxWorkdir(workdir string) string {
	if workdir == "." {
		return "/workspace"
	}
	return filepath.Join("/workspace", workdir)
}

func validateEnvironment(env map[string]string) error {
	for name := range env {
		if name == "" || strings.ContainsAny(name, "=\x00") {
			return &capability.Error{Message: "environment name is invalid"}
		}
	}
	return nil
}
