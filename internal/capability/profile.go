package capability

import (
	"fmt"
	"path/filepath"
	"strings"
)

const CodePolicyDenied = "POLICY_DENIED"

type Error struct {
	Message string
}

func (e *Error) Error() string { return CodePolicyDenied + ": " + e.Message }

type FilesystemMode string

const (
	FilesystemNone           FilesystemMode = "none"
	FilesystemWorkspaceRead  FilesystemMode = "workspace-read"
	FilesystemWorkspaceWrite FilesystemMode = "workspace-write"
	FilesystemDeclaredPaths  FilesystemMode = "declared-paths"
)

type ProcessMode string

const (
	ProcessNone            ProcessMode = "none"
	ProcessDeclaredCommand ProcessMode = "declared-command"
)

type NetworkMode string

const NetworkNone NetworkMode = "none"

type Filesystem struct {
	Mode  FilesystemMode
	Paths []string
}

type Profile struct {
	Filesystem Filesystem
	Process    ProcessMode
	Network    NetworkMode
	Secrets    []string
}

func FromConfig(config map[string]any) (Profile, error) {
	raw, ok := config["capability"]
	if !ok {
		return Profile{}, &Error{Message: "capability profile is required"}
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return Profile{}, &Error{Message: "capability profile must be an object"}
	}

	p := Profile{
		Filesystem: Filesystem{Mode: FilesystemNone},
		Process:    ProcessNone,
		Network:    NetworkNone,
	}
	if value, ok := m["filesystem"]; ok {
		mode, ok := value.(string)
		if !ok {
			return Profile{}, &Error{Message: "filesystem capability must be a string"}
		}
		p.Filesystem.Mode = FilesystemMode(mode)
	}
	if value, ok := m["process"]; ok {
		mode, ok := value.(string)
		if !ok {
			return Profile{}, &Error{Message: "process capability must be a string"}
		}
		p.Process = ProcessMode(mode)
	}
	if value, ok := m["network"]; ok {
		mode, ok := value.(string)
		if !ok {
			return Profile{}, &Error{Message: "shell network capability must be none"}
		}
		p.Network = NetworkMode(mode)
	}
	if value, ok := m["secrets"]; ok {
		secrets, err := parseSecrets(value)
		if err != nil {
			return Profile{}, err
		}
		p.Secrets = secrets
	}
	if value, ok := m["x-proceed-paths"]; ok {
		paths, err := parsePaths(value)
		if err != nil {
			return Profile{}, err
		}
		p.Filesystem.Paths = paths
	}
	return p, nil
}

func (p Profile) Validate(workspaceRoot string) error {
	if workspaceRoot == "" {
		return &Error{Message: "workspace root is required"}
	}
	if p.Process != ProcessDeclaredCommand {
		return &Error{Message: "process capability must be declared-command"}
	}
	if p.Network != NetworkNone {
		return &Error{Message: "shell network capability must be none"}
	}
	switch p.Filesystem.Mode {
	case FilesystemNone, FilesystemWorkspaceRead, FilesystemWorkspaceWrite:
		if len(p.Filesystem.Paths) > 0 {
			return &Error{Message: "filesystem paths require declared-paths"}
		}
	case FilesystemDeclaredPaths:
		if len(p.Filesystem.Paths) == 0 {
			return &Error{Message: "declared-paths requires at least one path"}
		}
	default:
		return &Error{Message: fmt.Sprintf("unknown filesystem capability %q", p.Filesystem.Mode)}
	}
	for _, path := range p.Filesystem.Paths {
		if err := validateRelativePath(path); err != nil {
			return err
		}
	}
	for _, secret := range p.Secrets {
		if normalized, ok := NormalizeSecretReference(secret); !ok || normalized != secret {
			return &Error{Message: "secret reference has invalid name"}
		}
	}
	return nil
}

func (p Profile) AllowsSecret(name string) bool {
	normalized, ok := NormalizeSecretReference(name)
	if !ok {
		return false
	}
	for _, declared := range p.Secrets {
		if declared == normalized {
			return true
		}
	}
	return false
}

func parseSecrets(raw any) ([]string, error) {
	if value, ok := raw.(string); ok {
		if value == "none" {
			return nil, nil
		}
		return nil, &Error{Message: "secrets capability must be none or a list"}
	}
	values, ok := raw.([]any)
	if !ok {
		return nil, &Error{Message: "secrets capability must be none or a list"}
	}
	secrets := make([]string, 0, len(values))
	for _, value := range values {
		name, ok := value.(string)
		if !ok {
			return nil, &Error{Message: "secret references must be strings"}
		}
		normalized, ok := NormalizeSecretReference(name)
		if !ok {
			return nil, &Error{Message: "secret reference has invalid name"}
		}
		secrets = append(secrets, normalized)
	}
	return secrets, nil
}

func NormalizeSecretReference(raw string) (string, bool) {
	name := raw
	if strings.HasPrefix(raw, "${") || strings.HasSuffix(raw, "}") {
		if !strings.HasPrefix(raw, "${") || !strings.HasSuffix(raw, "}") {
			return "", false
		}
		name = strings.TrimSuffix(strings.TrimPrefix(raw, "${"), "}")
	}
	if name == "" {
		return "", false
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '.' && r != '_' && r != '-' {
			return "", false
		}
	}
	return name, true
}

func parsePaths(raw any) ([]string, error) {
	values, ok := raw.([]any)
	if !ok {
		return nil, &Error{Message: "declared paths must be a list"}
	}
	paths := make([]string, 0, len(values))
	for _, value := range values {
		path, ok := value.(string)
		if !ok {
			return nil, &Error{Message: "declared paths must be strings"}
		}
		if err := validateRelativePath(path); err != nil {
			return nil, err
		}
		paths = append(paths, filepath.Clean(path))
	}
	return paths, nil
}

func validateRelativePath(path string) error {
	if path == "" || filepath.IsAbs(path) {
		return &Error{Message: "declared path must be relative to the workspace"}
	}
	clean := filepath.Clean(path)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return &Error{Message: "declared path cannot escape the workspace"}
	}
	return nil
}
