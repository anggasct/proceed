package capability

import (
	"errors"
	"testing"
)

func TestFromConfigRequiresProcessCapability(t *testing.T) {
	p, err := FromConfig(map[string]any{
		"capability": map[string]any{
			"filesystem": "workspace-read",
		},
	})
	if err != nil {
		t.Fatalf("FromConfig() error = %v", err)
	}
	var policyErr *Error
	if err := p.Validate("/workspace"); err == nil || !errors.As(err, &policyErr) {
		t.Fatalf("Validate() error = %v, want policy denial", err)
	}
}

func TestFromConfigRejectsPathEscape(t *testing.T) {
	_, err := FromConfig(map[string]any{
		"capability": map[string]any{
			"filesystem":      "declared-paths",
			"process":         "declared-command",
			"x-proceed-paths": []any{"../escape"},
		},
	})
	if err == nil {
		t.Fatal("FromConfig() error = nil, want policy denial")
	}
	var policyErr *Error
	if !errors.As(err, &policyErr) {
		t.Fatalf("FromConfig() error = %T, want policy denial", err)
	}
}

func TestProfileAllowsDeclaredSecret(t *testing.T) {
	p, err := FromConfig(map[string]any{
		"capability": map[string]any{
			"filesystem": "none",
			"process":    "declared-command",
			"secrets":    []any{"${token}"},
		},
	})
	if err != nil {
		t.Fatalf("FromConfig() error = %v", err)
	}
	if err := p.Validate("/workspace"); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if !p.AllowsSecret("${token}") || p.AllowsSecret("other") {
		t.Fatalf("secret declaration mismatch: %+v", p.Secrets)
	}
}
