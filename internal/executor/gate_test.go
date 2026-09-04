package executor

import (
	"testing"
)

func gateConfig(scope string, expires any) map[string]any {
	exec := map[string]any{"kind": "human_approval", "scope": scope}
	if expires != nil {
		exec["expires_in_ms"] = expires
	}
	return map[string]any{"executor": exec}
}

func TestParseGate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     map[string]any
		scope   string
		expires int64
		wantErr bool
	}{
		{"valid with expiry", gateConfig("deploy-prod", float64(60000)), "deploy-prod", 60000, false},
		{"valid without expiry", gateConfig("deploy-prod", nil), "deploy-prod", 0, false},
		{"missing executor", map[string]any{}, "", 0, true},
		{"missing scope", gateConfig("", nil), "", 0, true},
		{"control chars in scope", gateConfig("deploy\tx", nil), "", 0, true},
		{"scope too long", gateConfig(string(make([]byte, 300)), nil), "", 0, true},
		{"negative expiry", gateConfig("s", float64(-1)), "", 0, true},
		{"fractional expiry", gateConfig("s", float64(1.5)), "", 0, true},
		{"scope wrong type", map[string]any{"executor": map[string]any{"scope": 7}}, "", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gate, err := ParseGate(tc.cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseGate(%v) = %+v, want error", tc.cfg, gate)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseGate(%v): %v", tc.cfg, err)
			}
			if gate.Scope != tc.scope || gate.ExpiresInMs != tc.expires {
				t.Errorf("gate = %+v, want scope %q expires %d", gate, tc.scope, tc.expires)
			}
		})
	}
}
