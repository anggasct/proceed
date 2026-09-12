package config

import "testing"

func TestTriggerValidation(t *testing.T) {
	cases := []struct {
		name     string
		triggers []Trigger
		wantErr  string
	}{
		{"empty name", []Trigger{{Name: "", Secret: "s"}}, "require a name and a secret"},
		{"empty secret", []Trigger{{Name: "deploy", Secret: ""}}, "require a name and a secret"},
		{"duplicate name", []Trigger{{Name: "a", Secret: "s"}, {Name: "a", Secret: "t"}}, "duplicate trigger name"},
		{"valid", []Trigger{{Name: "a", Secret: "s"}, {Name: "b", Secret: "t"}}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{DataDir: DefaultDataDir, Bind: DefaultBind, Triggers: tc.triggers}
			err := cfg.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestTriggerSecretLookup(t *testing.T) {
	cfg := Config{Triggers: []Trigger{{Name: "deploy", Secret: "s3cret"}}}
	if v, ok := cfg.TriggerSecret("deploy"); !ok || v != "s3cret" {
		t.Fatalf("lookup = %q %v", v, ok)
	}
	if _, ok := cfg.TriggerSecret("missing"); ok {
		t.Fatal("missing trigger must not resolve")
	}
}
