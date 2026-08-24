package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "proceed.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestResolveDefaultsWhenNoFile(t *testing.T) {
	cfg, err := Resolve("", func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != DefaultDataDir || cfg.Bind != DefaultBind {
		t.Fatalf("defaults = %q %q", cfg.DataDir, cfg.Bind)
	}
	if !cfg.LoopbackBind() {
		t.Fatal("default bind must be loopback")
	}
}

func TestPrecedenceFlagBeatsEnvBeatsFileBeatsDefault(t *testing.T) {
	path := writeConfig(t, `
data_dir: ./from-file
bind: 127.0.0.1:7001
`)
	cfg, err := Resolve(path, func(key string) string {
		if key == "PROCEED_DATA_DIR" {
			return "./from-env"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != "./from-env" {
		t.Fatalf("env must beat file: %q", cfg.DataDir)
	}
	if cfg.Bind != "127.0.0.1:7001" {
		t.Fatalf("file must beat default: %q", cfg.Bind)
	}

	cfg.DataDir = "./from-flag"
	if cfg.DataDir != "./from-flag" {
		t.Fatal("flag override failed")
	}

	empty, err := Resolve("", func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if empty.Bind != DefaultBind {
		t.Fatalf("default must be lowest: %q", empty.Bind)
	}
}

func TestNonLoopbackBindIsExplicitConfig(t *testing.T) {
	cfg, err := Resolve(writeConfig(t, "bind: 0.0.0.0:7331"), func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LoopbackBind() {
		t.Fatal("0.0.0.0 must not count as loopback")
	}
	if _, err := Resolve(writeConfig(t, "bind: no-host"), func(string) string { return "" }); err == nil {
		t.Fatal("invalid bind must fail")
	}
}

func TestTokenValidation(t *testing.T) {
	path := writeConfig(t, `
tokens:
  - name: viewer
    token: secret-one
    scopes: [read]
  - name: operator
    token: secret-two
    scopes: [read, run, approve, admin]
`)
	cfg, err := Resolve(path, func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	scopes, ok := cfg.TokenScopes("secret-two")
	if !ok || len(scopes) != 4 {
		t.Fatalf("operator scopes = %v", scopes)
	}
	if _, ok := cfg.TokenScopes("wrong"); ok {
		t.Fatal("unknown token must not resolve")
	}

	if _, err := Resolve(writeConfig(t, `
tokens:
  - name: bad
    token: x
    scopes: [root]
`), func(string) string { return "" }); err == nil {
		t.Fatal("unknown scope must fail")
	}
	if _, err := Resolve(writeConfig(t, `
tokens:
  - name: dup
    token: x
    scopes: [read]
  - name: dup
    token: y
    scopes: [read]
`), func(string) string { return "" }); err == nil {
		t.Fatal("duplicate token name must fail")
	}
	if _, err := Resolve(writeConfig(t, `
tokens:
  - name: none
    token: x
    scopes: []
`), func(string) string { return "" }); err == nil {
		t.Fatal("scopeless token must fail")
	}
}
