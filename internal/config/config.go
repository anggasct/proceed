package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultDataDir = "./.proceed"
	DefaultBind    = "127.0.0.1:7331"
)

var validScopes = map[string]bool{
	"read": true, "run": true, "approve": true, "admin": true, "event": true,
}

type Token struct {
	Name   string   `yaml:"name" json:"name"`
	Token  string   `yaml:"token" json:"-"`
	Scopes []string `yaml:"scopes" json:"scopes"`
}

type Trigger struct {
	Name   string `yaml:"name" json:"name"`
	Secret string `yaml:"secret" json:"-"`
}

type Config struct {
	DataDir   string            `yaml:"data_dir"`
	Bind      string            `yaml:"bind"`
	Tokens    []Token           `yaml:"tokens"`
	AgentCLIs map[string]string `yaml:"agent_clis"`
	Triggers  []Trigger         `yaml:"triggers"`
}

type fileConfig struct {
	DataDir   string            `yaml:"data_dir"`
	Bind      string            `yaml:"bind"`
	Tokens    []Token           `yaml:"tokens"`
	AgentCLIs map[string]string `yaml:"agent_clis"`
	Triggers  []Trigger         `yaml:"triggers"`
}

func Resolve(path string, getenv func(string) string) (Config, error) {
	cfg := Config{DataDir: DefaultDataDir, Bind: DefaultBind}
	if path != "" {
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			var fc fileConfig
			if err := yaml.Unmarshal(data, &fc); err != nil {
				return Config{}, fmt.Errorf("config file %s: %w", path, err)
			}
			if fc.DataDir != "" {
				cfg.DataDir = fc.DataDir
			}
			if fc.Bind != "" {
				cfg.Bind = fc.Bind
			}
			cfg.Tokens = fc.Tokens
			cfg.AgentCLIs = fc.AgentCLIs
			cfg.Triggers = fc.Triggers
		case os.IsNotExist(err):
		default:
			return Config{}, err
		}
	}
	if v := getenv("PROCEED_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := getenv("PROCEED_BIND"); v != "" {
		cfg.Bind = v
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("data dir must not be empty")
	}
	if err := validateBind(c.Bind); err != nil {
		return err
	}
	seenName := map[string]bool{}
	seenToken := map[string]bool{}
	for _, token := range c.Tokens {
		if token.Name == "" || token.Token == "" {
			return fmt.Errorf("token entries require a name and a token value")
		}
		if seenName[token.Name] {
			return fmt.Errorf("duplicate token name %q", token.Name)
		}
		if seenToken[token.Token] {
			return fmt.Errorf("duplicate token value for %q", token.Name)
		}
		seenName[token.Name] = true
		seenToken[token.Token] = true
		if len(token.Scopes) == 0 {
			return fmt.Errorf("token %q requires at least one scope", token.Name)
		}
		for _, scope := range token.Scopes {
			if !validScopes[scope] {
				return fmt.Errorf("token %q has unknown scope %q", token.Name, scope)
			}
		}
	}
	for name, path := range c.AgentCLIs {
		if name == "" {
			return fmt.Errorf("agent CLI entries require a name")
		}
		if path == "" || !filepath.IsAbs(path) {
			return fmt.Errorf("agent CLI %q requires an absolute binary path", name)
		}
	}
	seenTrigger := map[string]bool{}
	for _, trigger := range c.Triggers {
		if trigger.Name == "" || trigger.Secret == "" {
			return fmt.Errorf("trigger entries require a name and a secret")
		}
		if seenTrigger[trigger.Name] {
			return fmt.Errorf("duplicate trigger name %q", trigger.Name)
		}
		seenTrigger[trigger.Name] = true
	}
	return nil
}

func validateBind(bind string) error {
	host, port, err := net.SplitHostPort(bind)
	if err != nil {
		return fmt.Errorf("bind address %q must be host:port", bind)
	}
	if port == "" {
		return fmt.Errorf("bind address %q requires a port", bind)
	}
	if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("bind port %q must be numeric", port)
	}
	if host != "" && net.ParseIP(host) == nil && strings.ToLower(host) != "localhost" {
		return fmt.Errorf("bind host %q must be an IP address or localhost", host)
	}
	return nil
}

func (c *Config) LoopbackBind() bool {
	host, _, err := net.SplitHostPort(c.Bind)
	if err != nil {
		return false
	}
	if strings.ToLower(host) == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *Config) TokenScopes(token string) ([]string, bool) {
	for _, entry := range c.Tokens {
		if entry.Token == token {
			return entry.Scopes, true
		}
	}
	return nil, false
}

func (c *Config) TriggerSecret(name string) (string, bool) {
	for _, entry := range c.Triggers {
		if entry.Name == name {
			return entry.Secret, true
		}
	}
	return "", false
}
