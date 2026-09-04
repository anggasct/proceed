package executor

import (
	"fmt"
	"math"
	"strings"
)

const maxGateScopeLen = 256

type Gate struct {
	Scope       string
	ExpiresInMs int64
}

func ParseGate(cfg map[string]any) (Gate, error) {
	raw, ok := cfg["executor"].(map[string]any)
	if !ok {
		return Gate{}, fmt.Errorf("node has no executor config")
	}
	scope, _ := raw["scope"].(string)
	if scope == "" {
		return Gate{}, fmt.Errorf("human_approval executor requires scope")
	}
	if len(scope) > maxGateScopeLen || strings.ContainsFunc(scope, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return Gate{}, fmt.Errorf("human_approval scope must be a bounded single-line string")
	}
	var expires int64
	if v, ok := raw["expires_in_ms"].(float64); ok {
		if v < 0 || v != math.Trunc(v) || v > math.MaxInt64 {
			return Gate{}, fmt.Errorf("human_approval expires_in_ms must be a non-negative integer")
		}
		expires = int64(v)
	}
	return Gate{Scope: scope, ExpiresInMs: expires}, nil
}
