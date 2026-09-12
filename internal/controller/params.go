package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"proceed/internal/compiler"
	"proceed/internal/executor"
	"proceed/internal/store"
)

type ParamBinding struct {
	Name      string
	Value     string
	SecretRef bool
}

type BoundRunParams struct {
	Digest string
	Values []store.RunParamValue
	Refs   map[string]string
	Texts  map[string]string
}

func (b *BoundRunParams) Start() *store.RunParamsStart {
	if b == nil {
		return nil
	}
	return &store.RunParamsStart{Digest: b.Digest, Values: b.Values}
}

func paramInvalid(format string, args ...any) error {
	return &compiler.Error{
		Code: compiler.CodeGraphInvalid,
		Diagnostics: []compiler.Diagnostic{{
			Rule:     compiler.RuleParamDeclaration,
			Location: "params",
			Message:  fmt.Sprintf(format, args...),
		}},
	}
}

func BindRunParams(decls []compiler.Param, bindings []ParamBinding) (*BoundRunParams, error) {
	bound := map[string]ParamBinding{}
	for _, b := range bindings {
		if b.Name == "" {
			return nil, paramInvalid("param name must not be empty")
		}
		bound[b.Name] = b
	}
	out := &BoundRunParams{Refs: map[string]string{}, Texts: map[string]string{}}
	digestValues := map[string]any{}
	for i := range decls {
		d := &decls[i]
		b, isBound := bound[d.Name]
		delete(bound, d.Name)
		var text, ref string
		switch {
		case isBound:
			t, r, err := coerceParam(d, b)
			if err != nil {
				return nil, err
			}
			text, ref = t, r
		case d.Required:
			return nil, paramInvalid("missing required param %q", d.Name)
		case d.HasDefault:
			text = d.Default
			if d.Type == "secret" {
				r, ok := compiler.ParseSecretRef(d.Default)
				if !ok {
					return nil, paramInvalid("default for secret param %q must be a ${NAME} reference", d.Name)
				}
				ref = r
			}
		default:
			continue
		}
		value := store.RunParamValue{Name: d.Name, Type: d.Type}
		if d.Type == "secret" {
			out.Refs[d.Name] = ref
		} else {
			out.Texts[d.Name] = text
			value.Value = &text
			switch d.Type {
			case "int", "float":
				digestValues[d.Name] = json.Number(text)
			case "bool":
				digestValues[d.Name] = text == "true"
			default:
				digestValues[d.Name] = text
			}
		}
		out.Values = append(out.Values, value)
	}
	for name := range bound {
		return nil, paramInvalid("param %q is not declared", name)
	}
	digest, err := json.Marshal(digestValues)
	if err != nil {
		return nil, err
	}
	out.Digest = string(digest)
	return out, nil
}

func coerceParam(d *compiler.Param, b ParamBinding) (text, ref string, err error) {
	switch d.Type {
	case "string":
		return b.Value, "", nil
	case "int":
		v, perr := strconv.ParseInt(b.Value, 10, 64)
		if perr != nil {
			return "", "", paramInvalid("param %q must be an integer", d.Name)
		}
		return strconv.FormatInt(v, 10), "", nil
	case "float":
		v, perr := strconv.ParseFloat(b.Value, 64)
		if perr != nil {
			return "", "", paramInvalid("param %q must be a number", d.Name)
		}
		return strconv.FormatFloat(v, 'g', -1, 64), "", nil
	case "bool":
		if b.Value != "true" && b.Value != "false" {
			return "", "", paramInvalid("param %q must be true or false", d.Name)
		}
		return b.Value, "", nil
	case "secret":
		name := ""
		if b.SecretRef {
			name = b.Value
		} else if r, ok := compiler.ParseSecretRef(b.Value); ok {
			name = r
		}
		if name == "" || !compiler.IsValidName(name) {
			return "", "", paramInvalid("secret param %q requires a ${NAME} reference", d.Name)
		}
		return "", name, nil
	}
	return "", "", paramInvalid("param %q has unknown type %q", d.Name, d.Type)
}

func (c *Controller) interpolateNodeParams(ctx context.Context, runID, graphVersionID string, kind executor.Kind, cfg map[string]any) error {
	rawExec, ok := cfg["executor"].(map[string]any)
	if !ok {
		return nil
	}
	rows, err := c.store.RunParams(ctx, runID)
	if err != nil {
		return err
	}
	refs := c.paramRefsFor(runID)
	replaceIn := func(s string) (string, error) {
		if !compiler.HasParamRef(s) {
			return s, nil
		}
		return compiler.ReplaceParamRefs(s, func(name string) (string, error) {
			return c.resolveParamValue(ctx, runID, graphVersionID, name, rows, refs)
		})
	}
	switch kind {
	case executor.Shell:
		if command, ok := rawExec["command"].([]any); ok {
			for i, item := range command {
				s, ok := item.(string)
				if !ok {
					continue
				}
				replaced, err := replaceIn(s)
				if err != nil {
					return err
				}
				command[i] = replaced
			}
		}
		if env, ok := rawExec["x-proceed-env"].(map[string]any); ok {
			for k, v := range env {
				s, ok := v.(string)
				if !ok {
					continue
				}
				replaced, err := replaceIn(s)
				if err != nil {
					return err
				}
				env[k] = replaced
			}
		}
	case executor.HTTP:
		if u, ok := rawExec["url"].(string); ok {
			replaced, err := replaceIn(u)
			if err != nil {
				return err
			}
			rawExec["url"] = replaced
		}
		if raw, ok := rawExec["body"]; ok {
			replaced, err := replaceParamValue(raw, replaceIn)
			if err != nil {
				return err
			}
			rawExec["body"] = replaced
		}
	}
	return nil
}

func (c *Controller) resolveParamValue(ctx context.Context, runID, graphVersionID, name string, rows map[string]store.RunParamRow, refs map[string]string) (string, error) {
	row, ok := rows[name]
	if !ok {
		return "", fmt.Errorf("param %q has no bound value", name)
	}
	if row.Type != "secret" && row.Value.Valid {
		return row.Value.String, nil
	}
	ref := refs[name]
	if ref == "" {
		decls, err := c.store.GraphParams(ctx, graphVersionID)
		if err != nil {
			return "", err
		}
		for _, d := range decls {
			if d.Name == name && d.HasDefault {
				if r, ok := compiler.ParseSecretRef(d.Default); ok {
					ref = r
				}
			}
		}
	}
	if ref == "" {
		ref = name
	}
	if c.cfg.Secrets == nil {
		return "", fmt.Errorf("secret param %q cannot be resolved: no secret resolver configured", name)
	}
	value, err := c.cfg.Secrets.Resolve(ctx, ref)
	if err != nil {
		return "", fmt.Errorf("secret param %q could not be resolved", name)
	}
	return string(value), nil
}

func replaceParamValue(v any, replace func(string) (string, error)) (any, error) {
	switch x := v.(type) {
	case string:
		return replace(x)
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			replaced, err := replaceParamValue(item, replace)
			if err != nil {
				return nil, err
			}
			out[i] = replaced
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			key, err := replace(k)
			if err != nil {
				return nil, err
			}
			replaced, err := replaceParamValue(val, replace)
			if err != nil {
				return nil, err
			}
			out[key] = replaced
		}
		return out, nil
	default:
		return v, nil
	}
}
