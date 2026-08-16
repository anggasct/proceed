package compiler

import (
	"strconv"

	yaml "gopkg.in/yaml.v3"
)

const CompiledSchemaVersion = "proceed.compiled/v1"

func NodeToJSONValue(n *yaml.Node) (any, error) {
	if n == nil {
		return nil, nil
	}
	if n.Kind == yaml.AliasNode {
		return NodeToJSONValue(n.Alias)
	}
	switch n.Kind {
	case yaml.ScalarNode:
		switch n.Tag {
		case "!!null":
			return nil, nil
		case "!!bool":
			return n.Value == "true" || n.Value == "True" || n.Value == "TRUE", nil
		case "!!int":
			return parseIntScalar(n)
		case "!!float":
			f, err := strconv.ParseFloat(n.Value, 64)
			if err != nil {
				return nil, graphInvalid(Diagnostic{Rule: RuleParse, Message: err.Error()})
			}
			return f, nil
		default:
			return n.Value, nil
		}
	case yaml.SequenceNode:
		out := make([]any, 0, len(n.Content))
		for _, item := range n.Content {
			v, err := NodeToJSONValue(item)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case yaml.MappingNode:
		out := make(map[string]any, len(n.Content)/2)
		for i := 0; i+1 < len(n.Content); i += 2 {
			key := n.Content[i]
			if key.Kind == yaml.AliasNode {
				key = key.Alias
			}
			v, err := NodeToJSONValue(n.Content[i+1])
			if err != nil {
				return nil, err
			}
			out[key.Value] = v
		}
		return out, nil
	default:
		return nil, graphInvalid(Diagnostic{Rule: RuleParse, Message: "unsupported node"})
	}
}

func parseIntScalar(n *yaml.Node) (any, error) {
	v, err := strconv.ParseInt(n.Value, 0, 64)
	if err != nil {
		return nil, graphInvalid(Diagnostic{Rule: RuleParse, Message: err.Error()})
	}
	return v, nil
}
