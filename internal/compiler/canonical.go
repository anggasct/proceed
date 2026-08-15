package compiler

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	yaml "gopkg.in/yaml.v3"
)

func CanonicalJSON(src []byte) ([]byte, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(src, &root); err != nil {
		return nil, graphInvalid(Diagnostic{Rule: RuleParse, Message: err.Error()})
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return nil, graphInvalid(Diagnostic{Rule: RuleParse, Message: "empty document"})
	}
	var out []byte
	if err := encodeNode(root.Content[0], false, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func DefinitionDigest(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func encodeNode(n *yaml.Node, inExtension bool, out *[]byte) error {
	if n.Kind == yaml.AliasNode {
		return encodeNode(n.Alias, inExtension, out)
	}
	switch n.Kind {
	case yaml.ScalarNode:
		return encodeScalar(n, inExtension, out)
	case yaml.SequenceNode:
		*out = append(*out, '[')
		for i, item := range n.Content {
			if i > 0 {
				*out = append(*out, ',')
			}
			if err := encodeNode(item, inExtension, out); err != nil {
				return err
			}
		}
		*out = append(*out, ']')
		return nil
	case yaml.MappingNode:
		type pair struct {
			key   string
			value *yaml.Node
		}
		pairs := make([]pair, 0, len(n.Content)/2)
		for i := 0; i+1 < len(n.Content); i += 2 {
			key := n.Content[i]
			if key.Kind == yaml.AliasNode {
				key = key.Alias
			}
			pairs = append(pairs, pair{key: key.Value, value: n.Content[i+1]})
		}
		sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].key < pairs[j].key })
		*out = append(*out, '{')
		for i, p := range pairs {
			if i > 0 {
				*out = append(*out, ',')
			}
			encodeString(p.key, out)
			*out = append(*out, ':')
			if err := encodeNode(p.value, inExtension || strings.HasPrefix(p.key, "x-"), out); err != nil {
				return err
			}
		}
		*out = append(*out, '}')
		return nil
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			*out = append(*out, 'n', 'u', 'l', 'l')
			return nil
		}
		return encodeNode(n.Content[0], inExtension, out)
	default:
		return graphInvalid(Diagnostic{Rule: RuleParse, Message: fmt.Sprintf("unsupported node kind %d", n.Kind)})
	}
}

func encodeScalar(n *yaml.Node, inExtension bool, out *[]byte) error {
	switch n.Tag {
	case "!!null":
		*out = append(*out, 'n', 'u', 'l', 'l')
		return nil
	case "!!bool":
		if strings.EqualFold(n.Value, "true") {
			*out = append(*out, 't', 'r', 'u', 'e')
		} else {
			*out = append(*out, 'f', 'a', 'l', 's', 'e')
		}
		return nil
	case "!!int":
		v, err := strconv.ParseInt(n.Value, 0, 64)
		if err != nil {
			return graphInvalid(Diagnostic{Rule: RuleParse, Message: fmt.Sprintf("integer %q out of range", n.Value)})
		}
		*out = strconv.AppendInt(*out, v, 10)
		return nil
	case "!!float":
		if !inExtension {
			return graphInvalid(Diagnostic{Rule: RuleParse, Message: "floats are prohibited outside x-* fields"})
		}
		v, err := strconv.ParseFloat(n.Value, 64)
		if err != nil {
			return graphInvalid(Diagnostic{Rule: RuleParse, Message: fmt.Sprintf("invalid float %q", n.Value)})
		}
		*out = strconv.AppendFloat(*out, v, 'g', -1, 64)
		return nil
	default:
		encodeString(n.Value, out)
		return nil
	}
}

func encodeString(s string, out *[]byte) {
	*out = append(*out, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			*out = append(*out, '\\', '"')
		case c == '\\':
			*out = append(*out, '\\', '\\')
		case c == '\n':
			*out = append(*out, '\\', 'n')
		case c == '\r':
			*out = append(*out, '\\', 'r')
		case c == '\t':
			*out = append(*out, '\\', 't')
		case c == '\b':
			*out = append(*out, '\\', 'b')
		case c == '\f':
			*out = append(*out, '\\', 'f')
		case c < 0x20:
			*out = append(*out, '\\', 'u', '0', '0',
				hexDigits[c>>4], hexDigits[c&0xf])
		default:
			*out = append(*out, c)
		}
	}
	*out = append(*out, '"')
}

var hexDigits = [16]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'}
