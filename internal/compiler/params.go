package compiler

import (
	"regexp"
	"strings"
)

const (
	RuleParamDeclaration = "E115"
	RuleParamReference   = "E116"
)

var paramTypeSet = map[string]bool{
	"string": true, "int": true, "float": true, "bool": true, "secret": true,
}

var paramRefPattern = regexp.MustCompile(`\{\{\s*params\.([A-Za-z0-9._-]+)\s*\}\}`)

func IsValidParamType(t string) bool { return paramTypeSet[t] }

func HasParamRef(s string) bool { return paramRefPattern.MatchString(s) }

func EachParamRef(s string, fn func(name string)) {
	for _, m := range paramRefPattern.FindAllStringSubmatch(s, -1) {
		fn(m[1])
	}
}

func ReplaceParamRefs(s string, resolve func(name string) (string, error)) (string, error) {
	var failed error
	out := paramRefPattern.ReplaceAllStringFunc(s, func(match string) string {
		if failed != nil {
			return match
		}
		name := paramRefPattern.FindStringSubmatch(match)[1]
		value, err := resolve(name)
		if err != nil {
			failed = err
			return match
		}
		return value
	})
	if failed != nil {
		return "", failed
	}
	return out, nil
}

func ParseSecretRef(value string) (string, bool) {
	if !strings.HasPrefix(value, "${") || !strings.HasSuffix(value, "}") {
		return "", false
	}
	name := value[2 : len(value)-1]
	if !isName(name) {
		return "", false
	}
	return name, true
}

func IsValidName(s string) bool { return isName(s) }
