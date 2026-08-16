package compiler

import (
	"errors"
	"strings"
)

const CodeGraphInvalid = "GRAPH_INVALID"

type Diagnostic struct {
	Rule     string
	Location string
	Message  string
}

type Error struct {
	Code        string
	Diagnostics []Diagnostic
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString(e.Code)
	for _, d := range e.Diagnostics {
		b.WriteString("\n")
		b.WriteString(d.Rule)
		if d.Location != "" {
			b.WriteString(" ")
			b.WriteString(d.Location)
		}
		b.WriteString(": ")
		b.WriteString(d.Message)
	}
	return b.String()
}

func graphInvalid(diags ...Diagnostic) *Error {
	return &Error{Code: CodeGraphInvalid, Diagnostics: diags}
}

func AsGraphInvalid(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) && e.Code == CodeGraphInvalid {
		return e, true
	}
	return nil, false
}
