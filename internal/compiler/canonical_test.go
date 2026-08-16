package compiler

import (
	"bytes"
	"strings"
	"testing"
)

func TestCanonicalJSONKeyOrderAndWhitespaceInsensitivity(t *testing.T) {
	a := "schema: proceed/v1\nname: demo\nnodes:\n  - id: a\n    type: task\nedges: []\n"
	b := "# comment\nnodes:\n  - type: task\n    id: a\nedges: []\nname: demo\nschema: proceed/v1\n"
	ca, err := CanonicalJSON([]byte(a))
	if err != nil {
		t.Fatal(err)
	}
	cb, err := CanonicalJSON([]byte(b))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ca, cb) {
		t.Errorf("canonical forms differ:\n%s\n%s", ca, cb)
	}
	want := `{"edges":[],"name":"demo","nodes":[{"id":"a","type":"task"}],"schema":"proceed/v1"}`
	if string(ca) != want {
		t.Errorf("canonical = %s, want %s", ca, want)
	}
}

func TestDefinitionDigestStableAcrossReformatting(t *testing.T) {
	base := mustRead(t, "testdata/customer-research.yaml")
	reformatted := strings.Replace(string(base), "schema: proceed/v1\nname: customer-research",
		"name: customer-research   # renamed block\nschema: 'proceed/v1'", 1)
	ca, err := CanonicalJSON(base)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := CanonicalJSON([]byte(reformatted))
	if err != nil {
		t.Fatal(err)
	}
	if DefinitionDigest(ca) != DefinitionDigest(cb) {
		t.Error("digest must be identical for reformatted equivalents")
	}
}

func TestDefinitionDigestChangesOnTopologyChange(t *testing.T) {
	base := string(mustRead(t, "testdata/customer-research.yaml"))
	changed := strings.Replace(base, "when: verified }", "when: approved }", 1)
	ca, err := CanonicalJSON([]byte(base))
	if err != nil {
		t.Fatal(err)
	}
	cb, err := CanonicalJSON([]byte(changed))
	if err != nil {
		t.Fatal(err)
	}
	if DefinitionDigest(ca) == DefinitionDigest(cb) {
		t.Error("digest must differ on topology change")
	}
}

func TestCanonicalJSONNumbers(t *testing.T) {
	cases := []struct {
		src  string
		want string
	}{
		{src: "a: 42\n", want: `{"a":42}`},
		{src: "a: 0x10\n", want: `{"a":16}`},
		{src: "a: 0o17\n", want: `{"a":15}`},
		{src: "a: -7\n", want: `{"a":-7}`},
		{src: "a: +7\n", want: `{"a":7}`},
		{src: "a: true\n", want: `{"a":true}`},
		{src: "a: False\n", want: `{"a":false}`},
		{src: "a: null\n", want: `{"a":null}`},
		{src: "a: 'quoted'\n", want: `{"a":"quoted"}`},
		{src: "a: [3, 1, 2]\n", want: `{"a":[3,1,2]}`},
		{src: "a:\n  b: 2\n  c: 1\n", want: `{"a":{"b":2,"c":1}}`},
		{src: "x-float: 1.5\n", want: `{"x-float":1.5}`},
		{src: "a:\n  x-f: 0.25\n", want: `{"a":{"x-f":0.25}}`},
	}
	for _, tc := range cases {
		got, err := CanonicalJSON([]byte(tc.src))
		if err != nil {
			t.Fatalf("%q: %v", tc.src, err)
		}
		if string(got) != tc.want {
			t.Errorf("CanonicalJSON(%q) = %s, want %s", tc.src, got, tc.want)
		}
	}
}

func TestCanonicalJSONRejectsFloatOutsideExtensions(t *testing.T) {
	_, err := CanonicalJSON([]byte("timeout: 1.5\n"))
	if err == nil {
		t.Fatal("expected float rejection")
	}
	requireRule(t, err, RuleParse, "")
}

func TestCanonicalJSONStringEscaping(t *testing.T) {
	got, err := CanonicalJSON([]byte("a: \"line\\nbreak \\\"quoted\\\" <tag> é\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":"line\nbreak \"quoted\" <tag> é"}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestCanonicalJSONAliasesResolve(t *testing.T) {
	src := "base: &b\n  x: 1\nuse: *b\n"
	got, err := CanonicalJSON([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"base":{"x":1},"use":{"x":1}}`
	if string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
