package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"proceed/internal/query/why"
	"proceed/internal/store"
)

const graphUsage = `usage:
  proceed graph inspect <run-id> [--data-dir <dir>] [--config <file>]
  proceed graph why <run-id> <node-id> [--data-dir <dir>] [--config <file>]
`

func cmdGraph(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, graphUsage)
		return exitUsage
	}
	subcommand := args[0]
	if subcommand != "inspect" && subcommand != "why" {
		fmt.Fprintf(stderr, "proceed graph: unknown subcommand %q\n%s", subcommand, graphUsage)
		return exitUsage
	}
	wantPositional := 1
	if subcommand == "why" {
		wantPositional = 2
	}
	flags, positional, err := parseCommonFlags(args[1:])
	if err != nil || len(positional) != wantPositional {
		fmt.Fprint(stderr, graphUsage)
		return exitUsage
	}
	cfg, err := resolveConfig(flags)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	st, err := store.Open(cfg.DataDir + "/proceed.db")
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()

	var payload any
	switch subcommand {
	case "inspect":
		payload, err = st.RuntimeGraph(context.Background(), positional[0])
	case "why":
		payload, err = why.New(st).Explain(context.Background(), positional[0], positional[1])
	}
	if err != nil {
		return printClassified(err, stderr)
	}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	fmt.Fprintln(stdout, string(encoded))
	return exitOK
}
