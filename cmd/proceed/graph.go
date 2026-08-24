package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"proceed/internal/store"
)

const graphUsage = `usage:
  proceed graph inspect <run-id> [--data-dir <dir>] [--config <file>]
`

func cmdGraph(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, graphUsage)
		return exitUsage
	}
	subcommand := args[0]
	if subcommand != "inspect" {
		fmt.Fprintf(stderr, "proceed graph: unknown subcommand %q\n%s", subcommand, graphUsage)
		return exitUsage
	}
	flags, positional, err := parseCommonFlags(args[1:])
	if err != nil || len(positional) != 1 {
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
	g, err := st.RuntimeGraph(context.Background(), positional[0])
	if err != nil {
		return printClassified(err, stderr)
	}
	encoded, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	fmt.Fprintln(stdout, string(encoded))
	return exitOK
}
