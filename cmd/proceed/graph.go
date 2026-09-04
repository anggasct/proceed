package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"proceed/internal/improvement"
	"proceed/internal/query/export"
	"proceed/internal/query/why"
	"proceed/internal/store"
)

const graphUsage = `usage:
  proceed graph inspect <run-id> [--data-dir <dir>] [--config <file>]
  proceed graph why <run-id> <node-id> [--data-dir <dir>] [--config <file>]
  proceed graph export <run-id> --format mermaid|json [--data-dir <dir>] [--config <file>]
  proceed graph improvement <graph> [--data-dir <dir>] [--config <file>]
  proceed graph list [--status <status>] [--limit <n>] [--data-dir <dir>] [--config <file>]
`

func cmdGraph(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, graphUsage)
		return exitUsage
	}
	subcommand := args[0]
	if subcommand != "inspect" && subcommand != "why" && subcommand != "export" && subcommand != "improvement" && subcommand != "list" {
		fmt.Fprintf(stderr, "proceed graph: unknown subcommand %q\n%s", subcommand, graphUsage)
		return exitUsage
	}
	if subcommand == "export" {
		return cmdGraphExport(args[1:], stdout, stderr)
	}
	if subcommand == "list" {
		return cmdGraphList(args[1:], stdout, stderr)
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
	case "improvement":
		payload, err = improvement.New(st).GetOverview(context.Background(), positional[0])
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

func cmdGraphExport(args []string, stdout, stderr io.Writer) int {
	var format string
	var filtered []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--format" {
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "proceed graph export: --format requires a value")
				return exitUsage
			}
			format = args[i+1]
			i++
		} else if len(args[i]) > 9 && args[i][:9] == "--format=" {
			format = args[i][9:]
		} else {
			filtered = append(filtered, args[i])
		}
	}
	if format == "" {
		fmt.Fprintln(stderr, "proceed graph export: --format mermaid|json is required")
		return exitUsage
	}
	if err := validateExportFormat(format); err != nil {
		fmt.Fprintln(stderr, err.Error())
		return exitUsage
	}
	flags, positional, err := parseCommonFlags(filtered)
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
	out, err := runExport(context.Background(), st, positional[0], format)
	if err != nil {
		return printClassified(err, stderr)
	}
	if _, err := stdout.Write(out); err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	if len(out) == 0 || out[len(out)-1] != '\n' {
		fmt.Fprintln(stdout, "")
	}
	return exitOK
}

func validateExportFormat(format string) error {
	if err := export.ValidateFormat(format); err != nil {
		return fmt.Errorf("proceed graph export: unknown format %q: must be mermaid or json", format)
	}
	return nil
}

func runExport(ctx context.Context, st *store.Store, runID, format string) ([]byte, error) {
	return export.Export(ctx, st, runID, format)
}

func cmdGraphList(args []string, stdout, stderr io.Writer) int {
	var status, limitRaw string
	limit := 50
	var filtered []string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--status":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "proceed graph list: --status requires a value")
				return exitUsage
			}
			status = args[i+1]
			i++
		case len(args[i]) > 9 && args[i][:9] == "--status=":
			status = args[i][9:]
		case args[i] == "--limit":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "proceed graph list: --limit requires a value")
				return exitUsage
			}
			limitRaw = args[i+1]
			i++
		case len(args[i]) > 8 && args[i][:8] == "--limit=":
			limitRaw = args[i][8:]
		default:
			filtered = append(filtered, args[i])
		}
	}
	if limitRaw != "" {
		parsed, err := strconv.Atoi(limitRaw)
		if err != nil {
			return printClassified(store.NewCodeError("GRAPH_INVALID", "limit must be an integer"), stderr)
		}
		limit = parsed
	}
	flags, positional, err := parseCommonFlags(filtered)
	if err != nil || len(positional) != 0 {
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

	summaries, err := st.ListRuns(context.Background(), status, limit)
	if err != nil {
		return printClassified(err, stderr)
	}
	encoded, err := json.MarshalIndent(store.RunList{Runs: summaries}, "", "  ")
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	fmt.Fprintln(stdout, string(encoded))
	return exitOK
}
