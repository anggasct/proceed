package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"proceed/internal/compiler"
	"proceed/internal/config"
	"proceed/internal/controller"
	"proceed/internal/executor"
	httpexec "proceed/internal/executor/http"
	"proceed/internal/executor/shell"
	"proceed/internal/store"
)

const requestCancelTimeout = 5 * time.Second

const runUsage = `usage:
  proceed run <file> [--data-dir <dir>] [--config <file>]
`

type cliFlags struct {
	configPath string
	dataDir    string
	bind       string
}

func parseCommonFlags(args []string) (cliFlags, []string, error) {
	var f cliFlags
	var positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		var value string
		hasValue := false
		if strings.HasPrefix(arg, "--") && strings.Contains(arg, "=") {
			eq := strings.IndexByte(arg, '=')
			value = arg[eq+1:]
			arg = arg[:eq]
			hasValue = true
		}
		switch arg {
		case "--config", "--data-dir", "--bind":
			if !hasValue {
				if i+1 >= len(args) {
					return f, nil, fmt.Errorf("%s requires a value", arg)
				}
				i++
				value = args[i]
			}
			switch arg {
			case "--config":
				f.configPath = value
			case "--data-dir":
				f.dataDir = value
			case "--bind":
				f.bind = value
			}
		default:
			positional = append(positional, args[i])
		}
	}
	return f, positional, nil
}

func resolveConfig(f cliFlags) (config.Config, error) {
	path := f.configPath
	if path == "" {
		path = os.Getenv("PROCEED_CONFIG")
	}
	cfg, err := config.Resolve(path, os.Getenv)
	if err != nil {
		return cfg, err
	}
	if f.dataDir != "" {
		cfg.DataDir = f.dataDir
	}
	if f.bind != "" {
		cfg.Bind = f.bind
	}
	return cfg, nil
}

func buildPool() map[executor.Kind]executor.Executor {
	return map[executor.Kind]executor.Executor{
		executor.Shell: shell.New(),
		executor.HTTP:  httpexec.New(),
	}
}

func cmdRun(args []string, stdout, stderr io.Writer) int {
	flags, positional, err := parseCommonFlags(args)
	if err != nil || len(positional) != 1 {
		fmt.Fprint(stderr, runUsage)
		return exitUsage
	}
	cfg, err := resolveConfig(flags)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	path := positional[0]
	src, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	doc, err := compiler.Parse(src)
	if err != nil {
		return printClassified(err, stderr)
	}
	if err := compiler.Validate(doc); err != nil {
		return printClassified(err, stderr)
	}
	st, err := store.Open(cfg.DataDir + "/proceed.db")
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()
	frozen, err := st.FreezeDefinition(context.Background(), path, src, doc)
	if err != nil {
		if _, ok := compiler.AsGraphInvalid(err); ok {
			return printClassified(err, stderr)
		}
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}

	c, err := controller.New(st, controller.DefaultConfig(), buildPool())
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runID, err := c.Run(ctx, controller.RunInput{GraphVersionID: frozen.GraphVersionID})
	if err != nil {
		return printClassified(err, stderr)
	}
	fmt.Fprintf(stdout, "run %s started\n", runID)

	go func() {
		<-ctx.Done()
		cancelCtx, cancel := context.WithTimeout(context.Background(), requestCancelTimeout)
		defer cancel()
		_ = c.CancelRun(cancelCtx, runID)
	}()
	if err := c.Drain(ctx, runID); err != nil {
		if ctx.Err() != nil {
			return settleRunAfterInterrupt(st, runID, stderr)
		}
		return printClassified(err, stderr)
	}
	if ctx.Err() != nil {
		return settleRunAfterInterrupt(st, runID, stderr)
	}
	return runOutcomeExit(st, runID, stdout)
}

func runOutcomeExit(st *store.Store, runID string, stdout io.Writer) int {
	g, err := st.RuntimeGraph(context.Background(), runID)
	if err != nil {
		return exitCodeForError(err)
	}
	fmt.Fprintf(stdout, "run %s %s\n", g.RunID, g.Status)
	switch g.Status {
	case "completed":
		return exitOK
	case "cancelled":
		return exitCodeForClass("RUN_CANCELLED")
	case "failed":
		return failedRunExit(st, runID)
	default:
		return stalledRunExit(st, runID)
	}
}

func failedRunExit(st *store.Store, runID string) int {
	var reason string
	if err := st.DB().QueryRow(`
SELECT json_extract(ev.payload, '$.error') FROM event ev
WHERE ev.run_id = ? AND ev.type = 'node_failed'
ORDER BY ev.sequence DESC LIMIT 1`, runID).Scan(&reason); err == nil && reason != "" {
		if code := classFromText(reason); code != "" {
			return exitCodeForClass(code)
		}
	}
	return exitCodeForClass("NODE_FAILED")
}

func stalledRunExit(st *store.Store, runID string) int {
	var uncertain, waiting int
	if err := st.DB().QueryRow(`
SELECT
  COALESCE(SUM(CASE WHEN status = 'uncertain' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN status = 'waiting' THEN 1 ELSE 0 END), 0)
FROM run_node WHERE run_id = ?`, runID).Scan(&uncertain, &waiting); err == nil {
		if uncertain > 0 {
			return exitCodeForClass("EFFECT_UNCERTAIN")
		}
		if waiting > 0 {
			return exitCodeForClass("APPROVAL_REQUIRED")
		}
	}
	return exitUnclassified
}

func settleRunAfterInterrupt(st *store.Store, runID string, stderr io.Writer) int {
	g, err := st.RuntimeGraph(context.Background(), runID)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: interrupted; run state unavailable: %v\n", err)
		return exitCodeForError(err)
	}
	fmt.Fprintf(stderr, "proceed: interrupted; run %s %s\n", g.RunID, g.Status)
	return exitCodeForClass("RUN_CANCELLED")
}

func classFromText(text string) string {
	for class := range classExitCodes {
		if textContainsClass(text, class) {
			return class
		}
	}
	return ""
}

func printClassified(err error, stderr io.Writer) int {
	fmt.Fprintln(stderr, err.Error())
	return exitCodeForError(err)
}
