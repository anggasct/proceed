package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"proceed/internal/compiler"
	"proceed/internal/controller"
	"proceed/internal/store"
)

const scheduleUsage = `usage:
  proceed schedule add --name <name> --graph <file> --cron "<cron>" [--data-dir <dir>] [--config <file>]
  proceed schedule list [--data-dir <dir>] [--config <file>]
  proceed schedule pause <name> [--data-dir <dir>] [--config <file>]
  proceed schedule resume <name> [--data-dir <dir>] [--config <file>]
  proceed schedule remove <name> [--data-dir <dir>] [--config <file>]
`

func cmdSchedule(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, scheduleUsage)
		return exitUsage
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "add":
		return cmdScheduleAdd(rest, stdout, stderr)
	case "list":
		return cmdScheduleList(rest, stdout, stderr)
	case "pause":
		return cmdSchedulePause(rest, false, stdout, stderr)
	case "resume":
		return cmdSchedulePause(rest, true, stdout, stderr)
	case "remove":
		return cmdScheduleRemove(rest, stdout, stderr)
	default:
		fmt.Fprint(stderr, scheduleUsage)
		return exitUsage
	}
}

type scheduleFlags struct {
	name       string
	graph      string
	cron       string
	configPath string
	dataDir    string
	positional []string
}

func parseScheduleFlags(args []string) (scheduleFlags, error) {
	var f scheduleFlags
	for i := 0; i < len(args); i++ {
		arg := args[i]
		value := ""
		hasValue := false
		if eq := strings.IndexByte(arg, '='); eq > 0 && strings.HasPrefix(arg, "--") {
			value = arg[eq+1:]
			arg = arg[:eq]
			hasValue = true
		}
		switch arg {
		case "--name", "--graph", "--cron", "--data-dir", "--config":
			if !hasValue {
				if i+1 >= len(args) {
					return f, fmt.Errorf("%s requires a value", arg)
				}
				i++
				value = args[i]
			}
			switch arg {
			case "--name":
				f.name = value
			case "--graph":
				f.graph = value
			case "--cron":
				f.cron = value
			case "--data-dir":
				f.dataDir = value
			case "--config":
				f.configPath = value
			}
		default:
			f.positional = append(f.positional, arg)
		}
	}
	return f, nil
}

func openScheduleStore(f scheduleFlags) (*store.Store, error) {
	cfg, err := resolveConfig(cliFlags{configPath: f.configPath, dataDir: f.dataDir})
	if err != nil {
		return nil, err
	}
	return store.Open(filepath.Join(cfg.DataDir, "proceed.db"))
}

func cmdScheduleAdd(args []string, stdout, stderr io.Writer) int {
	f, err := parseScheduleFlags(args)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUsage
	}
	if f.name == "" || f.graph == "" || f.cron == "" {
		fmt.Fprint(stderr, scheduleUsage)
		return exitUsage
	}
	sched, err := controller.ParseCron(f.cron)
	if err != nil {
		return printClassified(err, stderr)
	}
	next, ok := sched.Next(time.Now())
	if !ok {
		return printClassified(controller.CronNeverMatches(f.cron), stderr)
	}
	src, err := os.ReadFile(f.graph)
	if err != nil {
		return printClassified(store.NewCodeError(store.CodeGraphInvalid,
			"graph file %s cannot be read", f.graph), stderr)
	}
	doc, err := compiler.Parse(src)
	if err != nil {
		return printClassified(err, stderr)
	}
	if err := compiler.Validate(doc); err != nil {
		return printClassified(err, stderr)
	}
	st, err := openScheduleStore(f)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()
	frozen, err := st.FreezeDefinition(context.Background(), f.graph, src, doc)
	if err != nil {
		if _, ok := compiler.AsGraphInvalid(err); ok {
			return printClassified(err, stderr)
		}
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	if err := st.AddSchedule(context.Background(), f.name, frozen.GraphVersionID, f.cron, next.UnixMilli()); err != nil {
		return printClassified(err, stderr)
	}
	fmt.Fprintf(stdout, "schedule %s added (next fire %s)\n", f.name, next.UTC().Format(time.RFC3339))
	return exitOK
}

func cmdScheduleList(args []string, stdout, stderr io.Writer) int {
	f, err := parseScheduleFlags(args)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUsage
	}
	st, err := openScheduleStore(f)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()
	schedules, err := st.ListSchedules(context.Background())
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	encoded, err := json.Marshal(map[string]any{"schedules": schedules})
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	fmt.Fprintln(stdout, string(encoded))
	return exitOK
}

func cmdSchedulePause(args []string, enable bool, stdout, stderr io.Writer) int {
	f, err := parseScheduleFlags(args)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUsage
	}
	if len(f.positional) != 1 || f.positional[0] == "" {
		fmt.Fprint(stderr, scheduleUsage)
		return exitUsage
	}
	name := f.positional[0]
	st, err := openScheduleStore(f)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()
	updated, err := st.SetScheduleEnabled(context.Background(), name, enable)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	if !updated {
		fmt.Fprintf(stderr, "proceed: schedule %s not found\n", name)
		return exitUnclassified
	}
	state := "paused"
	if enable {
		state = "resumed"
	}
	fmt.Fprintf(stdout, "schedule %s %s\n", name, state)
	return exitOK
}

func cmdScheduleRemove(args []string, stdout, stderr io.Writer) int {
	f, err := parseScheduleFlags(args)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUsage
	}
	if len(f.positional) != 1 || f.positional[0] == "" {
		fmt.Fprint(stderr, scheduleUsage)
		return exitUsage
	}
	name := f.positional[0]
	st, err := openScheduleStore(f)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()
	removed, err := st.RemoveSchedule(context.Background(), name)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	if !removed {
		fmt.Fprintf(stderr, "proceed: schedule %s not found\n", name)
		return exitUnclassified
	}
	fmt.Fprintf(stdout, "schedule %s removed\n", name)
	return exitOK
}
