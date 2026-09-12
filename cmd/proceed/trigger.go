package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"proceed/internal/compiler"
	"proceed/internal/controller"
	"proceed/internal/store"
)

const triggerUsage = `usage:
  proceed trigger add --name <name> --graph <file> [--data-dir <dir>] [--config <file>]
  proceed trigger list [--data-dir <dir>] [--config <file>]
  proceed trigger remove <name> [--data-dir <dir>] [--config <file>]
`

func cmdTrigger(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, triggerUsage)
		return exitUsage
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "add":
		return cmdTriggerAdd(rest, stdout, stderr)
	case "list":
		return cmdTriggerList(rest, stdout, stderr)
	case "remove":
		return cmdTriggerRemove(rest, stdout, stderr)
	default:
		fmt.Fprint(stderr, triggerUsage)
		return exitUsage
	}
}

type triggerFlags struct {
	name       string
	graph      string
	configPath string
	dataDir    string
	positional []string
}

func parseTriggerFlags(args []string) (triggerFlags, error) {
	var f triggerFlags
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
		case "--name", "--graph", "--data-dir", "--config":
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

func openTriggerStore(f triggerFlags) (*store.Store, string, error) {
	cfg, err := resolveConfig(cliFlags{configPath: f.configPath, dataDir: f.dataDir})
	if err != nil {
		return nil, "", err
	}
	st, err := store.Open(filepath.Join(cfg.DataDir, "proceed.db"))
	if err != nil {
		return nil, cfg.DataDir, err
	}
	return st, cfg.DataDir, nil
}

func cmdTriggerAdd(args []string, stdout, stderr io.Writer) int {
	f, err := parseTriggerFlags(args)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUsage
	}
	if f.name == "" || f.graph == "" {
		fmt.Fprint(stderr, triggerUsage)
		return exitUsage
	}
	src, err := os.ReadFile(f.graph)
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
	st, _, err := openTriggerStore(f)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()

	c, err := controller.New(st, controller.DefaultConfig(), buildPool(nil))
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	if err := c.ValidateLease(context.Background()); err != nil {
		return printClassified(err, stderr)
	}
	frozen, err := st.FreezeDefinition(context.Background(), f.graph, src, doc)
	if err != nil {
		c.ReleaseLease()
		if _, ok := compiler.AsGraphInvalid(err); ok {
			return printClassified(err, stderr)
		}
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	if err := st.AddWebhookTrigger(context.Background(), f.name, frozen.GraphVersionID); err != nil {
		c.ReleaseLease()
		return printClassified(err, stderr)
	}
	c.ReleaseLease()
	fmt.Fprintf(stdout, "trigger %s bound to version %s\n", f.name, frozen.GraphVersionID)
	return exitOK
}

func cmdTriggerList(args []string, stdout, stderr io.Writer) int {
	f, err := parseTriggerFlags(args)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUsage
	}
	st, _, err := openTriggerStore(f)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()
	triggers, err := st.ListWebhookTriggers(context.Background())
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	encoded, err := json.Marshal(map[string]any{"triggers": triggers})
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	fmt.Fprintln(stdout, string(encoded))
	return exitOK
}

func cmdTriggerRemove(args []string, stdout, stderr io.Writer) int {
	f, err := parseTriggerFlags(args)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUsage
	}
	if len(f.positional) != 1 || f.positional[0] == "" {
		fmt.Fprint(stderr, triggerUsage)
		return exitUsage
	}
	name := f.positional[0]
	st, _, err := openTriggerStore(f)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()
	removed, err := st.RemoveWebhookTrigger(context.Background(), name)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	if !removed {
		fmt.Fprintf(stderr, "proceed: trigger %s not found\n", name)
		return exitUnclassified
	}
	fmt.Fprintf(stdout, "trigger %s removed\n", name)
	return exitOK
}
