package main

import (
	"fmt"
	"io"
	"os"
)

var version = "dev"

const usageText = `proceed — compile, run, and explain graphs of work

Usage:
  proceed <command> [arguments]

Commands:
  validate    Compile and validate a graph definition
  run         Execute a graph to terminal completion
  serve       Run the controller with the local HTTP API
  graph       inspect | why | export — read-only run views
  approve     Record an approval gate decision
  reconcile   Resolve an uncertain effect
  store       export | import — backup and restore

Run "proceed <command> -h" for command flags.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return 2
	}
	switch args[0] {
	case "-h", "--help", "help":
		fmt.Fprint(stdout, usageText)
		return 0
	case "-v", "--version", "version":
		fmt.Fprintln(stdout, "proceed "+version)
		return 0
	}
	if _, known := commands[args[0]]; known {
		switch args[0] {
		case "validate":
			return cmdValidate(args[1:], stdout, stderr)
		case "run":
			return cmdRun(args[1:], stdout, stderr)
		case "serve":
			return cmdServe(args[1:], stdout, stderr)
		case "graph":
			return cmdGraph(args[1:], stdout, stderr)
		case "store":
			return cmdStore(args[1:], stdout, stderr)
		default:
			fmt.Fprintf(stderr, "proceed %s: not implemented\n", args[0])
			return 1
		}
	}
	fmt.Fprintf(stderr, "proceed: unknown command %q\n\n", args[0])
	fmt.Fprint(stderr, usageText)
	return 2
}

var commands = map[string]struct{}{
	"validate":  {},
	"run":       {},
	"serve":     {},
	"graph":     {},
	"approve":   {},
	"reconcile": {},
	"store":     {},
}
