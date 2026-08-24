package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"proceed/internal/compiler"
	"proceed/internal/store"
)

func cmdValidate(args []string, stdout, stderr io.Writer) int {
	var positional []string
	dataDir := ".proceed"
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--data-dir" || args[i] == "-data-dir":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "proceed validate: --data-dir requires a value")
				return 2
			}
			i++
			dataDir = args[i]
		case strings.HasPrefix(args[i], "--data-dir="):
			dataDir = strings.TrimPrefix(args[i], "--data-dir=")
		default:
			positional = append(positional, args[i])
		}
	}
	if len(positional) != 1 {
		fmt.Fprintln(stderr, "usage: proceed validate <file> [--data-dir <dir>]")
		return 2
	}
	path := positional[0]
	src, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return 1
	}
	doc, err := compiler.Parse(src)
	if err != nil {
		return printGraphInvalid(err, stderr)
	}
	if err := compiler.Validate(doc); err != nil {
		return printGraphInvalid(err, stderr)
	}
	st, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return 1
	}
	defer st.Close()
	frozen, err := st.FreezeDefinition(context.Background(), path, src, doc)
	if err != nil {
		if _, ok := compiler.AsGraphInvalid(err); ok {
			return printGraphInvalid(err, stderr)
		}
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return 1
	}
	state := "frozen"
	if !frozen.Created {
		state = "already frozen"
	}
	fmt.Fprintf(stdout, "%s %s nodes=%d edges=%d version=%s (%s)\n",
		doc.Name, frozen.Digest, len(doc.Nodes), len(doc.Edges), frozen.GraphVersionID, state)
	return 0
}

func printGraphInvalid(err error, stderr io.Writer) int {
	fmt.Fprintln(stderr, err.Error())
	return exitCodeForClass("GRAPH_INVALID")
}
