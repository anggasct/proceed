package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"proceed/internal/store"
)

const storeUsage = `usage:
  proceed store export --data-dir <dir> --output <archive>
  proceed store import --input <archive> --data-dir <dir>
`

type usageErr struct{ msg string }

func (e *usageErr) Error() string { return e.msg }

func cmdStore(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, storeUsage)
		return 2
	}
	var err error
	switch args[0] {
	case "export":
		err = runStoreExport(args[1:], stdout)
	case "import":
		err = runStoreImport(args[1:], stdout)
	default:
		fmt.Fprintf(stderr, "proceed store: unknown subcommand %q\n%s", args[0], storeUsage)
		return 2
	}
	if err != nil {
		var ue *usageErr
		if errors.As(err, &ue) {
			fmt.Fprintf(stderr, "proceed store: %v\n%s", err, storeUsage)
			return 2
		}
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return 1
	}
	return 0
}

func parseStoreFlags(args []string) (map[string]string, error) {
	flags := map[string]string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") && strings.Contains(arg, "=") {
			eq := strings.IndexByte(arg, '=')
			flags[arg[:eq]] = arg[eq+1:]
			continue
		}
		switch arg {
		case "--data-dir", "--output", "--input":
			if i+1 >= len(args) {
				return nil, &usageErr{msg: fmt.Sprintf("%s requires a value", arg)}
			}
			i++
			flags[arg] = args[i]
		default:
			return nil, &usageErr{msg: fmt.Sprintf("unexpected argument %q", arg)}
		}
	}
	return flags, nil
}

func runStoreExport(args []string, stdout io.Writer) error {
	flags, err := parseStoreFlags(args)
	if err != nil {
		return err
	}
	dataDir := flags["--data-dir"]
	if dataDir == "" {
		return &usageErr{msg: "--data-dir is required"}
	}
	output := flags["--output"]
	if output == "" {
		return &usageErr{msg: "--output is required"}
	}
	if err := store.Export(context.Background(), dataDir, output); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "exported %s\n", output)
	return nil
}

func runStoreImport(args []string, stdout io.Writer) error {
	flags, err := parseStoreFlags(args)
	if err != nil {
		return err
	}
	dataDir := flags["--data-dir"]
	if dataDir == "" {
		return &usageErr{msg: "--data-dir is required"}
	}
	input := flags["--input"]
	if input == "" {
		return &usageErr{msg: "--input is required"}
	}
	if err := store.Import(context.Background(), input, dataDir); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "imported %s into %s\n", input, dataDir)
	return nil
}
