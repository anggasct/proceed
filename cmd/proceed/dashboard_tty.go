package main

import (
	"io"
	"os"

	isatty "github.com/mattn/go-isatty"
)

func isDashboardTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	return isatty.IsTerminal(f.Fd())
}

func isDashboardTerminalStdin(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	return isatty.IsTerminal(f.Fd())
}
