//go:build linux

package main

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func setDashboardRawMode(f *os.File) (func(), error) {
	fd := int(f.Fd())
	before, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return nil, fmt.Errorf("cannot read terminal mode: %w", err)
	}
	raw := *before
	raw.Lflag &^= unix.ICANON | unix.ECHO
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &raw); err != nil {
		return nil, fmt.Errorf("cannot set terminal mode: %w", err)
	}
	return func() {
		_ = unix.IoctlSetTermios(fd, unix.TCSETS, before)
	}, nil
}

func dashboardWidth(w io.Writer) int {
	f, ok := w.(*os.File)
	if !ok {
		return 80
	}
	ws, err := unix.IoctlGetWinsize(int(f.Fd()), unix.TIOCGWINSZ)
	if err != nil || ws.Col == 0 {
		return 80
	}
	return int(ws.Col)
}
