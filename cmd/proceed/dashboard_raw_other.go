//go:build !linux

package main

import (
	"errors"
	"io"
	"os"
)

func setDashboardRawMode(f *os.File) (func(), error) {
	return nil, errors.New("dashboard single-key input requires Linux")
}

func dashboardWidth(w io.Writer) int {
	return 80
}
