//go:build !linux

package agent

import (
	"errors"
	"os/exec"
)

func terminateProcessGroup(cmd *exec.Cmd) error {
	return errors.New("agent process groups require Linux")
}
