//go:build !linux

package shell

import (
	"errors"
	"os/exec"
)

func configureCommand(cmd *exec.Cmd) error {
	return errors.New("shell sandbox requires Linux")
}

func terminateProcessGroup(cmd *exec.Cmd) error {
	return errors.New("shell process groups require Linux")
}
