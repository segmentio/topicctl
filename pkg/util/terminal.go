package util

import (
	"os"

	"golang.org/x/crypto/ssh/terminal"
)

// InTerminal determines whether we're running in a terminal or not.
//
// Implementation from 	https://rosettacode.org/wiki/Check_output_device_is_a_terminal#Go.
func InTerminal() bool {
	return terminal.IsTerminal(int(os.Stdout.Fd()))
}
