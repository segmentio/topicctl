package util

import (
	"os"

	"golang.org/x/term"
)

// InTerminal determines whether we're running in a terminal or not.
//
// Implementation from 	https://rosettacode.org/wiki/Check_output_device_is_a_terminal#Go.
func InTerminal() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}
