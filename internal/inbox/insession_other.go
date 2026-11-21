//go:build !linux && !darwin

package inbox

import "errors"

// errNoProcessTable reports that this platform has no process table reader, so
// only the current process's own environment can be checked.
var errNoProcessTable = errors.New("the process table is not readable on this platform")

func platformParentPID(int) (int, error) { return 0, errNoProcessTable }

func platformEnviron(int) ([]string, error) { return nil, errNoProcessTable }
