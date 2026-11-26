//go:build !linux && !darwin

package inbox

import "errors"

var errNoProcessTable = errors.New("the process table is not readable on this platform")

func platformParentPID(int) (int, error) { return 0, errNoProcessTable }

func platformEnviron(int) ([]string, error) { return nil, errNoProcessTable }
