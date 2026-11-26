//go:build !linux

package harness

import "errors"

func processStartTime(int) (uint64, error) {
	return 0, errors.ErrUnsupported
}

func processNamespace(int) (uint64, error) {
	return 0, errors.ErrUnsupported
}
