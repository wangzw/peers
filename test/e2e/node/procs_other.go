//go:build !linux

package node

import "errors"

// ListProcs returns errors.ErrUnsupported; it is only implemented on Linux.
func ListProcs(func(Proc) bool) ([]Proc, error) {
	return nil, errors.ErrUnsupported
}

// Ancestors returns errors.ErrUnsupported; it is only implemented on Linux.
func Ancestors(pid int) ([]int, error) {
	return nil, errors.ErrUnsupported
}
