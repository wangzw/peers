//go:build !linux && !darwin

package config

import "net/netip"

func defaultRouteAddresses() ([]netip.Addr, error) {
	return nil, nil
}
