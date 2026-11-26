//go:build peers_testenv

package main

import (
	"os"
	"strconv"
)

// parseProtocolOverrides reads the protocol versions an end-to-end test asks
// for; an absent, malformed or out-of-range value yields 0, meaning the
// generated packages' version.
func parseProtocolOverrides(getenv func(string) string) (local, link uint32) {
	return parseProtocolVersion(getenv("PEERS_TEST_LOCAL_PROTOCOL_VERSION")),
		parseProtocolVersion(getenv("PEERS_TEST_LINK_PROTOCOL_VERSION"))
}

func parseProtocolVersion(v string) uint32 {
	if v == "" {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(n)
}

// init replaces the test seam; the replacement reads the environment on every
// call, so a test can change a value before launching a process.
func init() {
	protocolOverrides = func() (local, link uint32) {
		return parseProtocolOverrides(os.Getenv)
	}
}
