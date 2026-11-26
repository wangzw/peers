package node

import "sort"

// Every Broker the suite starts gets a refill interval far beyond a run, so a
// filled burst sent back to back cannot see a refilled token. It also gets a
// one-second replacement interval instead of the product's 60: a hop spends at
// most one replacement interval, so the value must stay far below discBound(2)
// and dadAddressChangeBound (TestDiscoveryRevokeWithin60s and
// TestCardFollowsAddressChange).
//
// Only a build carrying the peers_testenv tag reads the variables; a release
// build ignores them.
const (
	testInboundLimitIntervalEnv     = "PEERS_TEST_INBOUND_LIMIT_INTERVAL"
	testInboundLimitInterval        = "1h"
	testDirectoryReplaceIntervalEnv = "PEERS_TEST_DIRECTORY_REPLACE_INTERVAL"
	testDirectoryReplaceInterval    = "1s"
)

// BaseEnv is the complete environment of every child process. Nothing is
// inherited from os.Environ, so a test run from inside a Claude Code session
// cannot pass that session's variables to the processes under test.
type BaseEnv struct {
	// Home becomes HOME.
	Home string
	// Path becomes PATH.
	Path string
	// Extra adds variables, for example PEERS_TEST_RUNTIME_ROOT.
	Extra map[string]string
}

// Build returns the environment as NAME=value strings, sorted by name. LANG is
// always C.UTF-8. A value of "" in more removes the variable, so a caller can
// drop one of the defaults or an extra.
func (e BaseEnv) Build(more map[string]string) []string {
	values := map[string]string{
		"HOME":                          e.Home,
		"PATH":                          e.Path,
		"LANG":                          "C.UTF-8",
		testInboundLimitIntervalEnv:     testInboundLimitInterval,
		testDirectoryReplaceIntervalEnv: testDirectoryReplaceInterval,
		// The test runner may itself sit in a Claude Code session's process
		// tree, so the Broker would otherwise be refused by the parent-chain
		// walk; only the peers_testenv build reads the switch.
		"PEERS_TEST_SKIP_ANCESTOR_SESSION_CHECK": "1",
	}
	for name, value := range e.Extra {
		setEnv(values, name, value)
	}
	for name, value := range more {
		setEnv(values, name, value)
	}

	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)

	env := make([]string, 0, len(names))
	for _, name := range names {
		env = append(env, name+"="+values[name])
	}
	return env
}

func setEnv(values map[string]string, name, value string) {
	if value == "" {
		delete(values, name)
		return
	}
	values[name] = value
}
