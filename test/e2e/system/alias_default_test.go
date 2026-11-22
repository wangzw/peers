//go:build e2e && linux

package e2e

import (
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// aliasSysHostname must carry a domain, so the assertion proves the alias
	// keeps only the first label.
	aliasSysHostname = "hostA.example.com"
	// aliasSysSystemAlias is the system alias: the first hostname label.
	aliasSysSystemAlias = "hostA"
	// aliasSysUserAlias is the user alias: the first label and the
	// account running the Broker.
	aliasSysUserAlias = "hostA.alice"
	// aliasSysTopic is the topic both sessions join so each Broker forwards
	// the other's agents across the link.
	aliasSysTopic = "alias"
	// aliasSysAliceConfig is the user Broker's configuration file.
	aliasSysAliceConfig = "/home/alice/.peers/config.yaml"
	// aliasSysRelinkBound bounds the user Broker's redial after a
	// restart: link.reconnect_max is 2s and the link is up the moment the
	// system Broker lists alice's agent again, so 2s + 1s covers it.
	// No product timer is longer, so this is the whole wait.
	aliasSysRelinkBound = 3 * time.Second
	// aliasSysRecoverBound is the fallback for one observation after the link
	// is up. The observation is event driven and no product timer governs it.
	aliasSysRecoverBound = harness.EventTimeout
)

// TestREQBrokerPairing002_DefaultAliasHostAndAccount covers both forms of
// REQ-broker-pairing-002 on one host: the system Broker takes the first
// label of the hostname, and the user Broker run by alice takes that
// label, a dot and the account name. Neither listen port nor advertise enters
// either alias.
func TestREQBrokerPairing002_DefaultAliasHostAndAccount(t *testing.T) {
	if !syscontainer.InContainer() {
		// The system Broker needs root and the two Brokers need two
		// accounts, so this runs in the privileged systemd container.
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	// The hostname is set from the account database's host, not taken from
	// whatever name Docker gave the container.
	if err := syscall.Sethostname([]byte(aliasSysHostname)); err != nil {
		t.Fatalf("setting the hostname to %q: %v", aliasSysHostname, err)
	}

	// The image has no alice account, and the requirement names alice.
	mustRun(t, "root", nil, "useradd", "--create-home", "--shell", "/bin/sh", "alice")

	// The system Broker starts with the default configuration: an empty
	// broker block, with no alias and no listen override. The file must exist
	// for the root session's writes to be permitted. It takes the default port
	// 17443.
	systemBroker := syscontainer.StartSystemBroker(t, "broker: {}\n")
	t.Cleanup(func() { systemBroker.Stop(t) })

	// The user Broker needs its own port because 17443 belongs to the
	// system Broker. The link timers make the restart bound exact.
	writeAccountFile(t, "alice", aliasSysAliceConfig,
		"broker:\n  listen: \":17445\"\nlink:\n  reconnect_min: 1s\n  reconnect_max: 2s\n")
	userBroker := syscontainer.StartUserBroker(t, "alice")

	rootSession := syscontainer.NewSession(t, "root", "x1")
	aliceSession := syscontainer.NewSession(t, "alice", "a1")
	e2eSysWaitRegistered(t, rootSession)
	e2eSysWaitRegistered(t, aliceSession)
	e2eSysJoinTopic(t, rootSession, aliasSysTopic)
	e2eSysJoinTopic(t, aliceSession, aliasSysTopic)

	// alice is the dialer, so a restart of the user Broker makes it
	// redial the system Broker without a new connect_broker call.
	key := createPairingKey(t, rootSession)
	if alias := connectBroker(t, aliceSession, "127.0.0.1:17443", key); alias == "" {
		t.Fatal("connect_broker returned no alias")
	}

	// The system Broker: the hostname's first label alone.
	aliasSysWaitSuffix(t, aliceSession, "x1", "@"+aliasSysSystemAlias, aliasSysRelinkBound)
	// The user Broker: the first label and the account.
	aliasSysWaitSuffix(t, rootSession, "a1", "@"+aliasSysUserAlias, aliasSysRelinkBound)
	aliasSysRequireSelf(t, aliceSession, "a1", "@"+aliasSysUserAlias)

	// A message crosses the two Brokers by the addresses their default aliases
	// form, and each side sees the other's address as the sender.
	rootSelf := e2eSysSelfAddress(t, rootSession)
	aliceSelf := e2eSysSelfAddress(t, aliceSession)
	if text, isError := aliceSession.Call(t, "send_message", map[string]any{"to": rootSelf, "message": "alias-ping"}); isError {
		t.Fatalf("send_message from alice to root: %s", text)
	}
	ping, ok := rootSession.Fake.WaitFrame(aliasSysRelinkBound)
	if !ok {
		t.Fatalf("root's session did not receive the message within %s", aliasSysRelinkBound)
	}
	if ping.FromName != aliceSelf {
		t.Fatalf("the message is signed %q, want alice's address %q", ping.FromName, aliceSelf)
	}
	if !strings.Contains(ping.Content, "alias-ping") {
		t.Fatalf("the delivered content %q does not contain alias-ping", ping.Content)
	}
	if text, isError := rootSession.Call(t, "send_message", map[string]any{"to": ping.FromName, "message": "alias-pong"}); isError {
		t.Fatalf("send_message from root to alice: %s", text)
	}
	pong, ok := aliceSession.Fake.WaitFrame(aliasSysRelinkBound)
	if !ok {
		t.Fatalf("alice's session did not receive the reply within %s", aliasSysRelinkBound)
	}
	if pong.FromName != rootSelf {
		t.Fatalf("the reply is signed %q, want root's address %q", pong.FromName, rootSelf)
	}
	if !strings.Contains(pong.Content, "alias-pong") {
		t.Fatalf("the delivered content %q does not contain alias-pong", pong.Content)
	}

	// Three settings that must not move the user alias. Each restarts
	// the user Broker with a new configuration; the system Broker
	// keeps running.
	for _, tc := range []struct {
		name   string
		config string
	}{
		{
			name:   "listen port 17444",
			config: "broker:\n  listen: \":17444\"\n",
		},
		{
			name:   "empty listen",
			config: "broker:\n  listen: \"\"\n",
		},
		{
			name:   "advertise a.example:443",
			config: "broker:\n  listen: \":17446\"\n  advertise: a.example:443\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closeSession(t, aliceSession)
			userBroker.Stop(t)
			writeAccountFile(t, "alice", aliasSysAliceConfig,
				tc.config+"link:\n  reconnect_min: 1s\n  reconnect_max: 2s\n")
			userBroker = syscontainer.StartUserBroker(t, "alice")

			aliceSession = syscontainer.NewSession(t, "alice", "a1")
			e2eSysWaitRegistered(t, aliceSession)
			e2eSysJoinTopic(t, aliceSession, aliasSysTopic)

			aliasSysWaitSuffix(t, rootSession, "a1", "@"+aliasSysUserAlias, aliasSysRelinkBound)
			aliasSysRequireSelf(t, aliceSession, "a1", "@"+aliasSysUserAlias)
			// The system alias is untouched by the user restart.
			aliasSysWaitSuffix(t, aliceSession, "x1", "@"+aliasSysSystemAlias, aliasSysRecoverBound)
		})
	}
}

// aliasSysWaitSuffix waits until session lists an agent named name whose
// address ends with suffix.
func aliasSysWaitSuffix(t *testing.T, session *syscontainer.Session, name, suffix string, bound time.Duration) {
	t.Helper()
	harness.EventuallyPoll(t, bound, "the address of "+name+" seen by "+session.Account+" ends with "+suffix, func(deadline time.Time) (bool, string) {
		out, state := e2eSysListAgentsPoll(t, deadline, session, map[string]any{"topic": aliasSysTopic})
		if state != "" {
			return false, state
		}
		for _, agent := range out.Agents {
			if strings.HasPrefix(agent.Address, name+"@") {
				if strings.HasSuffix(agent.Address, suffix) {
					return true, ""
				}
				return false, "address is " + agent.Address
			}
		}
		return false, "the agent is not listed"
	})
}

// aliasSysRequireSelf checks the session's own address, which the Broker
// reports without waiting on the link.
func aliasSysRequireSelf(t *testing.T, session *syscontainer.Session, name, suffix string) {
	t.Helper()
	self := e2eSysSelfAddress(t, session)
	if !strings.HasPrefix(self, name+"@") || !strings.HasSuffix(self, suffix) {
		t.Fatalf("%s's self address = %q, want %s...%s", name, self, name+"@", suffix)
	}
}
