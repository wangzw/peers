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
	// aliasSysHostname carries a domain so the assertions prove the alias keeps
	// only the first label.
	aliasSysHostname    = "hostA.example.com"
	aliasSysSystemAlias = "hostA"
	aliasSysUserAlias   = "hostA.alice"
	aliasSysTopic       = "alias"
	aliasSysAliceConfig = "/home/alice/.peers/config.yaml"
	// aliasSysRelinkBound is the user Broker's 2s reconnect_max plus 1s;
	// aliasSysRelinkWait adds the EventTimeout fallback for a frame wait.
	aliasSysRelinkBound = 3 * time.Second
	aliasSysRelinkWait  = aliasSysRelinkBound + harness.EventTimeout
)

// TestREQBrokerPairing002_DefaultAliasHostAndAccount checks that the system
// Broker's default alias is the first hostname label and the user Broker's is
// that label, a dot and the account name.
func TestREQBrokerPairing002_DefaultAliasHostAndAccount(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	if err := syscall.Sethostname([]byte(aliasSysHostname)); err != nil {
		t.Fatalf("setting the hostname to %q: %v", aliasSysHostname, err)
	}

	mustRun(t, "root", nil, "useradd", "--create-home", "--shell", "/bin/sh", "alice")

	// The configuration file must exist for the root session's writes to be
	// permitted.
	systemBroker := syscontainer.StartSystemBroker(t, "broker: {}\n")
	t.Cleanup(func() { systemBroker.Stop(t) })

	writeAccountFile(t, "alice", aliasSysAliceConfig,
		"broker:\n  listen: \":17445\"\nlink:\n  reconnect_min: 1s\n  reconnect_max: 2s\n")
	userBroker := syscontainer.StartUserBroker(t, "alice")

	rootSession := syscontainer.NewSession(t, "root", "x1")
	aliceSession := syscontainer.NewSession(t, "alice", "a1")
	e2eSysWaitRegistered(t, rootSession)
	e2eSysWaitRegistered(t, aliceSession)
	e2eSysJoinTopic(t, rootSession, aliasSysTopic)
	e2eSysJoinTopic(t, aliceSession, aliasSysTopic)

	// alice is the dialer, so a restart of its user Broker redials the system
	// Broker without a new connect_broker call.
	key := createPairingKey(t, rootSession)
	if alias := connectBroker(t, aliceSession, "127.0.0.1:17443", key); alias == "" {
		t.Fatal("connect_broker returned no alias")
	}

	aliasSysWaitSuffix(t, aliceSession, "x1", "@"+aliasSysSystemAlias)
	aliasSysWaitSuffix(t, rootSession, "a1", "@"+aliasSysUserAlias)
	aliasSysRequireSelf(t, aliceSession, "a1", "@"+aliasSysUserAlias)

	rootSelf := e2eSysSelfAddress(t, rootSession)
	aliceSelf := e2eSysSelfAddress(t, aliceSession)
	if text, isError := aliceSession.Call(t, "send_message", map[string]any{"to": rootSelf, "message": "alias-ping"}); isError {
		t.Fatalf("send_message from alice to root: %s", text)
	}
	ping, ok := rootSession.Fake.WaitFrame(aliasSysRelinkWait)
	if !ok {
		t.Fatalf("root's session did not receive the message within %s", aliasSysRelinkWait)
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
	pong, ok := aliceSession.Fake.WaitFrame(aliasSysRelinkWait)
	if !ok {
		t.Fatalf("alice's session did not receive the reply within %s", aliasSysRelinkWait)
	}
	if pong.FromName != rootSelf {
		t.Fatalf("the reply is signed %q, want root's address %q", pong.FromName, rootSelf)
	}
	if !strings.Contains(pong.Content, "alias-pong") {
		t.Fatalf("the delivered content %q does not contain alias-pong", pong.Content)
	}

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

			aliasSysWaitSuffix(t, rootSession, "a1", "@"+aliasSysUserAlias)
			aliasSysRequireSelf(t, aliceSession, "a1", "@"+aliasSysUserAlias)
			aliasSysWaitSuffix(t, aliceSession, "x1", "@"+aliasSysSystemAlias)
		})
	}
}

func aliasSysWaitSuffix(t *testing.T, session *syscontainer.Session, name, suffix string) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "the address of "+name+" seen by "+session.Account+" ends with "+suffix, func(deadline time.Time) (bool, string) {
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

func aliasSysRequireSelf(t *testing.T, session *syscontainer.Session, name, suffix string) {
	t.Helper()
	self := e2eSysSelfAddress(t, session)
	if !strings.HasPrefix(self, name+"@") || !strings.HasSuffix(self, suffix) {
		t.Fatalf("%s's self address = %q, want %s...%s", name, self, name+"@", suffix)
	}
}
