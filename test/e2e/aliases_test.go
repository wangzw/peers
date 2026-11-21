//go:build e2e

package e2e

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

// alWaitBound is the bound of a wait that no product timer governs. When the
// wait polls a tool through EventuallyPoll, the harness lifts it to
// ToolCallWaitBound (fourteen seconds) so one poll call gets its own bound.
const alWaitBound = 3 * time.Second

// alRelinkBound is reconnect_max (400ms) + dial_timeout (500ms) + 3s.
const alRelinkBound = 3900 * time.Millisecond

// alTopic is the topic the sessions use to observe each other's addresses.
const alTopic = "t"

// TestAliases covers fingerprint stability, the default and configured alias,
// initiator and acceptor alias conflicts, and peer renames on a flat network.
func TestAliases(t *testing.T) {
	// The compose file reads HOSTA_NAME for hostA's hostname, and the harness
	// passes the test process's environment through. The default alias takes
	// the first dot-separated label, so the domain must be part of the injected
	// hostname for the assertion to prove it.
	t.Setenv("HOSTA_NAME", "hostA.example.com")

	c := harness.Up(t, harness.TopoFlat(3))

	// REQ-broker-pairing-002 names the account alice. The image's account is
	// peer, so rename its entry in the account database before any Broker
	// starts; the host test process keeps its uid and home directory.
	c.Root(t, "hostA", "sed", "-i", "s/^peer:/alice:/", "/etc/passwd")
	if got := strings.TrimSpace(c.Root(t, "hostA", "getent", "passwd", "alice")); got == "" {
		t.Fatalf("hostA's account database has no alice entry after the rename")
	}

	nodes := map[string]*harness.Node{}
	for _, name := range []string{"hostA", "hostB", "hostC"} {
		nodes[name] = c.Node(name)
		alStartBroker(t, nodes[name], alConfig())
	}
	ops := map[string]*harness.Session{}
	for _, name := range []string{"hostA", "hostB", "hostC"} {
		ops[name] = alOperator(t, nodes[name])
	}
	xNode, yNode, zNode := nodes["hostA"], nodes["hostB"], nodes["hostC"]
	opX, opY, opZ := ops["hostA"], ops["hostB"], ops["hostC"]

	x1 := alSession(t, xNode, "x1")
	y1 := alSession(t, yNode, "y1")
	z1 := alSession(t, zNode, "z1")
	defer func() {
		x1.End(t)
		y1.End(t)
		z1.End(t)
	}()

	// X is the dialer: it pairs with Y, so changing X's own listener keeps X
	// responsible for rebuilding the link.
	pk := alCreateKey(t, opY)
	alConnect(t, opX, alShownAddress(t, c, "hostB", pk.Addresses), pk.Key).MustOK(t)
	alWaitConnected(t, xNode, "hostB.peer", alRelinkBound)
	alWaitConnected(t, yNode, "hostA.alice", alRelinkBound)

	t.Run("REQ-broker-pairing-002 default alias", func(t *testing.T) {
		// The system form (the hostname label alone) is covered in
		// test/e2e/system, which can run a system Broker as root. This
		// user Broker takes the hostname label and the account: alice is
		// the account running it, and the domain is dropped.
		alWaitAgentSuffix(t, y1, "x1", "@hostA.alice", alWaitBound)

		// broker.advertise changes what is shown to peers, never the alias.
		alRestart(t, c, "hostA", alConfig("broker:\n  advertise: a.example:443\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "hostA.alice", alRelinkBound)
		alWaitAgentSuffix(t, y1, "x1", "@hostA.alice", alWaitBound)

		// The listen port does not enter the alias.
		alRestart(t, c, "hostA", alConfig("broker:\n  listen: \":17444\"\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "hostA.alice", alRelinkBound)
		alWaitAgentSuffix(t, y1, "x1", "@hostA.alice", alWaitBound)

		// Without a listener the alias is unchanged too.
		alRestart(t, c, "hostA", alConfig("broker:\n  listen: \"\"\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "hostA.alice", alRelinkBound)
		alWaitAgentSuffix(t, y1, "x1", "@hostA.alice", alWaitBound)
	})

	t.Run("REQ-broker-pairing-003 configured alias", func(t *testing.T) {
		alRestart(t, c, "hostA", alConfig("broker:\n  alias: build-box\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "build-box", alRelinkBound)

		self := alSelfAddress(t, x1, "x1")
		if !strings.HasSuffix(self, "@build-box") {
			t.Fatalf("self_address = %q, want it to end with @build-box", self)
		}
		alWaitAgentSuffix(t, y1, "x1", "@build-box", alWaitBound)
	})

	t.Run("REQ-broker-pairing-001 fingerprint stable", func(t *testing.T) {
		own := alIdentityFingerprint(t, xNode)
		if got := alOnlyPeerFingerprint(t, yNode); got != own {
			t.Fatalf("Y sees X's fingerprint as %q, want %q", got, own)
		}

		// A plain restart must not change the identity.
		alRestart(t, c, "hostA", alConfig("broker:\n  alias: build-box\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "build-box", alRelinkBound)
		if got := alOnlyPeerFingerprint(t, yNode); got != own {
			t.Fatalf("the fingerprint changed after a restart: %q, want %q", got, own)
		}

		// A changed alias must not change the identity either.
		alRestart(t, c, "hostA", alConfig("broker:\n  alias: x2\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "x2", alRelinkBound)
		if got := alOnlyPeerFingerprint(t, yNode); got != own {
			t.Fatalf("the fingerprint changed after an alias change: %q, want %q", got, own)
		}

		// A changed listen address must not change it.
		alRestart(t, c, "hostA", alConfig("broker:\n  alias: x2\n  listen: \":17444\"\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "x2", alRelinkBound)
		if got := alOnlyPeerFingerprint(t, yNode); got != own {
			t.Fatalf("the fingerprint changed after a listen change: %q, want %q", got, own)
		}
	})

	t.Run("REQ-broker-pairing-004 initiator conflict", func(t *testing.T) {
		// Start from the default alias so the observations below are exact.
		alRestart(t, c, "hostA", alConfig(), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")

		// Both Y and Z report the alias macbook. X pairs with Y first and then
		// needs a local alias for Z.
		alRestart(t, c, "hostB", alConfig("broker:\n  alias: macbook\n"), opY, y1)
		opY = alOperator(t, yNode)
		y1 = alSession(t, yNode, "y1")
		alRestart(t, c, "hostC", alConfig("broker:\n  alias: macbook\n"), opZ, z1)
		opZ = alOperator(t, zNode)
		z1 = alSession(t, zNode, "z1")

		keyY := alCreateKey(t, opY)
		alConnect(t, opX, alShownAddress(t, c, "hostB", keyY.Addresses), keyY.Key).MustOK(t)
		alWaitConnected(t, xNode, "macbook", alRelinkBound)

		keyZ := alCreateKey(t, opZ)
		addrZ := alShownAddress(t, c, "hostC", keyZ.Addresses)
		alConnect(t, opX, addrZ, keyZ.Key).MustCode(t, "needs_local_alias")
		out := alDecodeConnect(t, alConnect(t, opX, addrZ, keyZ.Key, "macbook-2"))
		if out.Alias != "macbook-2" {
			t.Fatalf("alias = %q, want macbook-2", out.Alias)
		}
		alWaitConnected(t, xNode, "macbook-2", alRelinkBound)

		yAddr, zAddr := alWaitRemoteAgents(t, x1, "y1", "z1")
		if !strings.HasSuffix(yAddr, "@macbook") {
			t.Fatalf("Y's session is %q, want it to end with @macbook", yAddr)
		}
		if !strings.HasSuffix(zAddr, "@macbook-2") {
			t.Fatalf("Z's session is %q, want it to end with @macbook-2", zAddr)
		}

		// Each address reaches only its own Broker.
		x1.Call(t, "send_message", map[string]any{"to": yAddr, "message": "to-y"}).MustOK(t)
		frame := y1.WaitFrame(t, alWaitBound)
		if !strings.Contains(frame.Content, "to-y") {
			t.Fatalf("Y received %q, want a message containing to-y", frame.Content)
		}
		if frames := z1.Fake().Frames(); len(frames) != 0 {
			t.Fatalf("Z received %d frames addressed to Y", len(frames))
		}

		x1.Call(t, "send_message", map[string]any{"to": zAddr, "message": "to-z"}).MustOK(t)
		frame = z1.WaitFrame(t, alWaitBound)
		if !strings.Contains(frame.Content, "to-z") {
			t.Fatalf("Z received %q, want a message containing to-z", frame.Content)
		}
		if frames := y1.Fake().Frames(); len(frames) != 1 {
			t.Fatalf("Y has %d frames, want the single to-y message", len(frames))
		}
	})

	t.Run("REQ-broker-pairing-024 acceptor conflict", func(t *testing.T) {
		// A fresh three hosts: A already has the peer Y with the alias
		// macbook, and Z, reporting the same alias, pairs with A.
		conflict := harness.Up(t, harness.TopoFlat(3))
		aNode, yNode, zNode := conflict.Node("hostA"), conflict.Node("hostB"), conflict.Node("hostC")
		alStartBroker(t, aNode, alConfig())
		alStartBroker(t, yNode, alConfig("broker:\n  alias: macbook\n"))
		alStartBroker(t, zNode, alConfig("broker:\n  alias: macbook\n"))
		aOp, yOp, zOp := alOperator(t, aNode), alOperator(t, yNode), alOperator(t, zNode)

		keyY := alCreateKey(t, yOp)
		alConnect(t, aOp, alShownAddress(t, conflict, "hostB", keyY.Addresses), keyY.Key).MustOK(t)
		alWaitConnected(t, aNode, "macbook", alRelinkBound)

		keyZ := alCreateKey(t, aOp)
		alConnect(t, zOp, alShownAddress(t, conflict, "hostA", keyZ.Addresses), keyZ.Key).MustOK(t)

		harness.Eventually(t, alWaitBound, "A lists two peers under different aliases", func() (bool, string) {
			peers := alPeers(t, aNode)
			if len(peers) != 2 {
				return false, fmt.Sprintf("A has %d peers", len(peers))
			}
			aliases := alPeerAliases(peers)
			if aliases[0] == aliases[1] {
				return false, fmt.Sprintf("both peers use the alias %q", aliases[0])
			}
			return true, ""
		})
		peers := alPeers(t, aNode)
		byFingerprint := make(map[string]string, len(peers))
		for _, peer := range peers {
			byFingerprint[hex.EncodeToString(peer.GetFingerprint())] = peer.GetAlias()
		}

		// The alias belongs to the identity, not to whichever entry the table
		// happens to list last: Y keeps its self-reported alias and the second
		// reporter, Z, gets the fingerprint suffix.
		yAlias, yOK := byFingerprint[alIdentityFingerprint(t, yNode)]
		zAlias, zOK := byFingerprint[alIdentityFingerprint(t, zNode)]
		if !yOK || !zOK {
			t.Fatalf("A does not list both Y and Z: %v", byFingerprint)
		}
		if yAlias != "macbook" {
			t.Fatalf("Y's alias is %q, want macbook", yAlias)
		}
		if !strings.HasPrefix(zAlias, "macbook-") || len(zAlias) < len("macbook-")+4 || !alHexSuffix(zAlias[len("macbook-"):]) {
			t.Fatalf("Z's alias %q is not macbook-<4+ hex digits>", zAlias)
		}
	})

	t.Run("REQ-broker-pairing-026 peer renames into conflict", func(t *testing.T) {
		// A fresh three hosts: B (hostB) and C (hostC) both dial A. B then
		// renames itself to hostC, the alias C already uses on A.
		conflict := harness.Up(t, harness.TopoFlat(3))
		aNode, bNode, cNode := conflict.Node("hostA"), conflict.Node("hostB"), conflict.Node("hostC")
		alStartBroker(t, aNode, alConfig())
		alStartBroker(t, bNode, alConfig("broker:\n  alias: hostB\n"))
		alStartBroker(t, cNode, alConfig("broker:\n  alias: hostC\n"))
		aOp, bOp, cOp := alOperator(t, aNode), alOperator(t, bNode), alOperator(t, cNode)

		a1 := alSession(t, aNode, "a1")
		b1 := alSession(t, bNode, "b1")
		c1 := alSession(t, cNode, "c1")
		defer func() {
			a1.End(t)
			b1.End(t)
			c1.End(t)
		}()

		for _, pair := range []struct {
			op   *harness.Session
			node string
		}{{bOp, "hostB"}, {cOp, "hostC"}} {
			key := alCreateKey(t, aOp)
			alConnect(t, pair.op, alShownAddress(t, conflict, "hostA", key.Addresses), key.Key).MustOK(t)
			alWaitConnected(t, aNode, pair.node, alRelinkBound)
		}

		// Rename B to hostC. B is the dialer, so it reconnects to A on its
		// own after the restart.
		alRestart(t, conflict, "hostB", alConfig("broker:\n  alias: hostC\n"), bOp, b1)
		b1 = alSession(t, bNode, "b1")

		var bAddr string
		harness.EventuallyPoll(t, alRelinkBound, "A keeps C under @hostC and gives B a local alias", func(deadline time.Time) (bool, string) {
			out, state := alListAgentsPoll(t, deadline, a1)
			if state != "" {
				return false, state
			}
			var cAddr string
			bAddr = ""
			for _, agent := range out.Agents {
				switch alAgentName(agent.Address) {
				case "c1":
					cAddr = agent.Address
				case "b1":
					bAddr = agent.Address
				}
			}
			if cAddr == "" || bAddr == "" {
				return false, fmt.Sprintf("addresses: c1=%q b1=%q", cAddr, bAddr)
			}
			hostC := 0
			for _, agent := range out.Agents {
				if strings.HasSuffix(agent.Address, "@hostC") {
					hostC++
				}
			}
			if hostC != 1 || !strings.HasSuffix(cAddr, "@hostC") {
				return false, fmt.Sprintf("%d addresses end with @hostC; c1 is %q", hostC, cAddr)
			}
			if strings.HasSuffix(bAddr, "@hostC") {
				return false, fmt.Sprintf("B's session still ends with @hostC: %q", bAddr)
			}
			at := strings.LastIndexByte(bAddr, '@')
			if at < 0 {
				return false, fmt.Sprintf("B's address %q has no alias", bAddr)
			}
			bAlias := bAddr[at+1:]
			if !strings.HasPrefix(bAlias, "hostC-") || len(bAlias) < len("hostC-")+4 || !alHexSuffix(bAlias[len("hostC-"):]) {
				return false, fmt.Sprintf("B's alias is %q, want hostC-<4+ hex digits>", bAlias)
			}
			return true, ""
		})

		// A message from B reaches A signed from A's view, which is the
		// assigned local alias, not hostC.
		selfA := alSelfAddress(t, a1, "a1")
		b1.Call(t, "send_message", map[string]any{"to": selfA, "message": "renamed"}).MustOK(t)
		frame := a1.WaitFrame(t, alWaitBound)
		if frame.FromName != bAddr {
			t.Fatalf("the message is signed %q, want B's address %q", frame.FromName, bAddr)
		}
	})

	t.Run("REQ-broker-pairing-040 released alias stays with its peer", func(t *testing.T) {
		// A fresh three hosts: B (hostB) and C (hostC) both dial A. B gives up
		// hostB, and C then tries to take it. The address A already handed out
		// for B must not start naming C.
		tomb := harness.Up(t, harness.TopoFlat(3))
		aNode, bNode, cNode := tomb.Node("hostA"), tomb.Node("hostB"), tomb.Node("hostC")
		alStartBroker(t, aNode, alConfig())
		alStartBroker(t, bNode, alConfig("broker:\n  alias: hostB\n"))
		alStartBroker(t, cNode, alConfig("broker:\n  alias: hostC\n"))
		aOp, bOp, cOp := alOperator(t, aNode), alOperator(t, bNode), alOperator(t, cNode)

		a1 := alSession(t, aNode, "a1")
		b1 := alSession(t, bNode, "b1")
		c1 := alSession(t, cNode, "c1")
		defer func() {
			a1.End(t)
			b1.End(t)
			c1.End(t)
		}()

		for _, pair := range []struct {
			op   *harness.Session
			node string
		}{{bOp, "hostB"}, {cOp, "hostC"}} {
			key := alCreateKey(t, aOp)
			alConnect(t, pair.op, alShownAddress(t, tomb, "hostA", key.Addresses), key.Key).MustOK(t)
			alWaitConnected(t, aNode, pair.node, alRelinkBound)
		}
		bFP := alIdentityFingerprint(t, bNode)
		cFP := alIdentityFingerprint(t, cNode)

		// B stops answering to hostB. Once A has seen the new alias, hostB is
		// one of B's retired aliases and no longer names B.
		alSetConfig(t, bOp, "broker.alias", "hostB2")
		alWaitPeerAlias(t, aNode, bFP, "hostB2", alRelinkBound)

		// C takes hostB. A already handed out an address under that name for B,
		// so it must give C a local alias instead of letting C adopt it.
		alSetConfig(t, cOp, "broker.alias", "hostB")
		var cAlias string
		harness.Eventually(t, alRelinkBound, "A gives C a local alias instead of hostB", func() (bool, string) {
			for _, peer := range alPeers(t, aNode) {
				if hex.EncodeToString(peer.GetFingerprint()) != cFP {
					continue
				}
				alias := peer.GetAlias()
				if alias == "hostB" {
					return false, "C still holds hostB"
				}
				if !strings.HasPrefix(alias, "hostB-") || !alHexSuffix(alias[len("hostB-"):]) {
					return false, fmt.Sprintf("C's alias is %q, want hostB-<hex digits>", alias)
				}
				cAlias = alias
				return true, ""
			}
			return false, "C is not listed"
		})
		if cAlias == "hostB" {
			t.Fatalf("C's alias is hostB, want a local alias")
		}

		// hostB names nobody now: a send to it is refused instead of reaching
		// C's agent.
		r := a1.Call(t, "send_message", map[string]any{"to": "c1@hostB", "message": "was-for-b"})
		r.MustCode(t, "not_found")
		if frames := c1.Fake().Frames(); len(frames) != 0 {
			t.Fatalf("C received %d frames addressed to hostB, want none", len(frames))
		}

		// A message from C is signed from A's view, which is C's local alias, so
		// it never carries the alias B gave up.
		selfA := alSelfAddress(t, a1, "a1")
		c1.Call(t, "send_message", map[string]any{"to": selfA, "message": "from-c"}).MustOK(t)
		frame := a1.WaitFrame(t, alWaitBound)
		if strings.HasSuffix(frame.FromName, "@hostB") {
			t.Fatalf("the message from C is signed %q, which still ends with @hostB", frame.FromName)
		}
		if want := "c1@" + cAlias; frame.FromName != want {
			t.Fatalf("the message is signed %q, want C's address %q", frame.FromName, want)
		}
	})
}

// TestLongHostnameAlias covers REQ-broker-pairing-037: the default alias of a
// long hostname shortens the hostname part first and, only if it is still too
// long, the account part, so the alias stays at most 24 characters. The
// account here is peer (4 characters) and the hostname 26, so the hostname
// keeps 24-1-4 = 19 characters.
func TestLongHostnameAlias(t *testing.T) {
	// The compose file reads HOSTA_NAME for hostA's hostname; the harness
	// passes the test process's environment through to docker compose.
	t.Setenv("HOSTA_NAME", "abcdefghijklmnopqrstuvwxyz0123")

	c := harness.Up(t, harness.TopoFlat(2))
	xNode, yNode := c.Node("hostA"), c.Node("hostB")
	alStartBroker(t, xNode, alConfig())
	alStartBroker(t, yNode, alConfig())
	opX, opY := alOperator(t, xNode), alOperator(t, yNode)

	x1 := alSession(t, xNode, "x1")
	y1 := alSession(t, yNode, "y1")
	defer func() {
		x1.End(t)
		y1.End(t)
	}()

	key := alCreateKey(t, opY)
	alConnect(t, opX, alShownAddress(t, c, "hostB", key.Addresses), key.Key).MustOK(t)
	alWaitConnected(t, yNode, "abcdefghijklmnopqrs.peer", alRelinkBound)

	alWaitAgentSuffix(t, y1, "x1", "@abcdefghijklmnopqrs.peer", alWaitBound)
	if self := alSelfAddress(t, x1, "x1"); !strings.HasSuffix(self, "@abcdefghijklmnopqrs.peer") {
		t.Fatalf("self_address = %q, want it to end with @abcdefghijklmnopqrs.peer", self)
	}
}

// alConfig merges the shortened timers with the given fragments.
func alConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

// alStartBroker starts a Broker and fails when it does not come up.
func alStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

// alRestart stops the Broker and starts it with config after ending every MCP
// server process passed in stop.
func alRestart(t *testing.T, c *harness.Cluster, node string, config string, stop ...*harness.Session) {
	t.Helper()
	for _, s := range stop {
		s.End(t)
	}
	n := c.Node(node)
	n.StopBroker(t, syscall.SIGTERM)
	alStartBroker(t, n, config)
}

// alOperator starts an operator session: an MCP server process without
// CLAUDE_CODE_MESSAGING_SOCKET, which never registers as an agent.
func alOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, alWaitBound)
	return s
}

// alSession starts a registered session in alTopic.
func alSession(t *testing.T, node *harness.Node, name string) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true})
	registrationWaitRegistered(t, s, name)
	s.Call(t, "join_topic", map[string]any{"name": alTopic}).MustOK(t)
	return s
}

// alCreateKey creates one pairing key.
func alCreateKey(t *testing.T, op *harness.Session) mcpserver.CreatePairingKeyOut {
	t.Helper()
	r := op.Call(t, "create_pairing_key", map[string]any{})
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out
}

// alConnect calls connect_broker.
func alConnect(t *testing.T, op *harness.Session, address, key string, localAlias ...string) harness.Result {
	t.Helper()
	args := map[string]any{"address": address, "key": key}
	if len(localAlias) > 0 && localAlias[0] != "" {
		args["local_alias"] = localAlias[0]
	}
	return op.Call(t, "connect_broker", args)
}

// alDecodeConnect decodes a successful connect_broker result.
func alDecodeConnect(t *testing.T, r harness.Result) mcpserver.ConnectBrokerOut {
	t.Helper()
	out, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker: %v", err)
	}
	return out
}

// alSetConfig changes one configuration key through set_config and fails unless
// the Broker accepts it.
func alSetConfig(t *testing.T, s *harness.Session, key, value string) {
	t.Helper()
	s.Call(t, "set_config", map[string]any{"key": key, "value": value}).MustOK(t)
}

// alShownAddress picks the shown address of node on the flat network.
func alShownAddress(t *testing.T, c *harness.Cluster, node string, shown []string) string {
	t.Helper()
	want := net.JoinHostPort(c.Addr(node, "net"), "17443")
	for _, address := range shown {
		if address == want {
			return address
		}
	}
	if len(shown) > 0 {
		return shown[0]
	}
	t.Fatalf("create_pairing_key returned no shown address")
	return ""
}

// alPeers lists node's peers through the local interface probe.
func alPeers(t *testing.T, node *harness.Node) []*localpb.BrokerInfo {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("list_brokers: %s", detail.GetMessage())
	}
	return resp.GetBrokers()
}

// alPeerAliases renders peer aliases for failure messages.
func alPeerAliases(peers []*localpb.BrokerInfo) []string {
	out := make([]string, 0, len(peers))
	for _, peer := range peers {
		out = append(out, peer.GetAlias())
	}
	return out
}

// alWaitConnected waits until node lists alias as connected.
func alWaitConnected(t *testing.T, node *harness.Node, alias string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the link to "+alias+" is connected", func() (bool, string) {
		resp, detail := node.Oracle().ListBrokers(t)
		if detail != nil {
			return false, detail.GetMessage()
		}
		for _, peer := range resp.GetBrokers() {
			if peer.GetAlias() != alias {
				continue
			}
			if peer.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
				return false, fmt.Sprintf("status is %s", peer.GetStatus())
			}
			return true, ""
		}
		return false, "the peer is not listed"
	})
}

// alWaitPeerAlias waits until node lists the peer with the given fingerprint
// under alias.
func alWaitPeerAlias(t *testing.T, node *harness.Node, fingerprint, alias string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the peer "+fingerprint+" is listed as "+alias, func() (bool, string) {
		for _, peer := range alPeers(t, node) {
			if hex.EncodeToString(peer.GetFingerprint()) != fingerprint {
				continue
			}
			if peer.GetAlias() == alias {
				return true, ""
			}
			return false, fmt.Sprintf("its alias is %q", peer.GetAlias())
		}
		return false, "the peer is not listed"
	})
}

// alOnlyPeerFingerprint returns the hex fingerprint of node's only peer.
func alOnlyPeerFingerprint(t *testing.T, node *harness.Node) string {
	t.Helper()
	peers := alPeers(t, node)
	if len(peers) != 1 {
		t.Fatalf("node has %d peers, want exactly one", len(peers))
	}
	return hex.EncodeToString(peers[0].GetFingerprint())
}

// alIdentityFingerprint derives a node's fingerprint from its certificate.
func alIdentityFingerprint(t *testing.T, node *harness.Node) string {
	t.Helper()
	path := filepath.Join(node.AccountHome(), ".peers", "identity.crt")
	data := node.ReadFile(t, path)
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("%s is not a PEM certificate", path)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

// alListAgentsPoll calls list_agents from a wait and returns the decoded answer.
// The call is bounded by the wait's deadline and a transport error is returned
// as state.
func alListAgentsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", map[string]any{})
}

// alWaitAgentSuffix waits until s lists name with an address ending in suffix.
func alWaitAgentSuffix(t *testing.T, s *harness.Session, name, suffix string, bound time.Duration) {
	t.Helper()
	harness.EventuallyPoll(t, bound, "the address of "+name+" ends with "+suffix, func(deadline time.Time) (bool, string) {
		r, err := s.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		out, err := harness.Decode[mcpserver.ListAgentsOut](string(r.Structured))
		if err != nil {
			return false, err.Error()
		}
		for _, agent := range out.Agents {
			if alAgentName(agent.Address) != name {
				continue
			}
			if strings.HasSuffix(agent.Address, suffix) {
				return true, ""
			}
			return false, fmt.Sprintf("address is %q", agent.Address)
		}
		return false, "the agent is not listed"
	})
}

// alSelfAddress returns the session's own address, after waiting for it to be
// registered under name.
func alSelfAddress(t *testing.T, s *harness.Session, name string) string {
	t.Helper()
	return registrationWaitRegistered(t, s, name).Self
}

// alWaitRemoteAgents waits until s lists both names and returns their
// addresses.
func alWaitRemoteAgents(t *testing.T, s *harness.Session, firstName, secondName string) (string, string) {
	t.Helper()
	var first, second string
	harness.EventuallyPoll(t, alRelinkBound, "both remote agents are listed", func(deadline time.Time) (bool, string) {
		r, err := s.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		if r.IsError {
			return false, r.Text
		}
		out, err := harness.Decode[mcpserver.ListAgentsOut](string(r.Structured))
		if err != nil {
			return false, err.Error()
		}
		first, second = "", ""
		for _, agent := range out.Agents {
			switch alAgentName(agent.Address) {
			case firstName:
				first = agent.Address
			case secondName:
				second = agent.Address
			}
		}
		if first == "" || second == "" {
			return false, fmt.Sprintf("%s=%q %s=%q", firstName, first, secondName, second)
		}
		return true, ""
	})
	return first, second
}

// alAgentName returns the name part of an agent address. The address is
// "<name> [ref]@<alias>"; the ref exists only while two agents share a name.
func alAgentName(address string) string {
	before, _, _ := strings.Cut(address, "@")
	if i := strings.Index(before, " ["); i >= 0 {
		before = before[:i]
	}
	return strings.TrimSpace(before)
}

// alHexSuffix reports whether s is a non-empty run of lowercase hex digits.
func alHexSuffix(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
