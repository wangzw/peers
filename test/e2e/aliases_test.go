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

const alWaitBound = harness.EventTimeout

const alRelinkBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout

const alTopic = "t"

// TestAliases covers fingerprint stability, the default and configured alias,
// initiator and acceptor alias conflicts, and peer renames on a flat network.
func TestAliases(t *testing.T) {
	// The default alias takes the first dot-separated label, so the injected
	// hostname carries a domain.
	t.Setenv("HOSTA_NAME", "hostA.example.com")

	c := harness.Up(t, harness.TopoFlat(3))

	// Rename the image's peer account to alice before any Broker starts; the
	// host test process keeps its uid and home directory.
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
		alWaitAgentSuffix(t, y1, "x1", "@hostA.alice", harness.ListAgentsWaitBound)

		alRestart(t, c, "hostA", alConfig("broker:\n  advertise: a.example:443\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "hostA.alice", alRelinkBound)
		alWaitAgentSuffix(t, y1, "x1", "@hostA.alice", harness.ListAgentsWaitBound)

		alRestart(t, c, "hostA", alConfig("broker:\n  listen: \":17444\"\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "hostA.alice", alRelinkBound)
		alWaitAgentSuffix(t, y1, "x1", "@hostA.alice", harness.ListAgentsWaitBound)

		alRestart(t, c, "hostA", alConfig("broker:\n  listen: \"\"\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "hostA.alice", alRelinkBound)
		alWaitAgentSuffix(t, y1, "x1", "@hostA.alice", harness.ListAgentsWaitBound)
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
		alWaitAgentSuffix(t, y1, "x1", "@build-box", harness.ListAgentsWaitBound)
	})

	t.Run("REQ-broker-pairing-001 fingerprint stable", func(t *testing.T) {
		own := alIdentityFingerprint(t, xNode)
		if got := alOnlyPeerFingerprint(t, yNode); got != own {
			t.Fatalf("Y sees X's fingerprint as %q, want %q", got, own)
		}

		alRestart(t, c, "hostA", alConfig("broker:\n  alias: build-box\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "build-box", alRelinkBound)
		if got := alOnlyPeerFingerprint(t, yNode); got != own {
			t.Fatalf("the fingerprint changed after a restart: %q, want %q", got, own)
		}

		alRestart(t, c, "hostA", alConfig("broker:\n  alias: x2\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "x2", alRelinkBound)
		if got := alOnlyPeerFingerprint(t, yNode); got != own {
			t.Fatalf("the fingerprint changed after an alias change: %q, want %q", got, own)
		}

		alRestart(t, c, "hostA", alConfig("broker:\n  alias: x2\n  listen: \":17444\"\n"), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")
		alWaitConnected(t, yNode, "x2", alRelinkBound)
		if got := alOnlyPeerFingerprint(t, yNode); got != own {
			t.Fatalf("the fingerprint changed after a listen change: %q, want %q", got, own)
		}
	})

	t.Run("REQ-broker-pairing-004 initiator conflict", func(t *testing.T) {
		alRestart(t, c, "hostA", alConfig(), opX, x1)
		opX = alOperator(t, xNode)
		x1 = alSession(t, xNode, "x1")

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

		// B is the dialer, so it reconnects to A on its own after the restart.
		alRestart(t, conflict, "hostB", alConfig("broker:\n  alias: hostC\n"), bOp, b1)
		b1 = alSession(t, bNode, "b1")

		var bAddr string
		harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "A keeps C under @hostC and gives B a local alias", func(deadline time.Time) (bool, string) {
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

		selfA := alSelfAddress(t, a1, "a1")
		b1.Call(t, "send_message", map[string]any{"to": selfA, "message": "renamed"}).MustOK(t)
		frame := a1.WaitFrame(t, alWaitBound)
		if frame.FromName != bAddr {
			t.Fatalf("the message is signed %q, want B's address %q", frame.FromName, bAddr)
		}
	})

	t.Run("REQ-broker-pairing-040 released alias stays with its peer", func(t *testing.T) {
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

		alSetConfig(t, bOp, "broker.alias", "hostB2")
		alWaitPeerAlias(t, aNode, bFP, "hostB2", alRelinkBound)

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

		r := a1.Call(t, "send_message", map[string]any{"to": "c1@hostB", "message": "was-for-b"})
		r.MustCode(t, "not_found")
		if frames := c1.Fake().Frames(); len(frames) != 0 {
			t.Fatalf("C received %d frames addressed to hostB, want none", len(frames))
		}

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

// TestLongHostnameAlias checks that a long hostname is shortened before the
// account so the default alias stays at most 24 characters.
func TestLongHostnameAlias(t *testing.T) {
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

	alWaitAgentSuffix(t, y1, "x1", "@abcdefghijklmnopqrs.peer", harness.ListAgentsWaitBound)
	if self := alSelfAddress(t, x1, "x1"); !strings.HasSuffix(self, "@abcdefghijklmnopqrs.peer") {
		t.Fatalf("self_address = %q, want it to end with @abcdefghijklmnopqrs.peer", self)
	}
}

func alConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

func alStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

func alRestart(t *testing.T, c *harness.Cluster, node string, config string, stop ...*harness.Session) {
	t.Helper()
	for _, s := range stop {
		s.End(t)
	}
	n := c.Node(node)
	n.StopBroker(t, syscall.SIGTERM)
	alStartBroker(t, n, config)
}

func alOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, alWaitBound)
	return s
}

func alSession(t *testing.T, node *harness.Node, name string) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true})
	registrationWaitRegistered(t, s, name)
	s.Call(t, "join_topic", map[string]any{"name": alTopic}).MustOK(t)
	return s
}

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

func alConnect(t *testing.T, op *harness.Session, address, key string, localAlias ...string) harness.Result {
	t.Helper()
	args := map[string]any{"address": address, "key": key}
	if len(localAlias) > 0 && localAlias[0] != "" {
		args["local_alias"] = localAlias[0]
	}
	return op.Call(t, "connect_broker", args)
}

func alDecodeConnect(t *testing.T, r harness.Result) mcpserver.ConnectBrokerOut {
	t.Helper()
	out, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker: %v", err)
	}
	return out
}

func alSetConfig(t *testing.T, s *harness.Session, key, value string) {
	t.Helper()
	s.Call(t, "set_config", map[string]any{"key": key, "value": value}).MustOK(t)
}

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

func alPeers(t *testing.T, node *harness.Node) []*localpb.BrokerInfo {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("list_brokers: %s", detail.GetMessage())
	}
	return resp.GetBrokers()
}

func alPeerAliases(peers []*localpb.BrokerInfo) []string {
	out := make([]string, 0, len(peers))
	for _, peer := range peers {
		out = append(out, peer.GetAlias())
	}
	return out
}

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

func alOnlyPeerFingerprint(t *testing.T, node *harness.Node) string {
	t.Helper()
	peers := alPeers(t, node)
	if len(peers) != 1 {
		t.Fatalf("node has %d peers, want exactly one", len(peers))
	}
	return hex.EncodeToString(peers[0].GetFingerprint())
}

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

func alListAgentsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListAgentsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListAgentsOut](t, deadline, s, "list_agents", map[string]any{})
}

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

func alSelfAddress(t *testing.T, s *harness.Session, name string) string {
	t.Helper()
	return registrationWaitRegistered(t, s, name).Self
}

func alWaitRemoteAgents(t *testing.T, s *harness.Session, firstName, secondName string) (string, string) {
	t.Helper()
	var first, second string
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "both remote agents are listed", func(deadline time.Time) (bool, string) {
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

// An address is "<name> [ref]@<alias>"; the ref exists only while two agents
// share a name.
func alAgentName(address string) string {
	before, _, _ := strings.Cut(address, "@")
	if i := strings.Index(before, " ["); i >= 0 {
		before = before[:i]
	}
	return strings.TrimSpace(before)
}

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
