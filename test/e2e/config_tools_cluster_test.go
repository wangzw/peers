//go:build e2e

package e2e

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

const (
	// ctcWaitBound is a wait with no product timer behind it.
	ctcWaitBound = harness.EventTimeout

	// ctcReloadBound is the configuration file watcher's debounce (100ms) plus
	// the harness.EventTimeout margin.
	ctcReloadBound = 100*time.Millisecond + harness.EventTimeout

	// ctcReconnectBound is the reconnect timer plus the dial timeout and the
	// EventTimeout margin.
	ctcReconnectBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout
)

// ctcStaticKeyOne, ctcStaticKeyTwo and ctcStaticKeyThree are legal static
// keys; the shortest legal length is 16 characters.
const (
	ctcStaticKeyOne   = "cluster-key-00001"
	ctcStaticKeyTwo   = "cluster-key-00002"
	ctcStaticKeyThree = "cluster-key-00003"
)

// TestConfigToolsCluster checks the pairing audit trail, that the audit log
// holds no keys or message bodies, and the static key when it is read, refused
// by the tools or removed from the file.
func TestConfigToolsCluster(t *testing.T) {
	t.Run("PairingAndLinkEventsInOrder", func(t *testing.T) {
		t.Parallel()
		c := harness.Up(t, harness.TopoFlat(2))
		aNode, bNode := c.Node("hostA"), c.Node("hostB")
		pkStartBroker(t, aNode, pkConfig())
		pkStartBroker(t, bNode, pkConfig())

		b1 := srSession(t, bNode, "b1")
		defer b1.End(t)
		a1 := srSession(t, aNode, "a1")
		defer a1.End(t)

		bAgent := ctcAgentAddress(t, b1, "b1")
		aAgent := ctcAgentAddress(t, a1, "a1")
		aFingerprint := pkIdentityFingerprint(t, aNode)
		bAccount := ctcAccountName(t, bNode)
		aAccount := ctcAccountName(t, aNode)
		since := time.Now().Add(-time.Second)

		key := pkCreateKey(t, b1)
		address := pkShownAddress(t, c, "hostB", key.Addresses)

		rejectedAt := time.Now().Add(-time.Second)
		pkRejectReason(t, pkConnect(t, a1, address, "not-the-right-key", ""), bNode, aNode, rejectedAt, "key invalid")
		pkConnect(t, a1, address, key.Key, "").MustOK(t)
		pkWaitConnected(t, aNode, "hostB.peer", ctcReconnectBound)
		pkWaitConnected(t, bNode, "hostA.peer", ctcReconnectBound)

		c.Cut(t, "hostA", "hostB")
		harness.Eventually(t, ctcWaitBound, "the acceptor notices the cut", func() (bool, string) {
			if ctcPeerConnected(t, bNode, "hostA.peer") {
				return false, "the link to A is still connected"
			}
			return true, ""
		})
		c.Restore(t)
		pkWaitConnected(t, bNode, "hostA.peer", ctcReconnectBound)
		pkWaitConnected(t, aNode, "hostB.peer", ctcReconnectBound)

		pkDisconnect(t, b1, "hostA.peer")

		pairingWants := []ctcEventWant{
			{
				name: "pairing_key_created by B's agent",
				match: func(e *localpb.AuditEvent) bool {
					return e.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_KEY_CREATED &&
						e.GetActor() == bAgent && e.GetAccount() == bAccount
				},
			},
			{
				name: "pairing_failed written by the acceptor",
				match: func(e *localpb.AuditEvent) bool {
					return e.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_FAILED &&
						e.GetActor() == "" && e.GetPeer().GetAlias() == "" &&
						hex.EncodeToString(e.GetPeer().GetFingerprint()) == aFingerprint &&
						e.GetDetail() == "key invalid"
				},
			},
			{
				name: "pairing_succeeded written by the acceptor",
				match: func(e *localpb.AuditEvent) bool {
					return e.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_SUCCEEDED &&
						e.GetActor() == "" && e.GetPeer().GetAlias() == "hostA.peer"
				},
			},
		}
		linkWants := []ctcEventWant{
			{
				name: "link_down naming A",
				match: func(e *localpb.AuditEvent) bool {
					return e.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_LINK_DOWN &&
						e.GetPeer().GetAlias() == "hostA.peer"
				},
			},
			{
				name: "link_up naming A",
				match: func(e *localpb.AuditEvent) bool {
					return e.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_LINK_UP &&
						e.GetPeer().GetAlias() == "hostA.peer"
				},
			},
		}
		unpairWants := []ctcEventWant{
			{
				name: "unpaired by B's agent",
				match: func(e *localpb.AuditEvent) bool {
					return e.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_UNPAIRED &&
						e.GetActor() == bAgent && e.GetAccount() == bAccount &&
						e.GetPeer().GetAlias() == "hostA.peer"
				},
			},
		}
		bWants := append(append(append([]ctcEventWant{}, pairingWants...), linkWants...), unpairWants...)
		ctcWaitOrderedEvents(t, bNode, since, bWants)
		ctcRequireOneEach(t, bNode, since, pairingWants)
		ctcRequireOneEach(t, bNode, since, unpairWants)
		ctcRequireLinkCycle(t, bNode, since, linkWants[0], linkWants[1])

		aWants := []ctcEventWant{
			{
				name: "pairing_failed by A's agent",
				match: func(e *localpb.AuditEvent) bool {
					return e.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_FAILED &&
						e.GetActor() == aAgent && e.GetAccount() == aAccount
				},
			},
			{
				name: "pairing_succeeded by A's agent",
				match: func(e *localpb.AuditEvent) bool {
					return e.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_SUCCEEDED &&
						e.GetActor() == aAgent && e.GetAccount() == aAccount
				},
			},
		}
		ctcWaitOrderedEvents(t, aNode, since, aWants)
		ctcRequireOneEach(t, aNode, since, aWants)
	})

	t.Run("AuditHasNoSecrets", func(t *testing.T) {
		t.Parallel()
		c := harness.Up(t, harness.TopoFlat(2))
		aNode, bNode := c.Node("hostA"), c.Node("hostB")
		pkStartBroker(t, aNode, pkConfig())
		pkStartBroker(t, bNode, pkConfig())

		a1 := srSession(t, aNode, "a1")
		defer a1.End(t)
		b1 := srSession(t, bNode, "b1")
		defer b1.End(t)
		opB := pkOperator(t, bNode)
		defer opB.End(t)

		key := pkCreateKey(t, opB)
		address := pkShownAddress(t, c, "hostB", key.Addresses)
		pkConnect(t, a1, address, key.Key, "").MustOK(t)
		pkWaitConnected(t, bNode, "hostA.peer", ctcReconnectBound)
		pkWaitConnected(t, aNode, "hostB.peer", ctcReconnectBound)

		body := srMark("audit-body-")
		a1.Call(t, "send_message", map[string]any{"to": "b1@hostB.peer", "message": body}).MustOK(t)
		b1.WaitFrame(t, ctcWaitBound)

		nodes := []struct {
			name string
			node *harness.Node
		}{
			{"A", aNode},
			{"B", bNode},
		}
		// Both audit files must carry the pairing events before their absence of
		// secrets means anything.
		for _, entry := range nodes {
			ctcWaitAuditFileContains(t, entry.node, "pairing_succeeded")
		}
		for _, entry := range nodes {
			data := entry.node.ReadFile(t, ctcAuditLogPath(entry.node))
			for _, needle := range []string{key.Key, body} {
				if bytes.Contains(data, []byte(needle)) {
					t.Fatalf("%s's audit log contains %q", entry.name, needle)
				}
			}
		}
	})

	t.Run("AuditHasNoSecretsTerminalConnect", func(t *testing.T) {
		t.Parallel()
		c := harness.Up(t, harness.TopoFlat(2))
		aNode, bNode := c.Node("hostA"), c.Node("hostB")
		pkStartBroker(t, aNode, pkConfig())
		pkStartBroker(t, bNode, pkConfig())

		opB := pkOperator(t, bNode)
		defer opB.End(t)

		key := pkCreateKey(t, opB)
		address := pkShownAddress(t, c, "hostB", key.Addresses)
		res := aNode.Exec(t, []string{"peers", "broker", "connect", address, "--key", key.Key}, nil)
		if res.ExitCode != 0 {
			t.Fatalf("broker connect exited %d\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
		}
		pkWaitConnected(t, aNode, "hostB.peer", ctcReconnectBound)
		pkWaitConnected(t, bNode, "hostA.peer", ctcReconnectBound)

		nodes := []struct {
			name string
			node *harness.Node
		}{
			{"A", aNode},
			{"B", bNode},
		}
		// Both audit files must carry the pairing events before their absence of
		// secrets means anything.
		for _, entry := range nodes {
			ctcWaitAuditFileContains(t, entry.node, "pairing_succeeded")
		}
		for _, entry := range nodes {
			data := entry.node.ReadFile(t, ctcAuditLogPath(entry.node))
			if bytes.Contains(data, []byte(key.Key)) {
				t.Fatalf("%s's audit log contains the terminal-connect pairing key %q", entry.name, key.Key)
			}
		}
	})

	t.Run("StaticKeyRemovedRejects", func(t *testing.T) {
		t.Parallel()
		c := harness.Up(t, harness.TopoFlat(3))
		aNode, bNode, cNode := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")
		pkStartBroker(t, aNode, pkConfig(pkStaticKeyYAML(ctcStaticKeyOne)))
		pkStartBroker(t, bNode, pkConfig())
		pkStartBroker(t, cNode, pkConfig())

		aOp := pkOperator(t, aNode)
		defer aOp.End(t)
		bOp := pkOperator(t, bNode)
		defer bOp.End(t)
		cOp := pkOperator(t, cNode)
		defer cOp.End(t)

		addrA := pkAddr(c, "hostA")
		pkConnect(t, bOp, addrA, ctcStaticKeyOne, "").MustOK(t)
		pkWaitConnected(t, aNode, "hostB.peer", ctcReconnectBound)
		pkWaitConnected(t, bNode, "hostA.peer", ctcReconnectBound)

		cFingerprint := pkIdentityFingerprint(t, cNode)
		aFingerprint := pkIdentityFingerprint(t, aNode)
		bFingerprint := pkIdentityFingerprint(t, bNode)

		since := time.Now().Add(-time.Second)
		aNode.WriteConfig(t, pkConfig())
		event := pkWaitAuditConfigChange(t, aNode, "pairing.static_key", since)
		for _, rendered := range []string{
			event.GetDetail(),
			ctRenderValue(event.GetOldValue()),
			ctRenderValue(event.GetNewValue()),
		} {
			if strings.Contains(rendered, ctcStaticKeyOne) {
				t.Fatalf("the configuration change audit record contains the removed key: %q", rendered)
			}
		}

		harness.Eventually(t, ctcReloadBound, "get_config reports the static key unset", func() (bool, string) {
			resp, detail := aNode.Oracle().GetConfig(t, "pairing.static_key")
			if detail != nil {
				return false, detail.GetMessage()
			}
			entries := resp.GetEntries()
			if len(entries) != 1 {
				return false, fmt.Sprintf("get_config returned %d entries", len(entries))
			}
			value := entries[0].GetValue()
			if _, ok := value.GetValue().(*localpb.ConfigValue_SecretIsSet); !ok {
				return false, "the value is not a secret marker"
			}
			if value.GetSecretIsSet() {
				return false, "the key is still reported as set"
			}
			return true, ""
		})

		rejectedAt := time.Now().Add(-time.Second)
		pkRejectReason(t, pkConnect(t, cOp, addrA, ctcStaticKeyOne, ""), aNode, cNode, rejectedAt, "key invalid")
		if bytes.Contains(pkPeersJSON(t, aNode), []byte(cFingerprint)) {
			t.Fatalf("A's peers.json still contains C's fingerprint %s", cFingerprint)
		}
		if bytes.Contains(pkPeersJSON(t, cNode), []byte(aFingerprint)) {
			t.Fatalf("C's peers.json still contains A's fingerprint %s", aFingerprint)
		}

		pkWaitConnected(t, aNode, "hostB.peer", ctcReconnectBound)
		pkWaitConnected(t, bNode, "hostA.peer", ctcReconnectBound)

		// B is in A's peer table throughout the window; without this positive
		// control an absent LINK_DOWN could also mean the peer was never there.
		if !bytes.Contains(pkPeersJSON(t, aNode), []byte(bFingerprint)) {
			t.Fatalf("A's peers.json does not contain B's fingerprint %s", bFingerprint)
		}
		events, detail := aNode.Oracle().Audit(t, since, 0)
		if detail != nil {
			t.Fatalf("reading A's audit: %s", detail.GetMessage())
		}
		for _, event := range events {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_LINK_DOWN &&
				event.GetPeer().GetAlias() == "hostB.peer" {
				t.Fatalf("the static key removal dropped the link to hostB: %s", event.GetDetail())
			}
		}
	})

	t.Run("LocalAliasTooLong", func(t *testing.T) {
		t.Parallel()
		c := harness.Up(t, harness.TopoFlat(2))
		aNode, bNode := c.Node("hostA"), c.Node("hostB")
		pkStartBroker(t, aNode, pkConfig())
		pkStartBroker(t, bNode, pkConfig())

		aOp := pkOperator(t, aNode)
		defer aOp.End(t)
		bOp := pkOperator(t, bNode)
		defer bOp.End(t)

		key := pkCreateKey(t, bOp)
		address := pkShownAddress(t, c, "hostB", key.Addresses)

		r := pkConnect(t, aOp, address, key.Key, strings.Repeat("a", 25))
		r.MustCode(t, "invalid_argument")
		if !strings.Contains(r.Text, "longer than 24 characters") {
			t.Fatalf("the refusal %q does not say the local alias is longer than 24 characters", r.Text)
		}
		if peers := pkPeers(t, aNode); len(peers) != 0 {
			t.Fatalf("the rejected attempt left A with peers %v", pkAliases(peers))
		}
		if peers := pkPeers(t, bNode); len(peers) != 0 {
			t.Fatalf("the rejected attempt left B with peers %v", pkAliases(peers))
		}
	})

	t.Run("StaticKeyReadHasNoSecret", func(t *testing.T) {
		t.Parallel()
		c := harness.Up(t, harness.TopoFlat(2))
		aNode, bNode := c.Node("hostA"), c.Node("hostB")
		pkStartBroker(t, bNode, pkConfig(pkStaticKeyYAML(ctcStaticKeyOne)))
		pkStartBroker(t, aNode, pkConfig())

		aOp := pkOperator(t, aNode)
		defer aOp.End(t)
		bOp := pkOperator(t, bNode)
		defer bOp.End(t)

		if got := ctcGetConfigTool(t, bOp, "pairing.static_key"); got != "(set)" {
			t.Fatalf("get_config pairing.static_key = %q, want (set)", got)
		}
		if text := ctcGetConfigRaw(t, bOp, ""); strings.Contains(text, ctcStaticKeyOne) {
			t.Fatalf("get_config returned the static key value: %s", text)
		}

		since := time.Now().Add(-time.Second)
		bNode.WriteConfig(t, pkConfig(pkStaticKeyYAML(ctcStaticKeyTwo)))
		pkWaitAuditConfigChange(t, bNode, "pairing.static_key", since)
		pkConnect(t, aOp, pkAddr(c, "hostB"), ctcStaticKeyTwo, "").MustOK(t)
		pkWaitConnected(t, bNode, "hostA.peer", ctcReconnectBound)

		if got := ctcGetConfigTool(t, bOp, "pairing.static_key"); got != "(set)" {
			t.Fatalf("get_config pairing.static_key = %q after the replacement, want (set)", got)
		}
		for _, key := range []string{"", "pairing.static_key"} {
			text := ctcGetConfigRaw(t, bOp, key)
			for _, secret := range []string{ctcStaticKeyOne, ctcStaticKeyTwo} {
				if strings.Contains(text, secret) {
					t.Fatalf("get_config returned the static key value %q: %s", secret, text)
				}
			}
		}
		// Both run logs exist and A's carries the pairing record, so an absent
		// secret is evidence about the written record rather than about an
		// empty log.
		aLog := aNode.BrokerLog(t)
		bLog := bNode.BrokerLog(t)
		if len(aLog) == 0 || len(bLog) == 0 {
			t.Fatalf("a Broker log is empty: A has %d bytes, B has %d bytes", len(aLog), len(bLog))
		}
		if got := bytes.Count(aLog, []byte(`"event":"peer_paired"`)); got != 1 {
			t.Fatalf("A's Broker log has %d peer_paired records, want 1", got)
		}
		logs := append(append([]byte(nil), aLog...), bLog...)
		for _, secret := range []string{ctcStaticKeyOne, ctcStaticKeyTwo} {
			if bytes.Contains(logs, []byte(secret)) {
				t.Fatalf("a Broker log contains the static key value %q", secret)
			}
		}
	})

	t.Run("StaticKeyToolsDoNotChange", func(t *testing.T) {
		t.Parallel()
		c := harness.Up(t, harness.TopoFlat(3))
		aNode, bNode, cNode := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")
		pkStartBroker(t, bNode, pkConfig(pkStaticKeyYAML(ctcStaticKeyOne)))
		pkStartBroker(t, aNode, pkConfig())
		pkStartBroker(t, cNode, pkConfig())

		aOp := pkOperator(t, aNode)
		defer aOp.End(t)
		bOp := pkOperator(t, bNode)
		defer bOp.End(t)
		cOp := pkOperator(t, cNode)
		defer cOp.End(t)

		before := bNode.ReadConfig(t)
		for _, value := range []string{ctcStaticKeyTwo, ctcStaticKeyThree, ""} {
			r := ctSetConfig(t, bOp, "pairing.static_key", value)
			r.MustCode(t, "invalid_argument")
			if !strings.Contains(r.Text, "can only be set in the configuration file") {
				t.Fatalf("the refusal %q does not say the key can only be set in the file", r.Text)
			}
			if value != "" && strings.Contains(r.Text, value) {
				t.Fatalf("the refusal %q contains the attempted value", r.Text)
			}
		}
		if after := bNode.ReadConfig(t); !bytes.Equal(before, after) {
			t.Fatalf("a refused static key change rewrote the configuration file: %s -> %s", before, after)
		}

		addrB := pkAddr(c, "hostB")
		pkConnect(t, aOp, addrB, ctcStaticKeyOne, "").MustOK(t)
		pkWaitConnected(t, bNode, "hostA.peer", ctcReconnectBound)
		rejectedAt := time.Now().Add(-time.Second)
		pkRejectReason(t, pkConnect(t, cOp, addrB, ctcStaticKeyTwo, ""), bNode, cNode, rejectedAt, "key invalid")
	})
}

type ctcEventWant struct {
	name  string
	match func(*localpb.AuditEvent) bool
}

func ctcWaitOrderedEvents(t *testing.T, n *harness.Node, since time.Time, wants []ctcEventWant) {
	t.Helper()
	harness.Eventually(t, ctcWaitBound, "the audit records appear in order", func() (bool, string) {
		events, detail := n.Oracle().Audit(t, since, 0)
		if detail != nil {
			return false, detail.GetMessage()
		}
		next := 0
		var matched []string
		for _, event := range events {
			if next < len(wants) && wants[next].match(event) {
				matched = append(matched, wants[next].name)
				next++
			}
		}
		if next == len(wants) {
			return true, ""
		}
		return false, fmt.Sprintf("matched %d of %d records %v; the next wanted is %q",
			next, len(wants), matched, wants[next].name)
	})
}

func ctcRequireOneEach(t *testing.T, n *harness.Node, since time.Time, wants []ctcEventWant) {
	t.Helper()
	events, detail := n.Oracle().Audit(t, since, 0)
	if detail != nil {
		t.Fatalf("reading the audit for the exact-one check: %s", detail.GetMessage())
	}
	for _, want := range wants {
		count := 0
		for _, event := range events {
			if want.match(event) {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("the audit holds %d records matching %q, want exactly 1", count, want.name)
		}
	}
}

func ctcRequireLinkCycle(t *testing.T, n *harness.Node, since time.Time, downWant, upWant ctcEventWant) {
	t.Helper()
	events, detail := n.Oracle().Audit(t, since, 0)
	if detail != nil {
		t.Fatalf("reading the audit for the link cycle: %s", detail.GetMessage())
	}
	var downs, ups []int
	for i, event := range events {
		if downWant.match(event) {
			downs = append(downs, i)
		}
		if upWant.match(event) {
			ups = append(ups, i)
		}
	}
	if len(downs) != 1 {
		t.Fatalf("the audit holds %d records matching %q, want exactly 1", len(downs), downWant.name)
	}
	if len(ups) != 2 {
		t.Fatalf("the audit holds %d records matching %q, want one per establishment (2)", len(ups), upWant.name)
	}
	if ups[0] >= downs[0] || downs[0] >= ups[1] {
		t.Fatalf("the link events are out of order: up at %d, down at %d, up at %d", ups[0], downs[0], ups[1])
	}
}

func ctcPeerConnected(t *testing.T, n *harness.Node, alias string) bool {
	t.Helper()
	resp, detail := n.Oracle().ListBrokers(t)
	if detail != nil {
		return false
	}
	for _, peer := range resp.GetBrokers() {
		if peer.GetAlias() == alias {
			return peer.GetStatus() == localpb.BrokerStatus_BROKER_STATUS_CONNECTED
		}
	}
	return false
}

func ctcAgentAddress(t *testing.T, s *harness.Session, name string) string {
	t.Helper()
	out, state := srListAgents(t, s)
	if state != "" {
		t.Fatalf("list_agents: %s", state)
	}
	if got, _, _ := srSplitAddress(out.Self); got != name {
		t.Fatalf("self_address is %q, want the agent %s", out.Self, name)
	}
	return out.Self
}

func ctcAccountName(t *testing.T, n *harness.Node) string {
	t.Helper()
	res := n.Exec(t, []string{"id", "-un"}, nil)
	if res.ExitCode != 0 {
		t.Fatalf("reading the node's user name: exit %d: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	name := strings.TrimSpace(res.Stdout)
	if name == "" {
		t.Fatal("reading the node's user name produced an empty name")
	}
	return name
}

func ctcGetConfigRaw(t *testing.T, s *harness.Session, key string) string {
	t.Helper()
	args := map[string]any{}
	if key != "" {
		args["key"] = key
	}
	r := s.Call(t, "get_config", args)
	r.MustOK(t)
	return string(r.Structured)
}

func ctcGetConfigTool(t *testing.T, s *harness.Session, key string) string {
	t.Helper()
	out, err := harness.Decode[mcpserver.GetConfigOut](ctcGetConfigRaw(t, s, key))
	if err != nil {
		t.Fatalf("decoding get_config: %v", err)
	}
	if len(out.Entries) != 1 {
		t.Fatalf("get_config %s returned %d entries, want 1", key, len(out.Entries))
	}
	return out.Entries[0].Value
}

func ctcAuditLogPath(n *harness.Node) string {
	return filepath.Join(n.AccountHome(), ".peers", "audit.log")
}

func ctcWaitAuditFileContains(t *testing.T, n *harness.Node, needle string) {
	t.Helper()
	path := ctcAuditLogPath(n)
	script := "cat " + shellQuote(path) + " 2>/dev/null"
	harness.Eventually(t, ctcWaitBound, "the audit file records "+needle, func() (bool, string) {
		res := n.Exec(t, []string{"sh", "-c", script}, nil)
		if res.ExitCode != 0 {
			return false, "the audit file cannot be read yet"
		}
		if !strings.Contains(res.Stdout, needle) {
			return false, "the audit file does not contain it yet"
		}
		return true, ""
	})
}
