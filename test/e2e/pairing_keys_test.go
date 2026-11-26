//go:build e2e

package e2e

import (
	"bytes"
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

const pkWaitBound = harness.EventTimeout

const pkRelinkBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout

var (
	pkStatic16         = "static-key-00001"
	pkStaticB          = "static-key-00002"
	pkStaticReplace    = "static-key-00003"
	pkStatic128        = strings.Repeat("k", 128)
	pkStaticRuntimeBad = strings.Repeat("z", 129)
	pkStaticShort      = strings.Repeat("x", 15)
)

// TestPairingKeys covers one-time keys, the preset static key, wrong keys, an
// unpaired dial and hot replacement of the static key.
func TestPairingKeys(t *testing.T) {
	c := harness.Up(t, harness.TopoFlat(4))
	nodes := map[string]*harness.Node{}
	for _, name := range []string{"hostA", "hostB", "hostC", "hostD"} {
		nodes[name] = c.Node(name)
		pkStartBroker(t, nodes[name], pkConfig())
	}
	ops := map[string]*harness.Session{}
	for _, name := range []string{"hostA", "hostB", "hostC", "hostD"} {
		ops[name] = pkOperator(t, nodes[name])
	}

	a, b, d := nodes["hostA"], nodes["hostB"], nodes["hostD"]
	nodeC := nodes["hostC"]
	opA, opB, opC, opD := ops["hostA"], ops["hostB"], ops["hostC"], ops["hostD"]
	addrA := pkAddr(c, "hostA")

	// Check the connect result against A's certificate-derived identity, not against itself.
	aFingerprint := pkIdentityFingerprint(t, a)

	t.Run("REQ-broker-pairing-006 key within validity", func(t *testing.T) {
		key := pkCreateKey(t, opA)
		expires, err := time.Parse(time.RFC3339, key.ExpiresAt)
		if err != nil {
			t.Fatalf("expires_at %q is not RFC 3339: %v", key.ExpiresAt, err)
		}
		if left := time.Until(expires); left <= 9*time.Minute || left > 10*time.Minute {
			t.Fatalf("the key expires in %s, want between 9 and 10 minutes", left)
		}

		address := pkShownAddress(t, c, "hostA", key.Addresses)
		r := pkConnect(t, opB, address, key.Key, "")
		r.MustOK(t)
		out := pkDecodeConnect(t, r)
		if out.Alias != "hostA.peer" {
			t.Fatalf("alias = %q, want hostA.peer", out.Alias)
		}
		if out.Fingerprint != aFingerprint {
			t.Fatalf("fingerprint = %q, want A's identity %q", out.Fingerprint, aFingerprint)
		}
		pkWaitConnected(t, b, "hostA.peer", pkRelinkBound)
		pkWaitConnected(t, a, "hostB.peer", pkRelinkBound)
	})

	t.Run("REQ-broker-pairing-038 connect prompt", func(t *testing.T) {
		key := pkCreateKey(t, opA)
		if key.ConnectPrompt == "" {
			t.Fatal("create_pairing_key returned no connect prompt")
		}
		for _, want := range []string{
			"Connect to the peers Broker at ",
			"using the one-time pairing key " + key.Key,
			"peers broker connect ",
			"--key " + key.Key,
		} {
			if !strings.Contains(key.ConnectPrompt, want) {
				t.Fatalf("the connect prompt does not contain %q:\n%s", want, key.ConnectPrompt)
			}
		}
		if len(key.Addresses) == 0 {
			t.Fatal("create_pairing_key returned no shown address")
		}
		for _, address := range key.Addresses {
			if !strings.Contains(key.ConnectPrompt, address) {
				t.Fatalf("the connect prompt does not name the shown address %q:\n%s", address, key.ConnectPrompt)
			}
		}

		// The earlier subtest left B paired with A; unpair B first so this
		// exercises the first pairing on B, not a reconnection of an existing
		// peer.
		pkDisconnect(t, opB, "hostA.peer")
		for _, peer := range pkPeers(t, b) {
			if peer.GetAlias() == "hostA.peer" && peer.GetIsPeer() {
				t.Fatal("B still lists A as a peer after disconnect_broker")
			}
		}

		cmd := pkPromptCommand(t, key.ConnectPrompt)
		res := b.Exec(t, []string{"sh", "-c", cmd}, map[string]string{
			"PATH": "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		})
		if res.ExitCode != 0 {
			t.Fatalf("running the prompt command %q on hostB: exit %d\nstdout:\n%s\nstderr:\n%s",
				cmd, res.ExitCode, res.Stdout, res.Stderr)
		}
		alias, fingerprint := pkPairedOutput(t, res.Stdout)
		if alias != "hostA.peer" {
			t.Fatalf("the prompt command paired with alias %q, want hostA.peer", alias)
		}
		if fingerprint != aFingerprint {
			t.Fatalf("the prompt command paired with fingerprint %q, want A's identity %q", fingerprint, aFingerprint)
		}
		pkWaitConnected(t, b, "hostA.peer", pkRelinkBound)
		pkWaitConnected(t, a, "hostB.peer", pkRelinkBound)

		bLog := b.BrokerLog(t)
		if !bytes.Contains(bLog, []byte(`"event":"peer_paired"`)) {
			t.Fatalf("B's Broker log has no peer_paired record:\n%s", bLog)
		}
		logs := append(append([]byte(nil), a.BrokerLog(t)...), bLog...)
		if bytes.Contains(logs, []byte(key.Key)) {
			t.Fatalf("a Broker log contains the pairing key %q", key.Key)
		}
		promptLine := strings.SplitN(key.ConnectPrompt, "\n", 2)[0]
		if bytes.Contains(logs, []byte(promptLine)) {
			t.Fatalf("a Broker log contains the connect prompt's first line %q", promptLine)
		}
	})

	t.Run("REQ-broker-pairing-007 one-time key used once", func(t *testing.T) {
		// The subtest above left B paired with A; this re-pairing must still
		// leave A with B as its only peer.
		key := pkCreateKey(t, opA)
		address := pkShownAddress(t, c, "hostA", key.Addresses)

		pkConnect(t, opB, address, key.Key, "").MustOK(t)
		pkWaitConnected(t, b, "hostA.peer", pkRelinkBound)

		since := time.Now().Add(-time.Second)
		r := pkConnect(t, opC, address, key.Key, "")
		pkRejectReason(t, r, a, nodeC, since, "key invalid")
		if !r.HasPrefix("Not connected") {
			t.Fatalf("the reuse error is %q, want it to start with Not connected", r.Text)
		}

		peers := pkPeers(t, a)
		if len(peers) != 1 || peers[0].GetAlias() != "hostB.peer" {
			t.Fatalf("A's peers are %v, want only hostB.peer", pkAliases(peers))
		}
	})

	t.Run("REQ-broker-pairing-008 no static key", func(t *testing.T) {
		// A runs the default configuration here and therefore has no static key.
		for _, key := range []string{"hello-world-key-123", "wrong-key-one", "wrong-key-two", "wrong-key-three"} {
			since := time.Now().Add(-time.Second)
			r := pkConnect(t, opD, addrA, key, "")
			pkRejectReason(t, r, a, d, since, "key invalid")
			if !r.HasPrefix("Not connected") {
				t.Fatalf("a wrong key returned %q, want it to start with Not connected", r.Text)
			}
		}

		if peers := pkPeers(t, d); len(peers) != 0 {
			t.Fatalf("D has peers %v, want none", pkAliases(peers))
		}
		for _, peer := range pkPeers(t, a) {
			if peer.GetAlias() == "hostD.peer" {
				t.Fatalf("A listed D after four rejected attempts")
			}
		}
	})

	t.Run("REQ-broker-pairing-011 wrong key stores nothing", func(t *testing.T) {
		since := time.Now().Add(-time.Second)
		r := pkConnect(t, opD, addrA, "definitely-not-the-key", "")
		pkRejectReason(t, r, a, d, since, "key invalid")

		aData := pkPeersJSON(t, a)
		dData := pkPeersJSON(t, d)
		dFingerprint := pkIdentityFingerprint(t, d)
		if bytes.Contains(aData, []byte(dFingerprint)) {
			t.Fatalf("A's peers.json contains D's fingerprint %s", dFingerprint)
		}
		if bytes.Contains(dData, []byte(aFingerprint)) {
			t.Fatalf("D's peers.json contains A's fingerprint %s", aFingerprint)
		}
		for _, peer := range pkPeers(t, a) {
			if peer.GetAlias() == "hostD.peer" {
				t.Fatalf("A listed D after a failed pairing")
			}
		}
	})

	t.Run("REQ-broker-pairing-010 static key and plain address", func(t *testing.T) {
		pkRestart(t, a, pkConfig(pkStaticKeyYAML(pkStatic16)), opA)
		opA = pkOperator(t, a)

		r := pkConnect(t, opB, addrA, pkStatic16, "")
		r.MustOK(t)
		out := pkDecodeConnect(t, r)
		if out.Alias != "hostA.peer" {
			t.Fatalf("alias = %q, want hostA.peer", out.Alias)
		}
		if out.Fingerprint != aFingerprint {
			t.Fatalf("fingerprint = %q, want A's identity %q", out.Fingerprint, aFingerprint)
		}
		pkWaitConnected(t, b, "hostA.peer", pkRelinkBound)
	})

	t.Run("REQ-broker-pairing-009 static key many times", func(t *testing.T) {
		for name, op := range map[string]*harness.Session{
			"hostB": opB, "hostC": opC, "hostD": opD,
		} {
			r := pkConnect(t, op, addrA, pkStatic16, "")
			r.MustOK(t)
			if out := pkDecodeConnect(t, r); out.Fingerprint != aFingerprint {
				t.Fatalf("%s saw fingerprint %q, want A's identity %q", name, out.Fingerprint, aFingerprint)
			}
		}
	})

	t.Run("REQ-broker-pairing-025 never generates a static key", func(t *testing.T) {
		pkRestart(t, a, pkConfig(), opA)
		opA = pkOperator(t, a)

		var keys []string
		for i := 0; i < 3; i++ {
			key := pkCreateKey(t, opA)
			if key.ExpiresAt == "" {
				t.Fatal("a generated key has no expires_at")
			}
			keys = append(keys, key.Key, strings.ReplaceAll(key.Key, "-", ""))
		}
		for _, needle := range keys {
			if pkDataDirContains(t, a, needle) {
				t.Fatalf("a generated one-time key is stored under %s/.peers", a.AccountHome())
			}
		}

		pkRestart(t, a, pkConfig(pkStaticKeyYAML(pkStaticB)), opA)
		opA = pkOperator(t, a)
		pkConnect(t, opD, addrA, pkStaticB, "").MustOK(t)
	})

	t.Run("REQ-broker-pairing-029 static key length", func(t *testing.T) {
		opA.End(t)
		a.StopBroker(t, syscall.SIGTERM)
		start := a.StartBroker(t, harness.BrokerSpec{ConfigYAML: pkConfig(pkStaticKeyYAML(pkStaticShort))})
		if start.Running {
			t.Fatal("the Broker started with a 15-character static key")
		}
		if start.ExitCode != 5 {
			t.Fatalf("exit code = %d, want 5; stderr: %s", start.ExitCode, start.StderrLast)
		}
		if !strings.Contains(start.StderrLast, "pairing.static_key") {
			t.Fatalf("stderr %q does not name pairing.static_key", start.StderrLast)
		}

		pkStartBroker(t, a, pkConfig(pkStaticKeyYAML(pkStatic16)))
		opA = pkOperator(t, a)
		pkConnect(t, opD, addrA, pkStatic16, "").MustOK(t)

		pkRestart(t, a, pkConfig(pkStaticKeyYAML(pkStatic128)), opA)
		opA = pkOperator(t, a)
		pkConnect(t, opD, addrA, pkStatic128, "").MustOK(t)

		a.WriteConfig(t, pkConfig(pkStaticKeyYAML(pkStaticRuntimeBad)))
		harness.Eventually(t, pkWaitBound, "the Broker keeps the current invalid static key", func() (bool, string) {
			log := a.BrokerLog(t)
			kept := bytes.Contains(log, []byte("keeping the current value"))
			named := bytes.Contains(log, []byte("pairing.static_key"))
			if kept && named {
				return true, ""
			}
			return false, fmt.Sprintf("broker log has keeping=%v key=%v", kept, named)
		})
		pkConnect(t, opC, addrA, pkStatic128, "").MustOK(t)
		pkConnect(t, opD, addrA, pkStaticRuntimeBad, "").MustCode(t, "pairing_rejected")
	})

	t.Run("REQ-broker-pairing-033 replace static key", func(t *testing.T) {
		// B paired with the old static key in the earlier static-key case and
		// must stay connected across the replacement.
		since := time.Now().Add(-time.Second)
		a.WriteConfig(t, pkConfig(pkStaticKeyYAML(pkStaticReplace)))
		pkWaitAuditConfigChange(t, a, "pairing.static_key", since)

		rejectedAt := time.Now().Add(-time.Second)
		pkRejectReason(t, pkConnect(t, opC, addrA, pkStatic128, ""), a, nodeC, rejectedAt, "key invalid")
		pkConnect(t, opD, addrA, pkStaticReplace, "").MustOK(t)
		pkWaitConnected(t, b, "hostA.peer", pkRelinkBound)
	})

	t.Run("REQ-broker-pairing-015 unpaired without key rejected", func(t *testing.T) {
		before := pkPeers(t, a)
		reason := pkProbeUnpairedHello(t, c, "hostB", addrA)
		if reason != "REJECT_REASON_NOT_A_PEER" {
			t.Fatalf("reject reason = %q, want REJECT_REASON_NOT_A_PEER", reason)
		}
		for _, peer := range pkPeers(t, a) {
			if peer.GetAlias() == "probe" {
				t.Fatal("A listed the unpaired probe as a Broker")
			}
		}
		if after := pkPeers(t, a); len(after) != len(before) {
			t.Fatalf("A's peer count changed from %d to %d after the probe", len(before), len(after))
		}
	})
}

func pkConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

func pkStaticKeyYAML(key string) string {
	return fmt.Sprintf("pairing:\n  static_key: %q\n", key)
}

func pkAddr(c *harness.Cluster, node string) string {
	return net.JoinHostPort(c.Addr(node, "net"), "17443")
}

func pkStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

func pkRestart(t *testing.T, node *harness.Node, config string, stop ...*harness.Session) {
	t.Helper()
	// End the MCP server processes before stopping the Broker.
	for _, s := range stop {
		s.End(t)
	}
	node.StopBroker(t, syscall.SIGTERM)
	pkStartBroker(t, node, config)
}

func pkOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, pkWaitBound)
	return s
}

func pkCreateKey(t *testing.T, op *harness.Session) mcpserver.CreatePairingKeyOut {
	t.Helper()
	r := op.Call(t, "create_pairing_key", map[string]any{})
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out
}

func pkPromptCommand(t *testing.T, prompt string) string {
	t.Helper()
	for _, line := range strings.Split(prompt, "\n") {
		if trimmed := strings.TrimSpace(line); strings.HasPrefix(trimmed, "peers broker connect ") {
			return trimmed
		}
	}
	t.Fatalf("the connect prompt carries no terminal command:\n%s", prompt)
	return ""
}

func pkPairedOutput(t *testing.T, stdout string) (alias, fingerprint string) {
	t.Helper()
	const prefix = "Paired with "
	line := strings.TrimSpace(stdout)
	if !strings.HasPrefix(line, prefix) {
		t.Fatalf("broker connect printed %q, want %q<alias> (<fingerprint>)", line, prefix)
	}
	rest := strings.TrimPrefix(line, prefix)
	open := strings.LastIndex(rest, " (")
	if open < 0 || !strings.HasSuffix(rest, ")") {
		t.Fatalf("broker connect printed %q, want %q<alias> (<fingerprint>)", line, prefix)
	}
	return strings.TrimSpace(rest[:open]), rest[open+2 : len(rest)-1]
}

func pkConnect(t *testing.T, op *harness.Session, address, key, localAlias string) harness.Result {
	t.Helper()
	args := map[string]any{"address": address, "key": key}
	if localAlias != "" {
		args["local_alias"] = localAlias
	}
	return op.Call(t, "connect_broker", args)
}

// The acceptor audits the failure by the initiator's fingerprint, whose alias
// stays empty until the proof verifies.
func pkRejectReason(t *testing.T, r harness.Result, acceptor, initiator *harness.Node, since time.Time, reason string) {
	t.Helper()
	r.MustCode(t, "pairing_rejected")
	wantText := "invalid"
	if strings.Contains(reason, "expired") {
		wantText = "expired"
	}
	if !strings.Contains(r.Text, wantText) {
		t.Fatalf("the dialer's error is %q, want it to name %q", r.Text, wantText)
	}
	initiatorFingerprint := pkIdentityFingerprint(t, initiator)
	harness.Eventually(t, pkWaitBound, "the pairing failure is audited as "+reason, func() (bool, string) {
		events, detail := acceptor.Oracle().Audit(t, since, 0)
		if detail != nil {
			return false, detail.GetMessage()
		}
		var seen []string
		for _, event := range events {
			if event.GetEvent() != localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_FAILED {
				continue
			}
			if hex.EncodeToString(event.GetPeer().GetFingerprint()) != initiatorFingerprint {
				continue
			}
			if event.GetPeer().GetAlias() != "" {
				return false, fmt.Sprintf("pairing_failed for the initiator carries the unverified alias %q", event.GetPeer().GetAlias())
			}
			seen = append(seen, event.GetDetail())
			if event.GetDetail() == reason {
				return true, ""
			}
		}
		return false, fmt.Sprintf("pairing_failed events for initiator %s carry details %v", initiatorFingerprint, seen)
	})
}

func pkDecodeConnect(t *testing.T, r harness.Result) mcpserver.ConnectBrokerOut {
	t.Helper()
	out, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker: %v", err)
	}
	return out
}

func pkShownAddress(t *testing.T, c *harness.Cluster, node string, shown []string) string {
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

func pkPeers(t *testing.T, node *harness.Node) []*localpb.BrokerInfo {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("list_brokers: %s", detail.GetMessage())
	}
	return resp.GetBrokers()
}

func pkAliases(peers []*localpb.BrokerInfo) []string {
	out := make([]string, 0, len(peers))
	for _, peer := range peers {
		out = append(out, peer.GetAlias())
	}
	return out
}

func pkWaitConnected(t *testing.T, node *harness.Node, alias string, bound time.Duration) {
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

func pkDisconnect(t *testing.T, op *harness.Session, broker string) {
	t.Helper()
	r := op.Call(t, "disconnect_broker", map[string]any{"broker": broker})
	r.MustOK(t)
}

func pkPeersJSON(t *testing.T, node *harness.Node) []byte {
	t.Helper()
	path := filepath.Join(node.AccountHome(), ".peers", "peers.json")
	res := node.Exec(t, []string{"sh", "-c", "if [ -f " + shellQuote(path) + " ]; then cat " + shellQuote(path) + "; fi"}, nil)
	if res.ExitCode != 0 {
		t.Fatalf("reading %s: exit %d: %s", path, res.ExitCode, res.Stderr)
	}
	return []byte(res.Stdout)
}

func pkDataDirContains(t *testing.T, node *harness.Node, needle string) bool {
	t.Helper()
	dir := filepath.Join(node.AccountHome(), ".peers")
	// The peer table is renamed into place from a temp file, so skip *.tmp-*
	// and tolerate a file vanishing between listing and reading.
	const script = `dir=$1; needle=$2; status=1
for f in $(find "$dir" -type f ! -name '*.tmp-*' 2>/dev/null); do
	if grep -qF -e "$needle" -- "$f" 2>/dev/null; then status=0; break; fi
done
exit $status`
	res := node.Exec(t, []string{"sh", "-c", script, "sh", dir, needle}, nil)
	switch res.ExitCode {
	case 0:
		return true
	case 1:
		return false
	default:
		t.Fatalf("searching %s for %q: exit %d: %s", dir, needle, res.ExitCode, res.Stderr)
		return false
	}
}

func pkIdentityFingerprint(t *testing.T, node *harness.Node) string {
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

func pkWaitAuditConfigChange(t *testing.T, node *harness.Node, key string, since time.Time) *localpb.AuditEvent {
	t.Helper()
	var found *localpb.AuditEvent
	harness.Eventually(t, pkWaitBound, "the configuration file change of "+key+" is audited", func() (bool, string) {
		events, detail := node.Oracle().Audit(t, since, 0)
		if detail != nil {
			return false, detail.GetMessage()
		}
		for _, event := range events {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_FILE_CHANGED && event.GetKey() == key {
				found = event
				return true, ""
			}
		}
		return false, "no config_file_changed record yet"
	})
	return found
}

func pkProbeUnpairedHello(t *testing.T, c *harness.Cluster, fromNode, addr string) string {
	t.Helper()
	res := c.Node(fromNode).Exec(t, []string{"/usr/local/bin/e2e-node", "probe-hello", "--addr", addr}, nil)
	if res.ExitCode != 0 {
		t.Fatalf("the link probe exited %d: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	for _, line := range strings.Split(res.Stdout, "\n") {
		if reason, ok := strings.CutPrefix(strings.TrimSpace(line), "REASON="); ok {
			return reason
		}
	}
	t.Fatalf("the link probe printed no reason: %q", res.Stdout)
	return ""
}
