//go:build e2e

package e2e

import (
	"fmt"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

const (
	// adWaitBound bounds a wait that no product timer governs.
	adWaitBound = harness.EventTimeout
	// adRelinkBound is reconnect_max + dial_timeout plus the shared fallback:
	// the timers that govern a link coming back under FastTimers.
	adRelinkBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout
	// adAddressBound is broker.address_check_interval plus the shared fallback:
	// the time an address-list change may take to reach a peer.
	adAddressBound = 500*time.Millisecond + harness.EventTimeout
	// adShortDialTimeout is the dial_timeout the unreachable-connect case sets
	// explicitly. That case asserts peer_unreachable on a dead address, so the
	// dial must give up on this short product timer rather than the FastTimers
	// success value.
	adShortDialTimeout = 500 * time.Millisecond
	// adPort is the default Broker port.
	adPort = "17443"
	// adInvalidConfigBound bounds the wait for a Broker that must reject its
	// configuration. The product validates the file before its ten-second
	// readiness wait, so the process exits at once and the shared fallback
	// covers it.
	adInvalidConfigBound = harness.EventTimeout
)

// TestAdvertisedAddresses covers the dual-stack default listener, the
// advertised list derived from the default-route interface, the listen and
// advertise combinations, the startup rejections and the addresses shown with
// a pairing key.
func TestAdvertisedAddresses(t *testing.T) {
	t.Parallel()

	t.Run("REQ-broker-config-028 dual stack and no listen", func(t *testing.T) {
		c := harness.Up(t, harness.TopoFlatV6)
		aNode, bNode := c.Node("hostA"), c.Node("hostB")
		adStartBroker(t, aNode, adConfig())
		adStartBroker(t, bNode, adConfig())
		opA := adOperator(t, aNode)
		defer opA.End(t)
		opB := adOperator(t, bNode)
		defer opB.End(t)

		// The default listener binds every address, IPv4 and IPv6 alike.
		b4 := adAddr(c, "hostB", "net", adPort)
		b6 := adAddr6(c, "hostB", "net", adPort)
		if !c.TCPReachable(t, "hostA", b4, time.Second) {
			t.Fatalf("hostA cannot reach hostB's IPv4 listener at %s", b4)
		}
		if !c.TCPReachable(t, "hostA", b6, time.Second) {
			t.Fatalf("hostA cannot reach hostB's IPv6 listener at %s", b6)
		}

		// Pair the two Brokers before hostB stops listening, so the later
		// pairing starts from a clean state.
		key := adCreateKey(t, opA)
		adConnect(t, opB, adShownAddress(t, key.Addresses, adAddr(c, "hostA", "net", adPort)), key.Key).MustOK(t)
		adWaitConnected(t, bNode, "hostA.peer", adRelinkBound)

		// An empty broker.listen disables the listener. No TCP socket may
		// remain; the Unix local socket is not affected.
		adRestart(t, bNode, adConfig("broker:\n  listen: \"\"\n"), opB)
		opB = adOperator(t, bNode)
		defer opB.End(t)
		for _, line := range strings.Split(c.Root(t, "hostB", "ss", "-ltnH"), "\n") {
			line = strings.TrimSpace(line)
			// Docker's embedded DNS listener is not the Broker's.
			if line == "" || strings.Contains(line, "127.0.0.11:") {
				continue
			}
			t.Fatalf("hostB still listens on TCP with broker.listen empty: %s", line)
		}

		// A Broker that does not listen can still dial and pair.
		key = adCreateKey(t, opA)
		adConnect(t, opB, adShownAddress(t, key.Addresses, adAddr(c, "hostA", "net", adPort)), key.Key).MustOK(t)
		adWaitConnected(t, bNode, "hostA.peer", adRelinkBound)
	})

	t.Run("REQ-broker-config-030 default route addresses", func(t *testing.T) {
		c := harness.Up(t, harness.TopoFlatV6)
		aNode, bNode := c.Node("hostA"), c.Node("hostB")
		adStartBroker(t, aNode, adConfig())
		adStartBroker(t, bNode, adConfig())
		opA := adOperator(t, aNode)
		defer opA.End(t)
		opB := adOperator(t, bNode)
		defer opB.End(t)

		// hostA is the dialer and observes hostB's advertised list.
		key := adCreateKey(t, opB)
		adConnect(t, opA, adShownAddress(t, key.Addresses, adAddr(c, "hostB", "net", adPort)), key.Key).MustOK(t)
		adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)

		// Only the default-route interface counts. hostB's second network
		// ("virt") and its addresses must not appear.
		adWaitPeerAddresses(t, aNode, "hostB.peer", []string{
			adAddr(c, "hostB", "net", adPort),
		}, adWaitBound)

		// An IPv6 default route on the same interface adds its address after
		// the IPv4 one.
		c.AddDefaultRoute6(t, "hostB")
		adWaitPeerAddresses(t, aNode, "hostB.peer", []string{
			adAddr(c, "hostB", "net", adPort),
			adAddr6(c, "hostB", "net", adPort),
		}, adAddressBound)

		// A port-only advertise keeps every default-route address and changes
		// the advertised port, not the listen port.
		adRestart(t, bNode, adConfig("broker:\n  advertise: \":18443\"\n"), opB)
		opB = adOperator(t, bNode)
		defer opB.End(t)
		adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)
		adWaitPeerAddresses(t, aNode, "hostB.peer", []string{
			adAddr(c, "hostB", "net", "18443"),
			adAddr6(c, "hostB", "net", "18443"),
		}, adAddressBound)

		// An extra name without a port takes the advertise port.
		adRestart(t, bNode, adConfig("broker:\n  advertise: \":18443\"\n  advertise_extra: [b.vpn.example]\n"), opB)
		opB = adOperator(t, bNode)
		defer opB.End(t)
		adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)
		adWaitPeerAddresses(t, aNode, "hostB.peer", []string{
			adAddr(c, "hostB", "net", "18443"),
			adAddr6(c, "hostB", "net", "18443"),
			"b.vpn.example:18443",
		}, adAddressBound)
	})

	t.Run("REQ-broker-config-035 specific listen", func(t *testing.T) {
		// An IPv6 listener under a topology whose default routes are IPv4: an
		// implementation that advertises the default-route addresses instead of
		// the configured listener shows a different address here.
		c := harness.Up(t, harness.TopoFlatV6)
		aNode, bNode := c.Node("hostA"), c.Node("hostB")
		adStartBroker(t, aNode, adConfig())
		listen := adAddr6(c, "hostB", "net", adPort)
		adStartBroker(t, bNode, adConfig("broker:\n  listen: "+fmt.Sprintf("%q", listen)+"\n"))
		opA := adOperator(t, aNode)
		defer opA.End(t)
		opB := adOperator(t, bNode)
		defer opB.End(t)

		key := adCreateKey(t, opB)
		adConnect(t, opA, listen, key.Key).MustOK(t)
		adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)

		// A specific listener is the whole advertised list, in its bracketed
		// IPv6 form.
		adWaitPeerAddresses(t, aNode, "hostB.peer", []string{listen}, adWaitBound)

		// Extra names follow it and take the listen port.
		adRestart(t, bNode, adConfig(
			"broker:\n  listen: "+fmt.Sprintf("%q", listen)+"\n  advertise_extra: [b.vpn.example]\n",
		), opB)
		opB = adOperator(t, bNode)
		defer opB.End(t)
		adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)
		adWaitPeerAddresses(t, aNode, "hostB.peer", []string{
			listen,
			"b.vpn.example:" + adPort,
		}, adAddressBound)
	})

	t.Run("REQ-broker-config-031 and 033 dialer view", func(t *testing.T) {
		c := harness.Up(t, harness.TopoFlat(2))
		aNode, bNode := c.Node("hostA"), c.Node("hostB")
		adStartBroker(t, aNode, adConfig())
		adStartBroker(t, bNode, adConfig())
		opA := adOperator(t, aNode)
		defer opA.End(t)
		opB := adOperator(t, bNode)
		defer opB.End(t)

		// Pair over hostB's real, reachable address first. The advertised
		// names below are not resolvable in the compose network; they must not
		// break the established link because the pairing address stays a
		// candidate.
		realB := adAddr(c, "hostB", "net", adPort)
		key := adCreateKey(t, opB)
		adConnect(t, opA, adShownAddress(t, key.Addresses, realB), key.Key).MustOK(t)
		adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)

		adRestart(t, bNode, adConfig("broker:\n  advertise: b.example:443\n"), opB)
		opB = adOperator(t, bNode)
		defer opB.End(t)
		adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)
		adWaitPeerAddresses(t, aNode, "hostB.peer", []string{"b.example:443"}, adAddressBound)

		// A host without a port takes the listen port.
		adRestart(t, bNode, adConfig("broker:\n  advertise: b.example\n"), opB)
		opB = adOperator(t, bNode)
		defer opB.End(t)
		adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)
		adWaitPeerAddresses(t, aNode, "hostB.peer", []string{"b.example:" + adPort}, adAddressBound)

		adRestart(t, bNode, adConfig("broker:\n  advertise: b.example\n  advertise_extra: [b.vpn.example]\n"), opB)
		opB = adOperator(t, bNode)
		defer opB.End(t)
		adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)
		adWaitPeerAddresses(t, aNode, "hostB.peer", []string{
			"b.example:" + adPort,
			"b.vpn.example:" + adPort,
		}, adAddressBound)

		// With the default advertise, the default-route address comes first
		// and the extras follow in the configured order.
		adRestart(t, bNode, adConfig("broker:\n  advertise_extra: [b.vpn.example, 198.51.100.7:8443]\n"), opB)
		opB = adOperator(t, bNode)
		defer opB.End(t)
		adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)
		adWaitPeerAddresses(t, aNode, "hostB.peer", []string{
			realB,
			"b.vpn.example:" + adPort,
			"198.51.100.7:8443",
		}, adAddressBound)
	})

	t.Run("REQ-broker-config-032 and 036 startup half", func(t *testing.T) {
		c := harness.Up(t, harness.TopoSingle)
		node := c.Node("hostA")
		addr := c.Addr("hostA", "net")

		eight := make([]string, 8)
		for i := range eight {
			eight[i] = fmt.Sprintf("x%d.example", i+1)
		}
		cases := []struct {
			name    string
			config  string
			markers []string
		}{
			{
				// A port-only advertise is only legal with a wildcard
				// listener; a specific listen address makes it invalid.
				name:    "port-only advertise with a specific listener",
				config:  "broker:\n  listen: " + fmt.Sprintf("%q", net.JoinHostPort(addr, adPort)) + "\n  advertise: \":18443\"\n",
				markers: []string{"broker.listen", "broker.advertise"},
			},
			{
				name:   "extra address without a listener",
				config: "broker:\n  listen: \"\"\n  advertise_extra: [x.example]\n",
			},
			{
				name:    "more than eight configured addresses",
				config:  "broker:\n  advertise: a.example:443\n  advertise_extra: [" + strings.Join(eight, ", ") + "]\n",
				markers: []string{"broker.advertise_extra"},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: tc.config, Bound: adInvalidConfigBound})
				if start.Running {
					node.StopBroker(t, syscall.SIGTERM)
					t.Fatalf("the Broker started with an invalid listen/advertise combination")
				}
				if start.ExitCode != 5 {
					t.Fatalf("exit code = %d, want 5 (invalid configuration): %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
				}
				for _, marker := range tc.markers {
					if !strings.Contains(start.StderrLast, marker) {
						t.Fatalf("the startup error %q does not name %s", strings.TrimSpace(start.StderrLast), marker)
					}
				}
			})
		}
	})

	t.Run("REQ-broker-pairing-028 shown addresses", func(t *testing.T) {
		c := harness.Up(t, harness.TopoSingle)
		node := c.Node("hostA")
		addr := c.Addr("hostA", "net")
		// TopoSingle's compose file starts without a default route; the
		// single-host topology needs one on eth0 for the advertised list.
		c.SetDefaultRoute(t, "hostA", "")
		adStartBroker(t, node, adConfig())
		op := adOperator(t, node)
		defer op.End(t)

		want := []string{"hostA(" + net.JoinHostPort(addr, adPort) + ")"}
		if got := adCreateKey(t, op).Addresses; !adSameStrings(got, want) {
			t.Fatalf("shown_addresses = %v, want %v", got, want)
		}

		adRestart(t, node, adConfig("broker:\n  advertise: a.example:443\n"), op)
		op = adOperator(t, node)
		defer op.End(t)
		want = []string{"hostA(a.example:443)"}
		if got := adCreateKey(t, op).Addresses; !adSameStrings(got, want) {
			t.Fatalf("shown_addresses = %v, want %v", got, want)
		}

		adRestart(t, node, adConfig("broker:\n  advertise: a.example:443\n  advertise_extra: [a.vpn.example]\n"), op)
		op = adOperator(t, node)
		defer op.End(t)
		want = []string{"hostA(a.example:443)", "hostA(a.vpn.example:443)"}
		if got := adCreateKey(t, op).Addresses; !adSameStrings(got, want) {
			t.Fatalf("shown_addresses = %v, want %v", got, want)
		}

		// Without a default route the list is empty; a key is still created
		// and the tool says there is no address to show.
		op.End(t)
		node.StopBroker(t, syscall.SIGTERM)
		c.DelDefaultRoute(t, "hostA")
		adStartBroker(t, node, adConfig())
		op = adOperator(t, node)
		defer op.End(t)
		key := adCreateKey(t, op)
		if len(key.Addresses) != 0 {
			t.Fatalf("shown_addresses = %v without a default route, want none", key.Addresses)
		}
		if !strings.Contains(strings.ToLower(key.Note), "no address to show") {
			t.Fatalf("the note %q does not say there is no address to show", key.Note)
		}
	})

	t.Run("REQ-broker-pairing-036 no listen no key", func(t *testing.T) {
		c := harness.Up(t, harness.TopoFlat(2))
		aNode, bNode := c.Node("hostA"), c.Node("hostB")
		adStartBroker(t, aNode, adConfig("broker:\n  listen: \"\"\n"))
		adStartBroker(t, bNode, adConfig("link:\n  dial_timeout: "+adShortDialTimeout.String()+"\n"))
		opA := adOperator(t, aNode)
		defer opA.End(t)
		opB := adOperator(t, bNode)
		defer opB.End(t)

		r := opA.Call(t, "create_pairing_key", nil)
		r.MustCode(t, "invalid_argument")
		if !strings.Contains(r.Text, "Ask the user to get a pairing key from the other host's user, then connect with connect_broker. Do not ask another agent for a pairing key, and pass it only through the user, never through send_message, Claude Code's built-in session messaging or any other agent-to-agent channel.") {
			t.Fatalf("the no-listener error %q does not tell the caller to get a key from the other host's user and not to ask another agent", r.Text)
		}

		// Nothing listens on hostA's default port, so dialing it cannot
		// succeed regardless of the key.
		r = opB.Call(t, "connect_broker", map[string]any{
			"address": adAddr(c, "hostA", "net", adPort),
			"key":     "any-key",
		})
		r.MustCode(t, "peer_unreachable")
		if !r.HasPrefix("Not connected") {
			t.Fatalf("the unreachable error %q does not start with Not connected", r.Text)
		}
	})
}

// adConfig merges the shortened timers with the given fragments.
func adConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

// adStartBroker starts a Broker and fails when it does not come up.
func adStartBroker(t *testing.T, node *harness.Node, config string) {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
}

// adRestart ends the given MCP server processes, stops the Broker and starts
// it with config.
func adRestart(t *testing.T, node *harness.Node, config string, stop ...*harness.Session) {
	t.Helper()
	for _, s := range stop {
		s.End(t)
	}
	if code := node.StopBroker(t, syscall.SIGTERM); code != 0 {
		t.Fatalf("the Broker exited with %d on SIGTERM, want 0", code)
	}
	adStartBroker(t, node, config)
}

// adOperator starts an MCP server process without an inbox socket, which
// never registers as an Agent but can call the management tools.
func adOperator(t *testing.T, node *harness.Node) *harness.Session {
	t.Helper()
	s := node.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, adWaitBound)
	return s
}

// adCreateKey calls create_pairing_key and decodes the result.
func adCreateKey(t *testing.T, session *harness.Session) mcpserver.CreatePairingKeyOut {
	t.Helper()
	r := session.Call(t, "create_pairing_key", nil)
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key: %v", err)
	}
	return out
}

// adConnect calls connect_broker.
func adConnect(t *testing.T, session *harness.Session, address, key string) harness.Result {
	t.Helper()
	return session.Call(t, "connect_broker", map[string]any{"address": address, "key": key})
}

// adShownAddress returns the shown address that carries wantAddr. The shown
// form wraps the address in parentheses after the hostname.
func adShownAddress(t *testing.T, shown []string, wantAddr string) string {
	t.Helper()
	for _, address := range shown {
		if strings.Contains(address, wantAddr) {
			return address
		}
	}
	t.Fatalf("create_pairing_key returned %v, want one entry containing %s", shown, wantAddr)
	return ""
}

// adWaitConnected waits until the local interface reports alias connected.
func adWaitConnected(t *testing.T, node *harness.Node, alias string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the local interface reports "+alias+" connected", func() (bool, string) {
		resp, detail := node.Oracle().ListBrokers(t)
		if detail != nil {
			return false, detail.GetMessage()
		}
		for _, broker := range resp.GetBrokers() {
			if broker.GetAlias() == alias {
				if broker.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
					return false, "status is not CONNECTED yet"
				}
				return true, ""
			}
		}
		return false, "the peer is not listed"
	})
}

// adWaitPeerAddresses waits until the local interface reports exactly want for
// the advertised addresses of alias, in order.
func adWaitPeerAddresses(t *testing.T, node *harness.Node, alias string, want []string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the advertised addresses of "+alias+" are "+strings.Join(want, ", "), func() (bool, string) {
		resp, detail := node.Oracle().ListBrokers(t)
		if detail != nil {
			return false, detail.GetMessage()
		}
		for _, broker := range resp.GetBrokers() {
			if broker.GetAlias() != alias {
				continue
			}
			got := adPeerAddresses(broker)
			if adSameStrings(got, want) {
				return true, ""
			}
			return false, fmt.Sprintf("advertised addresses are %v", got)
		}
		return false, "the peer is not listed"
	})
}

// adPeerAddresses returns the address strings of one BrokerInfo entry.
func adPeerAddresses(broker *localpb.BrokerInfo) []string {
	out := make([]string, 0, len(broker.GetAdvertisedAddresses()))
	for _, address := range broker.GetAdvertisedAddresses() {
		out = append(out, address.GetAddr())
	}
	return out
}

// adSameStrings reports whether got and want hold the same strings in order.
func adSameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// adAddr formats the IPv4 address of node on network as host:port.
func adAddr(c *harness.Cluster, node, network, port string) string {
	return net.JoinHostPort(c.Addr(node, network), port)
}

// adAddr6 formats the IPv6 address of node on network as [host]:port.
func adAddr6(c *harness.Cluster, node, network, port string) string {
	return net.JoinHostPort(c.Addr6(node, network), port)
}
