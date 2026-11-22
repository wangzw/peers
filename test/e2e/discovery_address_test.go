//go:build e2e

package e2e

import (
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

const (
	// dadPortNum is the default Broker port as a number, for the SYN counter.
	dadPortNum = 17443

	// dadReconcileInterval matches directory.reconcile_interval in dadConfig:
	// one propagation hop costs at most one interval.
	dadReconcileInterval = time.Second

	// dadPropagationBound bounds a wait for a card crossing two hops: twice the
	// reconciliation interval plus the tree's EventTimeout margin.
	dadPropagationBound = 2*dadReconcileInterval + harness.EventTimeout

	// dadPropagationQuietWindow is the observation window of a negative check
	// that watches a card across two hops. It must span more than one
	// reconciliation interval so a maintenance pass would show a change it
	// watches for; it keeps the three-second margin instead of EventTimeout
	// because a negative check pays its whole window.
	dadPropagationQuietWindow = 2*dadReconcileInterval + 3*time.Second

	// dadReconnectMax is link.reconnect_max under harness.FastTimers.
	dadReconnectMax = 400 * time.Millisecond

	// dadKeepaliveInterval and dadKeepaliveTimeout are link.keepalive_interval
	// and link.keepalive_timeout in dadConfig. Deleting the interface address
	// leaves the established TCP connection in place, so the dialer notices the
	// dead link only when its keepalive ping times out. gRPC raises client
	// probe intervals below ten seconds to its own minimum, so ten seconds is
	// the shortest useful interval; the timeout is shortened instead.
	dadKeepaliveInterval = 10 * time.Second
	dadKeepaliveTimeout  = time.Second

	// dadAddressChangeBound is the bound for a changed interface address: one
	// address check, the dialer's keepalive probe and timeout, one reconnect,
	// two propagation hops and the tree's EventTimeout margin.
	dadAddressChangeBound = dadReconcileInterval + dadKeepaliveInterval +
		dadKeepaliveTimeout + dadReconnectMax + 2*dadReconcileInterval +
		harness.EventTimeout
)

// TestCardFollowsAddressChange covers REQ-broker-discovery-016: when a public
// Broker's interface address changes without a restart, it re-signs its card
// and peers two hops away see the new address instead of the old one.
func TestCardFollowsAddressChange(t *testing.T) {
	c, _, _, _ := dadCluster(t)
	cNode := c.Node("hostC")

	oldAddr := adAddr(c, "hostA", "net", adPort)
	fpA := dadWaitCardAt(t, cNode, oldAddr, dadPropagationBound)

	// Move hostA to the last usable address of its own subnet and stop
	// carrying the old one. The default route stays on the same interface, so
	// the derived advertised list follows the change.
	//
	// The old address goes first: adding the new one as a secondary of the
	// same prefix and then deleting the primary removes both on this kernel,
	// which would leave the interface with no IPv4 address at all.
	dev, oldCIDR := dadInterfaceOf(t, c, "hostA", c.Addr("hostA", "net"))
	newCIDR, newHost := dadReplaceLastOctet(t, oldCIDR, 200)
	c.Root(t, "hostA", "ip", "addr", "del", oldCIDR, "dev", dev)
	c.Root(t, "hostA", "ip", "addr", "add", newCIDR, "dev", dev)
	c.Root(t, "hostA", "ip", "route", "replace", "default", "dev", dev)
	newAddr := net.JoinHostPort(newHost, adPort)

	// hostA keeps running: nothing but the address check, the keepalive
	// timeout on the now-dead link and the reissued card may move the peer's
	// view.
	dadWaitCardAddresses(t, cNode, fpA, []string{newAddr}, dadAddressChangeBound)
	if _, ok := dadFindByAddress(dadListBrokers(t, cNode), oldAddr); ok {
		t.Fatalf("hostC still lists the old address %s", oldAddr)
	}
}

// TestCardShowsConfiguredAddresses covers REQ-broker-config-031 and 033: the
// configured advertised list, including the extras and their order, reaches the
// Broker that dialed hostA and a Broker two hops away that holds its card,
// whenever the list changes. The extras are asserted before hostA is public, as
// REQ-broker-config-033 requires, and after it.
func TestCardShowsConfiguredAddresses(t *testing.T) {
	c := harness.Up(t, harness.TopoFlat(4))
	aNode, bNode, cNode, dNode := c.Node("hostA"), c.Node("hostB"), c.Node("hostC"), c.Node("hostD")

	// hostA starts private: the extras must reach its dialer before the card
	// exists, through the accept-side announcement alone.
	adStartBroker(t, aNode, dadConfig(false))
	adStartBroker(t, bNode, dadConfig(false))
	adStartBroker(t, cNode, dadConfig(false))
	adStartBroker(t, dNode, dadConfig(false))
	opA := adOperator(t, aNode)
	defer opA.End(t)
	opB := adOperator(t, bNode)
	defer opB.End(t)
	opC := adOperator(t, cNode)
	defer opC.End(t)
	opD := adOperator(t, dNode)
	defer opD.End(t)

	aAddr := adAddr(c, "hostA", "net", adPort)
	bAddr := adAddr(c, "hostB", "net", adPort)

	// hostB dials hostA and hostC dials hostB, so a card published by hostA
	// reaches hostC two hops away once hostA is public.
	key := adCreateKey(t, opA)
	adConnect(t, opB, adShownAddress(t, key.Addresses, aAddr), key.Key).MustOK(t)
	adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)

	key = adCreateKey(t, opB)
	adConnect(t, opC, adShownAddress(t, key.Addresses, bAddr), key.Key).MustOK(t)
	adWaitConnected(t, bNode, "hostC.peer", adRelinkBound)

	// hostD dials hostA. Its result is the dialer's view the requirements name.
	key = adCreateKey(t, opA)
	r := adConnect(t, opD, adShownAddress(t, key.Addresses, aAddr), key.Key)
	r.MustOK(t)
	connected, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker: %v", err)
	}
	if connected.Fingerprint == "" {
		t.Fatal("connect_broker returned no fingerprint")
	}
	fpA := connected.Fingerprint
	adWaitConnected(t, dNode, "hostA.peer", adRelinkBound)

	set := func(key, value string) {
		t.Helper()
		opA.Call(t, "set_config", map[string]any{"key": key, "value": value}).MustOK(t)
	}
	// wait checks the dialer's view of hostA and the card view on hostC.
	wait := func(want []string) {
		t.Helper()
		dadWaitPeerAddresses(t, dNode, fpA, want, dadPropagationBound)
		dadWaitCardAddresses(t, cNode, fpA, want, dadPropagationBound)
	}

	// REQ-broker-config-033: with hostA still private, its dialer sees the
	// default-route address followed by the extras in their configured order.
	extras := []string{aAddr, "b.vpn.example:" + adPort, "198.51.100.7:8443"}
	set("broker.advertise_extra", "[b.vpn.example, 198.51.100.7:8443]")
	dadWaitPeerAddresses(t, dNode, fpA, extras, dadPropagationBound)

	// Once hostA is public, the Broker holding its card sees the same list.
	set("broker.public", "true")
	dadWaitCardAddresses(t, cNode, fpA, extras, dadPropagationBound)

	// REQ-broker-config-031: a configured host address leads the list and the
	// default-route address is gone. The extras from the previous step are
	// cleared so only the configured primary remains.
	set("broker.advertise_extra", "[]")
	set("broker.advertise", "b.example:443")
	wait([]string{"b.example:443"})

	// A host without a port takes the listen port.
	set("broker.advertise", "b.example")
	wait([]string{"b.example:" + adPort})

	// An extra name follows the primary one.
	set("broker.advertise_extra", "[b.vpn.example]")
	wait([]string{"b.example:" + adPort, "b.vpn.example:" + adPort})

	// Back to the default primary: the interface address comes first, then the
	// extras in the configured order, each keeping its own port.
	set("broker.advertise", "")
	set("broker.advertise_extra", "[b.vpn.example, 198.51.100.7:8443]")
	wait(extras)
}

// TestNoDialToCardAddress covers REQ-broker-pairing-014: holding another
// Broker's card never makes a Broker dial its advertised address.
func TestNoDialToCardAddress(t *testing.T) {
	c, _, _, _ := dadCluster(t)
	cNode := c.Node("hostC")

	aIP := c.Addr("hostA", "net")
	_ = dadWaitCardAt(t, cNode, adAddr(c, "hostA", "net", adPort), dadPropagationBound)
	count := c.SYNCounter(t, "hostC", harness.Outbound, aIP, dadPortNum)
	harness.Consistently(t, dadPropagationQuietWindow,
		"hostC sends no SYN to hostA's card address",
		func() (bool, string) {
			if n := count(); n != 0 {
				return false, fmt.Sprintf("%d SYNs were sent", n)
			}
			return true, ""
		})
}

// dadCluster starts the three-node flat topology the address cases share: A is
// public, and A and C both pair to B, so C learns A's card through B. It
// returns the operators of the three nodes; the sessions end with the test.
func dadCluster(t *testing.T) (*harness.Cluster, *harness.Session, *harness.Session, *harness.Session) {
	t.Helper()
	c := harness.Up(t, harness.TopoFlat(3))
	aNode, bNode, cNode := c.Node("hostA"), c.Node("hostB"), c.Node("hostC")

	adStartBroker(t, aNode, dadConfig(true))
	adStartBroker(t, bNode, dadConfig(false))
	adStartBroker(t, cNode, dadConfig(false))

	opA := adOperator(t, aNode)
	t.Cleanup(func() { opA.End(t) })
	opB := adOperator(t, bNode)
	t.Cleanup(func() { opB.End(t) })
	opC := adOperator(t, cNode)
	t.Cleanup(func() { opC.End(t) })

	bAddr := adAddr(c, "hostB", "net", adPort)
	key := adCreateKey(t, opB)
	adConnect(t, opA, adShownAddress(t, key.Addresses, bAddr), key.Key).MustOK(t)
	adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)

	key = adCreateKey(t, opB)
	adConnect(t, opC, adShownAddress(t, key.Addresses, bAddr), key.Key).MustOK(t)
	adWaitConnected(t, cNode, "hostB.peer", adRelinkBound)

	return c, opA, opB, opC
}

// dadConfig builds one node's configuration with the shortened timers and the
// plan's one-second address check and reconciliation intervals.
func dadConfig(public bool) string {
	parts := []string{
		harness.FastTimers,
		"directory:\n  reconcile_interval: 1s\nbroker:\n  address_check_interval: 1s",
		"link:\n  keepalive_interval: 10s\n  keepalive_timeout: 1s",
	}
	if public {
		parts = append(parts, "broker:\n  public: true")
	}
	return harness.ConfigYAML(parts...)
}

// dadListBrokers returns the Brokers known to node.
func dadListBrokers(t *testing.T, node *harness.Node) *localpb.ListBrokersResponse {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("listing Brokers: %s", detail.GetMessage())
	}
	return resp
}

// dadFindByAddress returns a non-peer entry whose advertised addresses include
// addr.
func dadFindByAddress(resp *localpb.ListBrokersResponse, addr string) (*localpb.BrokerInfo, bool) {
	for _, broker := range resp.GetBrokers() {
		if broker.GetIsPeer() {
			continue
		}
		for _, advertised := range broker.GetAdvertisedAddresses() {
			if advertised.GetAddr() == addr {
				return broker, true
			}
		}
	}
	return nil, false
}

// dadWaitCardAt waits until node lists a non-peer whose advertised addresses
// include addr and returns that Broker's fingerprint as lowercase hex.
func dadWaitCardAt(t *testing.T, node *harness.Node, addr string, bound time.Duration) string {
	t.Helper()
	var fp string
	harness.Eventually(t, bound, "the card advertising "+addr+" reaches the node",
		func() (bool, string) {
			resp := dadListBrokers(t, node)
			broker, ok := dadFindByAddress(resp, addr)
			if !ok {
				return false, "no non-peer advertises " + addr
			}
			fp = hex.EncodeToString(broker.GetFingerprint())
			return true, ""
		})
	return fp
}

// dadWaitCardAddresses waits until node lists fp as a connectable non-peer
// whose advertised addresses are exactly want, in order.
func dadWaitCardAddresses(t *testing.T, node *harness.Node, fp string, want []string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the card of "+fp+" shows "+strings.Join(want, ", "),
		func() (bool, string) {
			resp := dadListBrokers(t, node)
			for _, broker := range resp.GetBrokers() {
				if hex.EncodeToString(broker.GetFingerprint()) != fp {
					continue
				}
				if broker.GetIsPeer() {
					return false, "the card is listed as a peer"
				}
				if got := broker.GetStatus(); got != localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE {
					return false, "status is " + got.String()
				}
				if got := adPeerAddresses(broker); !adSameStrings(got, want) {
					return false, fmt.Sprintf("addresses are %v", got)
				}
				return true, ""
			}
			return false, "the card is not listed"
		})
}

// dadWaitPeerAddresses waits until node lists fp as a peer whose advertised
// addresses are exactly want, in order.
func dadWaitPeerAddresses(t *testing.T, node *harness.Node, fp string, want []string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the peer view of "+fp+" shows "+strings.Join(want, ", "),
		func() (bool, string) {
			resp := dadListBrokers(t, node)
			for _, broker := range resp.GetBrokers() {
				if hex.EncodeToString(broker.GetFingerprint()) != fp {
					continue
				}
				if !broker.GetIsPeer() {
					return false, "the peer is listed as a non-peer"
				}
				if got := adPeerAddresses(broker); !adSameStrings(got, want) {
					return false, fmt.Sprintf("addresses are %v", got)
				}
				return true, ""
			}
			return false, "the peer is not listed"
		})
}

// dadInterfaceOf returns the interface and IPv4 CIDR that carry addr on node.
func dadInterfaceOf(t *testing.T, c *harness.Cluster, node, addr string) (string, string) {
	t.Helper()
	out := c.Root(t, node, "ip", "-o", "-4", "addr", "show", "to", addr)
	fields := strings.Fields(out)
	for i := 0; i+1 < len(fields); i++ {
		if fields[i] != "inet" || !strings.HasPrefix(fields[i+1], addr+"/") {
			continue
		}
		if i == 0 {
			break
		}
		return strings.TrimSuffix(fields[i-1], ":"), fields[i+1]
	}
	t.Fatalf("no interface carries %s: %q", addr, out)
	return "", ""
}

// dadReplaceLastOctet returns cidr with its last octet replaced by last, and
// the resulting host without the prefix.
func dadReplaceLastOctet(t *testing.T, cidr string, last byte) (string, string) {
	t.Helper()
	ip, network, err := net.ParseCIDR(cidr)
	if err != nil {
		t.Fatalf("parsing %q as an IPv4 CIDR: %v", cidr, err)
	}
	v4 := ip.To4()
	if v4 == nil {
		t.Fatalf("%q is not an IPv4 address", cidr)
	}
	v4[3] = last
	ones, _ := network.Mask.Size()
	host := v4.String()
	return fmt.Sprintf("%s/%d", host, ones), host
}
