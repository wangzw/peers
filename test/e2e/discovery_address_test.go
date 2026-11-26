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

	// dadReconcileInterval must match the reconciliation interval dadConfig
	// sets, so one propagation hop costs at most one interval.
	dadReconcileInterval = time.Second

	dadPropagationBound = 2*dadReconcileInterval + harness.EventTimeout

	// dadPropagationQuietWindow must span more than one reconciliation
	// interval and keeps a short margin because a negative check pays its
	// whole window.
	dadPropagationQuietWindow = 2*dadReconcileInterval + 3*time.Second

	// dadReconnectMax is link.reconnect_max under harness.FastTimers, copied
	// here as a literal (harness.FastReconnectMax holds the same value), so the
	// two must be kept in step.
	dadReconnectMax = 400 * time.Millisecond

	// gRPC raises client probe intervals below ten seconds to its own
	// minimum, so ten seconds is the shortest useful interval and the
	// timeout is shortened instead.
	dadKeepaliveInterval = 10 * time.Second
	dadKeepaliveTimeout  = time.Second

	dadAddressChangeBound = dadReconcileInterval + dadKeepaliveInterval +
		dadKeepaliveTimeout + dadReconnectMax + 2*dadReconcileInterval +
		harness.EventTimeout
)

// TestCardFollowsAddressChange checks that a public Broker whose interface
// address changes without a restart re-signs its card and peers two hops away
// see the new address.
func TestCardFollowsAddressChange(t *testing.T) {
	c, _, _, _ := dadCluster(t)
	cNode := c.Node("hostC")

	oldAddr := adAddr(c, "hostA", "net", adPort)
	fpA := dadWaitCardAt(t, cNode, oldAddr, dadPropagationBound)

	// Delete the old address before adding the new one: adding the new one as
	// a secondary and then deleting the primary removes both on this kernel.
	dev, oldCIDR := dadInterfaceOf(t, c, "hostA", c.Addr("hostA", "net"))
	newCIDR, newHost := dadReplaceLastOctet(t, oldCIDR, 200)
	c.Root(t, "hostA", "ip", "addr", "del", oldCIDR, "dev", dev)
	c.Root(t, "hostA", "ip", "addr", "add", newCIDR, "dev", dev)
	c.Root(t, "hostA", "ip", "route", "replace", "default", "dev", dev)
	newAddr := net.JoinHostPort(newHost, adPort)

	dadWaitCardAddresses(t, cNode, fpA, []string{newAddr}, dadAddressChangeBound)
	if _, ok := dadFindByAddress(dadListBrokers(t, cNode), oldAddr); ok {
		t.Fatalf("hostC still lists the old address %s", oldAddr)
	}
}

// TestCardShowsConfiguredAddresses checks that a configured advertised list,
// including extras and their order, reaches the dialer and a Broker two hops
// away whenever it changes.
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

	key := adCreateKey(t, opA)
	adConnect(t, opB, adShownAddress(t, key.Addresses, aAddr), key.Key).MustOK(t)
	adWaitConnected(t, aNode, "hostB.peer", adRelinkBound)

	key = adCreateKey(t, opB)
	adConnect(t, opC, adShownAddress(t, key.Addresses, bAddr), key.Key).MustOK(t)
	adWaitConnected(t, bNode, "hostC.peer", adRelinkBound)

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
	wait := func(want []string) {
		t.Helper()
		dadWaitPeerAddresses(t, dNode, fpA, want, dadPropagationBound)
		dadWaitCardAddresses(t, cNode, fpA, want, dadPropagationBound)
	}

	extras := []string{aAddr, "b.vpn.example:" + adPort, "198.51.100.7:8443"}
	set("broker.advertise_extra", "[b.vpn.example, 198.51.100.7:8443]")
	dadWaitPeerAddresses(t, dNode, fpA, extras, dadPropagationBound)

	set("broker.public", "true")
	dadWaitCardAddresses(t, cNode, fpA, extras, dadPropagationBound)

	set("broker.advertise_extra", "[]")
	set("broker.advertise", "b.example:443")
	wait([]string{"b.example:443"})

	set("broker.advertise", "b.example")
	wait([]string{"b.example:" + adPort})

	set("broker.advertise_extra", "[b.vpn.example]")
	wait([]string{"b.example:" + adPort, "b.vpn.example:" + adPort})

	set("broker.advertise", "")
	set("broker.advertise_extra", "[b.vpn.example, 198.51.100.7:8443]")
	wait(extras)
}

// TestNoDialToCardAddress checks that holding another Broker's card never
// makes a Broker dial its advertised address.
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

func dadListBrokers(t *testing.T, node *harness.Node) *localpb.ListBrokersResponse {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("listing Brokers: %s", detail.GetMessage())
	}
	return resp
}

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
