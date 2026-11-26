//go:build e2e

package e2e

import (
	"encoding/hex"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
)

// discReconcile is the directory.reconcile_interval every discovery case sets:
// one hop is one reconcile pass.
const discReconcile = time.Second

const discSlack = harness.EventTimeout

// discQuietSlack must span more than one reconciliation pass so a pass inside
// the window would show the change; it stays short because a negative check
// pays its whole window.
const discQuietSlack = 3 * time.Second

func discBound(hops int) time.Duration {
	return time.Duration(hops)*discReconcile + discSlack
}

func discQuietBound(hops int) time.Duration {
	return time.Duration(hops)*discReconcile + discQuietSlack
}

type discNode struct {
	*harness.Node

	name   string
	addr   string
	alias  string
	config string
	env    map[string]string
}

func discNewNode(t *testing.T, name, extra string, public bool) *discNode {
	t.Helper()
	return discNewNodeEnv(t, name, extra, public, nil)
}

func discNewNodeEnv(t *testing.T, name, extra string, public bool, env map[string]string) *discNode {
	t.Helper()
	host := harness.NewLocalHost(t, harness.LocalOptions{Name: "disc-" + name})
	d := &discNode{
		Node:  host,
		name:  name,
		addr:  host.FreeAddr(t),
		alias: "disc-" + name,
		env:   env,
	}
	d.config = discConfig(d.addr, d.alias, public, extra)
	d.start(t)
	return d
}

func discCluster(t *testing.T, names ...string) map[string]*discNode {
	t.Helper()
	out := make(map[string]*discNode, len(names))
	for _, name := range names {
		out[name] = discNewNode(t, name, "", false)
	}
	return out
}

func discConfig(addr, alias string, public bool, extra string) string {
	broker := "broker:\n  listen: " + discYAMLString(addr) + "\n  alias: " + alias
	if addr != "" {
		broker += "\n  advertise: " + discYAMLString(addr)
	}
	parts := []string{
		harness.FastTimers,
		"directory:\n  reconcile_interval: 1s",
		broker,
	}
	if public {
		parts = append(parts, "broker:\n  public: true")
	}
	if extra != "" {
		parts = append(parts, extra)
	}
	return harness.ConfigYAML(parts...)
}

func discYAMLString(addr string) string {
	if addr == "" {
		return `""`
	}
	return addr
}

// start starts the Broker; when the one retry moves the listen address because
// the port race was lost, addr and config follow the address it actually uses.
func (d *discNode) start(t *testing.T) {
	t.Helper()
	start := d.StartBroker(t, harness.BrokerSpec{ConfigYAML: d.config, Env: d.env})
	if !start.Running {
		t.Fatalf("Broker %s did not start: exit code %d: %s", d.name, start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
	if actual := start.Address(d.addr); actual != d.addr {
		d.config = strings.ReplaceAll(d.config, d.addr, actual)
		d.addr = actual
	}
}

func discOperator(t *testing.T, d *discNode) *harness.Session {
	t.Helper()
	s := d.StartSession(t, harness.SessionSpec{
		Name:        "operator",
		StartMCP:    true,
		NoSocketEnv: true,
		Workdir:     t.TempDir(),
	})
	harness.WaitConnected(t, s, harness.EventTimeout)
	return s
}

func discAgent(t *testing.T, d *discNode, name, workdir string) *harness.Session {
	t.Helper()
	return d.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true, Workdir: workdir})
}

// discCallOK polls a read-only or idempotent tool and retries a transport
// error; any other tool goes through CallUntilOK, which fails on a transport
// error instead of repeating a call the server may already have applied.
func discCallOK(t *testing.T, s *harness.Session, tool string, args map[string]any, bound time.Duration) harness.Result {
	t.Helper()
	if !pollableTool(tool) {
		return s.CallUntilOK(t, bound, tool, args)
	}
	pollBound, waitBound := harness.ToolPollBound(tool), bound
	if want := harness.ToolWaitBound(tool); waitBound < want {
		waitBound = want
	}
	var result harness.Result
	harness.EventuallyPoll(t, waitBound, "calling "+tool, func(deadline time.Time) (bool, string) {
		var err error
		result, err = s.PollCallWithin(t, deadline, pollBound, tool, args)
		if err != nil {
			return false, err.Error()
		}
		if result.IsError {
			return false, result.Text
		}
		return true, ""
	})
	return result
}

func discPair(t *testing.T, acceptor, dialer *discNode) mcpserver.ConnectBrokerOut {
	t.Helper()
	creator := discOperator(t, acceptor)
	defer creator.End(t)

	r := discCallOK(t, creator, "create_pairing_key", nil, harness.EventTimeout)
	key, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key on %s: %v", acceptor.name, err)
	}
	shown := false
	for _, address := range key.Addresses {
		// The shown form is "hostname(host:port)"; match the inner address.
		if strings.Contains(address, acceptor.addr) {
			shown = true
		}
	}
	if !shown {
		t.Fatalf("create_pairing_key on %s shows %v, want an entry carrying %s", acceptor.name, key.Addresses, acceptor.addr)
	}

	op := discOperator(t, dialer)
	defer op.End(t)
	r = discCallOK(t, op, "connect_broker", map[string]any{"address": acceptor.addr, "key": key.Key}, harness.EventTimeout)
	out, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker on %s: %v", dialer.name, err)
	}

	discWaitConnected(t, acceptor, dialer.alias, localpb.PeerRole_PEER_ROLE_ACCEPTOR, harness.EventTimeout)
	discWaitConnected(t, dialer, acceptor.alias, localpb.PeerRole_PEER_ROLE_DIALER, harness.EventTimeout)
	return out
}

func discWaitConnected(t *testing.T, d *discNode, alias string, role localpb.PeerRole, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, fmt.Sprintf("%s reports %s connected", d.name, alias), func() (bool, string) {
		brokers, state := discList(t, d)
		if state != "" {
			return false, state
		}
		for _, b := range brokers {
			if b.GetAlias() != alias {
				continue
			}
			if b.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTED {
				return false, "status is not CONNECTED"
			}
			if b.GetRole() != role {
				return false, fmt.Sprintf("role is %v, want %v", b.GetRole(), role)
			}
			return true, ""
		}
		return false, "the peer is not listed"
	})
}

func discList(t *testing.T, d *discNode) ([]*localpb.BrokerInfo, string) {
	t.Helper()
	resp, detail := d.Oracle().ListBrokers(t)
	if detail != nil {
		return nil, detail.GetMessage()
	}
	return resp.GetBrokers(), ""
}

func discFindFingerprint(brokers []*localpb.BrokerInfo, fpHex string) (*localpb.BrokerInfo, bool) {
	for _, b := range brokers {
		if hex.EncodeToString(b.GetFingerprint()) == fpHex {
			return b, true
		}
	}
	return nil, false
}

func discHasAddress(b *localpb.BrokerInfo, addr string) bool {
	for _, a := range b.GetAdvertisedAddresses() {
		if a.GetAddr() == addr {
			return true
		}
	}
	return false
}

func discWaitBroker(t *testing.T, d *discNode, fpHex string, bound time.Duration, check func(*localpb.BrokerInfo) string) *localpb.BrokerInfo {
	t.Helper()
	if check == nil {
		check = func(*localpb.BrokerInfo) string { return "" }
	}
	var found *localpb.BrokerInfo
	harness.Eventually(t, bound, fmt.Sprintf("%s lists broker %s", d.name, fpHex), func() (bool, string) {
		brokers, state := discList(t, d)
		if state != "" {
			return false, state
		}
		b, ok := discFindFingerprint(brokers, fpHex)
		if !ok {
			return false, "the broker is not listed"
		}
		if msg := check(b); msg != "" {
			return false, msg
		}
		found = b
		return true, ""
	})
	return found
}

// discAbsentConsistently polls with the short poll bound because the window is
// only a few seconds; a transport error is an inconclusive sample, not a pass.
func discAbsentConsistently(t *testing.T, d *discNode, window time.Duration, what string, fingerprints []string, alias, addr string) {
	t.Helper()
	harness.ConsistentlyPoll(t, window, harness.ToolCallPollBound, what, nil, func(deadline time.Time) harness.Observation {
		brokers, detail, err := d.Oracle().ListBrokersWithin(t, deadline, harness.ToolCallPollBound)
		if err != nil {
			return harness.Unobservable(err.Error())
		}
		if detail != nil {
			return harness.Observed(false, detail.GetMessage())
		}
		for _, b := range brokers.GetBrokers() {
			if hex.EncodeToString(b.GetFingerprint()) == "" {
				continue
			}
			for _, fp := range fingerprints {
				if hex.EncodeToString(b.GetFingerprint()) == fp {
					return harness.Observed(false, "fingerprint "+fp+" is listed")
				}
			}
			if alias != "" && b.GetAlias() == alias {
				return harness.Observed(false, "alias "+alias+" is listed")
			}
			if addr != "" && discHasAddress(b, addr) {
				return harness.Observed(false, "address "+addr+" is listed")
			}
		}
		return harness.Observed(true, "")
	})
}

func discSetPublic(t *testing.T, d *discNode, public bool) {
	t.Helper()
	value := "false"
	if public {
		value = "true"
	}
	op := discOperator(t, d)
	defer op.End(t)
	r := discCallOK(t, op, "set_config", map[string]any{"key": "broker.public", "value": value}, harness.EventTimeout)
	out, err := harness.Decode[mcpserver.SetConfigOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding set_config on %s: %v", d.name, err)
	}
	if out.Key != "broker.public" || out.New != value {
		t.Fatalf("set_config on %s returned %+v, want broker.public=%s", d.name, out, value)
	}
}

func discPublicConfig(t *testing.T, d *discNode) string {
	t.Helper()
	op := discOperator(t, d)
	defer op.End(t)
	r := discCallOK(t, op, "get_config", map[string]any{"key": "broker.public"}, harness.EventTimeout)
	out, err := harness.Decode[mcpserver.GetConfigOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding get_config on %s: %v", d.name, err)
	}
	for _, entry := range out.Entries {
		if entry.Key == "broker.public" {
			return entry.Value
		}
	}
	t.Fatalf("get_config on %s returned no broker.public entry: %+v", d.name, out)
	return ""
}

func discListTopics(t *testing.T, s *harness.Session) (mcpserver.ListTopicsOut, string) {
	t.Helper()
	r := s.Call(t, "list_topics", nil)
	if r.IsError {
		return mcpserver.ListTopicsOut{}, r.Text
	}
	out, err := harness.Decode[mcpserver.ListTopicsOut](string(r.Structured))
	if err != nil {
		return mcpserver.ListTopicsOut{}, "decoding list_topics: " + err.Error()
	}
	return out, ""
}

func discListTopicsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListTopicsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListTopicsOut](t, deadline, s, "list_topics", nil)
}

func discJoinTopic(t *testing.T, s *harness.Session, name string, public bool, description string) harness.Result {
	t.Helper()
	args := map[string]any{"name": name}
	if public {
		args["public"] = true
	}
	if description != "" {
		args["description"] = description
	}
	return s.Call(t, "join_topic", args)
}

func discJoinOK(t *testing.T, s *harness.Session, name string, public bool, description string) harness.Result {
	t.Helper()
	args := map[string]any{"name": name}
	if public {
		args["public"] = true
	}
	if description != "" {
		args["description"] = description
	}
	return discCallOK(t, s, "join_topic", args, harness.EventTimeout)
}

func discPublicTopic(out mcpserver.ListTopicsOut, name string) (mcpserver.PublicTopicOut, bool) {
	for _, topic := range out.Public {
		if topic.Name == name {
			return topic, true
		}
	}
	return mcpserver.PublicTopicOut{}, false
}

func discHasJoined(out mcpserver.ListTopicsOut, topic string) bool {
	for _, name := range out.Joined {
		if name == topic {
			return true
		}
	}
	return false
}

// TestDiscoveryPrivateByDefault checks that a Broker that is not public leaks
// neither its card nor its private topics through a paired neighbor, while a
// declared public topic on a public Broker reaches the far end of the chain.
func TestDiscoveryPrivateByDefault(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b", "c")
	a, b, c := nodes["a"], nodes["b"], nodes["c"]
	fpA := discPair(t, a, b).Fingerprint
	discPair(t, b, c)

	ws := a.Workspace(t, "git", "https://github.com/org/repo", "")
	agent := discAgent(t, a, "worker", ws)
	defer agent.End(t)

	var privateTopics []string
	harness.EventuallyPoll(t, harness.EventTimeout, "A's Agent joins its automatic topic", func(deadline time.Time) (bool, string) {
		out, state := discListTopicsPoll(t, deadline, agent)
		if state != "" {
			return false, state
		}
		if len(out.Joined) == 0 {
			return false, "no joined topics yet"
		}
		privateTopics = append([]string(nil), out.Joined...)
		return true, ""
	})
	discJoinOK(t, agent, "t1", false, "")
	privateTopics = append(privateTopics, "t1")

	watcher := discAgent(t, c, "watcher", t.TempDir())
	defer watcher.End(t)
	harness.WaitRegistered(t, watcher, harness.EventTimeout)

	window := discQuietBound(2)
	discAbsentConsistently(t, c, window, "C never learns A from a private Broker", []string{fpA}, a.alias, a.addr)
	harness.ConsistentlyPoll(t, window, harness.TopicCallPollBound, "C never sees A's private topics", watcher.LogBrokerStack, func(deadline time.Time) harness.Observation {
		out, state := discListTopicsPoll(t, deadline, watcher)
		if state != "" {
			return harness.Unobservable(state)
		}
		for _, topic := range privateTopics {
			if _, ok := discPublicTopic(out, topic); ok {
				return harness.Observed(false, "public topics contain "+topic)
			}
		}
		return harness.Observed(true, "")
	})

	brokers, detail := c.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("C's ListBrokers failed: %s", detail.GetMessage())
	}
	status, detail := c.Oracle().Status(t)
	if detail != nil {
		t.Fatalf("C's Status failed: %s", detail.GetMessage())
	}
	brokersJSON, err := protojson.Marshal(brokers)
	if err != nil {
		t.Fatalf("marshalling C's ListBrokers: %v", err)
	}
	statusJSON, err := protojson.Marshal(status)
	if err != nil {
		t.Fatalf("marshalling C's Status: %v", err)
	}
	texts := map[string]string{
		"ListBrokers": string(brokersJSON),
		"Status":      string(statusJSON),
		"BrokerLog":   string(c.BrokerLog(t)),
	}
	for _, tool := range []string{"list_topics", "list_agents"} {
		result := watcher.Call(t, tool, nil)
		texts[tool+".text"] = result.Text
		texts[tool+".structured"] = string(result.Structured)
	}
	for _, forbidden := range []string{fpA, a.alias, a.addr} {
		for surface, text := range texts {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s on C contains %q although A is not public", surface, forbidden)
			}
		}
	}

	discSetPublic(t, a, true)
	const control = "public-control"
	discJoinOK(t, agent, control, true, "control topic")

	discJoinOK(t, agent, "t2", false, "")
	privateTopics = append(privateTopics, "t2")

	harness.EventuallyPoll(t, discBound(2), "C sees A's public control topic", func(deadline time.Time) (bool, string) {
		out, state := discListTopicsPoll(t, deadline, watcher)
		if state != "" {
			return false, state
		}
		topic, ok := discPublicTopic(out, control)
		if !ok {
			return false, fmt.Sprintf("public topics are %v", out.Public)
		}
		if topic.Description != "control topic" {
			return false, fmt.Sprintf("the control description is %q", topic.Description)
		}
		return true, ""
	})
	harness.ConsistentlyPoll(t, window, harness.TopicCallPollBound, "C never sees A's undeclared topics", watcher.LogBrokerStack, func(deadline time.Time) harness.Observation {
		out, state := discListTopicsPoll(t, deadline, watcher)
		if state != "" {
			return harness.Unobservable(state)
		}
		for _, topic := range privateTopics {
			if _, ok := discPublicTopic(out, topic); ok {
				return harness.Observed(false, "public topics contain "+topic)
			}
		}
		return harness.Observed(true, "")
	})
}

// TestDiscoveryCardReachesWholeNetwork checks that becoming public publishes a
// card that propagates hop by hop to the far end of a four-Broker chain, and
// that get_config agrees with the behavior.
func TestDiscoveryCardReachesWholeNetwork(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b", "c", "d")
	a, b, c, d := nodes["a"], nodes["b"], nodes["c"], nodes["d"]
	fpA := discPair(t, a, b).Fingerprint
	discPair(t, b, c)
	discPair(t, c, d)

	discSetPublic(t, a, true)
	if got := discPublicConfig(t, a); got != "true" {
		t.Fatalf("get_config on A reports broker.public=%q after set_config, want true", got)
	}

	checkCard := func(broker *localpb.BrokerInfo, peer bool, status localpb.BrokerStatus) string {
		if broker.GetAlias() != a.alias {
			return fmt.Sprintf("alias is %q, want %q", broker.GetAlias(), a.alias)
		}
		if broker.GetIsPeer() != peer {
			return fmt.Sprintf("is_peer is %v, want %v", broker.GetIsPeer(), peer)
		}
		if broker.GetStatus() != status {
			return fmt.Sprintf("status is %v, want %v", broker.GetStatus(), status)
		}
		addrs := broker.GetAdvertisedAddresses()
		if len(addrs) != 1 || addrs[0].GetKind() != "direct" || addrs[0].GetAddr() != a.addr {
			return fmt.Sprintf("addresses are %v, want [{direct %s}]", addrs, a.addr)
		}
		return ""
	}
	discWaitBroker(t, b, fpA, discBound(1), func(broker *localpb.BrokerInfo) string {
		return checkCard(broker, true, localpb.BrokerStatus_BROKER_STATUS_CONNECTED)
	})
	for _, holder := range []*discNode{c, d} {
		discWaitBroker(t, holder, fpA, discBound(3), func(broker *localpb.BrokerInfo) string {
			return checkCard(broker, false, localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE)
		})
	}

	if got := discConfigValue(t, a, "broker.alias"); got != a.alias {
		t.Fatalf("get_config on A reports broker.alias=%q, want %q", got, a.alias)
	}
	if got := discConfigValue(t, a, "broker.listen"); got != a.addr {
		t.Fatalf("get_config on A reports broker.listen=%q, want %q", got, a.addr)
	}

	if got := discConfigValue(t, a, "topics.auto_join"); got != "true" {
		t.Fatalf("get_config on A reports topics.auto_join=%q, want true", got)
	}
	const originTopic = "github.com/org/repo"
	ws := a.Workspace(t, "git", "https://"+originTopic, "")
	agent := discAgent(t, a, "config-agent", ws)
	defer agent.End(t)
	harness.EventuallyPoll(t, harness.EventTimeout, "the Agent joins its automatic topic", func(deadline time.Time) (bool, string) {
		out, state := discListTopicsPoll(t, deadline, agent)
		if state != "" {
			return false, state
		}
		if !discHasJoined(out, originTopic) {
			return false, fmt.Sprintf("joined topics are %v", out.Joined)
		}
		return true, ""
	})
}

func discConfigValue(t *testing.T, d *discNode, key string) string {
	t.Helper()
	op := discOperator(t, d)
	defer op.End(t)
	r := discCallOK(t, op, "get_config", map[string]any{"key": key}, harness.EventTimeout)
	out, err := harness.Decode[mcpserver.GetConfigOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding get_config on %s: %v", d.name, err)
	}
	for _, entry := range out.Entries {
		if entry.Key == key {
			return entry.Value
		}
	}
	t.Fatalf("get_config on %s returned no %s entry: %+v", d.name, key, out)
	return ""
}

// TestDiscoveryNoDialToCardHolder checks that holding another Broker's card
// does not make a Broker dial it: D, at the end of the chain, keeps A as a
// non-peer and A's log shows no contact from D.
func TestDiscoveryNoDialToCardHolder(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b", "c", "d")
	a, b, c, d := nodes["a"], nodes["b"], nodes["c"], nodes["d"]
	fpA := discPair(t, a, b).Fingerprint
	discPair(t, b, c)
	fpD := discPair(t, d, c).Fingerprint

	discSetPublic(t, a, true)
	discWaitBroker(t, d, fpA, discBound(3), nil)

	harness.Consistently(t, discQuietBound(2), "D keeps A as a non-peer without dialing it", func() (bool, string) {
		brokers, state := discList(t, d)
		if state != "" {
			return false, state
		}
		broker, ok := discFindFingerprint(brokers, fpA)
		if !ok {
			return false, "A's card is gone"
		}
		if broker.GetIsPeer() {
			return false, "A is listed as a peer"
		}
		if broker.GetLinkState() != localpb.LinkState_LINK_STATE_UNSPECIFIED {
			return false, fmt.Sprintf("link_state is %v, want UNSPECIFIED", broker.GetLinkState())
		}
		events, detail := a.Oracle().Audit(t, time.Time{}, 0)
		if detail != nil {
			return false, detail.GetMessage()
		}
		if auditHasPeer(events, fpD) {
			return false, "A's audit has a link event for D, which only holds a card"
		}
		return true, ""
	})

	discPair(t, a, d)
	harness.Eventually(t, discBound(1), "A's audit records the link from D", func() (bool, string) {
		events, detail := a.Oracle().Audit(t, time.Time{}, 0)
		if detail != nil {
			return false, detail.GetMessage()
		}
		if auditHasLinkUp(events, fpD) {
			return true, ""
		}
		return false, "no LINK_UP event names D"
	})
}

func auditHasPeer(events []*localpb.AuditEvent, fpHex string) bool {
	for _, event := range events {
		if hex.EncodeToString(event.GetPeer().GetFingerprint()) == fpHex {
			return true
		}
	}
	return false
}

func auditHasLinkUp(events []*localpb.AuditEvent, fpHex string) bool {
	for _, event := range events {
		if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_LINK_UP &&
			hex.EncodeToString(event.GetPeer().GetFingerprint()) == fpHex {
			return true
		}
	}
	return false
}

// TestDiscoveryPublicWithoutListen checks that a public Broker with no
// listener advertises an empty address list and shows as not directly
// reachable.
func TestDiscoveryPublicWithoutListen(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b", "c")
	a, b, c := nodes["a"], nodes["b"], nodes["c"]

	a.StopBroker(t, syscall.SIGTERM)
	a.addr = ""
	a.config = discConfig("", a.alias, true, "")
	a.start(t)

	discPair(t, b, a)
	discPair(t, b, c)

	brokers, state := discList(t, b)
	if state != "" {
		t.Fatalf("B's ListBrokers failed: %s", state)
	}
	var fpA string
	for _, broker := range brokers {
		if broker.GetAlias() == a.alias {
			fpA = hex.EncodeToString(broker.GetFingerprint())
		}
	}
	if fpA == "" {
		t.Fatalf("B does not list A after pairing: %v", brokers)
	}

	discWaitBroker(t, c, fpA, discBound(2), func(broker *localpb.BrokerInfo) string {
		if len(broker.GetAdvertisedAddresses()) != 0 {
			return fmt.Sprintf("addresses are %v, want none", broker.GetAdvertisedAddresses())
		}
		if broker.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_NOT_DIRECTLY_REACHABLE {
			return fmt.Sprintf("status is %v, want NOT_DIRECTLY_REACHABLE", broker.GetStatus())
		}
		if broker.GetIsPeer() {
			return "A is listed as a peer"
		}
		return ""
	})
}

// TestDiscoveryPrivateBrokerLearnsCards checks that a Broker that is not public
// still receives and lists public Brokers' cards.
func TestDiscoveryPrivateBrokerLearnsCards(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b", "c")
	a, b, c := nodes["a"], nodes["b"], nodes["c"]
	discPair(t, a, b)
	discSetPublic(t, c, true)
	fpC := discPair(t, c, b).Fingerprint

	discWaitBroker(t, a, fpC, discBound(1), func(broker *localpb.BrokerInfo) string {
		if broker.GetAlias() != c.alias {
			return fmt.Sprintf("alias is %q, want %q", broker.GetAlias(), c.alias)
		}
		if broker.GetIsPeer() {
			return "C is listed as a peer"
		}
		return ""
	})
}

// TestDiscoveryRevokeWithin60s checks that going private withdraws the card
// hop by hop and that a restarted Broker holding the old card does not
// resurrect it.
func TestDiscoveryRevokeWithin60s(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b", "c", "d")
	a, b, c, d := nodes["a"], nodes["b"], nodes["c"], nodes["d"]
	fpA := discPair(t, a, b).Fingerprint
	discPair(t, b, c)
	discPair(t, c, d)

	discSetPublic(t, a, true)
	discWaitBroker(t, d, fpA, discBound(3), nil)

	d.StopBroker(t, syscall.SIGTERM)

	// D stopped while holding A's card; the record must be on disk so the
	// restart below really starts from the old card.
	cardKey := dxDirectoryKey{Publisher: fpA, Kind: directorypb.RecordKind_RECORD_KIND_BROKER_CARD}
	if _, ok := dxDirectoryRecords(t, d.Node)[cardKey]; !ok {
		t.Fatalf("D's directory file holds no record for A after the stop")
	}

	discSetPublic(t, a, false)

	harness.Eventually(t, discBound(2), "C drops A's card after A becomes private", func() (bool, string) {
		brokers, state := discList(t, c)
		if state != "" {
			return false, state
		}
		if _, ok := discFindFingerprint(brokers, fpA); ok {
			return false, "A is still listed"
		}
		return true, ""
	})

	d.start(t)
	discWaitConnected(t, c, d.alias, localpb.PeerRole_PEER_ROLE_ACCEPTOR, harness.EventTimeout)

	discAbsentConsistently(t, c, discQuietBound(2), "C stays without A after D restarts", []string{fpA}, a.alias, a.addr)
	discAbsentConsistently(t, d, discQuietBound(1), "D drops the old card it held before restart", []string{fpA}, a.alias, a.addr)
}

// TestPublicTopicVisibleAcrossNetwork checks that two public declarations on a
// public Broker reach a Broker two hops away with their descriptions, and that
// a joined topic moves from the public list to the joined list.
func TestPublicTopicVisibleAcrossNetwork(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b", "c")
	a, b, c := nodes["a"], nodes["b"], nodes["c"]
	discPair(t, a, b)
	discPair(t, b, c)
	discSetPublic(t, a, true)

	first := discAgent(t, a, "first", t.TempDir())
	defer first.End(t)
	second := discAgent(t, a, "second", t.TempDir())
	defer second.End(t)
	discJoinOK(t, first, "infra-help", true, "ask infra")
	discJoinOK(t, second, "quiet", true, "")

	watcher := discAgent(t, c, "watcher", t.TempDir())
	defer watcher.End(t)

	harness.EventuallyPoll(t, discBound(2), "C sees both public topics from A", func(deadline time.Time) (bool, string) {
		out, state := discListTopicsPoll(t, deadline, watcher)
		if state != "" {
			return false, state
		}
		infra, ok := discPublicTopic(out, "infra-help")
		if !ok {
			return false, "infra-help is not public"
		}
		if infra.Description != "ask infra" {
			return false, fmt.Sprintf("infra-help's description is %q", infra.Description)
		}
		quiet, ok := discPublicTopic(out, "quiet")
		if !ok {
			return false, "quiet is not public"
		}
		if quiet.Description != "" {
			return false, fmt.Sprintf("quiet's description is %q, want empty", quiet.Description)
		}
		return true, ""
	})

	discJoinOK(t, watcher, "infra-help", false, "")
	out, state := discListTopics(t, watcher)
	if state != "" {
		t.Fatalf("list_topics failed: %s", state)
	}
	if !discHasJoined(out, "infra-help") {
		t.Fatalf("joined topics are %v, want infra-help", out.Joined)
	}
	if _, ok := discPublicTopic(out, "infra-help"); ok {
		t.Fatal("infra-help is still in the public list after joining it")
	}
	if _, ok := discPublicTopic(out, "quiet"); !ok {
		t.Fatal("quiet left the public list although the Agent did not join it")
	}
}

// TestPublicTopicRejectedOnPrivateBroker checks that a public join on a Broker
// that is not public fails with invalid_argument, does not join the topic and
// does not publish it.
func TestPublicTopicRejectedOnPrivateBroker(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b")
	a, b := nodes["a"], nodes["b"]
	discPair(t, a, b)

	agent := discAgent(t, a, "worker", t.TempDir())
	defer agent.End(t)
	// Wait for registration first: a call made while the Broker is still
	// registering can see not_registered instead of the rejection under test.
	harness.WaitRegistered(t, agent, harness.EventTimeout)
	r := discJoinTopic(t, agent, "infra-help", true, "ask infra")
	if !r.IsError {
		t.Fatalf("a public join on a private Broker succeeded: %s", r.Text)
	}
	code, _, ok := harness.ToolError(r.Text)
	if !ok {
		t.Fatalf("the tool error carries no code suffix: %s", r.Text)
	}
	if code != "invalid_argument" {
		t.Fatalf("error code is %q, want invalid_argument; result: %s", code, r.Text)
	}
	if !strings.Contains(r.Text, "broker.public") {
		t.Fatalf("the error does not mention broker.public: %s", r.Text)
	}

	out, state := discListTopics(t, agent)
	if state != "" {
		t.Fatalf("list_topics failed: %s", state)
	}
	if discHasJoined(out, "infra-help") {
		t.Fatalf("joined topics are %v although the public join failed", out.Joined)
	}

	watcher := discAgent(t, b, "watcher", t.TempDir())
	defer watcher.End(t)
	harness.WaitRegistered(t, watcher, harness.EventTimeout)
	harness.ConsistentlyPoll(t, discQuietBound(1), harness.TopicCallPollBound, "B never sees the rejected declaration", watcher.LogBrokerStack, func(deadline time.Time) harness.Observation {
		topics, state := discListTopicsPoll(t, deadline, watcher)
		if state != "" {
			return harness.Unobservable(state)
		}
		if _, ok := discPublicTopic(topics, "infra-help"); ok {
			return harness.Observed(false, "infra-help is public on B")
		}
		return harness.Observed(true, "")
	})
}

// TestPublicTopicGoneWhenBrokerPrivate checks that turning the Broker private
// withdraws its public declarations while its agents stay in their topics, and
// that turning it public again restores them.
func TestPublicTopicGoneWhenBrokerPrivate(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b", "c")
	a, b, c := nodes["a"], nodes["b"], nodes["c"]
	discPair(t, a, b)
	discPair(t, b, c)
	discSetPublic(t, a, true)

	agent := discAgent(t, a, "worker", t.TempDir())
	defer agent.End(t)
	discJoinOK(t, agent, "infra-help", true, "ask infra")

	watcher := discAgent(t, c, "watcher", t.TempDir())
	defer watcher.End(t)

	discWaitPublicTopic(t, watcher, "infra-help", discBound(2))
	discSetPublic(t, a, false)
	discWaitPublicTopicGone(t, watcher, "infra-help", discBound(2))

	out, state := discListTopics(t, agent)
	if state != "" {
		t.Fatalf("list_topics failed: %s", state)
	}
	if !discHasJoined(out, "infra-help") {
		t.Fatalf("joined topics are %v, want infra-help to survive going private", out.Joined)
	}

	discSetPublic(t, a, true)
	discWaitPublicTopic(t, watcher, "infra-help", discBound(2))
}

// TestPublicTopicFollowsLastMember checks that the declaration disappears when
// the last public member leaves or its session ends, and comes back when a
// member declares the topic publicly again.
func TestPublicTopicFollowsLastMember(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b")
	a, b := nodes["a"], nodes["b"]
	discPair(t, a, b)
	discSetPublic(t, a, true)

	agent := discAgent(t, a, "worker", t.TempDir())
	discJoinOK(t, agent, "infra-help", true, "ask infra")

	watcher := discAgent(t, b, "watcher", t.TempDir())
	defer watcher.End(t)

	discWaitPublicTopic(t, watcher, "infra-help", discBound(1))
	discCallOK(t, agent, "leave_topic", map[string]any{"name": "infra-help"}, harness.EventTimeout)
	discWaitPublicTopicGone(t, watcher, "infra-help", discBound(1))

	discJoinOK(t, agent, "infra-help", true, "ask infra")
	discWaitPublicTopic(t, watcher, "infra-help", discBound(1))

	agent.End(t)
	discWaitPublicTopicGone(t, watcher, "infra-help", discBound(1))
}

func discWaitPublicTopic(t *testing.T, s *harness.Session, name string, bound time.Duration) {
	t.Helper()
	harness.EventuallyPoll(t, bound, "public topics contain "+name, func(deadline time.Time) (bool, string) {
		out, state := discListTopicsPoll(t, deadline, s)
		if state != "" {
			return false, state
		}
		if _, ok := discPublicTopic(out, name); !ok {
			return false, fmt.Sprintf("public topics are %v", out.Public)
		}
		return true, ""
	})
}

func discWaitPublicTopicGone(t *testing.T, s *harness.Session, name string, bound time.Duration) {
	t.Helper()
	harness.EventuallyPoll(t, bound, "public topics no longer contain "+name, func(deadline time.Time) (bool, string) {
		out, state := discListTopicsPoll(t, deadline, s)
		if state != "" {
			return false, state
		}
		if _, ok := discPublicTopic(out, name); ok {
			return false, fmt.Sprintf("public topics are %v", out.Public)
		}
		return true, ""
	})
}
