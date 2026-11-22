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

// discReconcile is the directory.reconcile_interval every discovery case sets.
// The waits below are derived from it: one hop is one reconcile pass.
const discReconcile = time.Second

// discSlack is the margin added to every derived wait, after the related
// product timers: harness.EventTimeout is the tree's fallback for a wait no
// product timer governs. The longest discovery chain, the revoke case's
// discBound(3), discBound(2), one relink and two discQuietBound windows, stays
// under the requirement's sixty seconds with this margin.
const discSlack = harness.EventTimeout

// discQuietSlack is the margin added to a negative observation window. The
// window must span more than one reconciliation pass so a pass inside it would
// show a change the check watches for; it keeps a short margin because a
// negative check pays its whole window, unlike a wait that returns when the
// event arrives.
const discQuietSlack = 3 * time.Second

// discBound returns the wait for hops reconcile passes plus the shared slack.
// A negative observation uses discQuietBound instead.
func discBound(hops int) time.Duration {
	return time.Duration(hops)*discReconcile + discSlack
}

// discQuietBound returns the observation window of a negative check spanning
// hops reconcile passes plus the quiet margin.
func discQuietBound(hops int) time.Duration {
	return time.Duration(hops)*discReconcile + discQuietSlack
}

// discNode is one local host running a Broker for the discovery cases.
type discNode struct {
	*harness.Node

	name   string
	addr   string
	alias  string
	config string
	env    map[string]string
}

// discNewNode starts one local node with an explicit listen and advertise
// address and a stable alias. extra is merged into the configuration.
func discNewNode(t *testing.T, name, extra string, public bool) *discNode {
	t.Helper()
	return discNewNodeEnv(t, name, extra, public, nil)
}

// discNewNodeEnv is discNewNode with variables that override the node's base
// environment when its Broker starts. A nil map changes nothing.
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

// discCluster starts the named local nodes, all private.
func discCluster(t *testing.T, names ...string) map[string]*discNode {
	t.Helper()
	out := make(map[string]*discNode, len(names))
	for _, name := range names {
		out[name] = discNewNode(t, name, "", false)
	}
	return out
}

// discConfig builds the Broker configuration for a local discovery node.
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

// discYAMLString renders an empty address as a quoted scalar and every other
// address as the plain scalar the configuration parser expects.
func discYAMLString(addr string) string {
	if addr == "" {
		return `""`
	}
	return addr
}

// start starts the node's Broker with its configuration and fails when it does
// not come up. When the one retry moves the listen address because the port
// race was lost, addr and config follow it to the address the Broker actually
// uses, so later comparisons and dials do not go to a stale port.
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

// discOperator starts a session that can call the pairing and configuration
// tools but never registers as an Agent. It waits for the Broker connection
// before the caller's first tool call.
func discOperator(t *testing.T, d *discNode) *harness.Session {
	t.Helper()
	s := d.StartSession(t, harness.SessionSpec{
		Name:        "operator",
		StartMCP:    true,
		NoSocketEnv: true,
		Workdir:     t.TempDir(),
	})
	harness.WaitConnected(t, s, 3*time.Second)
	return s
}

// discAgent starts a registered Agent session on d.
func discAgent(t *testing.T, d *discNode, name, workdir string) *harness.Session {
	t.Helper()
	return d.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true, Workdir: workdir})
}

// discCallOK retries a tool that must succeed until it does or the bound
// expires. The MCP server may still be wiring itself to the Broker right after
// it starts, which it reports as an internal error asking for a retry. A
// read-only or idempotent tool polls with Session.PollCallWithin, so a
// transport error is retried; any other tool is a side effect and goes through
// Session.CallUntilOK, which fails on a transport error instead of repeating a
// call the server may already have applied. A topic tool spends
// topicRPCTimeout inside the handler before the update hint runs, so it polls
// with harness.TopicCallPollBound and the wait gets harness.TopicCallWaitBound.
func discCallOK(t *testing.T, s *harness.Session, tool string, args map[string]any, bound time.Duration) harness.Result {
	t.Helper()
	if !pollableTool(tool) {
		return s.CallUntilOK(t, bound, tool, args)
	}
	pollBound, waitBound := harness.ToolCallPollBound, bound
	if brokerTopicTool(tool) {
		pollBound, waitBound = harness.TopicCallPollBound, harness.TopicCallWaitBound
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

// discPair has acceptor create a pairing key and dialer use it. It returns the
// connect_broker result as dialer saw it, which names acceptor's fingerprint.
func discPair(t *testing.T, acceptor, dialer *discNode) mcpserver.ConnectBrokerOut {
	t.Helper()
	creator := discOperator(t, acceptor)
	defer creator.End(t)

	r := discCallOK(t, creator, "create_pairing_key", nil, 3*time.Second)
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
	r = discCallOK(t, op, "connect_broker", map[string]any{"address": acceptor.addr, "key": key.Key}, 3*time.Second)
	out, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker on %s: %v", dialer.name, err)
	}

	discWaitConnected(t, acceptor, dialer.alias, localpb.PeerRole_PEER_ROLE_ACCEPTOR, harness.EventTimeout)
	discWaitConnected(t, dialer, acceptor.alias, localpb.PeerRole_PEER_ROLE_DIALER, harness.EventTimeout)
	return out
}

// discWaitConnected waits until d lists the peer alias as connected with role.
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

// discList returns the Brokers d knows, or the product error text.
func discList(t *testing.T, d *discNode) ([]*localpb.BrokerInfo, string) {
	t.Helper()
	resp, detail := d.Oracle().ListBrokers(t)
	if detail != nil {
		return nil, detail.GetMessage()
	}
	return resp.GetBrokers(), ""
}

// discFindFingerprint returns the entry whose fingerprint matches fpHex.
func discFindFingerprint(brokers []*localpb.BrokerInfo, fpHex string) (*localpb.BrokerInfo, bool) {
	for _, b := range brokers {
		if hex.EncodeToString(b.GetFingerprint()) == fpHex {
			return b, true
		}
	}
	return nil, false
}

// discHasAddress reports whether b advertises addr.
func discHasAddress(b *localpb.BrokerInfo, addr string) bool {
	for _, a := range b.GetAdvertisedAddresses() {
		if a.GetAddr() == addr {
			return true
		}
	}
	return false
}

// discWaitBroker waits until d lists the fingerprint and check accepts the
// entry. A nil check accepts any entry.
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

// discAbsentConsistently asserts over the whole window that d lists none of
// the fingerprints, the alias and the address. The Oracle read is a poll call:
// a transport error is an inconclusive sample and is retried, and a call that
// spends the whole oracle bound does not answer is a hard failure.
func discAbsentConsistently(t *testing.T, d *discNode, window time.Duration, what string, fingerprints []string, alias, addr string) {
	t.Helper()
	harness.ConsistentlyPoll(t, window, harness.OracleCallBound, what, nil, func(deadline time.Time) harness.Observation {
		brokers, detail, err := d.Oracle().ListBrokersWithin(t, deadline)
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

// discSetPublic changes broker.public through set_config and checks the answer.
func discSetPublic(t *testing.T, d *discNode, public bool) {
	t.Helper()
	value := "false"
	if public {
		value = "true"
	}
	op := discOperator(t, d)
	defer op.End(t)
	r := discCallOK(t, op, "set_config", map[string]any{"key": "broker.public", "value": value}, 3*time.Second)
	out, err := harness.Decode[mcpserver.SetConfigOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding set_config on %s: %v", d.name, err)
	}
	if out.Key != "broker.public" || out.New != value {
		t.Fatalf("set_config on %s returned %+v, want broker.public=%s", d.name, out, value)
	}
}

// discPublicConfig reads broker.public through get_config.
func discPublicConfig(t *testing.T, d *discNode) string {
	t.Helper()
	op := discOperator(t, d)
	defer op.End(t)
	r := discCallOK(t, op, "get_config", map[string]any{"key": "broker.public"}, 3*time.Second)
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

// discListTopics calls list_topics and returns the decoded answer or its error
// text.
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

// discListTopicsPoll is discListTopics for a wait callback: the call is bounded
// by the wait's deadline and a transport error is returned as state.
func discListTopicsPoll(t *testing.T, deadline time.Time, s *harness.Session) (mcpserver.ListTopicsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListTopicsOut](t, deadline, s, "list_topics", nil)
}

// discJoinTopic joins name, declaring it public when public is set.
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

// discJoinOK joins a topic that must be accepted, retrying while the session
// finishes connecting.
func discJoinOK(t *testing.T, s *harness.Session, name string, public bool, description string) harness.Result {
	t.Helper()
	args := map[string]any{"name": name}
	if public {
		args["public"] = true
	}
	if description != "" {
		args["description"] = description
	}
	return discCallOK(t, s, "join_topic", args, 3*time.Second)
}

// discPublicTopic returns the public topic entry named name.
func discPublicTopic(out mcpserver.ListTopicsOut, name string) (mcpserver.PublicTopicOut, bool) {
	for _, topic := range out.Public {
		if topic.Name == name {
			return topic, true
		}
	}
	return mcpserver.PublicTopicOut{}, false
}

// discHasJoined reports whether out lists topic in its joined set.
func discHasJoined(out mcpserver.ListTopicsOut, topic string) bool {
	for _, name := range out.Joined {
		if name == topic {
			return true
		}
	}
	return false
}

// TestDiscoveryPrivateByDefault checks that a Broker that is not public leaks
// neither its card nor its private topics through a paired neighbor, and that
// a public Broker keeps a topic private unless a member declares it public. A
// public control topic on the same Broker must reach C, so the negative checks
// have a positive counterpart.
func TestDiscoveryPrivateByDefault(t *testing.T) {
	t.Parallel()

	nodes := discCluster(t, "a", "b", "c")
	a, b, c := nodes["a"], nodes["b"], nodes["c"]
	fpA := discPair(t, a, b).Fingerprint
	discPair(t, b, c)

	// A's Agent joins a private topic and, through its git workspace, the
	// automatic repository topic. Neither may leave A.
	ws := a.Workspace(t, "git", "https://github.com/org/repo", "")
	agent := discAgent(t, a, "worker", ws)
	defer agent.End(t)

	var privateTopics []string
	harness.EventuallyPoll(t, 3*time.Second, "A's Agent joins its automatic topic", func(deadline time.Time) (bool, string) {
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
	harness.WaitRegistered(t, watcher, 3*time.Second)

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

	// The text surfaces a client sees must not carry A's fingerprint, alias or
	// address either: the serialized local interface answers, the session tool
	// results and the Broker log.
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

	// With A public, a declared public topic must reach C; that is the
	// positive control for the negative checks below. The topic t1 and the
	// automatic repository topic were joined without a public declaration, so
	// they must stay out of C's public list even though A now publishes cards
	// and public declarations.
	discSetPublic(t, a, true)
	const control = "public-control"
	discJoinOK(t, agent, control, true, "control topic")

	// A non-public join after A went public must stay out of C's public list:
	// the declaration, not the Broker's visibility, decides.
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

	// Every hop sees the same fingerprint, alias and address; the direct peer
	// is connected, the card holders are connectable.
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

	// REQ-broker-config-001: the alias and listen address read back agree with
	// the Broker's behavior. The card just checked carries the configured
	// alias, and hostB dialed the configured listen address and reports A
	// connected.
	if got := discConfigValue(t, a, "broker.alias"); got != a.alias {
		t.Fatalf("get_config on A reports broker.alias=%q, want %q", got, a.alias)
	}
	if got := discConfigValue(t, a, "broker.listen"); got != a.addr {
		t.Fatalf("get_config on A reports broker.listen=%q, want %q", got, a.addr)
	}

	// The automatic topic join is the behavior that topics.auto_join controls.
	if got := discConfigValue(t, a, "topics.auto_join"); got != "true" {
		t.Fatalf("get_config on A reports topics.auto_join=%q, want true", got)
	}
	const originTopic = "github.com/org/repo"
	ws := a.Workspace(t, "git", "https://"+originTopic, "")
	agent := discAgent(t, a, "config-agent", ws)
	defer agent.End(t)
	harness.EventuallyPoll(t, 3*time.Second, "the Agent joins its automatic topic", func(deadline time.Time) (bool, string) {
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

// discConfigValue reads one configuration value through get_config.
func discConfigValue(t *testing.T, d *discNode, key string) string {
	t.Helper()
	op := discOperator(t, d)
	defer op.End(t)
	r := discCallOK(t, op, "get_config", map[string]any{"key": key}, 3*time.Second)
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

	// Positive control: a real pairing from D writes the event the negative
	// check above looks for.
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

// auditHasPeer reports whether an audit event names the peer with fingerprint
// fpHex.
func auditHasPeer(events []*localpb.AuditEvent, fpHex string) bool {
	for _, event := range events {
		if hex.EncodeToString(event.GetPeer().GetFingerprint()) == fpHex {
			return true
		}
	}
	return false
}

// auditHasLinkUp reports whether a LINK_UP audit event names the peer with
// fingerprint fpHex.
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

	// A has no listener but dials B; C dials B as well. The card reaches C
	// through B.
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
// hop by hop within the plan's bound (stricter than the requirement's 60
// seconds) and that a restarted Broker holding the old card does not
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
	// Wait for registration to complete before provoking the error the test
	// asserts on; the rejection itself must not be retried. get_config does not
	// need the Agent identity and can succeed while the Broker is still
	// registering, which would let the call below see not_registered instead of
	// the rejection under test.
	harness.WaitRegistered(t, agent, 3*time.Second)
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
	harness.WaitRegistered(t, watcher, 3*time.Second)
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
// withdraws its public declarations while its Agents stay in their topics, and
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
	discCallOK(t, agent, "leave_topic", map[string]any{"name": "infra-help"}, 3*time.Second)
	discWaitPublicTopicGone(t, watcher, "infra-help", discBound(1))

	discJoinOK(t, agent, "infra-help", true, "ask infra")
	discWaitPublicTopic(t, watcher, "infra-help", discBound(1))

	agent.End(t)
	discWaitPublicTopicGone(t, watcher, "infra-help", discBound(1))
}

// discWaitPublicTopic waits until the session's public list contains name.
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

// discWaitPublicTopicGone waits until the session's public list no longer
// contains name.
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
