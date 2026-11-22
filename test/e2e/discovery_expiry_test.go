//go:build e2e

package e2e

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/fakepeer/fakerec"
	"github.com/wangzw/peers/test/e2e/harness"
)

const (
	// dxReconcileInterval is the directory reconciliation interval every node
	// in these tests runs with; one propagation hop costs at most one interval.
	dxReconcileInterval = time.Second

	// dxPropagationBound bounds a card crossing two hops: twice the
	// reconciliation interval plus the tree's EventTimeout margin.
	dxPropagationBound = 2*dxReconcileInterval + harness.EventTimeout

	// dxExpiryBound bounds one Broker noticing an expired record: one
	// reconciliation interval plus the tree's EventTimeout margin.
	dxExpiryBound = dxReconcileInterval + harness.EventTimeout

	// dxQuietWindow is the observation window of a negative check. One
	// reconciliation interval has already passed and nothing else changes, so
	// the entry must stay absent for the whole window.
	dxQuietWindow = dxReconcileInterval + 3*time.Second

	// dxPublicTopic is the topic the expiry case declares publicly.
	dxPublicTopic = "infra-help"

	// dxClockEnv names the wall-clock offset file the peers_testenv hook
	// reads.
	dxClockEnv = "PEERS_TEST_CLOCK_FILE"

	// The total-limit case keeps the original cards short-lived so that the
	// renewals, not the originals, must keep the entries alive past their
	// first expiry.
	dxLimitMaxRecords  = 3
	dxLimitOriginalTTL = 48 * time.Hour
	dxLimitRenewalAt   = 24 * time.Hour
	dxLimitRenewalTTL  = 168 * time.Hour
	dxLimitCheckpoint  = 144 * time.Hour
)

// TestCardExpiresAfterSevenDays covers REQ-broker-discovery-007: a card issued
// by a public Broker stays visible for six days and disappears after seven.
// A is public and paired with B; C is paired with B and receives A's card
// through B. A stops serving before the clock moves, so only the expiry can
// remove the card.
func TestCardExpiresAfterSevenDays(t *testing.T) {
	aHost := harness.NewLocalHost(t, harness.LocalOptions{Name: "expiry-a"})
	bHost := harness.NewLocalHost(t, harness.LocalOptions{Name: "expiry-b"})
	cHost := harness.NewLocalHost(t, harness.LocalOptions{Name: "expiry-c"})

	listenA := aHost.FreeAddr(t)
	listenB := bHost.FreeAddr(t)
	listenC := cHost.FreeAddr(t)

	clockB := dxClockFile(t)
	clockC := dxClockFile(t)

	listenA = dxStartBroker(t, aHost, dxConfig(listenA, "expiry-a", true), listenA, nil)
	listenB = dxStartBroker(t, bHost, dxConfig(listenB, "expiry-b", false), listenB,
		map[string]string{dxClockEnv: clockB})
	dxStartBroker(t, cHost, dxConfig(listenC, "expiry-c", false), listenC,
		map[string]string{dxClockEnv: clockC})

	opA := adOperator(t, aHost)
	defer opA.End(t)
	opB := adOperator(t, bHost)
	defer opB.End(t)
	opC := adOperator(t, cHost)
	defer opC.End(t)

	fpA := dxPair(t, opA, opB, listenA)
	dxPair(t, opB, opC, listenB)
	dxWaitCard(t, cHost, fpA, []string{listenA}, dxPropagationBound)

	// A must not come back on its own, so end the session that could restart
	// it before stopping the Broker.
	opA.End(t)
	if code := aHost.StopBroker(t, syscall.SIGTERM); code != 0 {
		t.Fatalf("A's Broker exited with %d on SIGTERM, want 0", code)
	}

	// Six days after issuance the card is still live for every reader.
	dxSetClock(t, clockB, 144*time.Hour)
	dxSetClock(t, clockC, 144*time.Hour)
	harness.Consistently(t, dxQuietWindow, "C still lists A six days after issuance",
		func() (bool, string) {
			resp := dxListBrokers(t, cHost)
			if _, ok := dxFindBroker(resp, fpA); !ok {
				return false, "A is no longer listed"
			}
			return true, ""
		})

	// Eight days after issuance the record has expired. Nothing is left to
	// renew it because A is stopped, so every reader drops it.
	dxSetClock(t, clockB, 192*time.Hour)
	dxSetClock(t, clockC, 192*time.Hour)
	harness.Eventually(t, dxExpiryBound, "C drops A eight days after issuance",
		func() (bool, string) {
			resp := dxListBrokers(t, cHost)
			if _, ok := dxFindBroker(resp, fpA); ok {
				return false, "A is still listed"
			}
			return true, ""
		})
}

// TestCardRenewedDaily covers REQ-broker-discovery-008: while a public Broker
// runs, its card is renewed every day and never expires. A, B and C advance
// together, because a record issued on a clock far ahead of B's is rejected as
// too far in the future.
func TestCardRenewedDaily(t *testing.T) {
	aHost := harness.NewLocalHost(t, harness.LocalOptions{Name: "renew-a"})
	bHost := harness.NewLocalHost(t, harness.LocalOptions{Name: "renew-b"})
	cHost := harness.NewLocalHost(t, harness.LocalOptions{Name: "renew-c"})

	listenA := aHost.FreeAddr(t)
	listenB := bHost.FreeAddr(t)
	listenC := cHost.FreeAddr(t)

	clock := dxClockFile(t)
	clockEnv := map[string]string{dxClockEnv: clock}

	listenA = dxStartBroker(t, aHost, dxConfig(listenA, "renew-a", true), listenA, clockEnv)
	listenB = dxStartBroker(t, bHost, dxConfig(listenB, "renew-b", false), listenB, clockEnv)
	dxStartBroker(t, cHost, dxConfig(listenC, "renew-c", false), listenC, clockEnv)

	opA := adOperator(t, aHost)
	defer opA.End(t)
	opB := adOperator(t, bHost)
	defer opB.End(t)
	opC := adOperator(t, cHost)
	defer opC.End(t)

	fpA := dxPair(t, opA, opB, listenA)
	dxPair(t, opB, opC, listenB)
	dxWaitCard(t, cHost, fpA, []string{listenA}, dxPropagationBound)

	// Eleven steps of 23 hours reach 253 hours. A renewal is due after 24
	// hours, so the card expires at step eight (184h) unless it is renewed.
	//
	// The check after each step spans the propagation window instead of
	// returning on the first look: a maintenance pass has to run at the new
	// offset for a due renewal to be issued, and only a window of more than
	// one reconciliation interval guarantees that. A card that is never
	// renewed vanishes at step eight even though it was seen after step seven.
	for step := 1; step <= 11; step++ {
		offset := time.Duration(step) * 23 * time.Hour
		dxSetClock(t, clock, offset)
		harness.Consistently(t, dxPropagationBound,
			fmt.Sprintf("C lists A after step %d (%s)", step, offset),
			func() (bool, string) {
				resp := dxListBrokers(t, cHost)
				broker, ok := dxFindBroker(resp, fpA)
				if !ok {
					return false, "the card is not listed"
				}
				if got := adPeerAddresses(broker); !adSameStrings(got, []string{listenA}) {
					return false, fmt.Sprintf("addresses are %v", got)
				}
				return true, ""
			})
	}
}

// TestDirectoryTotalLimitKeepsRenewals covers REQ-broker-discovery-011: the
// total record limit rejects cards from publishers the directory does not know,
// while a renewal of an entry already held replaces it even though the limit is
// full. The originals expire after two days; only the renewals accepted at the
// full limit keep the three entries listed beyond that.
func TestDirectoryTotalLimitKeepsRenewals(t *testing.T) {
	t.Parallel()

	host := harness.NewLocalHost(t, harness.LocalOptions{Name: "limit-total"})
	b := &discNode{
		Node:  host,
		name:  "limit-total",
		addr:  host.FreeAddr(t),
		alias: "limit-total",
	}
	b.config = discConfig(b.addr, b.alias, false,
		fmt.Sprintf("directory:\n  max_records: %d", dxLimitMaxRecords))
	clock := dxClockFile(t)
	b.addr = dxStartBroker(t, host, b.config, b.addr, map[string]string{dxClockEnv: clock})

	f := startFakePeer(t)
	f.Pair(t, b.addr, hostilePairKey(t, b))
	f.WaitLinked(t, 3*time.Second)

	// Five valid cards from five publishers: three fit under the total limit.
	originals := make([]*directorypb.Record, 0, 5)
	addresses := make(map[string]string, 5)
	known := make(map[string]string, 5)
	for i := 0; i < 5; i++ {
		seed := fmt.Sprintf("limit-total-%d", i)
		addresses[seed] = fmt.Sprintf("198.51.100.%d:17443", 60+i)
		originals = append(originals,
			dxLimitCard(t, seed, 1, time.Now(), dxLimitOriginalTTL, addresses[seed]))
		known[hostileHexFingerprint(fakerec.PublisherOf(seed))] = seed
	}
	f.Records(t, originals...)

	accepted := make(map[string]string, dxLimitMaxRecords)
	harness.Eventually(t, discBound(1), "the directory keeps exactly three of five cards",
		func() (bool, string) {
			brokers, state := discList(t, b)
			if state != "" {
				return false, state
			}
			live := make(map[string]bool, dxLimitMaxRecords)
			for _, broker := range brokers {
				if broker.GetIsPeer() {
					continue
				}
				fp := hex.EncodeToString(broker.GetFingerprint())
				if _, ok := known[fp]; !ok {
					return false, "the directory lists a card it was not offered: " + fp
				}
				live[fp] = true
			}
			if len(live) != dxLimitMaxRecords {
				return false, fmt.Sprintf("%d of five cards are listed", len(live))
			}
			for fp := range live {
				accepted[fp] = known[fp]
			}
			return true, ""
		})

	// At 24h the originals are still live, so every renewal replaces a held
	// entry while the directory is at its total limit. The observation window
	// gives the reconcile pass that delivers them time to run before the clock
	// moves past the originals' expiry.
	dxSetClock(t, clock, dxLimitRenewalAt)
	renewed := make([]*directorypb.Record, 0, len(accepted))
	for _, seed := range accepted {
		renewed = append(renewed,
			dxLimitCard(t, seed, 2, time.Now().Add(dxLimitRenewalAt), dxLimitRenewalTTL, addresses[seed]))
	}
	f.Records(t, renewed...)
	harness.Consistently(t, dxPropagationBound, "the three cards stay live while their renewals arrive",
		func() (bool, string) {
			return dxLimitCardsPresent(t, b, accepted)
		})

	// The originals expired two days after issuance. Only the renewals taken
	// at the full limit can keep the three entries in the result.
	dxSetClock(t, clock, dxLimitCheckpoint)
	harness.Consistently(t, dxQuietWindow, "the three renewed cards remain after 144h",
		func() (bool, string) {
			return dxLimitCardsPresent(t, b, accepted)
		})

	// The renewals expire on their own schedule as well.
	dxSetClock(t, clock, dxLimitRenewalAt+dxLimitRenewalTTL)
	harness.Eventually(t, dxExpiryBound, "the renewed cards expire with their own expiry",
		func() (bool, string) {
			brokers, state := discList(t, b)
			if state != "" {
				return false, state
			}
			for _, broker := range brokers {
				if broker.GetIsPeer() {
					continue
				}
				if _, ok := accepted[hex.EncodeToString(broker.GetFingerprint())]; ok {
					return false, "a renewed card is still listed"
				}
			}
			return true, ""
		})
}

// dxLimitCard builds one card record for the total-limit case.
func dxLimitCard(t *testing.T, seed string, version uint64, issued time.Time, ttl time.Duration, addr string) *directorypb.Record {
	t.Helper()
	record, err := fakerec.Build(fakerec.Spec{
		Seed:      seed,
		Kind:      "card",
		Alias:     seed,
		Addresses: []fakerec.Addr{{Kind: "direct", Addr: addr}},
		Version:   version,
		ExpiresIn: ttl,
	}, issued)
	if err != nil {
		t.Fatalf("building a card for %s: %v", seed, err)
	}
	return record
}

// dxLimitCardsPresent reports that the directory lists exactly the cards whose
// fingerprints the map holds.
func dxLimitCardsPresent(t *testing.T, d *discNode, want map[string]string) (bool, string) {
	t.Helper()
	brokers, state := discList(t, d)
	if state != "" {
		return false, state
	}
	listed := make(map[string]bool, len(want))
	for _, broker := range brokers {
		if broker.GetIsPeer() {
			continue
		}
		fp := hex.EncodeToString(broker.GetFingerprint())
		if _, ok := want[fp]; !ok {
			return false, "an unexpected card is listed: " + fp
		}
		listed[fp] = true
	}
	if len(listed) != len(want) {
		return false, fmt.Sprintf("%d of %d cards are listed", len(listed), len(want))
	}
	return true, ""
}

// dxDirectoryRecord is one record in a Broker's directory.json.
type dxDirectoryRecord struct {
	Raw []byte `json:"raw"`
}

// dxDirectoryFile is the part of directory.json these tests read.
type dxDirectoryFile struct {
	Records []dxDirectoryRecord `json:"records"`
}

// dxDirectoryKey identifies a stored record by publisher and kind, because one
// publisher can hold a card and topic announcements in the same directory.
type dxDirectoryKey struct {
	Publisher string
	Kind      directorypb.RecordKind
}

// dxDirectoryRecords reads the records persisted by node's Broker, keyed by
// publisher fingerprint in lowercase hexadecimal and record kind.
func dxDirectoryRecords(t *testing.T, node *harness.Node) map[dxDirectoryKey]*directorypb.RecordBody {
	t.Helper()
	path := filepath.Join(node.AccountHome(), ".peers", "directory.json")
	data := node.ReadFile(t, path)
	var file dxDirectoryFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	out := make(map[dxDirectoryKey]*directorypb.RecordBody, len(file.Records))
	for _, rec := range file.Records {
		body := new(directorypb.RecordBody)
		if err := proto.Unmarshal(rec.Raw, body); err != nil {
			t.Fatalf("parsing a record in %s: %v", path, err)
		}
		key := dxDirectoryKey{
			Publisher: hex.EncodeToString(body.GetPublisher()),
			Kind:      body.GetKind(),
		}
		out[key] = body
	}
	return out
}

// TestPublicTopicExpiresWithDeclarer covers REQ-topics-012: a public topic
// declaration lives as long as its declarer's card. A declares the topic while
// public and is killed, so no session-end withdrawal removes the declaration
// first; B keeps it for six days and drops it once its clock passes the
// declaration's expiry.
func TestPublicTopicExpiresWithDeclarer(t *testing.T) {
	aHost := harness.NewLocalHost(t, harness.LocalOptions{Name: "topic-expiry-a"})
	bHost := harness.NewLocalHost(t, harness.LocalOptions{Name: "topic-expiry-b"})

	listenA := aHost.FreeAddr(t)
	listenB := bHost.FreeAddr(t)
	clockB := dxClockFile(t)

	listenA = dxStartBroker(t, aHost, dxConfig(listenA, "topic-expiry-a", true), listenA, nil)
	dxStartBroker(t, bHost, dxConfig(listenB, "topic-expiry-b", false), listenB,
		map[string]string{dxClockEnv: clockB})

	opA := adOperator(t, aHost)
	defer opA.End(t)
	opB := adOperator(t, bHost)
	defer opB.End(t)

	dxPair(t, opA, opB, listenA)

	// list_topics requires a registered agent, so B reads it through a real
	// session rather than the management-only operator.
	b1 := bHost.StartSession(t, harness.SessionSpec{Name: "b1", StartMCP: true})
	defer b1.End(t)
	a1 := aHost.StartSession(t, harness.SessionSpec{Name: "a1", StartMCP: true})
	defer a1.End(t)
	harness.EventuallyPoll(t, harness.TopicCallWaitBound, "a1 declares "+dxPublicTopic+" publicly",
		func(deadline time.Time) (bool, string) {
			r, err := a1.PollCallWithin(t, deadline, harness.TopicCallPollBound, "join_topic", map[string]any{
				"name":        dxPublicTopic,
				"public":      true,
				"description": "ask infra",
			})
			if err != nil {
				return false, err.Error()
			}
			if r.IsError {
				return false, r.Text
			}
			return true, ""
		})

	harness.EventuallyPoll(t, dxExpiryBound, "B lists "+dxPublicTopic,
		func(deadline time.Time) (bool, string) {
			out, state := dxListTopicsPoll(t, deadline, b1)
			if state != "" {
				return false, state
			}
			if !dxHasPublicTopic(out, dxPublicTopic) {
				return false, fmt.Sprintf("public topics are %v", out.Public)
			}
			return true, ""
		})

	// Kill A without ending a1: ending the session would withdraw the last
	// public member and the declaration would be gone before the clock moves.
	aHost.StopBroker(t, syscall.SIGKILL)

	// Six days after issuance the declaration is still within its seven-day
	// life and must stay visible.
	dxSetClock(t, clockB, 144*time.Hour)
	harness.ConsistentlyPoll(t, dxQuietWindow, harness.TopicCallPollBound, "B still lists "+dxPublicTopic+" after 144h", b1.LogBrokerStack,
		func(deadline time.Time) harness.Observation {
			out, state := dxListTopicsPoll(t, deadline, b1)
			if state != "" {
				return harness.Unobservable(state)
			}
			if !dxHasPublicTopic(out, dxPublicTopic) {
				return harness.Observed(false, fmt.Sprintf("public topics are %v", out.Public))
			}
			return harness.Observed(true, "")
		})

	// Eight days after issuance the declaration has expired. A is dead, so
	// only the expiry can remove it.
	dxSetClock(t, clockB, 192*time.Hour)
	harness.EventuallyPoll(t, dxExpiryBound, "B drops "+dxPublicTopic+" with its declarer's card",
		func(deadline time.Time) (bool, string) {
			out, state := dxListTopicsPoll(t, deadline, b1)
			if state != "" {
				return false, state
			}
			if dxHasPublicTopic(out, dxPublicTopic) {
				return false, dxPublicTopic + " is still listed"
			}
			return true, ""
		})

	// The listing hides an expired record even before a maintenance pass runs.
	// The stored record itself must be purged too, so a later restart cannot
	// bring it back.
	harness.Eventually(t, dxExpiryBound, "B's directory file drops "+dxPublicTopic,
		func() (bool, string) {
			for key, body := range dxDirectoryRecords(t, bHost) {
				if body.GetTopicAnnouncement().GetName() == dxPublicTopic {
					return false, "the declaration is still stored for " + key.Publisher
				}
			}
			return true, ""
		})
}

// dxClockFile creates a clock offset file that starts at zero and returns its
// path. The Broker holding it must run a build with the peers_testenv tag.
func dxClockFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "clock")
	dxSetClock(t, path, 0)
	return path
}

// dxSetClock replaces the offset file atomically: the hook rereads the file
// when its modification time or size changes and never sees a partial value.
func dxSetClock(t *testing.T, path string, offset time.Duration) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(offset.String()), 0o644); err != nil {
		t.Fatalf("writing the clock offset: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("installing the clock offset: %v", err)
	}
}

// dxConfig builds one node's configuration: the shortened timers, the one
// second reconciliation interval, one loopback address used both to listen and
// to advertise, and an explicit alias that keeps the Brokers sharing this host
// distinct.
func dxConfig(listen, alias string, public bool) string {
	parts := []string{
		harness.FastTimers,
		"directory:\n  reconcile_interval: 1s",
		fmt.Sprintf("broker:\n  alias: %q\n  listen: %q\n  advertise: %q", alias, listen, listen),
	}
	if public {
		parts = append(parts, "broker:\n  public: true")
	}
	return harness.ConfigYAML(parts...)
}

// dxStartBroker starts a Broker with the given environment and fails the test
// when it does not come up. It returns the address the Broker actually uses in
// place of requested.
func dxStartBroker(t *testing.T, node *harness.Node, config, requested string, env map[string]string) string {
	t.Helper()
	start := node.StartBroker(t, harness.BrokerSpec{ConfigYAML: config, Env: env})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s",
			start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
	return start.Address(requested)
}

// dxPair makes the dialer connect to the acceptor's listen address and returns
// the acceptor's fingerprint, which the dialer learns from the pairing result.
func dxPair(t *testing.T, acceptor, dialer *harness.Session, listen string) string {
	t.Helper()
	key := adCreateKey(t, acceptor)
	r := adConnect(t, dialer, adShownAddress(t, key.Addresses, listen), key.Key)
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.ConnectBrokerOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding connect_broker: %v", err)
	}
	if out.Fingerprint == "" {
		t.Fatalf("connect_broker returned no fingerprint")
	}
	return out.Fingerprint
}

// dxListBrokers returns the Brokers known to node.
func dxListBrokers(t *testing.T, node *harness.Node) *localpb.ListBrokersResponse {
	t.Helper()
	resp, detail := node.Oracle().ListBrokers(t)
	if detail != nil {
		t.Fatalf("listing Brokers: %s", detail.GetMessage())
	}
	return resp
}

// dxFindBroker returns the entry whose fingerprint is fp, written as lowercase
// hexadecimal.
func dxFindBroker(resp *localpb.ListBrokersResponse, fp string) (*localpb.BrokerInfo, bool) {
	for _, broker := range resp.GetBrokers() {
		if hex.EncodeToString(broker.GetFingerprint()) == fp {
			return broker, true
		}
	}
	return nil, false
}

// dxWaitCard waits until node lists fp as a connectable non-peer whose
// advertised addresses are exactly want, in order.
func dxWaitCard(t *testing.T, node *harness.Node, fp string, want []string, bound time.Duration, notes ...string) {
	t.Helper()
	what := "the card of " + fp + " shows " + strings.Join(want, ", ")
	if len(notes) > 0 {
		what += " " + notes[0]
	}
	harness.Eventually(t, bound, what, func() (bool, string) {
		resp := dxListBrokers(t, node)
		broker, ok := dxFindBroker(resp, fp)
		if !ok {
			return false, "the card is not listed"
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
	})
}

// dxListTopicsPoll calls list_topics from a wait and returns the decoded
// result: the call is bounded by the wait's deadline and a transport error is
// returned as state.
func dxListTopicsPoll(t *testing.T, deadline time.Time, session *harness.Session) (mcpserver.ListTopicsOut, string) {
	t.Helper()
	return pollDecode[mcpserver.ListTopicsOut](t, deadline, session, "list_topics", map[string]any{})
}

// dxHasPublicTopic reports whether name is among the public topics.
func dxHasPublicTopic(out mcpserver.ListTopicsOut, name string) bool {
	for _, topic := range out.Public {
		if topic.Name == name {
			return true
		}
	}
	return false
}
