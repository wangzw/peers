//go:build e2e

package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/test/e2e/fakepeer/fakerec"
	"github.com/wangzw/peers/test/e2e/harness"
)

// hostileRecordTTL is comfortably inside the directory's seven-day lifetime,
// so a record is rejected only for the reason a case tests.
const hostileRecordTTL = 168 * time.Hour

// replaceIntervalEnv names the variable the peers_testenv build reads for the
// replacement interval. The harness sets it to one second for every Broker so
// the other discovery cases can watch a withdrawal or a re-signed card move; a
// case that needs the release window overrides it per Broker.
const replaceIntervalEnv = "PEERS_TEST_DIRECTORY_REPLACE_INTERVAL"

// hostilePairKey creates a one-time pairing key through an operator session.
func hostilePairKey(t *testing.T, d *discNode) string {
	t.Helper()
	op := discOperator(t, d)
	defer op.End(t)
	r := discCallOK(t, op, "create_pairing_key", nil, 3*time.Second)
	key, err := harness.Decode[mcpserver.CreatePairingKeyOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding create_pairing_key on %s: %v", d.name, err)
	}
	return key.Key
}

// hostileBuild builds a directory record with the fields a case does not set
// filled in.
func hostileBuild(t *testing.T, spec fakerec.Spec) *directorypb.Record {
	t.Helper()
	if spec.Version == 0 {
		spec.Version = 1
	}
	if spec.ExpiresIn == 0 {
		spec.ExpiresIn = hostileRecordTTL
	}
	record, err := fakerec.Build(spec, time.Now())
	if err != nil {
		t.Fatalf("building a fake record: %v", err)
	}
	return record
}

// hostileCard builds a card record.
func hostileCard(t *testing.T, seed, alias, address, tamper string) *directorypb.Record {
	t.Helper()
	return hostileBuild(t, fakerec.Spec{
		Seed:      seed,
		Kind:      "card",
		Alias:     alias,
		Addresses: []fakerec.Addr{{Kind: "direct", Addr: address}},
		Tamper:    tamper,
	})
}

// hostileHexFingerprint renders a publisher fingerprint as lowercase hex.
func hostileHexFingerprint(fp pairing.Fingerprint) string {
	return hex.EncodeToString(fp[:])
}

// hostileNonPeers returns the entries that are not paired peers.
func hostileNonPeers(brokers []*localpb.BrokerInfo) []*localpb.BrokerInfo {
	out := make([]*localpb.BrokerInfo, 0, len(brokers))
	for _, broker := range brokers {
		if !broker.GetIsPeer() {
			out = append(out, broker)
		}
	}
	return out
}

// hostileAbsent observes for the whole window that d lists none of the
// fingerprints.
func hostileAbsent(t *testing.T, d *discNode, window time.Duration, what string, fingerprints ...string) {
	t.Helper()
	harness.Consistently(t, window, what, func() (bool, string) {
		brokers, state := discList(t, d)
		if state != "" {
			return false, state
		}
		for _, broker := range brokers {
			got := hex.EncodeToString(broker.GetFingerprint())
			for _, want := range fingerprints {
				if got == want {
					return false, "fingerprint " + got + " is listed"
				}
			}
		}
		return true, ""
	})
}

// TestDirectoryRejectsTamperedCards checks that a tampered card and an
// impersonated card are dropped while a valid card from the same unpaired
// (but paired) source propagates: the signature covers the payload and the
// publisher must match the presented public key.
func TestDirectoryRejectsTamperedCards(t *testing.T) {
	t.Parallel()

	b := discNewNode(t, "hostile-b", "", false)
	c := discNewNode(t, "hostile-c", "", false)
	discPair(t, b, c)

	f := startFakePeer(t)
	f.Pair(t, b.addr, hostilePairKey(t, b))
	f.WaitLinked(t, 3*time.Second)

	const (
		goodSeed         = "tamper-good"
		tamperedSeed     = "tamper-addresses"
		impersonatedSeed = "tamper-publisher"
	)
	good := hostileCard(t, goodSeed, "card-good", "198.51.100.10:17443", "")
	tampered := hostileCard(t, tamperedSeed, "card-tampered", "198.51.100.11:17443", "addresses")
	impersonated := hostileCard(t, impersonatedSeed, "card-impersonated", "198.51.100.12:17443", "publisher")
	f.Records(t, good, tampered, impersonated)

	fpGood := hostileHexFingerprint(fakerec.PublisherOf(goodSeed))
	fpTampered := hostileHexFingerprint(fakerec.PublisherOf(tamperedSeed))
	fpImpersonated := hostileHexFingerprint(fakerec.PublisherOf(impersonatedSeed))
	claimed := pairing.Fingerprint(sha256.Sum256([]byte("other:" + impersonatedSeed)))
	fpClaimed := hostileHexFingerprint(claimed)

	for _, node := range []*discNode{b, c} {
		discWaitBroker(t, node, fpGood, discBound(1), func(broker *localpb.BrokerInfo) string {
			if broker.GetAlias() != "card-good" {
				return fmt.Sprintf("alias is %q, want card-good", broker.GetAlias())
			}
			if broker.GetIsPeer() {
				return "the control card is listed as a peer"
			}
			return ""
		})
	}

	for _, node := range []*discNode{b, c} {
		hostileAbsent(t, node, discQuietBound(2), node.name+" rejects the tampered and impersonated cards", fpTampered, fpImpersonated, fpClaimed)
	}
}

// TestDirectoryRejectsUnpairedSource checks that records pushed over a stream
// that never completed pairing are dropped, while the same records are
// accepted once the source is paired.
func TestDirectoryRejectsUnpairedSource(t *testing.T) {
	t.Parallel()

	b := discNewNode(t, "unpaired-b", "", false)
	f := startFakePeer(t)

	const seed = "unpaired-card"
	card := hostileCard(t, seed, "unpaired-card", "198.51.100.20:17443", "")
	f.Records(t, card)

	reason := f.DialUnpaired(t, b.addr)
	if reason == "" {
		t.Fatal("the unpaired stream closed without a reason")
	}

	fp := hostileHexFingerprint(fakerec.PublisherOf(seed))
	hostileAbsent(t, b, discQuietBound(1), "B rejects records from an unpaired source", fp)

	f.Pair(t, b.addr, hostilePairKey(t, b))
	f.WaitLinked(t, 3*time.Second)
	f.Records(t, card)

	discWaitBroker(t, b, fp, discBound(1), func(broker *localpb.BrokerInfo) string {
		if broker.GetAlias() != "unpaired-card" {
			return fmt.Sprintf("alias is %q, want unpaired-card", broker.GetAlias())
		}
		return ""
	})
}

// TestDirectoryPerLinkLimit checks that one link cannot fill the directory:
// a link stops at directory.max_records_per_link while a second link still
// adds its record under the total limit.
func TestDirectoryPerLinkLimit(t *testing.T) {
	t.Parallel()

	b := discNewNode(t, "limit-b", "directory:\n  max_records: 10\n  max_records_per_link: 4", false)

	first := startFakePeer(t)
	first.Pair(t, b.addr, hostilePairKey(t, b))
	first.WaitLinked(t, 3*time.Second)
	second := startFakePeer(t)
	second.Pair(t, b.addr, hostilePairKey(t, b))
	second.WaitLinked(t, 3*time.Second)

	records := make([]*directorypb.Record, 0, 10)
	for i := 0; i < 10; i++ {
		seed := fmt.Sprintf("limit-%d", i)
		records = append(records, hostileCard(t, seed, seed, fmt.Sprintf("198.51.100.%d:17443", 30+i), ""))
	}
	first.Records(t, records...)

	harness.Eventually(t, discBound(1), "B holds exactly four records from the first link", func() (bool, string) {
		brokers, state := discList(t, b)
		if state != "" {
			return false, state
		}
		if got := len(hostileNonPeers(brokers)); got != 4 {
			return false, fmt.Sprintf("%d non-peer records", got)
		}
		return true, ""
	})
	harness.Consistently(t, discQuietBound(1), "B keeps the first link at four records", func() (bool, string) {
		brokers, state := discList(t, b)
		if state != "" {
			return false, state
		}
		if got := len(hostileNonPeers(brokers)); got != 4 {
			return false, fmt.Sprintf("%d non-peer records", got)
		}
		return true, ""
	})

	const extraSeed = "limit-extra"
	second.Records(t, hostileCard(t, extraSeed, extraSeed, "198.51.100.99:17443", ""))
	fpExtra := hostileHexFingerprint(fakerec.PublisherOf(extraSeed))
	discWaitBroker(t, b, fpExtra, discBound(1), nil)
	brokers, state := discList(t, b)
	if state != "" {
		t.Fatalf("B's ListBrokers failed: %s", state)
	}
	if got := len(hostileNonPeers(brokers)); got != 5 {
		t.Fatalf("B lists %d non-peer records after the second link, want 5", got)
	}
}

// hostileAgentID returns the Broker-assigned id of the agent whose address
// starts with name@. ListTopics needs it to resolve the caller.
func hostileAgentID(t *testing.T, d *discNode, name string) string {
	t.Helper()
	st, detail := d.Oracle().Status(t)
	if detail != nil {
		t.Fatalf("reading the status of %s: %s", d.name, detail.GetMessage())
	}
	for _, agent := range st.GetAgents() {
		if strings.HasPrefix(agent.GetAddress(), name+"@") {
			return agent.GetAgentId()
		}
	}
	t.Fatalf("agent %q is not registered on %s: %+v", name, d.name, st.GetAgents())
	return ""
}

// TestDirectoryPublicTopicReceiveLimits covers the receive-side rules for
// public topic declarations: a declaration whose name or description breaks
// the format rules is left out of the local listing while the record still
// reaches the next Broker, and a description declared by a paired peer cannot
// be replaced by a remote publisher that claims the largest version.
func TestDirectoryPublicTopicReceiveLimits(t *testing.T) {
	t.Parallel()

	t.Run("REQ-topics-030 malformed declarations are not listed", func(t *testing.T) {
		t.Parallel()

		b := discNewNode(t, "mal-b", "", false)
		c := discNewNode(t, "mal-c", "", false)
		discPair(t, b, c)

		f := startFakePeer(t)
		f.Pair(t, b.addr, hostilePairKey(t, b))
		f.WaitLinked(t, 3*time.Second)

		// G observes the far Broker, so a record that reaches C through B is
		// visible even though C's listing leaves it out too.
		g := startFakePeer(t)
		g.Pair(t, c.addr, hostilePairKey(t, c))
		g.WaitLinked(t, 3*time.Second)

		const topicKind = "RECORD_KIND_TOPIC_ANNOUNCEMENT"
		longName := strings.Repeat("n", 300)
		specs := []fakerec.Spec{
			{Seed: "mal-long-description", Kind: "topic", Topic: "mal-long-description", Description: strings.Repeat("a", 60<<10)},
			{Seed: "mal-newline-name", Kind: "topic", Topic: "mal\nnewline", Description: "d"},
			{Seed: "mal-long-name", Kind: "topic", Topic: longName, Description: "d"},
		}
		records := make([]*directorypb.Record, 0, len(specs))
		for _, spec := range specs {
			records = append(records, hostileBuild(t, spec))
		}
		f.Records(t, records...)

		for _, spec := range specs {
			fp := hostileHexFingerprint(fakerec.PublisherOf(spec.Seed))
			object := spec.Topic
			harness.Eventually(t, discBound(3), "C receives the malformed declaration", func() (bool, string) {
				if versions := g.recordListVersions(t, fp, topicKind, object); len(versions) == 0 {
					return false, "C has not received it"
				}
				return true, ""
			})
		}

		watcher := discAgent(t, b, "mal-watcher", t.TempDir())
		defer watcher.End(t)
		harness.WaitRegistered(t, watcher, 3*time.Second)
		harness.ConsistentlyPoll(t, discQuietBound(2), harness.TopicCallPollBound, "B never lists the malformed declarations", watcher.LogBrokerStack, func(deadline time.Time) harness.Observation {
			out, state := discListTopicsPoll(t, deadline, watcher)
			if state != "" {
				return harness.Unobservable(state)
			}
			for _, spec := range specs {
				if _, ok := discPublicTopic(out, spec.Topic); ok {
					return harness.Observed(false, "the listing contains "+spec.Topic)
				}
			}
			return harness.Observed(true, "")
		})
	})

	t.Run("REQ-topics-031 peer description over a maximum version", func(t *testing.T) {
		t.Parallel()

		a := discNewNode(t, "pub-src-a", "", true)
		b := discNewNode(t, "pub-src-b", "", false)
		paired := discPair(t, a, b)

		agent := discAgent(t, a, "pub-declarer", t.TempDir())
		defer agent.End(t)
		harness.WaitRegistered(t, agent, 3*time.Second)
		discJoinOK(t, agent, "pub-source-topic", true, "from the paired peer")

		// The attacker pairs with A, so at B it is not a peer and its
		// declaration falls into the lowest tier whatever version it claims.
		f := startFakePeer(t)
		f.Pair(t, a.addr, hostilePairKey(t, a))
		f.WaitLinked(t, 3*time.Second)
		f.Records(t, hostileBuild(t, fakerec.Spec{
			Seed:        "pub-attacker",
			Kind:        "topic",
			Topic:       "pub-source-topic",
			Description: "from the attacker",
			Version:     math.MaxUint64,
		}))

		watcher := discAgent(t, b, "pub-watcher", t.TempDir())
		defer watcher.End(t)
		harness.WaitRegistered(t, watcher, 3*time.Second)
		agentID := hostileAgentID(t, b, "pub-watcher")

		harness.Eventually(t, discBound(3), "B merges the peer and the attacker declaration", func() (bool, string) {
			resp, detail := b.Oracle().ListTopics(t, agentID)
			if detail != nil {
				return false, detail.GetMessage()
			}
			for _, topic := range resp.GetPublicTopics() {
				if topic.GetName() != "pub-source-topic" {
					continue
				}
				switch {
				case topic.GetDescription() != "from the paired peer":
					return false, "description is " + topic.GetDescription()
				case topic.GetPublisherFingerprint() != paired.Fingerprint:
					return false, "publisher is " + topic.GetPublisherFingerprint()
				case topic.GetPublisherAlias() != a.alias:
					return false, "publisher alias is " + topic.GetPublisherAlias()
				case topic.GetDeclarers() != 2:
					return false, fmt.Sprintf("declarers is %d, want 2", topic.GetDeclarers())
				}
				return true, ""
			}
			return false, "the topic is not listed"
		})

		out, state := discListTopics(t, watcher)
		if state != "" {
			t.Fatalf("list_topics on B failed: %s", state)
		}
		topic, ok := discPublicTopic(out, "pub-source-topic")
		if !ok {
			t.Fatal("list_topics on B does not list pub-source-topic")
		}
		if topic.Description != "from the paired peer" {
			t.Fatalf("list_topics on B shows description %q, want the peer's", topic.Description)
		}
	})
}

// TestDirectoryHoldsRapidRecordReplacement checks that a newer version of a
// record accepted from a link is not taken within the replacement interval, so
// the next Broker sees only the first version while the local listing keeps the
// first version's alias.
func TestDirectoryHoldsRapidRecordReplacement(t *testing.T) {
	t.Parallel()

	// The suite shortens the interval to one second so the other discovery
	// cases can watch a withdrawal or a re-signed card propagate. This case
	// asserts a five-second window in which the held version must not change,
	// so both Brokers that accept the record from a link get the release
	// window instead: B holds the second version from its link, and C would
	// hold it from B even if B forwarded it.
	releaseInterval := map[string]string{replaceIntervalEnv: "1h"}
	b := discNewNodeEnv(t, "rep-b", "", false, releaseInterval)
	c := discNewNodeEnv(t, "rep-c", "", false, releaseInterval)
	discPair(t, b, c)

	f := startFakePeer(t)
	f.Pair(t, b.addr, hostilePairKey(t, b))
	f.WaitLinked(t, 3*time.Second)
	g := startFakePeer(t)
	g.Pair(t, c.addr, hostilePairKey(t, c))
	g.WaitLinked(t, 3*time.Second)

	const (
		seed     = "replace-card"
		cardKind = "RECORD_KIND_BROKER_CARD"
	)
	fp := hostileHexFingerprint(fakerec.PublisherOf(seed))
	buildCard := func(version uint64, alias, address string) *directorypb.Record {
		return hostileBuild(t, fakerec.Spec{
			Seed:      seed,
			Kind:      "card",
			Alias:     alias,
			Addresses: []fakerec.Addr{{Kind: "direct", Addr: address}},
			Version:   version,
		})
	}

	f.Records(t, buildCard(1, "replace-one", "198.51.100.61:17443"))
	harness.Eventually(t, discBound(3), "C receives the first version", func() (bool, string) {
		versions := g.recordListVersions(t, fp, cardKind, "")
		for _, version := range versions {
			if version == 1 {
				return true, ""
			}
		}
		return false, fmt.Sprintf("C has seen versions %v", versions)
	})
	discWaitBroker(t, b, fp, discBound(1), func(broker *localpb.BrokerInfo) string {
		if broker.GetAlias() != "replace-one" {
			return fmt.Sprintf("alias is %q, want replace-one", broker.GetAlias())
		}
		return ""
	})

	// The second version arrives well inside the replacement interval.
	f.Records(t, buildCard(2, "replace-two", "198.51.100.62:17443"))

	harness.Consistently(t, discQuietBound(2), "C receives only the first version", func() (bool, string) {
		for _, version := range g.recordListVersions(t, fp, cardKind, "") {
			if version != 1 {
				return false, fmt.Sprintf("C received version %d", version)
			}
		}
		return true, ""
	})
	harness.Consistently(t, discQuietBound(2), "B keeps the first version", func() (bool, string) {
		brokers, state := discList(t, b)
		if state != "" {
			return false, state
		}
		broker, ok := discFindFingerprint(brokers, fp)
		if !ok {
			return false, "the record is not listed"
		}
		if broker.GetAlias() != "replace-one" {
			return false, fmt.Sprintf("alias is %q, want replace-one", broker.GetAlias())
		}
		return true, ""
	})
}

// TestDirectoryMalformedDirectAddress checks that a card with a well-formed
// direct address and one containing an ESC byte is accepted, propagated and
// listed with only the well-formed entry (REQ-broker-discovery-018).
func TestDirectoryMalformedDirectAddress(t *testing.T) {
	t.Parallel()

	b := discNewNode(t, "badaddr-b", "", false)
	c := discNewNode(t, "badaddr-c", "", false)
	discPair(t, b, c)

	f := startFakePeer(t)
	f.Pair(t, b.addr, hostilePairKey(t, b))
	f.WaitLinked(t, 3*time.Second)

	const (
		seed = "malformed-direct-address"
		good = "198.51.100.4:17443"
	)
	f.Records(t, hostileBuild(t, fakerec.Spec{
		Seed:  seed,
		Kind:  "card",
		Alias: "card-bad-addr",
		Addresses: []fakerec.Addr{
			{Kind: "direct", Addr: "198.51.100.\x1b5:17443"},
			{Kind: "direct", Addr: good},
		},
	}))

	fp := hostileHexFingerprint(fakerec.PublisherOf(seed))
	for _, node := range []*discNode{b, c} {
		discWaitBroker(t, node, fp, discBound(2), func(broker *localpb.BrokerInfo) string {
			addrs := broker.GetAdvertisedAddresses()
			if len(addrs) != 1 || addrs[0].GetKind() != "direct" || addrs[0].GetAddr() != good {
				return fmt.Sprintf("addresses are %v, want only the well-formed %q", addrs, good)
			}
			if broker.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE {
				return fmt.Sprintf("status is %v, want CONNECTABLE", broker.GetStatus())
			}
			return ""
		})
	}
}

// TestDirectoryUnknownAddressKind checks that a card carrying an address kind
// this version does not know is accepted, shown without that entry and still
// propagated.
func TestDirectoryUnknownAddressKind(t *testing.T) {
	t.Parallel()

	b := discNewNode(t, "kind-b", "", false)
	c := discNewNode(t, "kind-c", "", false)
	discPair(t, b, c)

	f := startFakePeer(t)
	f.Pair(t, b.addr, hostilePairKey(t, b))
	f.WaitLinked(t, 3*time.Second)

	const seed = "unknown-kind"
	record := hostileBuild(t, fakerec.Spec{
		Seed:  seed,
		Kind:  "card",
		Alias: "card-kind",
		Addresses: []fakerec.Addr{
			{Kind: "relay", Addr: "relay.example:1"},
			{Kind: "direct", Addr: "198.51.100.4:17443"},
		},
	})
	f.Records(t, record)

	fp := hostileHexFingerprint(fakerec.PublisherOf(seed))
	for _, node := range []*discNode{b, c} {
		discWaitBroker(t, node, fp, discBound(1), func(broker *localpb.BrokerInfo) string {
			addrs := broker.GetAdvertisedAddresses()
			if len(addrs) != 1 || addrs[0].GetKind() != "direct" || addrs[0].GetAddr() != "198.51.100.4:17443" {
				return fmt.Sprintf("addresses are %v, want the direct entry only", addrs)
			}
			if broker.GetStatus() != localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE {
				return fmt.Sprintf("status is %v, want CONNECTABLE", broker.GetStatus())
			}
			return ""
		})
	}
}
