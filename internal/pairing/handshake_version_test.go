package pairing

import (
	"context"
	"errors"
	"testing"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// hsProtocolVersion is deliberately independent of linkpb.ProtocolVersion:
// pairing treats the version as opaque.
const hsProtocolVersion uint32 = 2

// hsRawPairRequest encodes a PairRequest by field number, leaving a zero version
// off the wire.
func hsRawPairRequest(ni, tag []byte, version uint32) []byte {
	payload := protowire.AppendTag(nil, 1, protowire.BytesType)
	payload = protowire.AppendBytes(payload, ni)
	payload = protowire.AppendTag(payload, 3, protowire.BytesType)
	payload = protowire.AppendBytes(payload, tag)
	if version != 0 {
		payload = protowire.AppendTag(payload, 4, protowire.VarintType)
		payload = protowire.AppendVarint(payload, uint64(version))
	}
	return payload
}

func hsVarintField(payload []byte, num protowire.Number) (uint64, bool) {
	for len(payload) > 0 {
		n, typ, tagLen := protowire.ConsumeTag(payload)
		if tagLen < 0 {
			return 0, false
		}
		payload = payload[tagLen:]
		if n == num && typ == protowire.VarintType {
			value, valueLen := protowire.ConsumeVarint(payload)
			if valueLen < 0 {
				return 0, false
			}
			return value, true
		}
		valueLen := protowire.ConsumeFieldValue(n, typ, payload)
		if valueLen < 0 {
			return 0, false
		}
		payload = payload[valueLen:]
	}
	return 0, false
}

// TestInitiatorSendsProtocolVersion checks that the initiator puts the
// connection's protocol version in field 4 of PairRequest.
func TestInitiatorSendsProtocolVersion(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	tr := &hsTransport{}
	initP := hsNewPairing(t, initS, tr, clk)

	acceptorFP, acceptorPub := hsKey(t)
	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), acceptorFP, acceptorPub)
	tr.raw = aConn

	request := make(chan []byte, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
		defer cancel()
		kind, payload, err := bConn.Receive(ctx)
		if err != nil || kind != kindPairRequest {
			request <- nil
			return
		}
		request <- payload
		// Connect does not return until this connection closes.
		_ = bConn.Close()
	}()

	_, _, _ = initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", "")

	payload := <-request
	if payload == nil {
		t.Fatal("the acceptor did not receive a PairRequest")
	}
	version, ok := hsVarintField(payload, 4)
	if !ok {
		t.Fatal("PairRequest carries no protocol_version in field 4")
	}
	if uint32(version) != hsProtocolVersion {
		t.Fatalf("PairRequest protocol_version = %d, want the connection's %d", version, hsProtocolVersion)
	}
}

func hsVersionMismatch(
	t *testing.T, accP *Pairing, key string, initiatorFP Fingerprint,
	initiatorPub []byte, remote string, version uint32,
) (int32, error) {
	t.Helper()
	aConn, bConn := hsConnPairFor(initiatorFP, initiatorPub, accP.identity.fingerprint(), accP.identity.publicKey())
	bConn.remote = remote
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()

	ni := makeSeq(11, nonceLength)
	hint := computeHint([]byte(key), ni, initiatorFP)
	if err := hsSendRaw(aConn, kindPairRequest, hsRawPairRequest(ni, hint, version)); err != nil {
		t.Fatalf("send PairRequest: %v", err)
	}
	kind, payload, err := receiveWithTimeout(aConn)
	if err != nil {
		t.Fatalf("receive the reply to PairRequest: %v", err)
	}
	if kind != kindPairReject {
		t.Fatalf("frame after a version mismatch = %d, want PairReject (a PairChallenge means the version was not compared)", kind)
	}
	var reject pairingpb.PairReject
	if err := proto.Unmarshal(payload, &reject); err != nil {
		t.Fatalf("decode PairReject: %v", err)
	}
	return int32(reject.GetReason()), awaitAccept(acceptErr)
}

// TestAcceptorRejectsProtocolVersionMismatch checks that a differing or unfilled
// protocol version is refused before any key lookup, audited as version
// incompatible and not counted as an attempt.
func TestAcceptorRejectsProtocolVersionMismatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint32
	}{
		{name: "older", version: hsProtocolVersion - 1},
		{name: "unfilled", version: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := newHSClock()
			accS := newHSSettings(t)
			accS.alias = "beta"
			accS.staticKey = ""
			accS.maxAttempts = 1
			accP := hsNewPairing(t, accS, &hsTransport{}, clk)

			key, _, _, err := accP.CreateOneTimeKey()
			if err != nil {
				t.Fatalf("CreateOneTimeKey: %v", err)
			}

			initiatorFP, initiatorPub := hsKey(t)
			const sameSource = "192.0.2.2:40002"
			const otherSource = "198.51.100.7:40003"
			const attempts = 3
			for i := 0; i < attempts; i++ {
				reason, acceptErr := hsVersionMismatch(t, accP, key, initiatorFP, initiatorPub, sameSource, tc.version)
				if reason != 5 {
					t.Fatalf("attempt %d: PairReject reason = %d, want 5 (version incompatible)", i, reason)
				}
				var pairErr *PairingError
				if !errors.As(acceptErr, &pairErr) {
					t.Fatalf("attempt %d: Accept error = %v (%T), want *PairingError", i, acceptErr, acceptErr)
				}
				if pairErr.Detail != "link protocol version incompatible with the other Broker; upgrade the older Broker" {
					t.Fatalf("attempt %d: rejection detail = %q", i, pairErr.Detail)
				}
			}
			if reason, _ := hsVersionMismatch(t, accP, key, initiatorFP, initiatorPub, otherSource, tc.version); reason != 5 {
				t.Fatalf("other source: PairReject reason = %d, want 5", reason)
			}

			wantSame := auditSourceMergeKey(sameSource)
			wantOther := auditSourceMergeKey(otherSource)
			if wantSame == "" || wantOther == "" || wantSame == wantOther {
				t.Fatalf("source merge keys are %q and %q, want distinct non-empty keys", wantSame, wantOther)
			}

			byKey := map[string]int{}
			var failed []config.Event
			for _, e := range accS.recorded() {
				if e.Event != config.EventPairingFailed {
					continue
				}
				failed = append(failed, e)
				byKey[e.MergeKey]++
				if e.Detail != "version incompatible" {
					t.Fatalf("audit detail = %q, want version incompatible", e.Detail)
				}
				if e.Peer == nil || e.Peer.Alias != "" {
					t.Fatalf("audit peer = %+v, want the fingerprint without an alias", e.Peer)
				}
			}
			if len(failed) != attempts+1 {
				t.Fatalf("pairing_failed audits = %d, want %d: %+v", len(failed), attempts+1, failed)
			}
			if got := byKey[wantSame]; got != attempts {
				t.Fatalf("audits under %q = %d, want %d merged-source events", wantSame, got, attempts)
			}
			if got := byKey[wantOther]; got != 1 {
				t.Fatalf("audits under %q = %d, want 1", wantOther, got)
			}
			if len(byKey) != 2 {
				t.Fatalf("distinct merge keys = %d, want 2: %v", len(byKey), byKey)
			}

			accP.keys.mu.Lock()
			total, bySource := accP.keys.attempts.total, len(accP.keys.attempts.bySource)
			accP.keys.mu.Unlock()
			if total != 0 || bySource != 0 {
				t.Fatalf("a version mismatch was counted as an attempt: total=%d sources=%d", total, bySource)
			}
		})
	}
}

// TestSameProtocolVersionPairs checks that equal versions still complete a
// handshake.
func TestSameProtocolVersionPairs(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	tr := &hsTransport{}
	initP := hsNewPairing(t, initS, tr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	peer, connectErr, acceptErr := pairWithHello(t, initP, accP, tr, "STATIC-KEY")
	if connectErr != nil {
		t.Fatalf("Connect: %v", connectErr)
	}
	if acceptErr != nil {
		t.Fatalf("Accept: %v", acceptErr)
	}
	if peer.Alias != "beta" {
		t.Fatalf("paired alias = %q, want beta", peer.Alias)
	}
}
