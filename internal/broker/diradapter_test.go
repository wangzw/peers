package broker

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory"
	"github.com/wangzw/peers/internal/pairing"
)

var (
	_ directoryOps         = (*directory.Directory)(nil)
	_ directoryOps         = (*discoveryDir)(nil)
	_ PublicTopicDirectory = (*directoryAdapter)(nil)
	_ BrokerCatalog        = (*directoryAdapter)(nil)
	_ ConnectWarner        = (*directoryAdapter)(nil)
	_ CardIssuer           = (*directoryAdapter)(nil)
)

func adapterFP(b byte) pairing.Fingerprint {
	var fp pairing.Fingerprint
	fp[0] = b
	return fp
}

func adapterBytes(b byte) []byte {
	fp := adapterFP(b)
	return fp[:]
}

// TestAdapterDeclareErrorMapping checks that each directory declaration error
// maps to the matching broker code and message.
func TestAdapterDeclareErrorMapping(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		code    localpb.ErrorCode
		message string
	}{
		{
			name:    "not public",
			err:     directory.ErrNotPublic,
			code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			message: "broker.public",
		},
		{
			name:    "wrapped not public",
			err:     fmt.Errorf("%w: x", directory.ErrNotPublic),
			code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			message: "broker.public",
		},
		{
			name:    "quota exceeded",
			err:     directory.ErrQuotaExceeded,
			code:    localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
			message: "maximum number of public topics",
		},
		{
			name:    "invalid topic",
			err:     directory.ErrInvalidTopic,
			code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			message: "invalid topic name",
		},
		{
			name:    "invalid description",
			err:     directory.ErrInvalidDescription,
			code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			message: "description must be valid UTF-8",
		},
		{
			name:    "unknown error",
			err:     errors.New("x"),
			code:    localpb.ErrorCode_ERROR_CODE_INTERNAL,
			message: "declaring the public topic failed: x",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &discoveryDir{declareErr: map[string]error{"infra": tc.err}}
			ad := newDirectoryAdapter(d)

			err := ad.DeclarePublicTopic("infra", "ask infra")
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("DeclarePublicTopic error = %v (%T), want *Error", err, err)
			}
			if e.Code != tc.code {
				t.Errorf("error code = %v, want %v", e.Code, tc.code)
			}
			if !strings.Contains(e.Message, tc.message) {
				t.Errorf("message %q does not contain %q", e.Message, tc.message)
			}
		})
	}

	t.Run("success", func(t *testing.T) {
		ad := newDirectoryAdapter(&discoveryDir{})
		if err := ad.DeclarePublicTopic("infra", ""); err != nil {
			t.Fatalf("DeclarePublicTopic returned %v, want nil", err)
		}
	})
}

// TestAdapterNonPeers checks that non-peer brokers are converted to local API
// entries and peers are skipped.
func TestAdapterNonPeers(t *testing.T) {
	dialer := directory.KnownBroker{
		Fingerprint: adapterFP(1),
		Alias:       "dialer",
		Addresses:   []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.1:17443"}},
		IsPeer:      true,
		Role:        pairing.RoleDialer,
		Status:      directory.StatusConnected,
	}
	acceptor := directory.KnownBroker{
		Fingerprint: adapterFP(2),
		Alias:       "acceptor",
		IsPeer:      true,
		Role:        pairing.RoleAcceptor,
		Status:      directory.StatusConnected,
	}
	connectable := directory.KnownBroker{
		Fingerprint: adapterFP(3),
		Alias:       "public-one",
		Addresses:   []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.3:17443"}},
		Status:      directory.StatusConnectable,
	}
	unreachable := directory.KnownBroker{
		Fingerprint: adapterFP(4),
		Alias:       "no-address",
		Status:      directory.StatusUnreachable,
		Detail:      "the card carries no usable address",
	}
	d := &discoveryDir{brokers: []directory.KnownBroker{dialer, acceptor, connectable, unreachable}}
	ad := newDirectoryAdapter(d)

	got := ad.NonPeers()
	want := []*localpb.BrokerInfo{
		{
			Alias:       "public-one",
			Fingerprint: adapterBytes(3),
			Status:      localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE,
			AdvertisedAddresses: []*localpb.AdvertisedAddress{
				{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.3:17443"},
			},
		},
		{
			Alias:        "no-address",
			Fingerprint:  adapterBytes(4),
			Status:       localpb.BrokerStatus_BROKER_STATUS_NOT_DIRECTLY_REACHABLE,
			StatusDetail: "the card carries no usable address",
		},
	}
	if len(got) != len(want) {
		t.Fatalf("NonPeers returned %d entries, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Errorf("NonPeers[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestAdapterCardAddresses checks that an entry's addresses are returned by
// fingerprint and a missing entry yields nil.
func TestAdapterCardAddresses(t *testing.T) {
	addresses := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.9:17443"},
	}
	acceptor := adapterFP(1)
	d := &discoveryDir{brokers: []directory.KnownBroker{
		{Fingerprint: adapterFP(2), Addresses: []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.2:17443"}}},
		{Fingerprint: acceptor, Addresses: addresses, IsPeer: true, Role: pairing.RoleAcceptor},
	}}
	ad := newDirectoryAdapter(d)

	got := ad.CardAddresses(acceptor)
	if len(got) != 1 || got[0] != addresses[0] {
		t.Fatalf("CardAddresses(acceptor) = %v, want %v", got, addresses)
	}
	if got := ad.CardAddresses(adapterFP(99)); got != nil {
		t.Fatalf("CardAddresses(unknown) = %v, want nil", got)
	}
}

// TestAdapterPublicTopicsConverts checks that directory public topics are
// converted field for field.
func TestAdapterPublicTopicsConverts(t *testing.T) {
	publisher := directory.Fingerprint{0xde, 0xad, 0xbe, 0xef}
	d := &discoveryDir{topics: []directory.PublicTopic{
		{Name: "infra-help", Description: "ask infra", Publisher: publisher, PublisherAlias: "alpha", Declarers: 3},
		{Name: "quiet", Description: ""},
	}}
	ad := newDirectoryAdapter(d)

	got := ad.PublicTopics()
	want := []PublicTopic{
		{Name: "infra-help", Description: "ask infra", Publisher: publisher, PublisherAlias: "alpha", Declarers: 3},
		{Name: "quiet", Description: ""},
	}
	if len(got) != len(want) {
		t.Fatalf("PublicTopics returned %d topics, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("PublicTopics[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestAdapterConnectWarnings checks that a card at a presented address with a
// different fingerprint produces a mismatch warning.
func TestAdapterConnectWarnings(t *testing.T) {
	dialed := adapterFP(1)
	cardOwner := adapterFP(2)
	peer := pairing.Peer{
		Fingerprint: dialed,
		Candidates: []pairing.Candidate{
			{Source: pairing.SourcePairing, Addr: "10.0.0.1:17443"},
		},
	}
	d := &discoveryDir{cards: map[string]pairing.Fingerprint{
		"10.0.0.1:17443": cardOwner,
	}}
	ad := newDirectoryAdapter(d)

	got := ad.ConnectWarnings(peer)
	if len(got) != 1 {
		t.Fatalf("ConnectWarnings returned %d warnings, want 1: %v", len(got), got)
	}
	if got[0].Kind != localpb.WarningKind_WARNING_KIND_FINGERPRINT_MISMATCH {
		t.Errorf("warning kind = %v, want WARNING_KIND_FINGERPRINT_MISMATCH", got[0].Kind)
	}
	for _, want := range []string{"10.0.0.1:17443", cardOwner.String(), dialed.String()} {
		if !strings.Contains(got[0].Message, want) {
			t.Errorf("message %q does not mention %q", got[0].Message, want)
		}
	}
}

// TestAdapterInstallFillsHooks checks that install fills Directory, Catalog,
// Warner and CardIssuer with the adapter.
func TestAdapterInstallFillsHooks(t *testing.T) {
	ad := newDirectoryAdapter(&discoveryDir{})
	deps := &Deps{}

	ad.install(deps)

	if deps.Directory != ad {
		t.Errorf("Deps.Directory = %v, want the adapter", deps.Directory)
	}
	if deps.Catalog != ad {
		t.Errorf("Deps.Catalog = %v, want the adapter", deps.Catalog)
	}
	if deps.Warner != ad {
		t.Errorf("Deps.Warner = %v, want the adapter", deps.Warner)
	}
	if deps.CardIssuer != ad {
		t.Errorf("Deps.CardIssuer = %v, want the adapter", deps.CardIssuer)
	}
}

// TestAdapterReissueForwards covers the Reissue half of the card-issuer hook.
func TestAdapterReissueForwards(t *testing.T) {
	d := &discoveryDir{}
	ad := newDirectoryAdapter(d)

	ad.Reissue()

	calls := d.recordedCalls()
	if len(calls) != 1 || calls[0] != "Reissue" {
		t.Fatalf("calls after Reissue = %v, want [Reissue]", calls)
	}
}
