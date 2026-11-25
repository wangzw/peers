package link

import (
	"bytes"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link/linkpb"
)

// TestHelloRoundTrip checks that Hello encodes and decodes with its version,
// alias, nonce and addresses.
func TestHelloRoundTrip(t *testing.T) {
	nonce := bytes.Repeat([]byte{0xab}, nonceSize)
	addrs := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.1:17443"},
		{Kind: "relay", Addr: "relay.example:17443"},
	}

	f := encodeHello(7, "alice", nonce, addrs)
	if f.GetKind() != linkpb.KindHello {
		t.Fatalf("kind = %d, want %d", f.GetKind(), linkpb.KindHello)
	}
	if f.GetRequestId() != 0 {
		t.Fatalf("request id = %d, want 0: a state frame must not carry a request id", f.GetRequestId())
	}
	if f.GetIsReply() {
		t.Fatalf("is_reply = true, want false: hello is not a reply")
	}

	hello, err := decodeHello(f)
	if err != nil {
		t.Fatalf("decodeHello: %v", err)
	}
	want := &linkpb.Hello{
		ProtocolVersion:     7,
		Alias:               "alice",
		Nonce:               nonce,
		AdvertisedAddresses: addrsToPB(addrs),
	}
	if !proto.Equal(hello, want) {
		t.Fatalf("decoded hello = %v, want %v", hello, want)
	}
}

// TestHelloRoundTripWithoutAddresses checks that a nil and an empty address
// list encode as an empty list.
func TestHelloRoundTripWithoutAddresses(t *testing.T) {
	for _, addrs := range [][]config.AdvertisedAddr{nil, {}} {
		f := encodeHello(1, "bob", bytes.Repeat([]byte{1}, nonceSize), addrs)
		hello, err := decodeHello(f)
		if err != nil {
			t.Fatalf("decodeHello: %v", err)
		}
		if len(hello.GetAdvertisedAddresses()) != 0 {
			t.Fatalf("addresses = %v, want empty", hello.GetAdvertisedAddresses())
		}
	}
}

// TestDecodeHelloRejectsBadNonce checks that a nonce whose length is not 32
// bytes is malformed.
func TestDecodeHelloRejectsBadNonce(t *testing.T) {
	for _, n := range []int{31, 33} {
		f := encodeHello(1, "alice", bytes.Repeat([]byte{1}, n), nil)
		if _, err := decodeHello(f); !errors.Is(err, errMalformed) {
			t.Fatalf("decodeHello with %d-byte nonce: err = %v, want errMalformed", n, err)
		}
	}
}

// TestDecodeRejectsWrongKind checks that decoding a frame with the wrong kind
// is malformed.
func TestDecodeRejectsWrongKind(t *testing.T) {
	f := encodePeerInfo("alice", nil)
	if _, err := decodeHello(f); !errors.Is(err, errMalformed) {
		t.Fatalf("decodeHello on PeerInfo frame: err = %v, want errMalformed", err)
	}
	if _, err := decodePeerInfo(encodeHello(1, "a", bytes.Repeat([]byte{1}, nonceSize), nil)); !errors.Is(err, errMalformed) {
		t.Fatalf("decodePeerInfo on Hello frame: err = %v, want errMalformed", err)
	}
}

// TestDecodeIgnoresUnknownFields checks that an unknown protobuf field in the
// payload is ignored.
func TestDecodeIgnoresUnknownFields(t *testing.T) {
	f := encodeHello(1, "alice", bytes.Repeat([]byte{1}, nonceSize), nil)
	// Field 99, wire type 0, value 1 encodes as 0x98 0x06 0x01; a later version
	// may add fields and the decoder must ignore them.
	f.Payload = append(f.Payload, 0x98, 0x06, 0x01)

	hello, err := decodeHello(f)
	if err != nil {
		t.Fatalf("decodeHello with unknown field: %v", err)
	}
	if hello.GetAlias() != "alice" {
		t.Fatalf("alias = %q, want alice", hello.GetAlias())
	}
}

// TestAddrsFromPBKeepsUnknownKinds checks that the conversion keeps unknown
// kinds and order and skips nil elements.
func TestAddrsFromPBKeepsUnknownKinds(t *testing.T) {
	in := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "a"},
		{Kind: "relay", Addr: "x"},
		{Kind: config.AdvertisedKindDirect, Addr: "b"},
	}
	got := addrsFromPB(addrsToPB(in))
	if len(got) != len(in) {
		t.Fatalf("addresses = %v, want %v", got, in)
	}
	for i := range in {
		if got[i] != in[i] {
			t.Fatalf("address[%d] = %v, want %v", i, got[i], in[i])
		}
	}

	if got := addrsFromPB([]*linkpb.AdvertisedAddress{nil, {Kind: "direct", Addr: "c"}}); len(got) != 1 || got[0].Addr != "c" {
		t.Fatalf("nil element not skipped: got %v", got)
	}
}

// TestUnsupportedReply checks that the unsupported reply carries the request id
// and the refused kind.
func TestUnsupportedReply(t *testing.T) {
	req := &linkpb.Frame{Kind: 777, RequestId: 9}
	reply := unsupportedReply(req)

	if reply.GetKind() != linkpb.KindUnsupported {
		t.Fatalf("kind = %d, want %d", reply.GetKind(), linkpb.KindUnsupported)
	}
	if reply.GetRequestId() != 9 {
		t.Fatalf("request id = %d, want 9: the reply must match the request", reply.GetRequestId())
	}
	if !reply.GetIsReply() {
		t.Fatalf("is_reply = false, want true")
	}
	kind, err := decodeUnsupported(reply)
	if err != nil {
		t.Fatalf("decodeUnsupported: %v", err)
	}
	if kind != 777 {
		t.Fatalf("request kind = %d, want 777", kind)
	}
}

// TestDecodeRejectRejectsUnknownReason checks that an unspecified or unknown
// reject reason is malformed.
func TestDecodeRejectRejectsUnknownReason(t *testing.T) {
	for name, reason := range map[string]linkpb.RejectReason{
		"unspecified": linkpb.RejectReason_REJECT_REASON_UNSPECIFIED,
		"unknown":     linkpb.RejectReason(7),
	} {
		f := encodeReject(reason)
		if _, err := decodeReject(f); !errors.Is(err, errMalformed) {
			t.Fatalf("%s reason: err = %v, want errMalformed", name, err)
		}
	}

	reason := linkpb.RejectReason_REJECT_REASON_NOT_A_PEER
	got, err := decodeReject(encodeReject(reason))
	if err != nil {
		t.Fatalf("decodeReject: %v", err)
	}
	if got != reason {
		t.Fatalf("reason = %v, want %v", got, reason)
	}
}

// TestClassify checks that classify separates state, request, reply and invalid
// frames.
func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		f    *linkpb.Frame
		want frameClass
	}{
		{"nil", nil, classInvalid},
		{"state", &linkpb.Frame{Kind: 200, RequestId: 0, IsReply: false}, classState},
		{"request", &linkpb.Frame{Kind: 200, RequestId: 5, IsReply: false}, classRequest},
		{"reply", &linkpb.Frame{Kind: 200, RequestId: 5, IsReply: true}, classReply},
		{"reply without request id", &linkpb.Frame{Kind: 200, RequestId: 0, IsReply: true}, classInvalid},
	}
	for _, tt := range tests {
		if got := classify(tt.f); got != tt.want {
			t.Fatalf("%s: classify = %v, want %v", tt.name, got, tt.want)
		}
	}
}
