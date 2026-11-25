package link

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link/linkpb"
)

// errMalformed reports a frame that does not match the link protocol; every
// decode error wraps it, so callers can tell a protocol violation from a
// transport failure.
var errMalformed = errors.New("malformed frame")

// nonceSize is the length of the Hello random value; both sides mix it into the
// salt, so a different length would produce different salts.
const nonceSize = 32

func encodeHello(version uint32, alias string, nonce []byte, addrs []config.AdvertisedAddr) *linkpb.Frame {
	return marshalFrame(linkpb.KindHello, &linkpb.Hello{
		ProtocolVersion:     version,
		Alias:               alias,
		Nonce:               nonce,
		AdvertisedAddresses: addrsToPB(addrs),
	})
}

func decodeHello(f *linkpb.Frame) (*linkpb.Hello, error) {
	msg := new(linkpb.Hello)
	if err := unmarshalFrame(f, linkpb.KindHello, msg); err != nil {
		return nil, err
	}
	if len(msg.Nonce) != nonceSize {
		return nil, fmt.Errorf("%w: hello nonce is %d bytes, want %d", errMalformed, len(msg.Nonce), nonceSize)
	}
	return msg, nil
}

func encodePeerInfo(alias string, addrs []config.AdvertisedAddr) *linkpb.Frame {
	return marshalFrame(linkpb.KindPeerInfo, &linkpb.PeerInfo{
		Alias:               alias,
		AdvertisedAddresses: addrsToPB(addrs),
	})
}

func decodePeerInfo(f *linkpb.Frame) (*linkpb.PeerInfo, error) {
	msg := new(linkpb.PeerInfo)
	if err := unmarshalFrame(f, linkpb.KindPeerInfo, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func encodeReject(reason linkpb.RejectReason) *linkpb.Frame {
	return marshalFrame(linkpb.KindReject, &linkpb.Reject{Reason: reason})
}

// decodeReject checks the kind and decodes the Reject payload; an unspecified
// or unknown reason is malformed, because the receiver cannot act on it.
func decodeReject(f *linkpb.Frame) (linkpb.RejectReason, error) {
	msg := new(linkpb.Reject)
	if err := unmarshalFrame(f, linkpb.KindReject, msg); err != nil {
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, err
	}
	reason := msg.GetReason()
	if reason == linkpb.RejectReason_REJECT_REASON_UNSPECIFIED {
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("%w: reject reason is unspecified", errMalformed)
	}
	if _, known := linkpb.RejectReason_name[int32(reason)]; !known {
		return linkpb.RejectReason_REJECT_REASON_UNSPECIFIED, fmt.Errorf("%w: unknown reject reason %d", errMalformed, reason)
	}
	return reason, nil
}

// unsupportedReply builds the reply to a request whose kind the receiver does
// not handle; it carries the request id so the requester can match it.
func unsupportedReply(req *linkpb.Frame) *linkpb.Frame {
	return &linkpb.Frame{
		Kind:      linkpb.KindUnsupported,
		RequestId: req.GetRequestId(),
		IsReply:   true,
		Payload:   marshalPayload(&linkpb.Unsupported{RequestKind: req.GetKind()}),
	}
}

func decodeUnsupported(f *linkpb.Frame) (uint32, error) {
	msg := new(linkpb.Unsupported)
	if err := unmarshalFrame(f, linkpb.KindUnsupported, msg); err != nil {
		return 0, err
	}
	return msg.GetRequestKind(), nil
}

func addrsToPB(addrs []config.AdvertisedAddr) []*linkpb.AdvertisedAddress {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]*linkpb.AdvertisedAddress, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, &linkpb.AdvertisedAddress{Kind: a.Kind, Addr: a.Addr})
	}
	return out
}

// addrsFromPB converts the wire form back to the advertised address list; it
// preserves order and keeps unknown kinds untouched, because filtering them is
// the peer table's job. Nil elements are skipped.
func addrsFromPB(addrs []*linkpb.AdvertisedAddress) []config.AdvertisedAddr {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]config.AdvertisedAddr, 0, len(addrs))
	for _, a := range addrs {
		if a == nil {
			continue
		}
		out = append(out, config.AdvertisedAddr{Kind: a.GetKind(), Addr: a.GetAddr()})
	}
	return out
}

type frameClass uint8

const (
	classInvalid frameClass = iota
	classState
	classRequest
	classReply
)

// classify decides whether a frame is a state update, a request or a reply; a
// reply flag without a request id matches nothing and is invalid.
func classify(f *linkpb.Frame) frameClass {
	if f == nil {
		return classInvalid
	}
	switch {
	case !f.GetIsReply() && f.GetRequestId() == 0:
		return classState
	case !f.GetIsReply():
		return classRequest
	case f.GetRequestId() != 0:
		return classReply
	default:
		return classInvalid
	}
}

func marshalFrame(kind uint32, msg proto.Message) *linkpb.Frame {
	return &linkpb.Frame{
		Kind:    kind,
		Payload: marshalPayload(msg),
	}
}

func unmarshalFrame(f *linkpb.Frame, kind uint32, msg proto.Message) error {
	if f == nil {
		return fmt.Errorf("%w: nil frame", errMalformed)
	}
	if f.GetKind() != kind {
		return fmt.Errorf("%w: frame kind %d, want %d", errMalformed, f.GetKind(), kind)
	}
	if err := proto.Unmarshal(f.GetPayload(), msg); err != nil {
		return fmt.Errorf("%w: %v", errMalformed, err)
	}
	return nil
}

// marshalPayload encodes a generated message. Encoding only fails on invalid
// UTF-8 in a string field, and the values here come from validated
// configuration, so the error is not actionable at this layer.
func marshalPayload(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil
	}
	return b
}
