package topic

import (
	"github.com/wangzw/peers/internal/link"
	"google.golang.org/protobuf/proto"
)

// Transport is the part of link that topic uses. broker passes its link instance.
type Transport interface {
	SendLatest(peer link.Fingerprint, f link.Frame) error
	// SendLatestKey queues f like SendLatest, keyed by a caller-chosen string,
	// and reports whether a queued frame with that key was replaced in place.
	SendLatestKey(peer link.Fingerprint, key string, f link.Frame) (replaced bool, err error)
	HandleState(kind link.FrameKind, h func(peer link.Fingerprint, f link.Frame))
	Subscribe(onUp func(peer link.Fingerprint, salt []byte), onDown func(peer link.Fingerprint))
}

// encodeFrame marshals m into a frame of the given kind.
func encodeFrame(kind uint32, m proto.Message) (link.Frame, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return link.Frame{}, err
	}
	return link.Frame{Kind: link.FrameKind(kind), Payload: b}, nil
}

// decodeFrame unmarshals the payload of f into m.
func decodeFrame(f link.Frame, m proto.Message) error {
	return proto.Unmarshal(f.Payload, m)
}

// frameKind returns the kind of f as the uint32 the topic protocol uses.
func frameKind(f link.Frame) uint32 { return uint32(f.Kind) }
