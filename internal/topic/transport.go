package topic

import (
	"github.com/wangzw/peers/internal/link"
	"google.golang.org/protobuf/proto"
)

// Transport is the part of link that topic uses; broker passes its link instance.
type Transport interface {
	// SendLatest queues f for peer, replacing a queued frame of the same kind.
	SendLatest(peer link.Fingerprint, f link.Frame) error
	// SendLatestKey queues f like SendLatest, keyed by a caller-chosen string,
	// and reports whether a queued frame with that key was replaced in place.
	SendLatestKey(peer link.Fingerprint, key string, f link.Frame) (replaced bool, err error)
	// HandleState registers h to receive frames of kind.
	HandleState(kind link.FrameKind, h func(peer link.Fingerprint, f link.Frame))
	// Subscribe registers onUp and onDown for link establishment and teardown.
	Subscribe(onUp func(peer link.Fingerprint, salt []byte), onDown func(peer link.Fingerprint))
}

func encodeFrame(kind uint32, m proto.Message) (link.Frame, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return link.Frame{}, err
	}
	return link.Frame{Kind: link.FrameKind(kind), Payload: b}, nil
}

func decodeFrame(f link.Frame, m proto.Message) error {
	return proto.Unmarshal(f.Payload, m)
}

func frameKind(f link.Frame) uint32 { return uint32(f.Kind) }
