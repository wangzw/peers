package directory

import (
	"context"

	"github.com/wangzw/peers/internal/link"
)

// linkAPI is the part of *link.Link that the adapter uses.
type linkAPI interface {
	Send(peer link.Fingerprint, f link.Frame) error
	Request(ctx context.Context, peer link.Fingerprint, f link.Frame) (reply link.Frame, sent bool, err error)
	HandleState(kind link.FrameKind, h func(peer link.Fingerprint, f link.Frame))
	HandleRequest(kind link.FrameKind, h func(ctx context.Context, peer link.Fingerprint, f link.Frame) link.Frame)
	Subscribe(onUp func(peer link.Fingerprint, salt []byte), onDown func(peer link.Fingerprint))
	State(peer link.Fingerprint) link.LinkState
}

// linkTransport implements Links on top of the link package, converting the
// directory's kind-and-payload frames to and from link.Frame.
type linkTransport struct {
	api linkAPI
}

var _ Links = (*linkTransport)(nil)

func newLinkTransport(api linkAPI) Links {
	return &linkTransport{api: api}
}

func (t *linkTransport) Send(peer Fingerprint, kind uint32, payload []byte) error {
	return t.api.Send(peer, link.Frame{Kind: kind, Payload: payload})
}

func (t *linkTransport) Request(ctx context.Context, peer Fingerprint, kind uint32, payload []byte) (replyKind uint32, reply []byte, sent bool, err error) {
	f, sent, err := t.api.Request(ctx, peer, link.Frame{Kind: kind, Payload: payload})
	if err != nil {
		return 0, nil, sent, err
	}
	return f.Kind, f.Payload, sent, nil
}

func (t *linkTransport) HandleState(kind uint32, h func(peer Fingerprint, payload []byte)) {
	t.api.HandleState(kind, func(peer link.Fingerprint, f link.Frame) {
		h(peer, f.Payload)
	})
}

func (t *linkTransport) HandleRequest(kind uint32, h func(ctx context.Context, peer Fingerprint, payload []byte) (replyKind uint32, reply []byte)) {
	t.api.HandleRequest(kind, func(ctx context.Context, peer link.Fingerprint, f link.Frame) link.Frame {
		replyKind, reply := h(ctx, peer, f.Payload)
		return link.Frame{Kind: replyKind, Payload: reply}
	})
}

func (t *linkTransport) Subscribe(onUp func(peer Fingerprint, salt []byte), onDown func(peer Fingerprint)) {
	t.api.Subscribe(onUp, onDown)
}

func (t *linkTransport) Status(peer Fingerprint) LinkView {
	return mapLinkState(t.api.State(peer))
}

func mapLinkState(st link.LinkState) LinkView {
	v := LinkView{PeerVersion: st.PeerVersion}
	switch st.Phase {
	case link.PhaseNone:
		v.Phase = LinkNone
	case link.PhaseDialing:
		v.Phase = LinkDialing
	case link.PhaseWaiting:
		v.Phase = LinkWaiting
	case link.PhaseHandshaking:
		v.Phase = LinkHandshaking
	case link.PhaseEstablished:
		v.Phase = LinkEstablished
	case link.PhaseBackoff:
		v.Phase = LinkBackoff
	case link.PhaseRejected:
		v.Phase = LinkRejected
	default:
		v.Phase = LinkNone
	}
	switch st.LastFailure {
	case link.FailNotAPeer:
		v.Reason = FailureNotAPeer
	case link.FailRoleMismatch:
		v.Reason = FailureRoleMismatch
	case link.FailVersionIncompatible:
		v.Reason = FailureVersionIncompatible
	default:
		v.Reason = FailureNone
	}
	return v
}
