package directory

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/wangzw/peers/internal/link"
)

type fakeLinkAPI struct {
	sentPeer  link.Fingerprint
	sentFrame link.Frame
	sendErr   error

	requestCtx   context.Context
	requestPeer  link.Fingerprint
	requestFrame link.Frame
	requestReply link.Frame
	requestSent  bool
	requestErr   error

	stateKind    link.FrameKind
	stateHandler func(link.Fingerprint, link.Frame)

	requestKind    link.FrameKind
	requestHandler func(context.Context, link.Fingerprint, link.Frame) link.Frame

	subscribedUp   func(link.Fingerprint, []byte)
	subscribedDown func(link.Fingerprint)

	statePeer  link.Fingerprint
	stateValue link.LinkState
}

var _ linkAPI = (*fakeLinkAPI)(nil)

func (f *fakeLinkAPI) Send(peer link.Fingerprint, fr link.Frame) error {
	f.sentPeer, f.sentFrame = peer, fr
	return f.sendErr
}

func (f *fakeLinkAPI) Request(ctx context.Context, peer link.Fingerprint, fr link.Frame) (link.Frame, bool, error) {
	f.requestCtx, f.requestPeer, f.requestFrame = ctx, peer, fr
	return f.requestReply, f.requestSent, f.requestErr
}

func (f *fakeLinkAPI) HandleState(kind link.FrameKind, h func(link.Fingerprint, link.Frame)) {
	f.stateKind, f.stateHandler = kind, h
}

func (f *fakeLinkAPI) HandleRequest(kind link.FrameKind, h func(context.Context, link.Fingerprint, link.Frame) link.Frame) {
	f.requestKind, f.requestHandler = kind, h
}

func (f *fakeLinkAPI) Subscribe(onUp func(link.Fingerprint, []byte), onDown func(link.Fingerprint)) {
	f.subscribedUp, f.subscribedDown = onUp, onDown
}

func (f *fakeLinkAPI) State(peer link.Fingerprint) link.LinkState {
	f.statePeer = peer
	return f.stateValue
}

// TestMapLinkState checks that every link phase and failure reason maps to the
// directory's view.
func TestMapLinkState(t *testing.T) {
	phases := []struct {
		in   link.Phase
		want LinkPhase
	}{
		{link.PhaseNone, LinkNone},
		{link.PhaseDialing, LinkDialing},
		{link.PhaseWaiting, LinkWaiting},
		{link.PhaseHandshaking, LinkHandshaking},
		{link.PhaseEstablished, LinkEstablished},
		{link.PhaseBackoff, LinkBackoff},
		{link.PhaseRejected, LinkRejected},
	}
	for _, tc := range phases {
		got := mapLinkState(link.LinkState{Phase: tc.in, PeerVersion: 2})
		if got.Phase != tc.want {
			t.Errorf("mapLinkState(Phase %d).Phase = %d, want %d", uint8(tc.in), uint8(got.Phase), uint8(tc.want))
		}
		if got.Reason != FailureNone {
			t.Errorf("mapLinkState(Phase %d).Reason = %d, want FailureNone", uint8(tc.in), uint8(got.Reason))
		}
		if got.PeerVersion != 2 {
			t.Errorf("mapLinkState(Phase %d).PeerVersion = %d, want 2", uint8(tc.in), got.PeerVersion)
		}
	}

	failures := []struct {
		in   link.FailReason
		want LinkFailure
	}{
		{link.FailNone, FailureNone},
		{link.FailUnreachable, FailureNone},
		{link.FailRoleMismatch, FailureRoleMismatch},
		{link.FailVersionIncompatible, FailureVersionIncompatible},
		{link.FailNotAPeer, FailureNotAPeer},
		{link.FailQueueFull, FailureNone},
		{link.FailConnectionLost, FailureNone},
		{link.FailProtocol, FailureNone},
	}
	for _, tc := range failures {
		got := mapLinkState(link.LinkState{Phase: link.PhaseBackoff, LastFailure: tc.in, PeerVersion: 2})
		if got.Reason != tc.want {
			t.Errorf("mapLinkState(FailReason %d).Reason = %d, want %d", uint8(tc.in), uint8(got.Reason), uint8(tc.want))
		}
		if got.Phase != LinkBackoff {
			t.Errorf("mapLinkState(FailReason %d).Phase = %d, want LinkBackoff", uint8(tc.in), uint8(got.Phase))
		}
		if got.PeerVersion != 2 {
			t.Errorf("mapLinkState(FailReason %d).PeerVersion = %d, want 2", uint8(tc.in), got.PeerVersion)
		}
	}

	if got := mapLinkState(link.LinkState{Phase: link.Phase(200)}); got.Phase != LinkNone {
		t.Errorf("mapLinkState(unknown phase).Phase = %d, want LinkNone", uint8(got.Phase))
	}
}

// TestAdapterFramesUseLinkFrame checks that the adapter converts frames to and
// from link.Frame in each direction.
func TestAdapterFramesUseLinkFrame(t *testing.T) {
	fake := &fakeLinkAPI{}
	links := newLinkTransport(fake)
	peer := Fingerprint{1, 2, 3}
	ctx := context.Background()

	payload := []byte{0xde, 0xad}
	if err := links.Send(peer, 300, payload); err != nil {
		t.Fatalf("Send returned %v", err)
	}
	if fake.sentPeer != peer {
		t.Errorf("link Send peer = %v, want %v", fake.sentPeer, peer)
	}
	if fake.sentFrame.Kind != 300 || !bytes.Equal(fake.sentFrame.Payload, payload) {
		t.Errorf("link Send frame = %+v, want kind 300 with payload %v", fake.sentFrame, payload)
	}

	sendErr := errors.New("send failed")
	fake.sendErr = sendErr
	if err := links.Send(peer, 300, payload); !errors.Is(err, sendErr) {
		t.Fatalf("Send error = %v, want %v", err, sendErr)
	}
	fake.sendErr = nil

	fake.requestSent = true
	fake.requestReply = link.Frame{Kind: 302, Payload: []byte{0xbe, 0xef}}
	reqPayload := []byte{0x01}
	kind, reply, sent, err := links.Request(ctx, peer, 301, reqPayload)
	if err != nil || !sent {
		t.Fatalf("Request returned sent=%v err=%v, want sent=true err=nil", sent, err)
	}
	if fake.requestCtx != ctx || fake.requestPeer != peer {
		t.Errorf("link Request called with ctx=%v peer=%v, want the same ctx and peer %v", fake.requestCtx, fake.requestPeer, peer)
	}
	if fake.requestFrame.Kind != 301 || !bytes.Equal(fake.requestFrame.Payload, reqPayload) {
		t.Errorf("link Request frame = %+v, want kind 301 with payload %v", fake.requestFrame, reqPayload)
	}
	if kind != 302 || !bytes.Equal(reply, []byte{0xbe, 0xef}) {
		t.Errorf("Request returned (%d, %v), want (302, [be ef])", kind, reply)
	}

	var gotPeer Fingerprint
	var gotPayload []byte
	links.HandleState(300, func(p Fingerprint, pl []byte) {
		gotPeer, gotPayload = p, pl
	})
	if fake.stateKind != 300 || fake.stateHandler == nil {
		t.Fatalf("link HandleState registered kind %d with handler registered=%v, want kind 300 with a handler", fake.stateKind, fake.stateHandler != nil)
	}
	statePayload := []byte{0xaa}
	fake.stateHandler(peer, link.Frame{Kind: 300, Payload: statePayload})
	if gotPeer != peer || !bytes.Equal(gotPayload, statePayload) {
		t.Errorf("state handler got peer=%v payload=%v, want peer %v payload %v", gotPeer, gotPayload, peer, statePayload)
	}

	links.HandleRequest(301, func(ctx context.Context, p Fingerprint, pl []byte) (uint32, []byte) {
		if p != peer || !bytes.Equal(pl, reqPayload) {
			t.Errorf("request handler got peer=%v payload=%v, want peer %v payload %v", p, pl, peer, reqPayload)
		}
		return 302, []byte{0xbe, 0xef}
	})
	if fake.requestKind != 301 || fake.requestHandler == nil {
		t.Fatalf("link HandleRequest registered kind %d with handler registered=%v, want kind 301 with a handler", fake.requestKind, fake.requestHandler != nil)
	}
	got := fake.requestHandler(ctx, peer, link.Frame{Kind: 301, Payload: reqPayload})
	if got.Kind != 302 || !bytes.Equal(got.Payload, []byte{0xbe, 0xef}) {
		t.Errorf("request handler reply = %+v, want kind 302 with payload [be ef]", got)
	}
}

// TestAdapterPassesUnsupportedError checks that an unsupported request error is
// returned as *link.UnsupportedError.
func TestAdapterPassesUnsupportedError(t *testing.T) {
	fake := &fakeLinkAPI{
		requestSent: true,
		requestErr:  &link.UnsupportedError{Kind: 301},
	}
	links := newLinkTransport(fake)

	kind, reply, sent, err := links.Request(context.Background(), Fingerprint{9}, 301, []byte{1})
	if !sent {
		t.Errorf("Request sent = false, want true")
	}
	if kind != 0 || reply != nil {
		t.Errorf("Request returned kind=%d reply=%v, want 0 and nil on error", kind, reply)
	}
	var unsupported *link.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("Request error = %v, want *link.UnsupportedError", err)
	}
	if unsupported.Kind != 301 {
		t.Errorf("UnsupportedError.Kind = %d, want 301", unsupported.Kind)
	}
}

// TestAdapterStatusMapsState checks that Status maps the link state into a
// LinkView.
func TestAdapterStatusMapsState(t *testing.T) {
	fake := &fakeLinkAPI{stateValue: link.LinkState{
		Phase:       link.PhaseRejected,
		LastFailure: link.FailVersionIncompatible,
		PeerVersion: 3,
	}}
	links := newLinkTransport(fake)
	peer := Fingerprint{5}

	got := links.Status(peer)
	if fake.statePeer != peer {
		t.Errorf("link State called with peer %v, want %v", fake.statePeer, peer)
	}
	want := LinkView{Phase: LinkRejected, Reason: FailureVersionIncompatible, PeerVersion: 3}
	if got != want {
		t.Errorf("Status = %+v, want %+v", got, want)
	}
}

// TestAdapterSubscribeForwards checks that Subscribe forwards both callbacks.
func TestAdapterSubscribeForwards(t *testing.T) {
	fake := &fakeLinkAPI{}
	links := newLinkTransport(fake)

	var ups, downs int
	links.Subscribe(func(Fingerprint, []byte) { ups++ }, func(Fingerprint) { downs++ })
	if fake.subscribedUp == nil || fake.subscribedDown == nil {
		t.Fatal("Subscribe did not forward both callbacks")
	}
	fake.subscribedUp(Fingerprint{1}, []byte{2})
	fake.subscribedDown(Fingerprint{1})
	if ups != 1 || downs != 1 {
		t.Errorf("callbacks invoked %d up and %d down times, want 1 and 1", ups, downs)
	}
}
