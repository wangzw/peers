package link

import (
	"context"
	"fmt"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
)

const reservedKindLimit FrameKind = 200

// HandleState registers the handler of a state frame kind. State frames of one
// link are handed to it one at a time, in arrival order. It panics on a
// reserved kind, a nil handler, or a kind already registered for either class.
func (l *Link) HandleState(kind FrameKind, h func(peer Fingerprint, f Frame)) {
	if h == nil {
		panic(fmt.Sprintf("link: nil state handler for frame kind %d", kind))
	}
	l.registerHandler(kind, h, nil)
}

// HandleRequest registers the handler of a request frame kind. Each request
// runs in its own goroutine and its returned frame is sent back as the reply;
// at most 64 requests of one peer run at once, shared by its connections. A
// handler must not wait for a reply on the same link, because the router stops
// reading while every slot is taken and the reply could never arrive. It panics
// on a reserved kind, a nil handler, or a kind already registered.
func (l *Link) HandleRequest(kind FrameKind, h func(ctx context.Context, peer Fingerprint, f Frame) Frame) {
	if h == nil {
		panic(fmt.Sprintf("link: nil request handler for frame kind %d", kind))
	}
	l.registerHandler(kind, nil, h)
}

func (l *Link) registerHandler(kind FrameKind, state func(Fingerprint, Frame), request func(context.Context, Fingerprint, Frame) Frame) {
	if kind < reservedKindLimit {
		panic(fmt.Sprintf("link: frame kind %d is reserved for link and pairing", kind))
	}
	l.regMu.Lock()
	defer l.regMu.Unlock()
	if _, ok := l.stateHandlers[kind]; ok {
		panic(fmt.Sprintf("link: frame kind %d is already registered", kind))
	}
	if _, ok := l.requestHandlers[kind]; ok {
		panic(fmt.Sprintf("link: frame kind %d is already registered", kind))
	}
	if state != nil {
		l.stateHandlers[kind] = state
	} else {
		l.requestHandlers[kind] = request
	}
}

// Send queues a state frame for peer. It returns immediately with
// ErrInvalidKind for a reserved kind, ErrNotEstablished when no link is up,
// ErrQueueFull when the send queue is full, and ErrFrameTooLarge when the
// encoded frame is above MaxMessageSize. An oversized frame is not queued and
// the link stays up.
func (l *Link) Send(peer Fingerprint, f Frame) error {
	wire, err := wireFrame(f)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.establishedConn(peer)
	if c == nil {
		return ErrNotEstablished
	}
	return c.enqueue(wire)
}

// SendLatest queues a state frame like Send, except that a frame of the same
// kind queued by SendLatest and not yet taken by the writer is replaced in
// place, keeping its queue position. A replacement that arrives after the
// writer took the frame is queued as a new frame.
func (l *Link) SendLatest(peer Fingerprint, f Frame) error {
	_, err := l.sendLatestKey(peer, kindKey(f.Kind), f)
	return err
}

// SendLatestKey is SendLatest keyed by the caller-chosen key instead of the
// frame kind; replaced reports whether a queued frame was replaced. Frames of
// different kinds that share a key replace each other.
func (l *Link) SendLatestKey(peer Fingerprint, key string, f Frame) (replaced bool, err error) {
	return l.sendLatestKey(peer, key, f)
}

func (l *Link) sendLatestKey(peer Fingerprint, key string, f Frame) (replaced bool, err error) {
	wire, err := wireFrame(f)
	if err != nil {
		return false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.establishedConn(peer)
	if c == nil {
		return false, ErrNotEstablished
	}
	return c.enqueueLatest(key, wire)
}

func wireFrame(f Frame) (*linkpb.Frame, error) {
	if f.Kind < reservedKindLimit {
		return nil, ErrInvalidKind
	}
	wire := &linkpb.Frame{Kind: f.Kind, Payload: f.Payload}
	if frameExceedsLimit(wire) {
		return nil, ErrFrameTooLarge
	}
	return wire, nil
}

// kindKey is the latest-slot key SendLatest uses for one kind, disjoint from
// the keys an upper layer passes to SendLatestKey.
func kindKey(kind FrameKind) string { return fmt.Sprintf("k:%d", kind) }

// Request sends a request frame and waits for the peer's reply under the same
// request id, until ctx is done or the link goes down. sent reports whether the
// frame reached the transport; false means it was still queued when the call
// gave up. It returns ErrInvalidKind for a reserved kind, ErrNotEstablished
// when no link is up, ErrQueueFull when the send queue is full,
// ErrFrameTooLarge when the frame exceeds MaxMessageSize, ErrLinkDown when the
// link goes down after the frame was written, ctx's error when ctx ends first,
// and *UnsupportedError when the peer does not support the kind.
func (l *Link) Request(ctx context.Context, peer Fingerprint, f Frame) (reply Frame, sent bool, err error) {
	if f.Kind < reservedKindLimit {
		return Frame{}, false, ErrInvalidKind
	}
	wire := &linkpb.Frame{Kind: f.Kind, Payload: f.Payload}

	l.mu.Lock()
	c := l.establishedConn(peer)
	var p *pendingReq
	if c != nil {
		p, err = c.enqueueRequest(wire)
	}
	l.mu.Unlock()

	if c == nil {
		return Frame{}, false, ErrNotEstablished
	}
	if err != nil {
		return Frame{}, false, err
	}

	select {
	case rf := <-p.reply:
		return replyFromFrame(rf)
	case <-ctx.Done():
		return Frame{}, c.cancelRequest(p), ctx.Err()
	case <-c.done():
		// A reply may have arrived just before the connection ended; prefer it
		// over reporting the link as down.
		select {
		case rf := <-p.reply:
			return replyFromFrame(rf)
		default:
		}
		if p.state.Load() == reqWritten {
			return Frame{}, true, ErrLinkDown
		}
		if sent := c.cancelRequest(p); sent {
			return Frame{}, true, ErrLinkDown
		}
		return Frame{}, false, fmt.Errorf("%w: link went down", ErrNotEstablished)
	}
}

// establishedConn returns the connection of peer when its link is established;
// the caller must hold l.mu.
func (l *Link) establishedConn(peer Fingerprint) *conn {
	rec := l.records[peer]
	if rec == nil || rec.phase != PhaseEstablished || rec.conn == nil {
		return nil
	}
	return rec.conn
}

func replyFromFrame(f *linkpb.Frame) (Frame, bool, error) {
	if f.GetKind() == linkpb.KindUnsupported {
		kind, err := decodeUnsupported(f)
		if err != nil {
			return Frame{}, true, err
		}
		return Frame{}, true, &UnsupportedError{Kind: kind}
	}
	return Frame{Kind: f.GetKind(), Payload: f.GetPayload()}, true, nil
}

// AnnounceAlias sends the current alias in a PeerInfo on every established
// link. A dialer side reports no addresses.
func (l *Link) AnnounceAlias() {
	alias := l.settings.Alias()
	addrs := l.settings.Advertised()
	l.announcePeerInfo(alias, addrs, true)
}

// AnnounceAddresses sends the current advertised address list in a PeerInfo on
// every established link on which this side is the acceptor; a dialer side
// never reports its addresses.
func (l *Link) AnnounceAddresses() {
	alias := l.settings.Alias()
	addrs := l.settings.Advertised()
	l.announcePeerInfo(alias, addrs, false)
}

func (l *Link) announcePeerInfo(alias string, addrs []config.AdvertisedAddr, withDialer bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for fp, rec := range l.records {
		if rec.conn == nil || rec.phase != PhaseEstablished {
			continue
		}
		if rec.role != pairing.RoleAcceptor && !withDialer {
			continue
		}
		list := addrs
		if rec.role != pairing.RoleAcceptor {
			list = nil
		}
		if err := rec.conn.enqueue(encodePeerInfo(alias, list)); err != nil {
			l.logger.Debug("link: dropping a PeerInfo announcement",
				"peer", fp.String(), "err", err)
		}
	}
}
