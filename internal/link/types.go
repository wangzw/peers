package link

import (
	"errors"
	"fmt"
	"time"

	"github.com/wangzw/peers/internal/pairing"
)

// Fingerprint identifies a peer by the SHA-256 digest of its certificate's
// SubjectPublicKeyInfo.
type Fingerprint = pairing.Fingerprint

// FrameKind is the kind field of a frame. Kinds below 200 are reserved for the
// link and pairing protocols; upper layers register their own kinds.
type FrameKind = uint32

// Frame is one frame exchanged with a peer. The request id and the reply flag
// are transport details and are not exposed to upper layers.
type Frame struct {
	Kind    FrameKind
	Payload []byte
}

// Phase is the phase of the link to one peer.
type Phase uint8

const (
	PhaseNone Phase = iota
	PhaseDialing
	PhaseWaiting
	PhaseHandshaking
	PhaseEstablished
	PhaseBackoff
	PhaseRejected
)

// FailReason is the reason a link attempt or an established link failed.
type FailReason uint8

const (
	FailNone FailReason = iota
	FailUnreachable
	FailRoleMismatch
	FailVersionIncompatible
	FailNotAPeer
	FailQueueFull
	FailConnectionLost
	FailProtocol
	FailInboundLimit
)

// LinkState is the state of the link to one peer.
type LinkState struct {
	Phase       Phase
	Role        pairing.Role
	LastFailure FailReason // most recent failure; reset to FailNone once established
	PeerVersion uint32     // peer's version when LastFailure is FailVersionIncompatible, 0 if unknown
	NextDial    time.Time  // PhaseBackoff only
}

var (
	// ErrNotEstablished reports that no link to the peer is established. The
	// broker maps it to peer_unreachable.
	ErrNotEstablished = errors.New("no link to the peer is established")
	// ErrQueueFull reports that the bounded send queue is full; the link is
	// closed as a result.
	ErrQueueFull = fmt.Errorf("%w: send queue full", ErrNotEstablished)
	// ErrLinkDown reports that the link went down after the request was sent
	// and before the reply arrived.
	ErrLinkDown = errors.New("the link went down before the reply arrived")
	// ErrClosed reports use of a Link that has been closed.
	ErrClosed = errors.New("the link is closed")
	// ErrInvalidKind reports a frame kind reserved for link and pairing.
	ErrInvalidKind = errors.New("frame kind is reserved for link and pairing")
	// ErrFrameTooLarge reports a frame whose encoded form exceeds
	// MaxMessageSize. The frame is refused before it is queued, so the link
	// stays up.
	ErrFrameTooLarge = errors.New("frame is larger than the message limit")
)

// UnsupportedError reports that the peer does not support the request kind.
type UnsupportedError struct {
	Kind FrameKind
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("the peer does not support request kind %d", e.Kind)
}
