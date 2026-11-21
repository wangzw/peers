package broker

import (
	"context"
	"log/slog"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

// peerID identifies a paired peer Broker.
type peerID = pairing.Fingerprint

// agentView is a read-only snapshot of one local Agent.
type agentView struct {
	ID        string // 64 lowercase hex chars, SHA-256 of "session:"+socket path
	Name      string // normalized session name
	Ref       string // current disambiguation ref, 6..12 lowercase hex, prefix of ID
	RefPinned bool   // Ref is 12 chars because Name belongs to another account
	// UID is the account that registered this Agent. It is 0 for a view built
	// without an account, which together with NameOwned keeps a by-name
	// resolution from treating such a view as the owner.
	UID uint32
	// NameOwnerUID is the account that owns Name, and NameOwned reports that
	// the name owner table has an entry for it. A bare name reaches only the
	// owner's Agents.
	NameOwnerUID uint32
	NameOwned    bool
	// PrevNameOwnerUID and PrevNameOwned do the same for PrevName.
	PrevNameOwnerUID uint32
	PrevNameOwned    bool
	// RefMandatory reports that the address and signature must carry a ref
	// because of how the name is held (see Broker.nameNeedsRefLocked).
	RefMandatory bool
	PrevName     string        // previous name inside the 10-minute window, else ""
	Session      inbox.Session // the record last read
	Inbox        inboxWriter   // the account's session backend; writes go through it
}

// localAgents is the local Agent table a delivery reads. *Broker implements it.
type localAgents interface {
	Lookup(id string) (agentView, bool)
	All() []agentView
	// Reread rereads the record, applies it to the table and returns the new
	// snapshot; errAgentGone (declared by broker-local) when the Agent is gone.
	Reread(id string) (agentView, error)
}

// remoteMember is one member entry a peer reports. Account is the publisher's
// account name, for display only. RefRequired means the publisher requires a
// disambiguating ref in the address; a bare name must not select it.
type remoteMember struct {
	Name        string
	Ref         string
	PrevName    string
	Account     string
	RefRequired bool
}

// visibility decides which Agents know each other. topicVisibility implements
// it over the topic table in production.
type visibility interface {
	VisibleLocal(a, b string) bool // two local Agent IDs
	VisibleRemote(local string, peer peerID, name, ref string) bool
	RemoteMembers(peer peerID) []remoteMember // entries in the current intersection
}

// peerTable is the part of the pairing peer table a delivery reads.
type peerTable interface {
	ResolveAlias(alias string) (peerID, bool) // committed peers only
	EffectiveAlias(p peerID) string
	Role(p peerID) pairing.Role
}

// deliveryLink is the part of the link a delivery uses. linkAdapter implements
// it in production.
type deliveryLink interface {
	Request(ctx context.Context, p peerID, f link.Frame) (reply link.Frame, sent bool, err error)
	HandleRequest(kind link.FrameKind, h func(ctx context.Context, p peerID, f link.Frame) link.Frame)
	Subscribe(onUp func(p peerID, salt []byte), onDown func(p peerID))
	Established(p peerID) bool // State(p).Phase == link.PhaseEstablished
	Kick(p peerID)
}

// inboxWriter writes one message to an account's session backend.
// SessionBackend implements it.
type inboxWriter interface {
	Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error
}

// sendSettings is the snapshot of the send.* configuration one operation reads
// once.
type sendSettings struct {
	RequestTimeout, LinkBackoff, InboxBackoff, InboxTimeout, DedupeWindow time.Duration
	LinkRetries, InboxRetries                                             int
}

// settingsSource supplies the send configuration, the local alias and the
// offline switch.
type settingsSource interface {
	Send() sendSettings
	LocalAlias() string
	// Offline reports the current broker.offline value. It is read afresh on
	// every delivery; a failed read is treated as online and logged.
	Offline() bool
}

// deliveryDeps is everything a delivery uses. The fields are interfaces so the
// unit tests can substitute fakes.
type deliveryDeps struct {
	Agents      localAgents
	Topics      visibility
	Peers       peerTable
	Link        deliveryLink
	Settings    settingsSource
	Clock       func() time.Time // dedupe expiry and inbound quota refill
	WaiterClock Clock            // link waiter timing; nil uses SystemClock
	NewID       func() string    // nil means newMessageID
	Log         *slog.Logger
}

// peerSend is one outgoing message to a remote peer.
type peerSend struct {
	Caller    agentView
	Peer      peerID
	Addr      parsedAddress
	MessageID string
	Text      string
	Deadline  time.Time
}

// delivery implements sending and receiving messages. Broker assembles one
// instance and the Local Send RPC and the link request handler share it.
type delivery struct {
	deps   deliveryDeps
	toPeer func(ctx context.Context, r peerSend) error

	dedupe  *dedupeTable
	inbound *inboundLimiter
	waiter  *linkWaiter

	// lifecycle is canceled by Run before link Close. A receive handler
	// processes on a child of it, so a blocked inbox write cannot hold link
	// Close open past shutdown.
	lifecycle context.Context
	stopFunc  context.CancelFunc
}

// newDelivery builds a delivery around deps. It only stores the dependencies;
// the send path is attached later by the assembly.
func newDelivery(deps deliveryDeps) *delivery {
	clock := deps.Clock
	if clock == nil {
		clock = time.Now
	}
	lifecycle, stop := context.WithCancel(context.Background())
	d := &delivery{deps: deps, lifecycle: lifecycle, stopFunc: stop}
	d.dedupe = newDedupeTable(clock, func() time.Duration {
		return d.deps.Settings.Send().DedupeWindow
	})
	d.inbound = newInboundLimiter(clock)
	d.waiter = newLinkWaiter(deps.Link, deps.WaiterClock)
	// A dropped link does not reset the quota: a peer could reconnect to spend
	// another burst. Only unpairing the peer forgets its buckets.
	d.toPeer = d.sendToPeer
	return d
}

// stop cancels the delivery lifecycle. Run calls it before link Close; it may
// be called more than once.
func (d *delivery) stop() {
	if d.stopFunc != nil {
		d.stopFunc()
	}
}

// processContext returns the context one received request is processed on. It
// is detached from the link, so a link drop cannot interrupt a write the sender
// may retry, but it follows the delivery lifecycle, so stopping the delivery
// cancels a write that would otherwise hold link Close open.
func (d *delivery) processContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if d.lifecycle == nil {
		return context.WithoutCancel(ctx), func() {}
	}
	pctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if d.lifecycle.Err() != nil {
		cancel()
	}
	stop := context.AfterFunc(d.lifecycle, cancel)
	return pctx, func() {
		stop()
		cancel()
	}
}

// logDeliveryRefusedOwner records a by-name delivery refused because the name
// belongs to another account. matched uids are the visible same-name Agents of
// other accounts, and from identifies the sender: a local Agent's ref or the
// peer fingerprint. It records no message text.
func (d *delivery) logDeliveryRefusedOwner(name string, ownerUID uint32, matchedUIDs []uint32, from string) {
	logger := d.deps.Log
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info(EvDeliveryRefusedOwner,
		"name", name,
		"owner_uid", ownerUID,
		"matched_uids", matchedUIDs,
		"from", from,
	)
}

// notSent returns an error for a message that was certainly not delivered. The
// fixed prefix distinguishes it from an unknown result in the message the model
// reads.
func notSent(code localpb.ErrorCode, format string, args ...any) *Error {
	return Errorf(code, "Not sent: "+format, args...)
}

// unknownResult returns an error for a message whose outcome cannot be
// determined. It must stay distinct from notSent (REQ-agent-messaging-027).
func unknownResult(format string, args ...any) *Error {
	return Errorf(localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN, "Delivery unknown: "+format, args...)
}

// inboxPackage implements inboxWriter with inbox.Deliver. Tests use it as the
// Inbox of agents backed by fakesession.
type inboxPackage struct{}

func (inboxPackage) Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error {
	return inbox.Deliver(ctx, s, sig, text, msgID, timeout)
}

var _ inboxWriter = inboxPackage{}
