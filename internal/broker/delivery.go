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

type peerID = pairing.Fingerprint

type agentView struct {
	ID        string
	Name      string
	Ref       string
	RefPinned bool
	// UID is 0 for a view built without an account; with NameOwned it keeps
	// by-name resolution from treating the view as the owner.
	UID uint32
	// NameOwnerUID is the account that owns Name; NameOwned reports the table
	// has an entry for it.
	NameOwnerUID uint32
	NameOwned    bool
	// PrevNameOwnerUID and PrevNameOwned do the same for PrevName.
	PrevNameOwnerUID uint32
	PrevNameOwned    bool
	// RefMandatory reports that the address and signature must carry a ref.
	RefMandatory bool
	PrevName     string // previous name inside the 10-minute window, else ""
	Session      inbox.Session
	Inbox        inboxWriter
}

// localAgents is the local Agent table a delivery reads.
type localAgents interface {
	Lookup(id string) (agentView, bool)
	All() []agentView
	// Reread rereads the record, applies it and returns the new snapshot; it
	// returns errAgentGone when the Agent is gone.
	Reread(id string) (agentView, error)
}

// remoteMember is one member entry a peer reports; Account is for display only
// and RefRequired means a bare name must not select it.
type remoteMember struct {
	Name        string
	Ref         string
	PrevName    string
	Account     string
	RefRequired bool
}

type visibility interface {
	VisibleLocal(a, b string) bool
	VisibleRemote(local string, peer peerID, name, ref string) bool
	// RemoteMembers returns the peer's entries in the current intersection.
	RemoteMembers(peer peerID) []remoteMember
}

type peerTable interface {
	// ResolveAlias resolves an alias among committed peers only.
	ResolveAlias(alias string) (peerID, bool)
	EffectiveAlias(p peerID) string
	Role(p peerID) pairing.Role
}

type deliveryLink interface {
	Request(ctx context.Context, p peerID, f link.Frame) (reply link.Frame, sent bool, err error)
	HandleRequest(kind link.FrameKind, h func(ctx context.Context, p peerID, f link.Frame) link.Frame)
	Subscribe(onUp func(p peerID, salt []byte), onDown func(p peerID))
	Established(p peerID) bool
	Kick(p peerID)
}

type inboxWriter interface {
	Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error
}

type sendSettings struct {
	RequestTimeout, LinkBackoff, InboxBackoff, InboxTimeout, DedupeWindow time.Duration
	LinkRetries, InboxRetries                                             int
}

type settingsSource interface {
	Send() sendSettings
	LocalAlias() string
	// Offline reports the current broker.offline value, read afresh each
	// delivery; a failed read is treated as online and logged.
	Offline() bool
}

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

type peerSend struct {
	Caller    agentView
	Peer      peerID
	Addr      parsedAddress
	MessageID string
	Text      string
	Deadline  time.Time
}

// delivery implements sending and receiving messages; one instance is shared by
// the Local Send RPC and the link request handler.
type delivery struct {
	deps   deliveryDeps
	toPeer func(ctx context.Context, r peerSend) error

	dedupe  *dedupeTable
	inbound *inboundLimiter
	waiter  *linkWaiter

	// lifecycle is canceled by Run before link Close; a receive handler runs on a
	// child, so a blocked inbox write cannot hold Close open.
	lifecycle context.Context
	stopFunc  context.CancelFunc
}

// newDelivery builds a delivery around deps; the send path is attached later by
// the assembly.
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
	// A dropped link must not reset the quota: a peer could reconnect to spend
	// another burst; only unpairing forgets its buckets.
	d.toPeer = d.sendToPeer
	return d
}

// stop cancels the delivery lifecycle; it may be called more than once.
func (d *delivery) stop() {
	if d.stopFunc != nil {
		d.stopFunc()
	}
}

// processContext is detached from the link but follows the delivery lifecycle,
// so a link drop cannot interrupt a retryable write and stopping cancels a write
// that would hold link Close open.
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

func (d *delivery) logger() *slog.Logger {
	if d.deps.Log != nil {
		return d.deps.Log
	}
	return slog.Default()
}

func (d *delivery) logDeliveryRefusedOwner(name string, ownerUID uint32, matchedUIDs []uint32, from string) {
	logger := d.logger()
	logger.Info(EvDeliveryRefusedOwner,
		"name", name,
		"owner_uid", ownerUID,
		"matched_uids", matchedUIDs,
		"from", from,
	)
}

// notSent returns an error for a message that was certainly not delivered; its
// fixed prefix distinguishes it from an unknown result.
func notSent(code localpb.ErrorCode, format string, args ...any) *Error {
	return Errorf(code, "Not sent: "+format, args...)
}

// unknownResult returns an error for a message whose outcome cannot be
// determined; it must stay distinct from notSent.
func unknownResult(format string, args ...any) *Error {
	return Errorf(localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN, "Delivery unknown: "+format, args...)
}

// inboxPackage implements inboxWriter with inbox.Deliver; tests use it for
// agents backed by fakesession.
type inboxPackage struct{}

func (inboxPackage) Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error {
	return inbox.Deliver(ctx, s, sig, text, msgID, timeout)
}

var _ inboxWriter = inboxPackage{}
