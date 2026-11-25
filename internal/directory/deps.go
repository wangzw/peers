package directory

import (
	"context"
	"log/slog"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// Identity is this Broker's signing identity.
type Identity interface {
	// Fingerprint returns the fingerprint of the local certificate.
	Fingerprint() Fingerprint
	// PublicKey returns a copy of the raw 32-byte Ed25519 public key.
	PublicKey() []byte
	// Sign returns the Ed25519 signature over data.
	Sign(data []byte) []byte
}

// PeerTable is the peer table as the directory needs it.
type PeerTable interface {
	// IsPeer reports whether fp is a paired peer.
	IsPeer(fp Fingerprint) bool
	// Peers returns a copy of all peer entries.
	Peers() []pairing.Peer
	// EffectiveAlias returns the alias the local Broker uses for fp.
	EffectiveAlias(fp Fingerprint) string
}

// Links carries directory frames over established links, identified by kind
// and payload bytes.
type Links interface {
	// Send sends one frame without blocking. When the link to peer is not
	// established it returns a non-nil error immediately and does not queue
	// the frame.
	Send(peer Fingerprint, kind uint32, payload []byte) error
	// Request sends one frame and waits for its reply. sent reports whether
	// the frame was actually written; when it is false no reply will arrive.
	Request(ctx context.Context, peer Fingerprint, kind uint32, payload []byte) (replyKind uint32, reply []byte, sent bool, err error)
	// HandleState registers the handler for state frames of the given kind.
	// Frames arriving on one link are delivered in arrival order, and none is
	// dispatched on a link before the local onUp notification for that link has
	// returned: a frame sent from onUp is delivered only afterwards.
	HandleState(kind uint32, h func(peer Fingerprint, payload []byte))
	// HandleRequest registers the handler for request frames of the given
	// kind. Handlers may run concurrently; the returned bytes become the reply.
	HandleRequest(kind uint32, h func(ctx context.Context, peer Fingerprint, payload []byte) (replyKind uint32, reply []byte))
	// Subscribe registers link up and down notifications. Notifications are
	// delivered in order; no notification is replayed for a link established
	// before Subscribe.
	Subscribe(onUp func(peer Fingerprint, salt []byte), onDown func(peer Fingerprint))
	// Status returns the view of the link to peer; a zero LinkView means no
	// link record exists.
	Status(peer Fingerprint) LinkView
}

// Settings supplies the directory's configuration.
type Settings interface {
	// Public reports whether the Broker is public.
	Public() bool
	// Alias returns the broker.alias setting.
	Alias() string
	// Advertised returns at most eight advertised addresses; empty when the
	// Broker is not listening.
	Advertised() []config.AdvertisedAddr
	// DataDir returns the directory that holds directory.json.
	DataDir() (string, error)
	// ReconcileInterval returns directory.reconcile_interval.
	ReconcileInterval() time.Duration
	// MaxRecords returns directory.max_records.
	MaxRecords() int
	// MaxRecordsPerLink returns directory.max_records_per_link.
	MaxRecordsPerLink() int
	// MaxTopicsPerPublisher returns directory.max_topics_per_publisher.
	MaxTopicsPerPublisher() int
}

// Deps are the collaborators Open needs.
type Deps struct {
	Identity Identity         // required
	Peers    PeerTable        // required
	Links    Links            // required
	Settings Settings         // optional; the config package is used when nil
	Logger   *slog.Logger     // optional; slog.Default() when nil
	Now      func() time.Time // optional; time.Now when nil, decides only "the current instant"; timers always use real time
}
