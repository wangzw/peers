package directory

import (
	"time"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

// recordID identifies one directory record within the network. A revocation
// uses the kind and object of the record it revokes.
type recordID struct {
	publisher Fingerprint
	kind      directorypb.RecordKind
	object    string
}

// storedRecord is one record held by the directory. raw is kept exactly as it
// was signed so that it can be forwarded unchanged.
type storedRecord struct {
	id      recordID
	raw     []byte
	sig     []byte
	body    *directorypb.RecordBody // parsed after verification; read-only
	version uint64
	expires time.Time
	source  Fingerprint // link it was accepted from; zero means locally issued

	// acceptedAt is when the record was accepted from a link. A newer version
	// may replace it only once directoryReplaceInterval() has passed. It is kept
	// in memory
	// only; a record loaded from disk or seeded without a link leaves it zero
	// and may be replaced at once.
	acceptedAt time.Time

	// revokePending marks a locally issued record whose revocation could not be
	// signed. The maintenance loop retries it until a revocation replaces this
	// record. It is not persisted.
	revokePending bool
}
