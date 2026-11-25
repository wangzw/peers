package directory

import (
	"time"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

type recordID struct {
	publisher Fingerprint
	kind      directorypb.RecordKind
	object    string
}

type storedRecord struct {
	id      recordID
	raw     []byte
	sig     []byte
	body    *directorypb.RecordBody // parsed after verification; read-only
	version uint64
	expires time.Time
	source  Fingerprint // link it was accepted from; zero means locally issued

	// acceptedAt is when the record was accepted from a link; a newer version
	// may replace it only after directoryReplaceInterval(). It is kept in
	// memory only, so a record loaded from disk has the zero instant and may be
	// replaced at once.
	acceptedAt time.Time

	// revokePending marks a locally issued record whose revocation could not be
	// signed; the maintenance loop retries it. It is not persisted.
	revokePending bool
}
