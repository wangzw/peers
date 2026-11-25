package directory

import (
	"crypto/ed25519"
	"errors"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

// oversizedRecord builds and signs an announcement whose raw bytes exceed the
// record size limit; it signs the wire form directly so that the record reaches
// the receiving path.
func oversizedRecord(t *testing.T, seed byte) *directorypb.Record {
	t.Helper()
	pub, priv := recTestKey(seed)
	body := recTestBody(pub, recKindTopic, "news", 1, recTestExpiry(),
		&directorypb.TopicAnnouncement{Name: "news", Description: strings.Repeat("d", 66*1024)})
	return recTestWire(t, body, priv)
}

// TestParseRejectsOversizedRaw checks that parseRecord rejects a record whose
// raw bytes exceed 64 KiB as malformed.
func TestParseRejectsOversizedRaw(t *testing.T) {
	wire := oversizedRecord(t, 41)
	if len(wire.Raw) <= 64<<10 {
		t.Fatalf("test record raw is %d bytes, want more than %d", len(wire.Raw), 64<<10)
	}

	_, err := parseRecord(wire)
	if err == nil {
		t.Fatal("parseRecord accepted an oversized record")
	}
	if !errors.Is(err, errMalformed) {
		t.Fatalf("oversized record error = %v, want it to wrap errMalformed", err)
	}
}

// TestSignRecordRejectsOversizedRaw checks that signRecord applies the same
// size limit as parseRecord.
func TestSignRecordRejectsOversizedRaw(t *testing.T) {
	pub, priv := recTestKey(42)
	body := recTestBody(pub, recKindTopic, "news", 1, recTestExpiry(),
		&directorypb.TopicAnnouncement{Name: "news", Description: strings.Repeat("d", 66*1024)})

	_, err := signRecord(body, func(data []byte) []byte { return ed25519.Sign(priv, data) })
	if err == nil {
		t.Fatal("signRecord signed an oversized record")
	}
	if !errors.Is(err, errMalformed) {
		t.Fatalf("signRecord error = %v, want it to wrap errMalformed", err)
	}
}
