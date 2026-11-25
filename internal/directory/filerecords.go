package directory

import (
	"encoding/hex"

	"github.com/wangzw/peers/internal/directory/directorypb"
)

func toFileRecords(recs []*storedRecord) []fileRecord {
	sorted := append([]*storedRecord(nil), recs...)
	sortRecords(sorted)

	out := make([]fileRecord, 0, len(sorted))
	for _, r := range sorted {
		source := ""
		if !r.source.IsZero() {
			source = r.source.String()
		}
		out = append(out, fileRecord{Raw: r.raw, Signature: r.sig, Source: source})
	}
	return out
}

// fromFileRecords parses each entry and sets its source; an entry with an
// invalid Source or that fails parseRecord is dropped, never treated as locally
// issued.
func fromFileRecords(frs []fileRecord) (recs []*storedRecord, dropped int) {
	for _, fr := range frs {
		var source Fingerprint
		if fr.Source != "" {
			if !validSourceHex(fr.Source) {
				dropped++
				continue
			}
			b, err := hex.DecodeString(fr.Source)
			if err != nil {
				dropped++
				continue
			}
			copy(source[:], b)
		}

		r, err := parseRecord(&directorypb.Record{Raw: fr.Raw, Signature: fr.Signature})
		if err != nil {
			dropped++
			continue
		}
		r.source = source
		recs = append(recs, r)
	}
	return recs, dropped
}

func validSourceHex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
