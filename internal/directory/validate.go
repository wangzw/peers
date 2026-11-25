package directory

import (
	"net"
	"strconv"
	"unicode"
	"unicode/utf8"
)

// maxPublicTopicNameLen is the largest accepted public topic name in bytes. It
// duplicates topic.ValidateName, which this package cannot import; the two
// rules must be kept in sync by hand.
const maxPublicTopicNameLen = 256

const maxDirectAddrHostLen = 253

func validPublicTopicName(name string) bool {
	if len(name) == 0 || len(name) > maxPublicTopicNameLen {
		return false
	}
	if !utf8.ValidString(name) {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validDirectAddr(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if len(host) == 0 || len(host) > maxDirectAddrHostLen {
		return false
	}
	for _, r := range host {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return false
	}
	return true
}
