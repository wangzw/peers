package directory

import (
	"net"
	"strconv"
	"unicode"
	"unicode/utf8"
)

// maxPublicTopicNameLen is the largest accepted public topic name in bytes.
// The rule mirrors topic.ValidateName in internal/topic: this package does not
// import topic, so the two checks must be kept in sync by hand. A name this
// package refuses must also be refused there, and the reverse.
const maxPublicTopicNameLen = 256

// maxDirectAddrHostLen is the largest accepted host part of a "direct"
// advertised address, in bytes.
const maxDirectAddrHostLen = 253

// validPublicTopicName reports whether name may be listed as a public topic: 1
// to 256 bytes, valid UTF-8 and free of control characters. It mirrors
// topic.ValidateName; see maxPublicTopicNameLen.
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

// validDirectAddr reports whether addr is a well-formed "direct" advertised
// address: net.SplitHostPort separates a host and a port, the host is 1 to 253
// bytes with no control characters or whitespace, and the port is a decimal
// number from 1 to 65535.
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
