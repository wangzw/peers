// Package pairingprompt builds the paste-ready text that pairs two Brokers.
package pairingprompt

import (
	"fmt"
	"strings"
)

// ConnectPrompt returns text a user can hand to the other host's user to pair
// the two Brokers, with this Broker's addresses and the one-time key filled in.
// With no addresses there is nothing to name, so it returns the empty string.
func ConnectPrompt(addresses []string, key string) string {
	if len(addresses) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("To pair, paste this into a Claude Code session on the other host:\n")
	fmt.Fprintf(&b, "  Connect to the peers Broker at %s using the one-time pairing key %s", listAddresses(addresses), key)
	if len(addresses) > 1 {
		b.WriteString(", choosing whichever address is reachable from this host.")
	} else {
		b.WriteString(".")
	}
	b.WriteString("\nOr run this on the other host:\n")
	fmt.Fprintf(&b, "  peers broker connect %s --key %s\n", shellQuote(addresses[0]), key)
	if rest := addresses[1:]; len(rest) > 0 {
		fmt.Fprintf(&b, "  If that address is not reachable from the other host, use %s instead.\n", listQuoted(rest))
	}
	return strings.TrimSuffix(b.String(), "\n")
}

func listAddresses(addresses []string) string {
	switch len(addresses) {
	case 0:
		return ""
	case 1:
		return addresses[0]
	}
	return strings.Join(addresses[:len(addresses)-1], ", ") + " or " + addresses[len(addresses)-1]
}

func listQuoted(addresses []string) string {
	quoted := make([]string, len(addresses))
	for i, a := range addresses {
		quoted[i] = shellQuote(a)
	}
	return listAddresses(quoted)
}

func shellQuote(s string) string {
	if shellWordSafe(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// shellWordSafe reports whether s can stand bare as a shell word; a leading ~
// or = is unsafe because the shell expands ~ and reads a leading = as an
// assignment.
func shellWordSafe(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if !shellSafe(r) {
			return false
		}
		if i == 0 && (r == '~' || r == '=') {
			return false
		}
	}
	return true
}

func shellSafe(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	}
	return strings.ContainsRune("._-:@%+=/,~", r)
}
