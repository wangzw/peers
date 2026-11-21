// Package pairingprompt builds the paste-ready text that pairs two Brokers. It
// is shared by the MCP tool output and the terminal command so both name the
// same addresses, key and command.
package pairingprompt

import (
	"fmt"
	"strings"
)

// ConnectPrompt returns text a user can hand to the other host's user to pair
// the two Brokers. It fills in the addresses this Broker shows and the
// one-time pairing key: one indented sentence asks a Claude Code session on
// the other host to connect, and a second block gives the equivalent terminal
// command. With more than one address the sentence lists them all and, from
// the reading session's point of view, says to choose one reachable from this
// host, while the command uses the first address and offers the rest, each
// shell-quoted, as replacements.
//
// With no addresses there is nothing a pasted request could name, so it
// returns the empty string and the caller shows no prompt.
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

// listAddresses joins addresses for prose: "a", "a or b" and "a, b or c",
// without an Oxford comma.
func listAddresses(addresses []string) string {
	switch len(addresses) {
	case 0:
		return ""
	case 1:
		return addresses[0]
	}
	return strings.Join(addresses[:len(addresses)-1], ", ") + " or " + addresses[len(addresses)-1]
}

// listQuoted joins shell-quoted addresses the way listAddresses joins them, so
// each replacement can be pasted into a shell exactly as written.
func listQuoted(addresses []string) string {
	quoted := make([]string, len(addresses))
	for i, a := range addresses {
		quoted[i] = shellQuote(a)
	}
	return listAddresses(quoted)
}

// shellQuote renders s as one POSIX shell word. A word made only of characters
// no shell treats specially is left bare; anything else is single-quoted, with
// each internal single quote closed, escaped and reopened.
func shellQuote(s string) string {
	if shellWordSafe(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// shellWordSafe reports whether s can stand bare as a shell word. Besides the
// characters a shell treats specially anywhere, a leading ~ or = is not safe:
// the shell expands ~ and reads a leading = as the start of an assignment.
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

// shellSafe reports whether r may appear in an unquoted shell word.
func shellSafe(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	}
	return strings.ContainsRune("._-:@%+=/,~", r)
}
