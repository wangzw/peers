package mcpserver

import (
	"fmt"
	"strings"
)

// connectPrompt returns text a user can hand to the other host's user to pair
// the two Brokers. It fills in the addresses this Broker shows and the
// one-time pairing key: one indented sentence asks a Claude Code session on
// the other host to connect, and a second block gives the equivalent terminal
// command. With more than one address the sentence lists them all and says to
// choose one the other host can reach, while the command uses the first
// address and offers the rest as replacements.
//
// With no addresses there is nothing a pasted request could name, so it
// returns the empty string and the caller shows no prompt.
func connectPrompt(addresses []string, key string) string {
	if len(addresses) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("To pair, paste this into a Claude Code session on the other host:\n")
	fmt.Fprintf(&b, "  Connect to the peers Broker at %s using the one-time pairing key %s", listAddresses(addresses), key)
	if len(addresses) > 1 {
		b.WriteString(", choosing whichever address the other host can reach.")
	} else {
		b.WriteString(".")
	}
	b.WriteString("\nOr run this on the other host:\n")
	fmt.Fprintf(&b, "  peers broker connect %s --key %s\n", shellQuote(addresses[0]), key)
	if rest := addresses[1:]; len(rest) > 0 {
		fmt.Fprintf(&b, "  If that address is not reachable from the other host, use %s instead.\n", listAddresses(rest))
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

// shellQuote renders s as one POSIX shell word. A word made only of characters
// no shell treats specially is left bare; anything else is single-quoted, with
// each internal single quote closed, escaped and reopened.
func shellQuote(s string) string {
	if s != "" && strings.IndexFunc(s, func(r rune) bool { return !shellSafe(r) }) < 0 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// shellSafe reports whether r may appear in an unquoted shell word.
func shellSafe(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	}
	return strings.ContainsRune("._-:@%+=/,~", r)
}
