// This file carries no e2e build tag so that the test-name helper and its
// host-side test compile without one.

package syscontainer

import (
	"regexp"
	"strings"
)

func runTestFlag(name string) string {
	parts := strings.Split(name, "/")
	for i, part := range parts {
		parts[i] = "^" + regexp.QuoteMeta(part) + "$"
	}
	return strings.Join(parts, "/")
}
