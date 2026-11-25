package config

import (
	"strings"
	"testing"
)

// TestTruncateHelpersTrimOnlyShortenedValues checks that the alias-length
// truncate helpers leave a string at or below the limit alone and trim only a
// shortened value.
func TestTruncateHelpersTrimOnlyShortenedValues(t *testing.T) {
	if got := truncateAliasPart("abc.-", 5); got != "abc.-" {
		t.Fatalf("truncateAliasPart(abc.-, 5) = %q, want the value unchanged", got)
	}
	if got := truncateAliasPart("abc.-", 4); got != "abc" {
		t.Fatalf("truncateAliasPart(abc.-, 4) = %q, want the trailing . and - dropped", got)
	}

	if got := truncateRunes("abc", 5); got != "abc" {
		t.Fatalf("truncateRunes(abc, 5) = %q, want the value unchanged", got)
	}
	if got := truncateRunes("主机", 2); got != "主机" {
		t.Fatalf("truncateRunes(主机, 2) = %q, want both runes unchanged", got)
	}
}

// TestDefaultAliasFallsBackWhenAPartShortensToNothing checks that defaultAlias
// substitutes fallbackAlias for an emptied hostname part and drops an emptied
// account part.
func TestDefaultAliasFallsBackWhenAPartShortensToNothing(t *testing.T) {
	dashes := strings.Repeat("-", 30)
	tests := []struct {
		name     string
		mode     Mode
		hostname string
		account  string
		want     string
	}{
		{"system alias emptied by shortening uses broker", ModeSystem, dashes, "", fallbackAlias},
		{"user alias with no account and an emptied host uses broker", ModeUser, dashes, "", fallbackAlias},
		{
			"an emptied host is restored before the account is cut",
			ModeUser, dashes, strings.Repeat("b", 30),
			fallbackAlias + "." + strings.Repeat("b", 17),
		},
		{"an emptied account leaves the hostname", ModeUser, "hostA", dashes, "h"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := defaultAlias(tc.mode, tc.hostname, tc.account)
			if got != tc.want {
				t.Fatalf("defaultAlias(%d, %q, %q) = %q, want %q", tc.mode, tc.hostname, tc.account, got, tc.want)
			}
			if n := len([]rune(got)); n > AliasMaxLen {
				t.Fatalf("defaultAlias() length = %d, want at most %d: %q", n, AliasMaxLen, got)
			}
		})
	}
}
