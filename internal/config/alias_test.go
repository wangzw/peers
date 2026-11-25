package config

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestSanitizeHostname checks that every character outside the alias character
// set becomes one dash.
func TestSanitizeHostname(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"hostA", "hostA"},
		{"build-box.example", "build-box.example"},
		{"a-b_c.9", "a-b_c.9"},
		{"host name", "host-name"},
		{"host:name/1", "host-name-1"},
		{"主机", "--"},
		{"a主b", "a-b"},
		{"", ""},
	}
	for _, tc := range tests {
		if got := sanitizeHostname(tc.in); got != tc.want {
			t.Errorf("sanitizeHostname(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestREQBrokerPairing028_HostnameSanitized checks that illegal hostname
// characters become dashes, each multi-byte character becomes one dash, and an
// unreadable hostname yields an empty string and a log entry.
func TestREQBrokerPairing028_HostnameSanitized(t *testing.T) {
	t.Run("sanitizes every illegal character", func(t *testing.T) {
		if got := sanitizeHostname("work station (lab)"); got != "work-station--lab-" {
			t.Fatalf("sanitizeHostname() = %q, want %q", got, "work-station--lab-")
		}
	})

	t.Run("a multi-byte character becomes one dash", func(t *testing.T) {
		if got := sanitizeHostname("主机"); got != "--" {
			t.Fatalf("sanitizeHostname() = %q, want %q", got, "--")
		}
	})

	t.Run("an empty hostname stays empty", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		got := sanitizeHostnameFrom(func() (string, error) { return "", nil }, logger)
		if got != "" {
			t.Fatalf("sanitizeHostnameFrom(empty) = %q, want an empty string", got)
		}
		if buf.Len() != 0 {
			t.Fatalf("sanitizeHostnameFrom(empty) logged %q, want no log entry", buf.String())
		}
	})

	t.Run("an os.Hostname failure logs and returns empty", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		boom := errors.New("hostname unavailable")
		got := sanitizeHostnameFrom(func() (string, error) { return "", boom }, logger)
		if got != "" {
			t.Fatalf("sanitizeHostnameFrom(failure) = %q, want an empty string", got)
		}
		logged := buf.String()
		if !strings.Contains(logged, "hostname") || !strings.Contains(logged, "hostname unavailable") {
			t.Fatalf("log = %q, want it to name the hostname failure", logged)
		}
	})
}

// TestREQBrokerPairing002_DefaultAlias checks the default alias for each
// deployment mode, hostname and account combination.
func TestREQBrokerPairing002_DefaultAlias(t *testing.T) {
	tests := []struct {
		name     string
		mode     Mode
		hostname string
		account  string
		want     string
	}{
		{"system mode uses the hostname", ModeSystem, "hostA", "alice", "hostA"},
		{"system mode ignores the account", ModeSystem, "hostA", "", "hostA"},
		{"user mode appends the account", ModeUser, "hostA", "alice", "hostA.alice"},
		{"a domain name keeps its first label", ModeSystem, "hostA.example.com", "alice", "hostA"},
		{"a domain name keeps its first label in user mode", ModeUser, "hostA.example.com", "alice", "hostA.alice"},
		{"the hostname case is kept", ModeUser, "HostA.example.com", "Alice", "HostA.Alice"},
		{"illegal hostname characters are replaced", ModeSystem, "work station", "", "work-station"},
		{"illegal account characters are replaced", ModeUser, "hostA", "ali ce", "hostA.ali-ce"},
		{"an empty hostname falls back to broker", ModeSystem, "", "alice", "broker"},
		{"an empty hostname in user mode keeps the account", ModeUser, "", "alice", "broker.alice"},
		{"an empty account in user mode leaves the hostname alone", ModeUser, "hostA", "", "hostA"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := defaultAlias(tc.mode, tc.hostname, tc.account); got != tc.want {
				t.Fatalf("defaultAlias(%d, %q, %q) = %q, want %q", tc.mode, tc.hostname, tc.account, got, tc.want)
			}
		})
	}
}

// TestREQBrokerPairing037_AliasTruncation checks that the default alias is at
// most 24 characters, the hostname part is shortened first, and a shortened
// part never ends in "." or "-".
func TestREQBrokerPairing037_AliasTruncation(t *testing.T) {
	longHost := strings.Repeat("a", 30)
	tests := []struct {
		name     string
		mode     Mode
		hostname string
		account  string
		want     string
	}{
		{
			name:     "system mode cuts the hostname at 24",
			mode:     ModeSystem,
			hostname: longHost,
			want:     strings.Repeat("a", 24),
		},
		{
			name:     "user mode makes room for the account",
			mode:     ModeUser,
			hostname: longHost,
			account:  "alice",
			want:     strings.Repeat("a", 18) + ".alice",
		},
		{
			name:     "the hostname keeps one character before the account is cut",
			mode:     ModeUser,
			hostname: longHost,
			account:  strings.Repeat("b", 30),
			want:     "a." + strings.Repeat("b", 22),
		},
		{
			name:     "a truncated hostname drops a trailing dash",
			mode:     ModeUser,
			hostname: strings.Repeat("a", 17) + "-" + strings.Repeat("b", 9),
			account:  "alice",
			want:     strings.Repeat("a", 17) + ".alice",
		},
		{
			name:     "a truncated account drops a trailing dash",
			mode:     ModeUser,
			hostname: "hostA",
			account:  strings.Repeat("b", 21) + "-xyz",
			want:     "h." + strings.Repeat("b", 21),
		},
		{
			name:     "a truncated account drops a trailing dot",
			mode:     ModeUser,
			hostname: "hostA",
			account:  strings.Repeat("b", 21) + ".xyz",
			want:     "h." + strings.Repeat("b", 21),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := defaultAlias(tc.mode, tc.hostname, tc.account)
			if got != tc.want {
				t.Fatalf("defaultAlias() = %q, want %q", got, tc.want)
			}
			if n := len([]rune(got)); n > AliasMaxLen {
				t.Fatalf("defaultAlias() length = %d, want at most %d: %q", n, AliasMaxLen, got)
			}
			if strings.HasSuffix(got, ".") || strings.HasSuffix(got, "-") {
				t.Fatalf("defaultAlias() = %q, want a result that does not end in . or -", got)
			}
		})
	}
}

// TestREQBrokerPairing003_AliasValidation checks that a configured alias must
// be 1 to 24 characters from the alias character set.
func TestREQBrokerPairing003_AliasValidation(t *testing.T) {
	tests := []struct {
		alias   string
		wantErr bool
	}{
		{"a", false},
		{"build-box", false},
		{"a.b-c_d", false},
		{strings.Repeat("a", 24), false},
		{"", true},
		{strings.Repeat("a", 25), true},
		{"build box", true},
		{"主机", true},
		{"bad/name", true},
	}
	for _, tc := range tests {
		err := validAlias(tc.alias)
		if tc.wantErr && err == nil {
			t.Errorf("validAlias(%q) = nil, want an error", tc.alias)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("validAlias(%q) = %v, want nil", tc.alias, err)
		}
	}
}

// TestAliasValidationLongMessage checks that an over-long alias reports that it
// is longer than 24 characters.
func TestAliasValidationLongMessage(t *testing.T) {
	err := validAlias(strings.Repeat("a", 25))
	if err == nil || !strings.Contains(err.Error(), "longer than 24 characters") {
		t.Fatalf("validAlias(25 chars) = %v, want a message saying it is longer than 24 characters", err)
	}
}

// TestAliasValidationNamesInvalidCharactersFirst checks that a multi-byte alias
// is refused for its characters rather than its byte length.
func TestAliasValidationNamesInvalidCharactersFirst(t *testing.T) {
	err := validAlias(strings.Repeat("é", 13))
	if err == nil || !strings.Contains(err.Error(), "is not allowed") {
		t.Fatalf("validAlias(13 é) = %v, want a message naming the invalid character", err)
	}
	if strings.Contains(err.Error(), "longer than") {
		t.Fatalf("validAlias(13 é) = %v, want the character message rather than the length message", err)
	}
}

func testAliasLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, nil)), buf
}

// TestEffectiveAlias checks that the configured alias wins and that the default
// falls back through hostname, account and uid.
func TestEffectiveAlias(t *testing.T) {
	t.Run("configured alias wins", func(t *testing.T) {
		c := &core{
			explicitAlias: true,
			values:        map[string]Value{"broker.alias": stringValue("build-box")},
		}
		if got := c.effectiveAlias(); got != "build-box" {
			t.Fatalf("effectiveAlias() = %q, want build-box", got)
		}
	})

	t.Run("user mode combines the hostname and the account regardless of the listener", func(t *testing.T) {
		c := &core{
			mode:            ModeUser,
			hostname:        func() (string, error) { return "hostA.example.com", nil },
			accountName:     func() (string, error) { return "alice", nil },
			effectiveListen: listenAddr{set: true, host: "127.0.0.1", port: 17444},
			advertised:      []AdvertisedAddr{{Kind: AdvertisedKindDirect, Addr: "a.example:443"}},
			values:          make(map[string]Value),
		}
		if got := c.effectiveAlias(); got != "hostA.alice" {
			t.Fatalf("effectiveAlias() = %q, want hostA.alice", got)
		}
	})

	t.Run("system mode uses the hostname and never looks up an account", func(t *testing.T) {
		c := &core{
			mode:        ModeSystem,
			hostname:    func() (string, error) { return "hostA.example.com", nil },
			accountName: func() (string, error) { return "", errors.New("must not be called") },
			values:      make(map[string]Value),
			logger:      slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		}
		if got := c.effectiveAlias(); got != "hostA" {
			t.Fatalf("effectiveAlias() = %q, want hostA", got)
		}
	})

	t.Run("an unavailable account falls back to the uid with a warning", func(t *testing.T) {
		logger, buf := testAliasLogger()
		c := &core{
			mode:        ModeUser,
			hostname:    func() (string, error) { return "hostA", nil },
			accountName: func() (string, error) { return "", errors.New("account database unavailable") },
			values:      make(map[string]Value),
			logger:      logger,
		}
		want := "hostA." + strconv.Itoa(os.Geteuid())
		if got := c.effectiveAlias(); got != want {
			t.Fatalf("effectiveAlias() = %q, want %q", got, want)
		}
		logged := buf.String()
		if !strings.Contains(logged, "level=WARN") {
			t.Fatalf("log = %q, want a warning entry", logged)
		}
		if !strings.Contains(logged, "account name") || !strings.Contains(logged, strconv.Itoa(os.Geteuid())) {
			t.Fatalf("log = %q, want it to name the account lookup failure and the substituting uid", logged)
		}
	})

	t.Run("an empty account name falls back to the uid", func(t *testing.T) {
		logger, buf := testAliasLogger()
		c := &core{
			mode:        ModeUser,
			hostname:    func() (string, error) { return "hostA", nil },
			accountName: func() (string, error) { return "", nil },
			values:      make(map[string]Value),
			logger:      logger,
		}
		want := "hostA." + strconv.Itoa(os.Geteuid())
		if got := c.effectiveAlias(); got != want {
			t.Fatalf("effectiveAlias() = %q, want %q", got, want)
		}
		if !strings.Contains(buf.String(), "level=WARN") {
			t.Fatalf("log = %q, want a warning entry", buf.String())
		}
	})

	t.Run("an empty hostname in user mode keeps the account", func(t *testing.T) {
		c := &core{
			mode:        ModeUser,
			hostname:    func() (string, error) { return "", nil },
			accountName: func() (string, error) { return "alice", nil },
			values:      make(map[string]Value),
		}
		if got := c.effectiveAlias(); got != "broker.alice" {
			t.Fatalf("effectiveAlias() = %q, want broker.alice", got)
		}
	})

	t.Run("an empty hostname and an unavailable account use broker and the uid", func(t *testing.T) {
		logger, buf := testAliasLogger()
		c := &core{
			mode:        ModeUser,
			hostname:    func() (string, error) { return "", nil },
			accountName: func() (string, error) { return "", errors.New("account database unavailable") },
			values:      make(map[string]Value),
			logger:      logger,
		}
		want := fallbackAlias + "." + strconv.Itoa(os.Geteuid())
		if got := c.effectiveAlias(); got != want {
			t.Fatalf("effectiveAlias() = %q, want %q", got, want)
		}
		if !strings.Contains(buf.String(), "level=WARN") {
			t.Fatalf("log = %q, want a warning entry", buf.String())
		}
	})

	t.Run("an unreadable hostname falls back to broker", func(t *testing.T) {
		logger, buf := testAliasLogger()
		c := &core{
			mode:        ModeSystem,
			hostname:    func() (string, error) { return "", errors.New("hostname unavailable") },
			accountName: func() (string, error) { return "", nil },
			values:      make(map[string]Value),
			logger:      logger,
		}
		if got := c.effectiveAlias(); got != "broker" {
			t.Fatalf("effectiveAlias() = %q, want broker", got)
		}
		if logged := buf.String(); !strings.Contains(logged, "hostname") {
			t.Fatalf("log = %q, want it to name the hostname failure", logged)
		}
	})
}
