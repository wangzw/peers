package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestKeyTableCompleteAndDefaults checks that every key's type and default
// match the table and that no entry is missing.
func TestKeyTableCompleteAndDefaults(t *testing.T) {
	if got, want := len(keyTable), 32; got != want {
		t.Fatalf("key table has %d keys, want %d", got, want)
	}
	type expectation struct {
		kind KeyKind
		def  Value
	}
	want := map[string]expectation{
		"broker.alias":                       {keyString, stringValue("")},
		"broker.public":                      {keyBool, boolValue(false)},
		"broker.listen":                      {keyString, stringValue(":17443")},
		"broker.offline":                     {keyBool, boolValue(false)},
		"broker.advertise":                   {keyString, stringValue("")},
		"broker.advertise_extra":             {keyStringList, stringListValue(nil)},
		"broker.address_check_interval":      {keyDuration, durationValue(time.Minute)},
		"broker.local_socket_group":          {keyString, stringValue("")},
		"broker.local_socket_mode":           {keyString, stringValue("0666")},
		"pairing.static_key":                 {keySecret, secretValue("")},
		"pairing.max_failures":               {keyInt, intValue(5)},
		"pairing.max_attempts_per_minute":    {keyInt, intValue(10)},
		"topics.auto_join":                   {keyBool, boolValue(true)},
		"directory.reconcile_interval":       {keyDuration, durationValue(10 * time.Minute)},
		"directory.max_records":              {keyInt, intValue(1000)},
		"directory.max_records_per_link":     {keyInt, intValue(250)},
		"directory.max_topics_per_publisher": {keyInt, intValue(16)},
		"send.link_retries":                  {keyInt, intValue(5)},
		"send.link_backoff":                  {keyDuration, durationValue(500 * time.Millisecond)},
		"send.request_timeout":               {keyDuration, durationValue(20 * time.Second)},
		"send.inbox_retries":                 {keyInt, intValue(2)},
		"send.inbox_backoff":                 {keyDuration, durationValue(200 * time.Millisecond)},
		"send.inbox_timeout":                 {keyDuration, durationValue(5 * time.Second)},
		"send.dedupe_window":                 {keyDuration, durationValue(5 * time.Minute)},
		"link.dial_timeout":                  {keyDuration, durationValue(5 * time.Second)},
		"link.reconnect_min":                 {keyDuration, durationValue(time.Second)},
		"link.reconnect_max":                 {keyDuration, durationValue(time.Minute)},
		"link.keepalive_interval":            {keyDuration, durationValue(15 * time.Second)},
		"link.keepalive_timeout":             {keyDuration, durationValue(10 * time.Second)},
		"log.level":                          {keyString, stringValue("info")},
		"update.check":                       {keyBool, boolValue(true)},
		"update.check_interval":              {keyDuration, durationValue(24 * time.Hour)},
	}
	for name, spec := range keyTable {
		exp, ok := want[name]
		if !ok {
			t.Errorf("unexpected key %q in the table", name)
			continue
		}
		if spec.Kind != exp.kind {
			t.Errorf("%s: kind %v, want %v", name, spec.Kind, exp.kind)
		}
		if spec.Dynamic {
			if spec.Default.Kind != KindUnset {
				t.Errorf("%s: dynamic key must have no static default, got %+v", name, spec.Default)
			}
			delete(want, name)
			continue
		}
		if !valueEqual(spec.Default, exp.def) {
			t.Errorf("%s: default %+v, want %+v", name, spec.Default, exp.def)
		}
		delete(want, name)
	}
	for name := range want {
		t.Errorf("key %q is missing from the table", name)
	}
	got := defaultValue(keyTable["broker.alias"], defaultContext{
		mode:     ModeUser,
		hostname: "hostA",
		account:  "alice",
	})
	if got.String != "hostA.alice" {
		t.Errorf("default alias = %q, want hostA.alice", got.String)
	}
}

// TestTimeoutDocsNameTheToolLimit checks that the send and dial timeout
// descriptions name the fixed tool limit for calls made from a session.
func TestTimeoutDocsNameTheToolLimit(t *testing.T) {
	cases := map[string]string{
		"send.request_timeout": "Budget for one send call, i.e. the total time limit. A call made from a Claude Code session is also bounded by the fixed limit of the tool that makes it, 30 seconds for send_message, so any part of this value above that limit does not apply to calls made from a session.",
		"link.dial_timeout":    "Connection time limit for each address during dialing. A call made from a Claude Code session is also bounded by the fixed limit of the tool that makes it, 30 seconds for connect_broker, so any part of this value above that limit does not apply to calls made from a session; the terminal command peers broker connect has no limit of its own and uses this value in full.",
	}
	for name, want := range cases {
		spec, ok := lookupKey(name)
		if !ok {
			t.Fatalf("unknown key %q", name)
		}
		if spec.Doc != want {
			t.Errorf("%s doc = %q, want %q", name, spec.Doc, want)
		}
	}
}

// TestKeyNamesListsTheWholeTable checks that KeyNames returns every key in the
// table in the same order as allKeyNames.
func TestKeyNamesListsTheWholeTable(t *testing.T) {
	got := KeyNames()
	want := allKeyNames()
	if len(got) != len(want) {
		t.Fatalf("KeyNames returned %d keys, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("KeyNames[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestValidateValueBoundaries checks the accepted and rejected values at each
// key's boundaries.
func TestValidateValueBoundaries(t *testing.T) {
	cases := []struct {
		key   string
		value Value
		ok    bool
	}{
		{"broker.alias", stringValue("a"), true},
		{"broker.alias", stringValue(strings.Repeat("a", 24)), true},
		{"broker.alias", stringValue(strings.Repeat("a", 25)), false},
		{"broker.alias", stringValue("bad alias"), false},
		{"broker.listen", stringValue(""), true},
		{"broker.listen", stringValue(":17443"), true},
		{"broker.listen", stringValue("127.0.0.1:1"), true},
		{"broker.listen", stringValue("127.0.0.1:0"), false},
		{"broker.listen", stringValue("127.0.0.1:65536"), false},
		{"broker.listen", stringValue("no-port"), false},
		{"broker.advertise", stringValue(""), true},
		{"broker.advertise", stringValue("b.example"), true},
		{"broker.advertise", stringValue("b.example:443"), true},
		{"broker.advertise", stringValue(":18443"), true},
		{"broker.advertise", stringValue("0.0.0.0"), false},
		{"broker.advertise", stringValue("::"), false},
		{"broker.advertise", stringValue("direct://b.example"), false},
		{"broker.advertise_extra", stringListValue([]string{"b.example"}), true},
		{"broker.advertise_extra", stringListValue([]string{"b.example:8443"}), true},
		{"broker.advertise_extra", stringListValue([]string{":8443"}), false},
		{"broker.address_check_interval", durationValue(0), false},
		{"broker.address_check_interval", durationValue(-time.Second), false},
		{"broker.address_check_interval", durationValue(time.Nanosecond), true},
		{"broker.local_socket_mode", stringValue("666"), true},
		{"broker.local_socket_mode", stringValue("0666"), true},
		{"broker.local_socket_mode", stringValue("0777"), true},
		{"broker.local_socket_mode", stringValue("1000"), false},
		{"broker.local_socket_mode", stringValue("0888"), false},
		{"broker.local_socket_mode", stringValue("66"), false},
		{"pairing.max_failures", intValue(0), false},
		{"pairing.max_failures", intValue(1), true},
		{"send.link_retries", intValue(0), true},
		{"send.link_retries", intValue(-1), false},
		{"send.inbox_retries", intValue(0), true},
		{"send.inbox_retries", intValue(-1), false},
		{"log.level", stringValue("info"), true},
		{"log.level", stringValue("DEBUG"), false},
		{"log.level", stringValue("trace"), false},
		{"update.check", boolValue(true), true},
		{"update.check", boolValue(false), true},
		{"update.check_interval", durationValue(time.Hour), true},
		{"update.check_interval", durationValue(time.Hour - time.Nanosecond), false},
		{"update.check_interval", durationValue(0), false},
	}
	for _, tc := range cases {
		spec, ok := lookupKey(tc.key)
		if !ok {
			t.Fatalf("unknown key %q", tc.key)
		}
		err := validateValue(spec, tc.value)
		if tc.ok && err != nil {
			t.Errorf("%s=%+v: unexpected error %v", tc.key, tc.value, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("%s=%+v: expected an error", tc.key, tc.value)
		}
	}
}

// TestREQBrokerPairing029_StaticKeyLength covers the empty-means-unset rule and
// the 16 to 128 character bounds.
func TestREQBrokerPairing029_StaticKeyLength(t *testing.T) {
	spec, _ := lookupKey("pairing.static_key")
	cases := []struct {
		value string
		ok    bool
	}{
		{"", true},
		{strings.Repeat("k", 15), false},
		{strings.Repeat("k", 16), true},
		{strings.Repeat("k", 128), true},
		{strings.Repeat("k", 129), false},
	}
	for _, tc := range cases {
		err := validateValue(spec, secretValue(tc.value))
		if tc.ok && err != nil {
			t.Errorf("length %d: unexpected error %v", len(tc.value), err)
		}
		if !tc.ok && err == nil {
			t.Errorf("length %d: expected an error", len(tc.value))
		}
	}
}

// TestREQBrokerConfig024_ListenValidation keeps the default and the empty
// (do-not-listen) form distinct from a malformed address.
func TestREQBrokerConfig024_ListenValidation(t *testing.T) {
	defaultListen, err := parseListen(":17443")
	if err != nil || !defaultListen.set || !defaultListen.all || defaultListen.port != 17443 {
		t.Fatalf("default listen parsed as %+v, err %v", defaultListen, err)
	}
	empty, err := parseListen("")
	if err != nil || empty.set {
		t.Fatalf("empty listen parsed as %+v, err %v", empty, err)
	}
	specific, err := parseListen("192.0.2.20:17443")
	if err != nil || specific.all || specific.host != "192.0.2.20" {
		t.Fatalf("specific listen parsed as %+v, err %v", specific, err)
	}
}

// TestParseValue checks that ParseValue accepts and rejects the text forms of
// each key's type.
func TestParseValue(t *testing.T) {
	ok := []struct {
		key  string
		text string
		want Value
	}{
		{"broker.alias", "host-1", stringValue("host-1")},
		{"broker.listen", "", stringValue("")},
		{"broker.public", "true", boolValue(true)},
		{"broker.offline", "true", boolValue(true)},
		{"broker.offline", "false", boolValue(false)},
		{"pairing.max_failures", "7", intValue(7)},
		{"send.request_timeout", "10m", durationValue(10 * time.Minute)},
		{"broker.advertise_extra", "[a.example, b.example:8443]", stringListValue([]string{"a.example", "b.example:8443"})},
		{"broker.local_socket_mode", "0666", stringValue("0666")},
		{"pairing.static_key", "0123456789abcdef", secretValue("0123456789abcdef")},
	}
	for _, tc := range ok {
		got, err := ParseValue(tc.key, tc.text)
		if err != nil {
			t.Errorf("%s=%q: unexpected error %v", tc.key, tc.text, err)
			continue
		}
		if !valueEqual(got, tc.want) {
			t.Errorf("%s=%q: got %+v, want %+v", tc.key, tc.text, got, tc.want)
		}
	}
	bad := []struct {
		key  string
		text string
	}{
		{"no.such.key", "x"},
		{"broker.public", "yes"},
		{"broker.public", "1"},
		{"broker.offline", "maybe"},
		{"pairing.max_failures", "5.5"},
		{"pairing.max_failures", "abc"},
		{"send.request_timeout", "10"},
		{"broker.advertise_extra", "- a\n- b"},
		{"broker.advertise_extra", "a"},
		{"broker.advertise_extra", "[1, 2]"},
		{"pairing.static_key", "123"},
		{"broker.alias", "a\n---\nb"},
	}
	for _, tc := range bad {
		if _, err := ParseValue(tc.key, tc.text); err == nil {
			t.Errorf("%s=%q: expected an error", tc.key, tc.text)
		}
	}
	if _, err := ParseValue("no.such.key", "x"); !errors.Is(err, ErrUnknownKey) {
		t.Errorf("unknown key error = %v, want ErrUnknownKey", err)
	}
}
