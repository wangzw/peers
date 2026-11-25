package config

import (
	"errors"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// TestValidateValueEnforcesDurationFloor checks that validateValue enforces a
// spec's duration floor.
func TestValidateValueEnforcesDurationFloor(t *testing.T) {
	spec := &keySpec{Name: "synthetic.duration", Kind: keyDuration, MinDur: time.Minute}
	if err := validateValue(spec, durationValue(30*time.Second)); err == nil || err.Error() != "must be at least 1m0s" {
		t.Fatalf("validateValue(30s) = %v, want the minimum error", err)
	}
	if err := validateValue(spec, durationValue(2*time.Minute)); err != nil {
		t.Fatalf("validateValue(2m) = %v, want nil above the minimum", err)
	}
}

// TestParsePortRejectsEmptyText checks that an empty port is refused with
// "port is empty" by parsePort and by parseListen.
func TestParsePortRejectsEmptyText(t *testing.T) {
	if _, err := parsePort(""); err == nil || err.Error() != "port is empty" {
		t.Fatalf("parsePort(\"\") = %v, want the empty-port error", err)
	}
	if _, err := parseListen("127.0.0.1:"); err == nil || err.Error() != "port is empty" {
		t.Fatalf("parseListen(127.0.0.1:) = %v, want the empty-port error", err)
	}
}

// TestSplitHostPortIPv6Forms checks that splitHostPort accepts a bracketed
// IPv6 host and rejects the three malformed bracket forms and an unbracketed
// IPv6 address.
func TestSplitHostPortIPv6Forms(t *testing.T) {
	host, port, err := splitHostPort("[2001:db8::1]:17443")
	if err != nil {
		t.Fatalf("splitHostPort(bracketed IPv6): %v", err)
	}
	if host != "2001:db8::1" || port != "17443" {
		t.Fatalf("splitHostPort(bracketed IPv6) = %q, %q", host, port)
	}

	bad := []struct {
		in   string
		want string
	}{
		{"[2001:db8::1", "must be host:port"},
		{"[2001:db8::1]x", "must be host:port"},
		{"[2001:db8::1]", "must be host:port"},
		{"2001:db8::1:17443", "IPv6 hosts must be written in brackets"},
	}
	for _, tc := range bad {
		if _, _, err := splitHostPort(tc.in); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("splitHostPort(%q) error = %v, want %q", tc.in, err, tc.want)
		}
	}
}

// TestParseNodeRejectsUnsupportedNodeForm checks that parseNode enforces a
// key's own node form instead of guessing a Value.
func TestParseNodeRejectsUnsupportedNodeForm(t *testing.T) {
	modeSpec, ok := lookupKey("broker.local_socket_mode")
	if !ok {
		t.Fatal("broker.local_socket_mode is missing from the key table")
	}
	_, err := parseNode(modeSpec, &yaml.Node{Kind: yaml.SequenceNode}, false)
	if err == nil || !strings.Contains(err.Error(), "must be a YAML string or integer") {
		t.Fatalf("parseNode(local_socket_mode, sequence) = %v, want the string-or-integer error", err)
	}
}

// TestParseNodeReportsScalarDecodeFailures checks that parseNode refuses a
// scalar with the right tag but an undecodable value using the key's type
// message.
func TestParseNodeReportsScalarDecodeFailures(t *testing.T) {
	boolSpec, ok := lookupKey("broker.public")
	if !ok {
		t.Fatal("broker.public is missing from the key table")
	}
	_, err := parseNode(boolSpec, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "notabool"}, false)
	if err == nil || !strings.Contains(err.Error(), "must be a YAML boolean") {
		t.Fatalf("parseNode(undecodable bool) = %v, want the boolean error", err)
	}

	intSpec, ok := lookupKey("pairing.max_failures")
	if !ok {
		t.Fatal("pairing.max_failures is missing from the key table")
	}
	_, err = parseNode(intSpec, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: "notanint"}, false)
	if err == nil || !strings.Contains(err.Error(), "must be a YAML integer") {
		t.Fatalf("parseNode(undecodable int) = %v, want the integer error", err)
	}
}

// TestResolveAliasNodeFollowsChains checks that resolveAliasNode returns the
// target of an alias node.
func TestResolveAliasNodeFollowsChains(t *testing.T) {
	target := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "v"}
	alias := &yaml.Node{Kind: yaml.AliasNode, Alias: target}
	if got := resolveAliasNode(alias); got != target {
		t.Fatalf("resolveAliasNode(alias) = %v, want the target node", got)
	}
}

// TestParseValueEmptyAndMalformedDocuments checks that ParseValue handles the
// three empty-text outcomes by key kind and refuses malformed or
// multi-document input.
func TestParseValueEmptyAndMalformedDocuments(t *testing.T) {
	v, err := ParseValue("pairing.static_key", "   ")
	if err != nil {
		t.Fatalf("ParseValue(empty secret): %v", err)
	}
	if v.Kind != KindSecret || v.String != "" || v.SecretIsSet {
		t.Fatalf("ParseValue(empty secret) = %+v, want an unset secret", v)
	}

	bad := []struct {
		key  string
		text string
		want string
	}{
		{"pairing.max_failures", "   ", "missing value"},
		{"broker.alias", "# only a comment\n", "missing value"},
		{"broker.alias", "[unclosed", "not valid YAML"},
		{"broker.alias", "a\n---\n[unclosed", "not valid YAML"},
		{"send.request_timeout", `"10"`, "must be a duration such as 10m or 500ms"},
	}
	for _, tc := range bad {
		_, err := ParseValue(tc.key, tc.text)
		if !errors.Is(err, ErrInvalidValue) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("ParseValue(%s, %q) = %v, want an ErrInvalidValue containing %q", tc.key, tc.text, err, tc.want)
		}
	}
}
