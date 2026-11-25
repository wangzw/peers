package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPrefixedAdvertiseIsInvalidValue checks that a "kind://" prefix is rejected
// as an invalid address spelling by parseAdvertise, validateExtraItem and Set.
func TestPrefixedAdvertiseIsInvalidValue(t *testing.T) {
	prefixed := []string{
		"relay://b.example:443",
		"direct://b.example",
		"relay://[2001:db8::1]:443",
	}
	for _, in := range prefixed {
		_, err := parseAdvertise(in)
		if err == nil {
			t.Fatalf("parseAdvertise(%q) accepted a prefixed address", in)
		}
		if strings.Contains(strings.ToLower(err.Error()), "kind") {
			t.Fatalf("parseAdvertise(%q) still classifies the prefix as an address kind: %v", in, err)
		}
		if err := validateExtraItem(in); err == nil {
			t.Fatalf("validateExtraItem(%q) accepted a prefixed address", in)
		}
	}

	c, path := newValueCore(t, "", "")
	installCoreForTest(t, c)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		key string
		v   Value
	}{
		{"broker.advertise", stringValue("relay://b.example:443")},
		{"broker.advertise_extra", stringListValue([]string{"relay://b.example"})},
	}
	for _, tt := range tests {
		if _, err := Set(Actor{}, tt.key, tt.v); !errors.Is(err, ErrInvalidValue) {
			t.Fatalf("Set(%s, prefixed) error = %v, want ErrInvalidValue", tt.key, err)
		}
		v, err := Get(tt.key)
		if err != nil {
			t.Fatal(err)
		}
		if tt.key == "broker.advertise" {
			if v.String != "" {
				t.Fatalf("broker.advertise = %q after a rejected prefixed value", v.String)
			}
		} else if len(v.Strings) != 0 {
			t.Fatalf("broker.advertise_extra = %v after a rejected prefixed value", v.Strings)
		}
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("the config file changed after rejected prefixed values:\n%s", after)
	}
}

// TestPrefixedAdvertiseRejectedAtLoad checks that loading a configuration file
// with a prefixed address fails with ErrInvalidValue.
func TestPrefixedAdvertiseRejectedAtLoad(t *testing.T) {
	home := t.TempDir()
	c := newCore()
	c.skipDataCheck = true
	c.noBackground = true
	c.checkRuntime = func() (string, error) { return filepath.Join(home, "run"), nil }
	c.homeDir = func() (string, error) { return home, nil }
	c.hostname = func() (string, error) { return "hostA", nil }
	c.accountName = func() (string, error) { return "tester", nil }
	c.routesSrc = watchTestRoutes{}

	dir := filepath.Join(home, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("broker:\n  advertise: \"relay://b.example:443\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.close)

	err := c.load(ModeUser, "")
	if !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("load with a prefixed broker.advertise error = %v, want ErrInvalidValue", err)
	}
}
