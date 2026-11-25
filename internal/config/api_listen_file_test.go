package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestListenFromFile checks that ListenFromFile applies the same defaults and
// syntax rules as a Broker loading the file.
func TestListenFromFile(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		missing bool
		want    string
		wantErr string
	}{
		{name: "missing file uses the default", missing: true, want: ":17443"},
		{name: "configured value", body: "broker:\n  listen: \"127.0.0.1:7777\"\n", want: "127.0.0.1:7777"},
		{name: "empty value disables listening", body: "broker:\n  listen: \"\"\n", want: ""},
		{name: "absent key uses the default", body: "broker:\n  public: false\n", want: ":17443"},
		{name: "invalid syntax", body: "broker:\n  listen: \"not an address\"\n", wantErr: "broker.listen"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if !tc.missing {
				if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := ListenFromFile(path)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ListenFromFile error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ListenFromFile: %v", err)
			}
			if got != tc.want {
				t.Errorf("ListenFromFile = %q, want %q", got, tc.want)
			}
		})
	}
}
