package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSystemdUnescape checks the C-style escapes a hand-edited unit may use.
func TestSystemdUnescape(t *testing.T) {
	cases := []struct {
		name string
		in   byte
		want byte
	}{
		{"newline", 'n', '\n'},
		{"tab", 't', '\t'},
		{"carriage return", 'r', '\r'},
		{"space", 's', ' '},
		{"other", 'x', 'x'},
		{"backslash", '\\', '\\'},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := systemdUnescape(tc.in); got != tc.want {
				t.Errorf("systemdUnescape(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestSystemdFirstField checks which word of a systemd command line is the
// program the unit starts.
func TestSystemdFirstField(t *testing.T) {
	cases := []struct {
		name    string
		line    string
		want    string
		started bool
	}{
		{"plain", "/usr/local/bin/peers run", "/usr/local/bin/peers", true},
		{"leading whitespace", " \t/usr/local/bin/peers", "/usr/local/bin/peers", true},
		{"quoted", `"/opt/peer tools/bin/peers" --flag`, "/opt/peer tools/bin/peers", true},
		{"escaped space", `/opt/peer\ tools/bin/peers`, "/opt/peer tools/bin/peers", true},
		{"escaped newline", `/opt/a\nb`, "/opt/a\nb", true},
		{"escaped tab", `/opt/a\tb`, "/opt/a\tb", true},
		{"escaped carriage return", `/opt/a\rb`, "/opt/a\rb", true},
		{"escaped space code", `/opt/a\sb`, "/opt/a b", true},
		{"empty", "", "", false},
		{"whitespace only", "   \t", "", false},
		{"empty quotes", `""`, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, started := systemdFirstField(tc.line)
			if got != tc.want || started != tc.started {
				t.Errorf("systemdFirstField(%q) = %q, %v; want %q, %v", tc.line, got, started, tc.want, tc.started)
			}
		})
	}
}

// TestServiceBinaryReportsDefinitionErrors checks the failures ServiceBinary
// reports for a unit path it cannot use and a definition it cannot parse.
func TestServiceBinaryReportsDefinitionErrors(t *testing.T) {
	t.Run("unsupported platform", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "plan9"
		_, ok, err := ServiceBinary(opts)
		if err == nil || !strings.Contains(err.Error(), `cannot find the unit path on platform "plan9"`) {
			t.Fatalf("ServiceBinary error = %v, want an unsupported platform error", err)
		}
		if ok {
			t.Error("ServiceBinary reported a definition on an unsupported platform")
		}
	})

	t.Run("unit path cannot be inspected", func(t *testing.T) {
		opts := baseOptions(t)
		path, pathErr := unitPath(opts)
		if pathErr != nil {
			t.Fatal(pathErr)
		}
		writeFileUnder(t, opts.Root, "/home/alice/.config", "not a directory")
		_, ok, err := ServiceBinary(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot inspect "+path) {
			t.Fatalf("ServiceBinary error = %v, want an inspection error naming %s", err, path)
		}
		if ok {
			t.Error("ServiceBinary reported a definition it could not inspect")
		}
	})

	t.Run("definition cannot be read", func(t *testing.T) {
		opts := baseOptions(t)
		path, err := unitPath(opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(opts.Root, path), 0o755); err != nil {
			t.Fatal(err)
		}
		_, ok, err := ServiceBinary(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot read "+path) {
			t.Fatalf("ServiceBinary error = %v, want a read error naming %s", err, path)
		}
		if ok {
			t.Error("ServiceBinary reported a definition it could not read")
		}
	})

	t.Run("empty ExecStart", func(t *testing.T) {
		opts := baseOptions(t)
		path, err := unitPath(opts)
		if err != nil {
			t.Fatal(err)
		}
		writeFileUnder(t, opts.Root, path, "[Service]\nExecStart=-@+!:\n")
		_, ok, err := ServiceBinary(opts)
		if err == nil || !strings.Contains(err.Error(), "the ExecStart line is empty") {
			t.Fatalf("ServiceBinary error = %v, want the empty ExecStart error", err)
		}
		if ok {
			t.Error("ServiceBinary accepted an empty ExecStart line")
		}
	})

	t.Run("malformed plist", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.Home = "/Users/alice"
		path, err := unitPath(opts)
		if err != nil {
			t.Fatal(err)
		}
		writeFileUnder(t, opts.Root, path, "<plist><key>")
		_, ok, err := ServiceBinary(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot parse "+path) {
			t.Fatalf("ServiceBinary error = %v, want a parse error naming %s", err, path)
		}
		if ok {
			t.Error("ServiceBinary accepted a malformed plist")
		}
	})
}

// TestLaunchdProgramBinaryParsesProgramArguments checks that only the string
// inside the ProgramArguments array is read as the program.
func TestLaunchdProgramBinaryParsesProgramArguments(t *testing.T) {
	const defPath = "/Library/LaunchDaemons/io.github.wangzw.peers.plist"
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "first argument",
			content: `<plist><dict><key>ProgramArguments</key><array><string>/usr/local/bin/peers</string><string>run</string></array></dict></plist>`,
			want:    "/usr/local/bin/peers",
		},
		{
			name:    "string outside the arguments array",
			content: `<plist><array><string>/early</string></array><key>ProgramArguments</key><array><string>/late</string></array></plist>`,
			want:    "/late",
		},
		{
			name:    "empty arguments array before the populated one",
			content: `<plist><key>ProgramArguments</key><array></array><key>ProgramArguments</key><array><string>/bin/peers</string></array></plist>`,
			want:    "/bin/peers",
		},
		{
			name:    "no ProgramArguments",
			content: `<plist><dict><key>Label</key><string>io.github.wangzw.peers</string></dict></plist>`,
			want:    "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := launchdProgramBinary(tc.content, defPath)
			if err != nil {
				t.Fatalf("launchdProgramBinary: %v", err)
			}
			if got != tc.want {
				t.Errorf("launchdProgramBinary = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestLaunchdProgramBinaryReportsParseErrors checks that a plist whose values
// are not text is rejected rather than read as an empty program.
func TestLaunchdProgramBinaryReportsParseErrors(t *testing.T) {
	const defPath = "/Library/LaunchDaemons/io.github.wangzw.peers.plist"
	cases := []struct {
		name    string
		content string
	}{
		{"malformed XML", "<plist><key>"},
		{"invalid entity outside a key", "<plist>&bad</plist>"},
		{"key with mismatched child", "<plist><dict><key><nested></key></dict></plist>"},
		{"argument with mismatched child", "<plist><key>ProgramArguments</key><array><string><nested></string></array></plist>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := launchdProgramBinary(tc.content, defPath)
			if err == nil || !strings.Contains(err.Error(), "cannot parse "+defPath) {
				t.Fatalf("launchdProgramBinary error = %v, want a parse error naming %s", err, defPath)
			}
		})
	}
}
