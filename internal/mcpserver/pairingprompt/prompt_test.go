package pairingprompt

import "testing"

func TestConnectPrompt(t *testing.T) {
	const key = "PKR8-A8AH-AHPS-8XHJ"

	tests := []struct {
		name      string
		addresses []string
		key       string
		want      string
	}{
		{
			name:      "single address",
			addresses: []string{"laptop(10.12.72.11:17443)"},
			key:       key,
			want: `To pair, paste this into a Claude Code session on the other host:
  Connect to the peers Broker at laptop(10.12.72.11:17443) using the one-time pairing key PKR8-A8AH-AHPS-8XHJ.
Or run this on the other host:
  peers broker connect 'laptop(10.12.72.11:17443)' --key PKR8-A8AH-AHPS-8XHJ`,
		},
		{
			name:      "two addresses",
			addresses: []string{"laptop(10.12.72.11:17443)", "laptop(192.168.1.20:17443)"},
			key:       key,
			want: `To pair, paste this into a Claude Code session on the other host:
  Connect to the peers Broker at laptop(10.12.72.11:17443) or laptop(192.168.1.20:17443) using the one-time pairing key PKR8-A8AH-AHPS-8XHJ, choosing whichever address is reachable from this host.
Or run this on the other host:
  peers broker connect 'laptop(10.12.72.11:17443)' --key PKR8-A8AH-AHPS-8XHJ
  If that address is not reachable from the other host, use 'laptop(192.168.1.20:17443)' instead.`,
		},
		{
			name:      "three addresses have no Oxford comma",
			addresses: []string{"a:1", "b:2", "c:3"},
			key:       "K",
			want: `To pair, paste this into a Claude Code session on the other host:
  Connect to the peers Broker at a:1, b:2 or c:3 using the one-time pairing key K, choosing whichever address is reachable from this host.
Or run this on the other host:
  peers broker connect a:1 --key K
  If that address is not reachable from the other host, use b:2 or c:3 instead.`,
		},
		{
			name:      "several replacements are each shell-quoted",
			addresses: []string{"a:1", "host with space:2", "it's:3"},
			key:       "K",
			want: `To pair, paste this into a Claude Code session on the other host:
  Connect to the peers Broker at a:1, host with space:2 or it's:3 using the one-time pairing key K, choosing whichever address is reachable from this host.
Or run this on the other host:
  peers broker connect a:1 --key K
  If that address is not reachable from the other host, use 'host with space:2' or 'it'\''s:3' instead.`,
		},
		{
			name:      "plain address needs no quotes",
			addresses: []string{"10.12.72.11:17443"},
			key:       key,
			want: `To pair, paste this into a Claude Code session on the other host:
  Connect to the peers Broker at 10.12.72.11:17443 using the one-time pairing key PKR8-A8AH-AHPS-8XHJ.
Or run this on the other host:
  peers broker connect 10.12.72.11:17443 --key PKR8-A8AH-AHPS-8XHJ`,
		},
		{
			name:      "address with a space and an IPv6 bracket",
			addresses: []string{"host with space:17443", "[::1]:17443"},
			key:       key,
			want: `To pair, paste this into a Claude Code session on the other host:
  Connect to the peers Broker at host with space:17443 or [::1]:17443 using the one-time pairing key PKR8-A8AH-AHPS-8XHJ, choosing whichever address is reachable from this host.
Or run this on the other host:
  peers broker connect 'host with space:17443' --key PKR8-A8AH-AHPS-8XHJ
  If that address is not reachable from the other host, use '[::1]:17443' instead.`,
		},
		{
			name:      "address with a single quote",
			addresses: []string{"it's:17443"},
			key:       key,
			want: `To pair, paste this into a Claude Code session on the other host:
  Connect to the peers Broker at it's:17443 using the one-time pairing key PKR8-A8AH-AHPS-8XHJ.
Or run this on the other host:
  peers broker connect 'it'\''s:17443' --key PKR8-A8AH-AHPS-8XHJ`,
		},
		{
			name:      "no addresses",
			addresses: nil,
			key:       key,
			want:      "",
		},
		{
			name:      "empty address list",
			addresses: []string{},
			key:       key,
			want:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ConnectPrompt(tt.addresses, tt.key); got != tt.want {
				t.Errorf("ConnectPrompt(%q, %q) =\n%q\nwant\n%q", tt.addresses, tt.key, got, tt.want)
			}
		})
	}
}

// TestShellQuote covers the words left bare and the ones quoted. A leading ~
// or = is unsafe even though the same character is fine elsewhere in a word.
func TestShellQuote(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"host:17443", "host:17443"},
		{"a=b", "a=b"},
		{"a~b", "a~b"},
		{"~user", "'~user'"},
		{"=x", "'=x'"},
		{"host with space", "'host with space'"},
		{"it's", `'it'\''s'`},
		{"", "''"},
	}
	for _, tt := range tests {
		if got := shellQuote(tt.in); got != tt.want {
			t.Errorf("shellQuote(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
