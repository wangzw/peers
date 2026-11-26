package mcpserver

import "testing"

// TestNormalizeRemote checks that the same repository normalizes to the same
// topic name in every supported spelling and that an address naming no remote
// repository produces none.
func TestNormalizeRemote(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
		ok   bool
	}{
		{
			name: "scp-like form",
			raw:  "git@github.com:org/repo.git",
			want: "github.com/org/repo",
			ok:   true,
		},
		{
			name: "ssh URL",
			raw:  "ssh://git@github.com/org/repo.git",
			want: "github.com/org/repo",
			ok:   true,
		},
		{
			name: "https URL",
			raw:  "https://github.com/org/repo",
			want: "github.com/org/repo",
			ok:   true,
		},
		{
			name: "http URL",
			raw:  "http://host/x",
			want: "host/x",
			ok:   true,
		},
		{
			name: "ssh URL with user and port",
			raw:  "ssh://git@host:2222/org/repo",
			want: "host/org/repo",
			ok:   true,
		},
		{
			name: "URL with user, port, mixed case and trailing slash",
			raw:  "https://user:p%40ss@GitHub.COM:8443/Org/Repo.git/",
			want: "github.com/Org/Repo",
			ok:   true,
		},
		{
			name: "scp-like form with a numeric path prefix",
			raw:  "git@host:2222/org/repo.git",
			want: "host/2222/org/repo",
			ok:   true,
		},
		{
			name: "scp-like form with more than one @",
			raw:  "user@a@host:org/repo",
			want: "host/org/repo",
			ok:   true,
		},
		{
			name: "URL with IPv6 host",
			raw:  "ssh://[::1]:22/org/repo",
			want: "::1/org/repo",
			ok:   true,
		},
		{
			name: "git URL",
			raw:  "git://host/org/repo",
			want: "host/org/repo",
			ok:   true,
		},
		{
			name: "upper-case scheme",
			raw:  "HTTPS://Host/x",
			want: "host/x",
			ok:   true,
		},
		{
			name: "surrounding whitespace",
			raw:  "  git@github.com:org/repo.git\n",
			want: "github.com/org/repo",
			ok:   true,
		},
		{
			name: "file URL",
			raw:  "file:///srv/repo.git",
			want: "",
			ok:   false,
		},
		{
			name: "absolute local path",
			raw:  "/srv/repo.git",
			want: "",
			ok:   false,
		},
		{
			name: "relative local path",
			raw:  "../repo",
			want: "",
			ok:   false,
		},
		{
			name: "local path with a colon in a directory name",
			raw:  "./a:b/c",
			want: "",
			ok:   false,
		},
		{
			name: "unlisted scheme",
			raw:  "ftp://host/x",
			want: "",
			ok:   false,
		},
		{
			name: "URL without a path",
			raw:  "https://host",
			want: "",
			ok:   false,
		},
		{
			name: "URL whose path is only the .git suffix",
			raw:  "https://host/.git",
			want: "",
			ok:   false,
		},
		{
			name: "empty string",
			raw:  "",
			want: "",
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := normalizeRemote(tt.raw)
			if ok != tt.ok {
				t.Fatalf("normalizeRemote(%q) ok = %v, want %v", tt.raw, ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("normalizeRemote(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}
