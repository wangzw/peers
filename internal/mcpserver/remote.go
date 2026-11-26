package mcpserver

import (
	"net/url"
	"strings"
)

// normalizeRemote turns a git remote address into the default topic name: the
// lowercase host name, a slash, and the repository path with its case
// preserved. The second result is false when the address does not name a remote
// repository, such as a local path or a file URL.
func normalizeRemote(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)

	var host, path string
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", false
		}
		switch strings.ToLower(u.Scheme) {
		case "https", "http", "ssh", "git":
		default:
			return "", false
		}
		host = u.Hostname()
		path = u.Path
	} else {
		i := strings.Index(raw, ":")
		if i < 0 {
			return "", false
		}
		if slash := strings.Index(raw, "/"); slash >= 0 && slash < i {
			return "", false
		}
		// In the scp-like form the part before the first colon is [user@]host: a
		// leading user name ends at the last "@", and the host is what follows
		// it. Everything after the colon is the path, even when it starts with a
		// digit.
		hostPart := raw[:i]
		if at := strings.LastIndex(hostPart, "@"); at >= 0 {
			hostPart = hostPart[at+1:]
		}
		host = hostPart
		path = raw[i+1:]
	}

	path = strings.Trim(path, "/")
	path = strings.TrimSuffix(path, ".git")
	path = strings.TrimSuffix(path, "/")
	host = strings.ToLower(host)

	if host == "" || path == "" {
		return "", false
	}
	return host + "/" + path, true
}
