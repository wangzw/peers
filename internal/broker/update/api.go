// Package update checks the latest release of peers on GitHub, compares
// versions, and downloads, verifies and atomically installs a release archive.
// It never installs without the caller's explicit request.
package update

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// DefaultAPI is the GitHub API base used by release builds.
const DefaultAPI = "https://api.github.com"

const (
	repositoryOwner = "wangzw"
	repositoryName  = "peers"

	checksumsAsset = "checksums.txt"

	maxAPIBodySize int64 = 1 << 20
)

// Release describes the latest formal release.
type Release struct {
	Version string // without the v prefix, for example "0.1.2"
	URL     string // release page
}

// Latest returns the newest formal release of the repository behind api.
// Prereleases and drafts are rejected.
func Latest(ctx context.Context, c *http.Client, api string) (Release, error) {
	var payload struct {
		TagName string `json:"tag_name"`
		HTMLURL string `json:"html_url"`
	}
	if err := getJSON(ctx, c, releaseURL(api, "latest"), &payload); err != nil {
		return Release{}, err
	}
	version, err := releaseVersion(payload.TagName)
	if err != nil {
		return Release{}, err
	}
	return Release{Version: version, URL: payload.HTMLURL}, nil
}

func releaseURL(api, ref string) string {
	if api == "" {
		api = APIBase()
	}
	return strings.TrimRight(api, "/") + "/repos/" + repositoryOwner + "/" + repositoryName + "/releases/" + ref
}

func releaseVersion(tag string) (string, error) {
	version := strings.TrimPrefix(tag, "v")
	if !isReleaseVersion(version) {
		return "", fmt.Errorf("release tag %q is not a formal version", tag)
	}
	return version, nil
}

func getJSON(ctx context.Context, c *http.Client, url string, v any) error {
	resp, err := get(ctx, c, url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAPIBodySize+1))
	if err != nil {
		return fmt.Errorf("reading %s: %w", url, err)
	}
	if int64(len(body)) > maxAPIBodySize {
		return fmt.Errorf("response from %s exceeds the %d-byte limit", url, maxAPIBodySize)
	}
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("decoding %s: %w", url, err)
	}
	return nil
}

func get(ctx context.Context, c *http.Client, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building a request for %s: %w", url, err)
	}
	resp, err := httpClient(c).Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting %s: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("requesting %s: %s", url, resp.Status)
	}
	return resp, nil
}

func httpClient(c *http.Client) *http.Client {
	if c == nil {
		return http.DefaultClient
	}
	return c
}
