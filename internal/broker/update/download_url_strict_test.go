package update

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestStrictCheckDownloadURLRejectsAnUnparsableURL checks that a URL the
// standard parser rejects is reported as invalid rather than as a scheme
// violation.
func TestStrictCheckDownloadURLRejectsAnUnparsableURL(t *testing.T) {
	err := strictCheckDownloadURL("https://exa mple.com/asset")
	if err == nil {
		t.Fatal("strictCheckDownloadURL() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "invalid download URL") {
		t.Errorf("strictCheckDownloadURL() error = %v, want it to report an invalid URL", err)
	}
}

// TestStrictDownloadClientConsultsTheCallersCheck checks that a caller's
// redirect check runs first and that its error is returned unchanged.
func TestStrictDownloadClientConsultsTheCallersCheck(t *testing.T) {
	cause := errors.New("caller stopped the redirect")
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return cause
	}}
	req := &http.Request{URL: &url.URL{Scheme: "https", Host: "example.com"}}

	err := strictDownloadClient(client).CheckRedirect(req, nil)
	if !errors.Is(err, cause) {
		t.Errorf("CheckRedirect() error = %v, want the caller's error", err)
	}
}

// TestStrictDownloadClientStopsAfterTenRedirects checks the standard redirect
// limit applies when the caller set no check.
func TestStrictDownloadClientStopsAfterTenRedirects(t *testing.T) {
	req := &http.Request{URL: &url.URL{Scheme: "https", Host: "example.com"}}

	err := strictDownloadClient(&http.Client{}).CheckRedirect(req, make([]*http.Request, 10))
	if err == nil {
		t.Fatal("CheckRedirect() = nil after ten redirects, want an error")
	}
	if !strings.Contains(err.Error(), "10 redirects") {
		t.Errorf("CheckRedirect() error = %v, want it to report the limit", err)
	}
}

// TestStrictDownloadClientAllowsAnHTTPSRedirect checks that a caller check
// which allows the redirect still passes an https target.
func TestStrictDownloadClientAllowsAnHTTPSRedirect(t *testing.T) {
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return nil
	}}
	req := &http.Request{URL: &url.URL{Scheme: "https", Host: "example.com"}}

	if err := strictDownloadClient(client).CheckRedirect(req, nil); err != nil {
		t.Errorf("CheckRedirect() error = %v, want the https redirect to pass", err)
	}
}
