package update

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

// strictCheckDownloadURL requires an https URL; the host is not restricted
// because GitHub redirects assets to storage hosts.
func strictCheckDownloadURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid download URL %q: %w", raw, err)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("refusing to download from %q: downloads must use https", raw)
	}
	return nil
}

// strictDownloadClient returns a copy of c that rejects a redirect leaving
// https; a caller's CheckRedirect runs first and nil keeps the ten-redirect
// limit.
func strictDownloadClient(c *http.Client) *http.Client {
	c = httpClient(c)
	previous := c.CheckRedirect
	wrapped := *c
	wrapped.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if previous != nil {
			if err := previous(req, via); err != nil {
				return err
			}
		} else if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if req.URL.Scheme != "https" {
			return fmt.Errorf("refusing to follow a redirect to %q: downloads must use https", req.URL.Redacted())
		}
		return nil
	}
	return &wrapped
}
