//go:build !peers_testenv

package update

import "net/http"

// checkDownloadURL requires an https URL; the host is not restricted because
// GitHub redirects assets to storage hosts.
func checkDownloadURL(raw string) error {
	return strictCheckDownloadURL(raw)
}

// downloadClient returns a copy of c whose redirects must stay on https.
func downloadClient(c *http.Client) *http.Client {
	return strictDownloadClient(c)
}
