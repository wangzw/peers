//go:build peers_testenv

package update

import (
	"net/http"
	"os"
)

// testAPIOverride reports whether this test build was pointed at a fake API,
// the only case that accepts anything but https.
func testAPIOverride() bool {
	return os.Getenv(testAPIEnv) != ""
}

// checkDownloadURL accepts any URL only when testAPIEnv names a fake API;
// otherwise it applies the release rule.
func checkDownloadURL(raw string) error {
	if testAPIOverride() {
		return nil
	}
	return strictCheckDownloadURL(raw)
}

func downloadClient(c *http.Client) *http.Client {
	if testAPIOverride() {
		return httpClient(c)
	}
	return strictDownloadClient(c)
}
