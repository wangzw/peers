//go:build !peers_testenv

package update

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestApplyRejectsHTTPAssetURL checks that a release build refuses an asset URL
// on plain HTTP and leaves the target unchanged.
func TestApplyRejectsHTTPAssetURL(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if want := "/repos/wangzw/peers/releases/tags/v" + version; r.URL.Path != want {
			http.NotFound(w, r)
			return
		}
		payload := map[string]any{
			"tag_name": "v" + version,
			"assets": []map[string]string{
				{"name": testArchiveName(version), "browser_download_url": srv.URL + "/download/archive"},
				{"name": checksumsAsset, "browser_download_url": srv.URL + "/download/checksums"},
			},
		}
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Errorf("encoding the fake release: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	err := Apply(context.Background(), testApplyOptions(srv, target, version))
	if err == nil {
		t.Fatal("Apply() = nil, want an error for an http asset URL")
	}
	if !strings.Contains(err.Error(), "https") {
		t.Errorf("Apply() error = %v, want it to mention https", err)
	}
	assertUnchanged(t, target, original, before)
}

// TestApplyRejectsRedirectToHTTP checks that a release build refuses a redirect
// from https to plain HTTP and leaves the target unchanged.
func TestApplyRejectsRedirectToHTTP(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("only reached if the redirect is followed"))
	}))
	t.Cleanup(plain.Close)

	var tlsSrv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/wangzw/peers/releases/tags/v"+version, func(w http.ResponseWriter, _ *http.Request) {
		payload := map[string]any{
			"tag_name": "v" + version,
			"assets": []map[string]string{
				{"name": testArchiveName(version), "browser_download_url": tlsSrv.URL + "/download/archive"},
				{"name": checksumsAsset, "browser_download_url": tlsSrv.URL + "/download/checksums"},
			},
		}
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Errorf("encoding the fake release: %v", err)
		}
	})
	mux.HandleFunc("/download/archive", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+"/archive", http.StatusFound)
	})
	tlsSrv = httptest.NewTLSServer(mux)
	t.Cleanup(tlsSrv.Close)

	opts := ApplyOptions{
		Target:  target,
		Version: version,
		API:     tlsSrv.URL,
		Client:  tlsSrv.Client(),
		GOOS:    "linux",
		GOARCH:  "amd64",
	}
	err := Apply(context.Background(), opts)
	if err == nil {
		t.Fatal("Apply() = nil, want an error for a redirect to http")
	}
	if !strings.Contains(err.Error(), "https") {
		t.Errorf("Apply() error = %v, want it to mention https", err)
	}
	assertUnchanged(t, target, original, before)
}
