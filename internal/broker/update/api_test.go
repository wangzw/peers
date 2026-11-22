package update

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLatestReturnsNewestRelease(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if want := "/repos/wangzw/peers/releases/latest"; r.URL.Path != want {
			t.Errorf("path = %q, want %q", r.URL.Path, want)
		}
		_, _ = io.WriteString(w, `{"tag_name":"v0.2.0","html_url":"https://github.com/wangzw/peers/releases/tag/v0.2.0"}`)
	}))
	t.Cleanup(srv.Close)

	got, err := Latest(context.Background(), srv.Client(), srv.URL)
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if got.Version != "0.2.0" {
		t.Errorf("Latest().Version = %q, want %q", got.Version, "0.2.0")
	}
	if want := "https://github.com/wangzw/peers/releases/tag/v0.2.0"; got.URL != want {
		t.Errorf("Latest().URL = %q, want %q", got.URL, want)
	}
}

func TestLatestRejectsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	_, err := Latest(context.Background(), srv.Client(), srv.URL)
	if err == nil {
		t.Fatal("Latest() error = nil, want an error for a failing API")
	}
	// The error must carry the response's status, not only that the request
	// failed.
	for _, want := range []string{"500 Internal Server Error", "releases/latest"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Latest() error = %q, want it to contain %q", err, want)
		}
	}
}

func TestLatestRejectsOversizedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte{'x'}, int(maxAPIBodySize)+1))
	}))
	t.Cleanup(srv.Close)

	_, err := Latest(context.Background(), srv.Client(), srv.URL)
	if err == nil {
		t.Fatal("Latest() error = nil, want an error for an oversized response")
	}
	want := fmt.Sprintf("response from %s/repos/wangzw/peers/releases/latest exceeds the %d-byte limit", srv.URL, maxAPIBodySize)
	if !strings.Contains(err.Error(), want) {
		t.Errorf("Latest() error = %q, want it to contain %q", err, want)
	}
}

func TestLatestRejectsInvalidContent(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "malformed json",
			body: `not json`,
			want: []string{"decoding", "invalid character"},
		},
		{
			name: "missing tag",
			body: `{}`,
			want: []string{`release tag "" is not a formal version`},
		},
		{
			name: "tag is not a version",
			body: `{"tag_name":"latest"}`,
			want: []string{`release tag "latest" is not a formal version`},
		},
		{
			name: "tag is a prerelease",
			body: `{"tag_name":"v0.2.0-rc.1"}`,
			want: []string{`release tag "v0.2.0-rc.1" is not a formal version`},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(srv.Close)

			_, err := Latest(context.Background(), srv.Client(), srv.URL)
			if err == nil {
				t.Fatal("Latest() error = nil, want an error for invalid content")
			}
			// The error must explain what was wrong with the content, not only
			// that it was rejected.
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Latest() error = %q, want it to contain %q", err, want)
				}
			}
		})
	}
}
