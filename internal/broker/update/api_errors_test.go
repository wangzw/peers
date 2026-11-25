package update

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"
)

// TestLatestReportsABodyReadFailure checks that a response body which stops
// early returns the read error with the release URL.
func TestLatestReportsABodyReadFailure(t *testing.T) {
	cause := errors.New("body read failed")
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(iotest.ErrReader(cause)),
		}, nil
	})}

	_, err := Latest(context.Background(), client, "https://api.example.com")
	if !errors.Is(err, cause) {
		t.Fatalf("Latest() error = %v, want the body's read error", err)
	}
	want := "reading https://api.example.com/repos/wangzw/peers/releases/latest"
	if !strings.Contains(err.Error(), want) {
		t.Errorf("Latest() error = %v, want it to contain %q", err, want)
	}
}

// TestLatestRejectsAnUnparsableAPIBase checks that a base URL the request
// builder rejects is reported.
func TestLatestRejectsAnUnparsableAPIBase(t *testing.T) {
	_, err := Latest(context.Background(), nil, "https://exa mple.com")
	if err == nil {
		t.Fatal("Latest() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "building a request") {
		t.Errorf("Latest() error = %v, want it to report the request build", err)
	}
}

// TestHTTPClientDefaultsToTheStandardClient checks that a nil client resolves
// to the standard one.
func TestHTTPClientDefaultsToTheStandardClient(t *testing.T) {
	if got := httpClient(nil); got != http.DefaultClient {
		t.Errorf("httpClient(nil) = %p, want http.DefaultClient %p", got, http.DefaultClient)
	}
}
