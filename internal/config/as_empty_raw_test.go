package config

import "testing"

// TestConfigFileEmptyDocumentKeepsRaw checks that a comment-only file parses as
// an empty document whose bytes are returned unchanged.
func TestConfigFileEmptyDocumentKeepsRaw(t *testing.T) {
	body := []byte("# first line\n\n# second line\n")
	doc, err := parseConfigFile(body)
	if err != nil {
		t.Fatal(err)
	}
	if !doc.empty {
		t.Fatal("a file holding only comments did not parse as an empty document")
	}
	out, err := doc.bytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(body) {
		t.Errorf("bytes() = %q, want the exact parsed bytes %q", out, body)
	}
}
