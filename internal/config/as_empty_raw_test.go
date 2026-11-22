package config

import "testing"

// TestConfigFileEmptyDocumentKeepsRaw checks that a file holding only comments
// parses as an empty document that still carries its exact bytes. asEmpty
// records them so bytes returns the file unchanged; encoding the empty mapping
// instead would add a "{}" node and drop the original blank lines. The test
// drives the document helpers directly because a comment-only file never
// reaches bytes through Set, which clears raw before the first encode.
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
