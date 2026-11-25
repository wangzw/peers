package config

import (
	"strings"
	"testing"
)

// TestReadConfigFileRejectsDirectory checks that reading a directory as the
// configuration file is an error naming the unreadable path.
func TestReadConfigFileRejectsDirectory(t *testing.T) {
	_, err := readConfigFile(t.TempDir())
	if err == nil {
		t.Fatal("readConfigFile accepted a directory as the configuration file")
	}
	if !strings.Contains(err.Error(), "cannot read") {
		t.Fatalf("readConfigFile error = %v, want it to name the unreadable path", err)
	}
}

// TestConfigFileParseRejectsMalformedDocuments checks that parseConfigFile
// refuses a malformed second document, a non-mapping top level, an anchored or
// non-scalar mapping key, a merge key, and a sequence nested past the limit.
func TestConfigFileParseRejectsMalformedDocuments(t *testing.T) {
	deepSequence := "a: " + strings.Repeat("[", 40) + strings.Repeat("]", 40) + "\n"
	cases := []struct{ name, body, want string }{
		{"malformed second document", "a: 1\n---\n[\n", "config file parse error"},
		{"top level sequence", "- a\n", "top level must be a mapping"},
		{"anchored key", "&x k: 1\n", "anchors and aliases are not supported"},
		{"non-scalar key", "? [a, b]\n: 1\n", "is not a scalar"},
		{"merge key", "a:\n  <<: 1\n", "merge keys are not supported"},
		{"deep sequence", deepSequence, "nesting is deeper than 32 levels"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfigFile([]byte(tc.body))
			if err == nil {
				t.Fatalf("parseConfigFile accepted:\n%s", tc.body)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("parseConfigFile error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestLeadingCommentRequiresACommentOnlyDocument checks that the leading
// comment is the whole file only when every line is blank or a comment.
func TestLeadingCommentRequiresACommentOnlyDocument(t *testing.T) {
	commentOnly := "# a\n\n# b\n"
	if got := leadingComment([]byte(commentOnly)); got != commentOnly {
		t.Fatalf("leadingComment(comment-only) = %q, want the whole file", got)
	}
	if got := leadingComment([]byte("a: 1\n")); got != "" {
		t.Fatalf("leadingComment(file with content) = %q, want empty", got)
	}
}

// TestSectionAndLeafSplitsOnlyAtTheFirstDot checks that sectionAndLeaf splits
// at the first dot and leaves the remaining dots in the leaf.
func TestSectionAndLeafSplitsOnlyAtTheFirstDot(t *testing.T) {
	cases := []struct {
		key, section, leaf string
	}{
		{"broker.alias", "broker", "alias"},
		{"a.b.c", "a", "b.c"},
	}
	for _, tc := range cases {
		section, leaf := sectionAndLeaf(tc.key)
		if section != tc.section || leaf != tc.leaf {
			t.Errorf("sectionAndLeaf(%q) = (%q, %q), want (%q, %q)", tc.key, section, leaf, tc.section, tc.leaf)
		}
	}
}

// TestValueEqualComparesEachStringElement checks that two lists of the same
// length but different elements are not equal.
func TestValueEqualComparesEachStringElement(t *testing.T) {
	if !valueEqual(stringListValue([]string{"a", "b"}), stringListValue([]string{"a", "b"})) {
		t.Fatal("valueEqual reported two equal lists as different")
	}
	if valueEqual(stringListValue([]string{"a", "b"}), stringListValue([]string{"a", "c"})) {
		t.Fatal("valueEqual reported lists differing in the last element as equal")
	}
}

// TestConfigDocumentBytesAddsATrailingNewline checks that bytes adds a trailing
// newline to raw document bytes that lack one.
func TestConfigDocumentBytesAddsATrailingNewline(t *testing.T) {
	doc, err := parseConfigFile([]byte("broker:\n  alias: a"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := doc.bytes()
	if err != nil {
		t.Fatalf("bytes: %v", err)
	}
	if got, want := string(out), "broker:\n  alias: a\n"; got != want {
		t.Fatalf("bytes = %q, want %q", got, want)
	}
}

// TestConfigFileSetReplacesOnlyTheRealPlaceholder checks that the placeholder
// search ignores a section header that only starts with the section name, a
// comment that only starts with the leaf name, and a comment whose leaf is
// followed by something other than a colon and whitespace.
func TestConfigFileSetReplacesOnlyTheRealPlaceholder(t *testing.T) {
	spec, _ := lookupKey("broker.advertise")
	body := []byte("brokers:\n  alias: decoy\nbroker:\n  # advertiseX\n  # advertise:not-a-placeholder\n  # advertise: 203.0.113.5:17443\n  public: true\n")
	got, ok := replacePlaceholderLine(body, "broker", "advertise", spec, stringValue("198.51.100.7:17443"))
	if !ok {
		t.Fatalf("replacePlaceholderLine found no placeholder in:\n%s", body)
	}
	text := string(got)
	if !strings.Contains(text, "  advertise: 198.51.100.7:17443\n") {
		t.Fatalf("the placeholder was not replaced:\n%s", text)
	}
	for _, keep := range []string{"brokers:", "alias: decoy", "# advertiseX", "# advertise:not-a-placeholder"} {
		if !strings.Contains(text, keep) {
			t.Errorf("the decoy %q was lost:\n%s", keep, text)
		}
	}
	if strings.Contains(text, "# advertise: 203.0.113.5:17443") {
		t.Errorf("the placeholder survived:\n%s", text)
	}
	if n, want := strings.Count(text, "\n"), strings.Count(string(body), "\n"); n != want {
		t.Errorf("line count changed from %d to %d", want, n)
	}
}

// TestConfigFileSetPlaceholderPreservesCRLF checks that replacing a
// placeholder in a file with CRLF line endings keeps the carriage return on
// every line.
func TestConfigFileSetPlaceholderPreservesCRLF(t *testing.T) {
	spec, _ := lookupKey("broker.advertise")
	body := []byte("broker:\r\n  # advertise: old\r\n  public: true\r\n")
	got, ok := replacePlaceholderLine(body, "broker", "advertise", spec, stringValue("198.51.100.7:17443"))
	if !ok {
		t.Fatalf("replacePlaceholderLine found no CRLF placeholder in:\n%q", body)
	}
	lines := strings.Split(string(got), "\n")
	if len(lines) != strings.Count(string(body), "\n")+1 {
		t.Fatalf("line count changed:\n%q", got)
	}
	for i, line := range lines[:len(lines)-1] {
		if !strings.HasSuffix(line, "\r") {
			t.Errorf("line %d lost its carriage return: %q", i+1, line)
		}
	}
	if want := "  advertise: 198.51.100.7:17443\r"; lines[1] != want {
		t.Fatalf("replaced line = %q, want %q", lines[1], want)
	}
}

// TestSectionHeaderIndentRejectsALineWithContent checks that a line with
// content after the section colon is not a section header, while an indented
// header reports its indent.
func TestSectionHeaderIndentRejectsALineWithContent(t *testing.T) {
	if _, ok := sectionHeaderIndent("broker: value", "broker"); ok {
		t.Fatal("sectionHeaderIndent accepted a line with content after the colon")
	}
	indent, ok := sectionHeaderIndent("  broker:  # comment", "broker")
	if !ok || indent != 2 {
		t.Fatalf("sectionHeaderIndent(indented header) = (%d, %v), want (2, true)", indent, ok)
	}
}
