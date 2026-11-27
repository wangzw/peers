// Package refpage reads the website's reference pages for the tests of the
// packages they document.
package refpage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Langs lists the languages whose reference pages are checked.
var Langs = []string{"en", "zh"}

// Vocabulary holds the words that a language's reference pages use. Required
// and Optional mark a parameter, Empty and Derived give a key's default, and
// And continues a flag run to a second flag.
type Vocabulary struct {
	Required string // marks a required parameter
	Optional string // marks an optional parameter
	Empty    string // marks an empty default
	Derived  string // marks a default derived at runtime
	And      string // continues a flag run to a second flag
}

// Words returns the vocabulary of lang, and false for a language it does not
// know.
func Words(lang string) (Vocabulary, bool) {
	switch lang {
	case "en":
		return Vocabulary{
			Required: "required",
			Optional: "optional",
			Empty:    "empty",
			Derived:  "derived",
			And:      "and",
		}, true
	case "zh":
		return Vocabulary{
			Required: "必填",
			Optional: "可选",
			Empty:    "空",
			Derived:  "推导得出",
			And:      "与",
		}, true
	}
	return Vocabulary{}, false
}

// Read returns the bytes of the reference page page (its .md suffix omitted)
// for lang. It fails the test when the page cannot be read.
func Read(t testing.TB, lang, page string) []byte {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("refpage: %v", err)
	}
	path := filepath.Join(root, "docs", "site", "content", lang, "docs", "reference", page+".md")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("refpage: read %s: %v", path, err)
	}
	return src
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("no go.mod found above the working directory")
		}
		dir = parent
	}
}

// CommandSection is one command reference section: the command paths its
// heading names and the flags its list items document.
type CommandSection struct {
	Paths []string // command paths, with "" for the root command
	Flags []string // flag long names without the leading dashes
}

// CommandSections reads the command sections of a command reference page. A
// section starts at a level-2 heading beginning "## peers " and ends at the
// next level-2 heading of any kind; fenced code blocks are ignored throughout.
func CommandSections(src []byte, lang string) []CommandSection {
	words, ok := Words(lang)
	if !ok {
		return nil
	}
	var (
		sections []CommandSection
		current  *CommandSection
		rootAt   = -1
		root     []string
		fenceCh  byte
		fenceLen int
	)
	flush := func() {
		if current != nil {
			current.Flags = dedupe(current.Flags)
			sections = append(sections, *current)
			current = nil
		}
	}
	for _, raw := range strings.Split(string(src), "\n") {
		line := strings.TrimSpace(raw)
		if fenceLen > 0 {
			if fenceCloses(line, fenceCh, fenceLen) {
				fenceCh, fenceLen = 0, 0
			}
			continue
		}
		if ch, n, ok := fenceMarker(line); ok {
			fenceCh, fenceLen = ch, n
			continue
		}
		if level, text := heading(line); level == 2 {
			flush()
			if !strings.HasPrefix(text, "peers ") {
				continue
			}
			tail := strings.TrimPrefix(text, "peers ")
			if strings.HasPrefix(tail, "--") {
				root = append(root, headingFlag(tail))
				if rootAt < 0 {
					sections = append(sections, CommandSection{Paths: []string{""}})
					rootAt = len(sections) - 1
				}
				continue
			}
			current = &CommandSection{Paths: headingPaths(tail)}
			continue
		}
		if current != nil && strings.HasPrefix(line, "- ") {
			current.Flags = append(current.Flags, flagRun(strings.TrimPrefix(line, "- "), words.And)...)
		}
	}
	flush()
	if rootAt >= 0 {
		sections[rootAt].Flags = dedupe(root)
	}
	return sections
}

// heading returns the level and text of an ATX heading line, or level 0 when
// line is not a heading.
func heading(line string) (level int, text string) {
	n := 0
	for n < len(line) && line[n] == '#' {
		n++
	}
	if n == 0 || n > 6 || n >= len(line) || line[n] != ' ' {
		return 0, ""
	}
	return n, strings.TrimSpace(line[n:])
}

// fenceMarker returns the marker character and its run length when line opens
// a fenced code block, which needs at least three of the character.
func fenceMarker(line string) (ch byte, n int, ok bool) {
	if line == "" {
		return 0, 0, false
	}
	ch = line[0]
	if ch != '`' && ch != '~' {
		return 0, 0, false
	}
	for n < len(line) && line[n] == ch {
		n++
	}
	if n < 3 {
		return 0, 0, false
	}
	return ch, n, true
}

// fenceCloses reports whether line closes a block opened by n characters ch: a
// run of at least n of the same character with nothing else after it.
func fenceCloses(line string, ch byte, n int) bool {
	count := 0
	for count < len(line) && line[count] == ch {
		count++
	}
	return count >= n && strings.TrimSpace(line[count:]) == ""
}

// headingFlag returns the flag name that a root-flag heading documents.
func headingFlag(tail string) string {
	name := strings.TrimPrefix(tail, "--")
	if i := strings.IndexAny(name, " \t"); i >= 0 {
		name = name[:i]
	}
	return name
}

// headingPaths returns the command paths a command heading names.
func headingPaths(tail string) []string {
	parts := splitHeading(tail)
	if len(parts) == 0 {
		return nil
	}
	first := parts[0]
	paths := []string{first}
	var parent string
	if i := strings.LastIndexByte(first, ' '); i >= 0 {
		parent = first[:i]
	}
	for _, part := range parts[1:] {
		if parent == "" {
			paths = append(paths, part)
			continue
		}
		paths = append(paths, parent+" "+part)
	}
	return paths
}

// splitHeading splits a grouped command heading into its items.
func splitHeading(tail string) []string {
	var parts []string
	for _, chunk := range strings.Split(tail, ", ") {
		for _, part := range strings.Split(chunk, " and ") {
			if s := strings.TrimSpace(part); s != "" {
				parts = append(parts, s)
			}
		}
	}
	return parts
}

// flagRun returns the flag names documented by one list item's text.
func flagRun(text, and string) []string {
	var flags []string
	text = strings.TrimLeft(text, " \t")
	for {
		name, rest, ok := leadingFlagSpan(text)
		if !ok {
			break
		}
		flags = append(flags, name)
		text = skipShorthand(rest)
		text = strings.TrimLeft(text, " \t")
		if !strings.HasPrefix(text, and+" ") {
			break
		}
		text = strings.TrimLeft(strings.TrimPrefix(text, and), " \t")
		if _, _, ok := leadingFlagSpan(text); !ok {
			break
		}
	}
	return flags
}

// leadingFlagSpan reads a code span that begins text and names a long flag.
func leadingFlagSpan(text string) (name, rest string, ok bool) {
	if !strings.HasPrefix(text, "`") {
		return "", "", false
	}
	end := strings.IndexByte(text[1:], '`')
	if end < 0 {
		return "", "", false
	}
	content := text[1 : 1+end]
	if len(content) < 3 || !strings.HasPrefix(content, "--") || !isASCIILetter(content[2]) {
		return "", "", false
	}
	name = strings.TrimPrefix(content, "--")
	if i := strings.IndexAny(name, " \t"); i >= 0 {
		name = name[:i]
	}
	return name, text[1+end+1:], true
}

// skipShorthand drops a parenthesised single-dash shorthand that follows a
// flag, returning text unchanged when there is none.
func skipShorthand(text string) string {
	trimmed := strings.TrimLeft(text, " \t")
	if trimmed == "" {
		return text
	}
	var open, closeMark string
	switch {
	case strings.HasPrefix(trimmed, "("):
		open, closeMark = "(", ")"
	case strings.HasPrefix(trimmed, "（"):
		open, closeMark = "（", "）"
	default:
		return text
	}
	end := strings.Index(trimmed[len(open):], closeMark)
	if end < 0 {
		return text
	}
	inner := trimmed[len(open) : len(open)+end]
	shorthand, ok := CodeSpan(inner)
	if !ok || len(shorthand) < 2 || shorthand[0] != '-' || !isASCIILetter(shorthand[1]) {
		return text
	}
	return trimmed[len(open)+end+len(closeMark):]
}

func isASCIILetter(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

func dedupe(items []string) []string {
	seen := make(map[string]bool, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if seen[item] {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	return out
}

// FirstTable returns the body rows of the first pipe table in the page, each
// row's cells trimmed and otherwise unparsed. It returns an error when the
// page has no table.
func FirstTable(src []byte) ([][]string, error) {
	lines := strings.Split(string(src), "\n")
	var (
		fenceCh  byte
		fenceLen int
	)
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if fenceLen > 0 {
			if fenceCloses(line, fenceCh, fenceLen) {
				fenceCh, fenceLen = 0, 0
			}
			continue
		}
		if ch, n, ok := fenceMarker(line); ok {
			fenceCh, fenceLen = ch, n
			continue
		}
		if !isTableRow(line) || i+1 >= len(lines) || !isDelimiterRow(lines[i+1]) {
			continue
		}
		var rows [][]string
		for j := i + 2; j < len(lines); j++ {
			row := strings.TrimSpace(lines[j])
			if !isTableRow(row) || isDelimiterRow(row) {
				break
			}
			rows = append(rows, splitRow(row))
		}
		return rows, nil
	}
	return nil, errors.New("refpage: no pipe table found")
}

func isTableRow(line string) bool {
	return strings.HasPrefix(line, "|")
}

func isDelimiterRow(line string) bool {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "|") {
		return false
	}
	cells := splitRow(line)
	if len(cells) == 0 {
		return false
	}
	for _, cell := range cells {
		if cell == "" || !strings.ContainsRune(cell, '-') {
			return false
		}
		for _, r := range cell {
			if r != '-' && r != ':' {
				return false
			}
		}
	}
	return true
}

func splitRow(line string) []string {
	line = strings.TrimSpace(line)
	line = strings.TrimPrefix(line, "|")
	line = strings.TrimSuffix(line, "|")
	parts := strings.Split(line, "|")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// CodeSpan returns the content of cell when the cell is exactly one code span.
func CodeSpan(cell string) (string, bool) {
	s := strings.TrimSpace(cell)
	if len(s) < 2 || s[0] != '`' || s[len(s)-1] != '`' {
		return "", false
	}
	inner := s[1 : len(s)-1]
	if inner == "" || strings.ContainsRune(inner, '`') {
		return "", false
	}
	return inner, true
}

// Param is one tool parameter and whether the page marks it required.
type Param struct {
	Name     string // parameter name
	Required bool   // whether the page marks the parameter required
}

// Params returns every parameter documented in cell for lang, in the order the
// cell lists them. A code span that no required-or-optional group follows is
// skipped; a group whose last element is neither mark is an error.
func Params(cell, lang string) ([]Param, error) {
	words, ok := Words(lang)
	if !ok {
		return nil, fmt.Errorf("refpage: unknown language %q", lang)
	}
	open, comma, closeMark := "(", ",", ")"
	if lang == "zh" {
		open, comma, closeMark = "（", "，", "）"
	}
	var params []Param
	for i := 0; i < len(cell); {
		if cell[i] != '`' {
			i++
			continue
		}
		end := strings.IndexByte(cell[i+1:], '`')
		if end < 0 {
			break
		}
		name := cell[i+1 : i+1+end]
		after := i + 1 + end + 1
		j := after
		for j < len(cell) && (cell[j] == ' ' || cell[j] == '\t') {
			j++
		}
		if name == "" || !strings.HasPrefix(cell[j:], open) {
			i = after
			continue
		}
		groupStart := j + len(open)
		groupEnd := strings.Index(cell[groupStart:], closeMark)
		if groupEnd < 0 {
			i = after
			continue
		}
		group := cell[groupStart : groupStart+groupEnd]
		fields := strings.Split(group, comma)
		last := strings.TrimSpace(fields[len(fields)-1])
		switch last {
		case words.Required:
			params = append(params, Param{Name: name, Required: true})
		case words.Optional:
			params = append(params, Param{Name: name, Required: false})
		default:
			return nil, fmt.Errorf("refpage: parameter %q has mark %q, want %q or %q", name, last, words.Required, words.Optional)
		}
		i = after
	}
	return params, nil
}

// Diff returns the elements of want that got lacks and the elements of got
// that want lacks, both sorted.
func Diff(want, got []string) (missing, extra []string) {
	wantSet := make(map[string]bool, len(want))
	for _, s := range want {
		wantSet[s] = true
	}
	gotSet := make(map[string]bool, len(got))
	for _, s := range got {
		gotSet[s] = true
	}
	for s := range wantSet {
		if !gotSet[s] {
			missing = append(missing, s)
		}
	}
	for s := range gotSet {
		if !wantSet[s] {
			extra = append(extra, s)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}
