package config

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/testkit/refpage"
)

// TestReferencePageConfiguration checks that configuration.md agrees with the
// key table in both languages.
func TestReferencePageConfiguration(t *testing.T) {
	for _, lang := range refpage.Langs {
		t.Run(lang, func(t *testing.T) {
			src := refpage.Read(t, lang, "configuration")
			for _, problem := range checkConfigurationPage(src, lang) {
				t.Error(problem)
			}
		})
	}
}

// TestReferencePageConfigurationFixtures checks that each planted page defect
// is reported, in both languages.
func TestReferencePageConfigurationFixtures(t *testing.T) {
	fixtures := []struct {
		name  string
		plant func(*testing.T, []byte) []byte
		key   string
		want  func(refpage.Vocabulary) string
	}{
		{
			name:  "missing key row",
			plant: func(t *testing.T, src []byte) []byte { return dropConfigurationRow(t, src, "broker.public") },
			key:   "broker.public",
			want:  func(refpage.Vocabulary) string { return "missing key row" },
		},
		{
			name: "extra key row",
			plant: func(t *testing.T, src []byte) []byte {
				return insertConfigurationRow(t, src, "broker.alias",
					"| `broker.nonexistent` | string | `x` | Not a key. | Yes |")
			},
			key:  "broker.nonexistent",
			want: func(refpage.Vocabulary) string { return "unexpected key row" },
		},
		{
			name: "duration default changed",
			plant: func(t *testing.T, src []byte) []byte {
				return setConfigurationDefault(t, src, "broker.address_check_interval", "`2m`")
			},
			key:  "broker.address_check_interval",
			want: func(refpage.Vocabulary) string { return "1m" },
		},
		{
			name: "integer default changed",
			plant: func(t *testing.T, src []byte) []byte {
				return setConfigurationDefault(t, src, "pairing.max_failures", "`6`")
			},
			key:  "pairing.max_failures",
			want: func(refpage.Vocabulary) string { return "5" },
		},
		{
			name: "empty word replaced by a code span",
			plant: func(t *testing.T, src []byte) []byte {
				return setConfigurationDefault(t, src, "broker.advertise", "`\"\"`")
			},
			key:  "broker.advertise",
			want: func(words refpage.Vocabulary) string { return words.Empty },
		},
		{
			name: "derived word replaced by a code span",
			plant: func(t *testing.T, src []byte) []byte {
				return setConfigurationDefault(t, src, "broker.alias", "`workstation`")
			},
			key:  "broker.alias",
			want: func(words refpage.Vocabulary) string { return words.Derived },
		},
		{
			name: "duplicate key row",
			plant: func(t *testing.T, src []byte) []byte {
				return insertConfigurationRow(t, src, "broker.alias",
					"| `broker.alias` | string | `x` | Duplicate. | Yes |")
			},
			key:  "broker.alias",
			want: func(refpage.Vocabulary) string { return "duplicate key row" },
		},
	}
	for _, lang := range refpage.Langs {
		words, _ := refpage.Words(lang)
		for _, fixture := range fixtures {
			t.Run(lang+"/"+fixture.name, func(t *testing.T) {
				src := fixture.plant(t, refpage.Read(t, lang, "configuration"))
				problems := checkConfigurationPage(src, lang)
				if len(problems) != 1 {
					t.Fatalf("got %d problems, want 1: %v", len(problems), problems)
				}
				problem := problems[0]
				if problem.lang != lang {
					t.Errorf("problem names language %q, want %q", problem.lang, lang)
				}
				if problem.key != fixture.key {
					t.Errorf("problem names key %q, want %q", problem.key, fixture.key)
				}
				if want := fixture.want(words); !strings.Contains(problem.item, want) {
					t.Errorf("problem item %q does not mention %q", problem.item, want)
				}
			})
		}
	}
}

type referenceProblem struct {
	lang string
	key  string
	item string
}

func (p referenceProblem) String() string {
	return fmt.Sprintf("%s: key %q: %s", p.lang, p.key, p.item)
}

type defaultExpectation struct {
	word  string // the Empty or Derived word, empty when the default is a code span
	label string // the English name of word, used in problem text
	text  string // the code span content when word is empty
}

// checkConfigurationPage returns every disagreement between the configuration
// page src and the key table in lang, each naming the language, the key and the
// item.
func checkConfigurationPage(src []byte, lang string) []referenceProblem {
	words, ok := refpage.Words(lang)
	if !ok {
		return []referenceProblem{{lang: lang, item: fmt.Sprintf("unknown language %q", lang)}}
	}
	rows, err := refpage.FirstTable(src)
	if err != nil {
		return []referenceProblem{{lang: lang, item: err.Error()}}
	}
	cells := make(map[string]string, len(rows))
	var problems []referenceProblem
	for _, row := range rows {
		if len(row) < 3 {
			return []referenceProblem{{lang: lang, item: fmt.Sprintf("table row has %d cells, want at least 3", len(row))}}
		}
		key, ok := refpage.CodeSpan(row[0])
		if !ok {
			return []referenceProblem{{lang: lang, key: row[0], item: "key cell is not one code span"}}
		}
		if _, dup := cells[key]; dup {
			problems = append(problems, referenceProblem{lang: lang, key: key, item: "duplicate key row"})
			continue
		}
		cells[key] = row[2]
	}
	for _, key := range KeyNames() {
		spec, ok := keyTable[key]
		if !ok {
			problems = append(problems, referenceProblem{lang: lang, key: key, item: "key table has no spec"})
			continue
		}
		want, ok := expectedConfigurationDefault(spec, words)
		if !ok {
			problems = append(problems, referenceProblem{lang: lang, key: key, item: "no expected default for this key's kind"})
			continue
		}
		cell, ok := cells[key]
		if !ok {
			problems = append(problems, referenceProblem{lang: lang, key: key, item: "missing key row"})
			continue
		}
		if want.word != "" {
			if cell != want.word {
				problems = append(problems, referenceProblem{
					lang: lang, key: key,
					item: fmt.Sprintf("default %q, want the %s word %q", cell, want.label, want.word),
				})
			}
			continue
		}
		got, ok := refpage.CodeSpan(cell)
		if !ok || got != want.text {
			problems = append(problems, referenceProblem{
				lang: lang, key: key,
				item: fmt.Sprintf("default %q, want the code span %q", cell, want.text),
			})
		}
	}
	for key := range cells {
		if _, ok := keyTable[key]; !ok {
			problems = append(problems, referenceProblem{lang: lang, key: key, item: "unexpected key row"})
		}
	}
	sort.Slice(problems, func(i, j int) bool {
		if problems[i].key != problems[j].key {
			return problems[i].key < problems[j].key
		}
		return problems[i].item < problems[j].item
	})
	return problems
}

// expectedConfigurationDefault returns the default cell spec must show on the
// configuration page: a vocabulary word and its English label, or the text of
// the single code span holding the default.
func expectedConfigurationDefault(spec *keySpec, words refpage.Vocabulary) (defaultExpectation, bool) {
	if spec.Dynamic {
		return defaultExpectation{word: words.Derived, label: "derived"}, true
	}
	switch spec.Kind {
	case keyString:
		if spec.Default.String == "" {
			return defaultExpectation{word: words.Empty, label: "empty"}, true
		}
		return defaultExpectation{text: spec.Default.String}, true
	case keyStringList:
		if len(spec.Default.Strings) == 0 {
			return defaultExpectation{word: words.Empty, label: "empty"}, true
		}
		return defaultExpectation{text: "[" + strings.Join(spec.Default.Strings, ", ") + "]"}, true
	case keySecret:
		if !spec.Default.SecretIsSet {
			return defaultExpectation{word: words.Empty, label: "empty"}, true
		}
		return defaultExpectation{text: spec.Default.String}, true
	case keyBool:
		return defaultExpectation{text: strconv.FormatBool(spec.Default.Bool)}, true
	case keyInt:
		return defaultExpectation{text: strconv.FormatInt(spec.Default.Int, 10)}, true
	case keyDuration:
		return defaultExpectation{text: shortestDuration(spec.Default.Duration)}, true
	}
	return defaultExpectation{}, false
}

func configurationRow(line string) (key string, cells []string, ok bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "|") {
		return "", nil, false
	}
	cells = strings.Split(strings.Trim(line, "|"), "|")
	for i := range cells {
		cells[i] = strings.TrimSpace(cells[i])
	}
	if len(cells) < 3 {
		return "", nil, false
	}
	key, isSpan := refpage.CodeSpan(cells[0])
	if !isSpan {
		return "", nil, false
	}
	return key, cells, true
}

func dropConfigurationRow(t *testing.T, src []byte, key string) []byte {
	t.Helper()
	lines := strings.Split(string(src), "\n")
	for i, line := range lines {
		if k, _, ok := configurationRow(line); ok && k == key {
			lines = append(lines[:i:i], lines[i+1:]...)
			return []byte(strings.Join(lines, "\n"))
		}
	}
	t.Fatalf("configuration page has no row for %q", key)
	return nil
}

func insertConfigurationRow(t *testing.T, src []byte, afterKey, row string) []byte {
	t.Helper()
	lines := strings.Split(string(src), "\n")
	for i, line := range lines {
		if k, _, ok := configurationRow(line); ok && k == afterKey {
			out := make([]string, 0, len(lines)+1)
			out = append(out, lines[:i+1]...)
			out = append(out, row)
			out = append(out, lines[i+1:]...)
			return []byte(strings.Join(out, "\n"))
		}
	}
	t.Fatalf("configuration page has no row for %q", afterKey)
	return nil
}

func setConfigurationDefault(t *testing.T, src []byte, key, cell string) []byte {
	t.Helper()
	lines := strings.Split(string(src), "\n")
	for i, line := range lines {
		k, cells, ok := configurationRow(line)
		if !ok || k != key {
			continue
		}
		cells[2] = cell
		lines[i] = "| " + strings.Join(cells, " | ") + " |"
		return []byte(strings.Join(lines, "\n"))
	}
	t.Fatalf("configuration page has no row for %q", key)
	return nil
}
