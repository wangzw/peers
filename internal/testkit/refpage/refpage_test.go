package refpage

import (
	"reflect"
	"testing"
)

func page(lines ...string) string {
	out := ""
	for i, line := range lines {
		if i > 0 {
			out += "\n"
		}
		out += line
	}
	return out + "\n"
}

// TestCommandSectionsGroupedHeading checks that a grouped heading of three
// names and one of two names expand to every sibling command path.
func TestCommandSectionsGroupedHeading(t *testing.T) {
	src := page(
		"## peers service start, stop and restart",
		"",
		"- `--system` and `--user` select the level.",
		"",
		"## peers service online and offline",
		"",
		"- `--system` and `--user` select the level.",
	)
	got := CommandSections([]byte(src), "en")
	want := []CommandSection{
		{Paths: []string{"service start", "service stop", "service restart"}, Flags: []string{"system", "user"}},
		{Paths: []string{"service online", "service offline"}, Flags: []string{"system", "user"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CommandSections = %#v, want %#v", got, want)
	}
}

// TestCommandSectionsRootFlagHeading checks that root-flag headings merge into
// one section with the empty path.
func TestCommandSectionsRootFlagHeading(t *testing.T) {
	src := page(
		"## peers --version",
		"",
		"Prints the version.",
		"",
		"## peers --verbose",
		"",
		"Prints more.",
		"",
		"## peers broker list",
		"",
		"- `--alias <alias>` sets the alias.",
	)
	got := CommandSections([]byte(src), "en")
	want := []CommandSection{
		{Paths: []string{""}, Flags: []string{"version", "verbose"}},
		{Paths: []string{"broker list"}, Flags: []string{"alias"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CommandSections = %#v, want %#v", got, want)
	}
}

// TestCommandSectionsSectionBoundaries checks that a non-command level-2
// heading ends a section and a level-3 heading does not.
func TestCommandSectionsSectionBoundaries(t *testing.T) {
	src := page(
		"## peers broker list",
		"",
		"- `--alias <alias>` sets the alias.",
		"",
		"### Detail",
		"",
		"- `--detail` stays in the section.",
		"",
		"## Broker administration commands",
		"",
		"- `--ignored` follows a non-command heading.",
	)
	got := CommandSections([]byte(src), "en")
	want := []CommandSection{
		{Paths: []string{"broker list"}, Flags: []string{"alias", "detail"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CommandSections = %#v, want %#v", got, want)
	}
}

// TestCommandSectionsFlagRuns checks that a flag run continues across a
// shorthand, across the language's conjunction, and across a shorthand that is
// followed by the conjunction.
func TestCommandSectionsFlagRuns(t *testing.T) {
	cases := []struct {
		name string
		lang string
		src  string
		want []string
	}{
		{
			name: "en",
			lang: "en",
			src: page(
				"## peers update",
				"",
				"- `--yes` (`-y`) skips the question.",
				"- `--system` and `--user` select the level.",
			),
			want: []string{"yes", "system", "user"},
		},
		{
			name: "zh",
			lang: "zh",
			src: page(
				"## peers update",
				"",
				"- `--yes`（`-y`）跳过确认。",
				"- `--system` 与 `--user` 选择级别。",
			),
			want: []string{"yes", "system", "user"},
		},
		{
			name: "en shorthand then conjunction",
			lang: "en",
			src: page(
				"## peers update",
				"",
				"- `--yes` (`-y`) and `--force` skip.",
			),
			want: []string{"yes", "force"},
		},
		{
			name: "zh shorthand then conjunction",
			lang: "zh",
			src: page(
				"## peers update",
				"",
				"- `--yes`（`-y`）与 `--force` 跳过。",
			),
			want: []string{"yes", "force"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CommandSections([]byte(tc.src), tc.lang)
			want := []CommandSection{
				{Paths: []string{"update"}, Flags: tc.want},
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("CommandSections = %#v, want %#v", got, want)
			}
		})
	}
}

// TestCommandSectionsIgnoresUndocumentedFlagSpans checks that a flag code span
// in prose, and one later in a list item, are not counted.
func TestCommandSectionsIgnoresUndocumentedFlagSpans(t *testing.T) {
	src := page(
		"## peers broker list",
		"",
		"The `--verbose` option is not documented here.",
		"",
		"`--verbose` is described in prose.",
		"",
		"- `--alias <alias>` sets the alias; `--extra` is mentioned later.",
	)
	got := CommandSections([]byte(src), "en")
	want := []CommandSection{
		{Paths: []string{"broker list"}, Flags: []string{"alias"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CommandSections = %#v, want %#v", got, want)
	}
}

// TestCommandSectionsSkipsFencedBlocks checks that a list item inside a fenced
// block is not counted.
func TestCommandSectionsSkipsFencedBlocks(t *testing.T) {
	src := page(
		"## peers broker list",
		"",
		"```text",
		"- `--fenced` is example output.",
		"```",
		"",
		"- `--alias <alias>` is real.",
	)
	got := CommandSections([]byte(src), "en")
	want := []CommandSection{
		{Paths: []string{"broker list"}, Flags: []string{"alias"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CommandSections = %#v, want %#v", got, want)
	}
}

// TestCommandSectionsFenceClosing checks that a fence closes only on a line
// with at least as many backticks as its opening and nothing else, so an info
// string inside the fence does not end it.
func TestCommandSectionsFenceClosing(t *testing.T) {
	src := page(
		"## peers broker list",
		"",
		"```",
		"```text",
		"- `--ghost` is example output.",
		"```",
		"",
		"- `--alias <alias>` is real.",
	)
	got := CommandSections([]byte(src), "en")
	want := []CommandSection{
		{Paths: []string{"broker list"}, Flags: []string{"alias"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CommandSections = %#v, want %#v", got, want)
	}
}

// TestParams checks the required and optional marks of both languages, that a
// code span without a group is skipped, and that an unknown mark is an error.
func TestParams(t *testing.T) {
	cases := []struct {
		name    string
		cell    string
		lang    string
		want    []Param
		wantErr bool
	}{
		{
			name: "en both marks",
			cell: "`to` (string, required): the address. `key` (string, optional): a key such as `broker.alias`.",
			lang: "en",
			want: []Param{{Name: "to", Required: true}, {Name: "key", Required: false}},
		},
		{
			name: "zh both marks",
			cell: "`to`（字符串，必填）：地址。`key`（字符串，可选）：一个键，例如 `broker.alias`。",
			lang: "zh",
			want: []Param{{Name: "to", Required: true}, {Name: "key", Required: false}},
		},
		{
			name: "spaces before the group",
			cell: "`key`   (string, optional): one.",
			lang: "en",
			want: []Param{{Name: "key", Required: false}},
		},
		{
			name:    "unknown en mark",
			cell:    "`to` (string, mandatory): the address.",
			lang:    "en",
			wantErr: true,
		},
		{
			name:    "unknown zh mark",
			cell:    "`to`（字符串，必须）：地址。",
			lang:    "zh",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Params(tc.cell, tc.lang)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Params(%q) = %#v, want an error", tc.cell, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Params(%q) error: %v", tc.cell, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Params(%q) = %#v, want %#v", tc.cell, got, tc.want)
			}
		})
	}
}

// TestFirstTable checks that FirstTable returns the first of two tables and
// that a page with no table is an error.
func TestFirstTable(t *testing.T) {
	t.Run("first of two", func(t *testing.T) {
		src := page(
			"Intro.",
			"",
			"| A | B |",
			"|---|---|",
			"| 1 | 2 |",
			"|  3  |  4  |",
			"",
			"| C | D |",
			"|---|---|",
			"| 5 | 6 |",
		)
		got, err := FirstTable([]byte(src))
		if err != nil {
			t.Fatalf("FirstTable error: %v", err)
		}
		want := [][]string{{"1", "2"}, {"3", "4"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("FirstTable = %#v, want %#v", got, want)
		}
	})
	t.Run("none", func(t *testing.T) {
		src := page(
			"No tables here.",
			"",
			"```text",
			"| A | B |",
			"|---|---|",
			"| 1 | 2 |",
			"```",
		)
		if got, err := FirstTable([]byte(src)); err == nil {
			t.Fatalf("FirstTable = %#v, want an error", got)
		}
	})
}

// TestCodeSpan checks a cell that is exactly one code span and cells that are
// not.
func TestCodeSpan(t *testing.T) {
	cases := []struct {
		cell string
		want string
		ok   bool
	}{
		{"`broker.alias`", "broker.alias", true},
		{"  `key`  ", "key", true},
		{"`broker.alias` (string, required)", "", false},
		{"broker.alias", "", false},
		{"``", "", false},
	}
	for _, tc := range cases {
		got, ok := CodeSpan(tc.cell)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("CodeSpan(%q) = %q, %v; want %q, %v", tc.cell, got, ok, tc.want, tc.ok)
		}
	}
}

// TestWords checks both vocabularies and that an unknown language is refused.
func TestWords(t *testing.T) {
	en, ok := Words("en")
	if !ok {
		t.Fatal("Words(en) is not known")
	}
	wantEn := Vocabulary{Required: "required", Optional: "optional", Empty: "empty", Derived: "derived", And: "and"}
	if en != wantEn {
		t.Fatalf("Words(en) = %#v, want %#v", en, wantEn)
	}
	zh, ok := Words("zh")
	if !ok {
		t.Fatal("Words(zh) is not known")
	}
	wantZh := Vocabulary{Required: "必填", Optional: "可选", Empty: "空", Derived: "推导得出", And: "与"}
	if zh != wantZh {
		t.Fatalf("Words(zh) = %#v, want %#v", zh, wantZh)
	}
	if _, ok := Words("fr"); ok {
		t.Fatal("Words(fr) is known")
	}
}

// TestDiff checks that the set differences are returned sorted.
func TestDiff(t *testing.T) {
	missing, extra := Diff([]string{"b", "a", "c"}, []string{"c", "d", "a"})
	if !reflect.DeepEqual(missing, []string{"b"}) || !reflect.DeepEqual(extra, []string{"d"}) {
		t.Fatalf("Diff = %v, %v; want [b], [d]", missing, extra)
	}
	missing, extra = Diff([]string{"a", "a"}, nil)
	if !reflect.DeepEqual(missing, []string{"a"}) || len(extra) != 0 {
		t.Fatalf("Diff = %v, %v; want [a], []", missing, extra)
	}
}

// TestRead checks that Read returns a reference page's bytes.
func TestRead(t *testing.T) {
	src := Read(t, "en", "files")
	if len(src) == 0 {
		t.Fatal("Read returned no bytes")
	}
}
