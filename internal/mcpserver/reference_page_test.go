package mcpserver

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/testkit/refpage"
)

// toolInventory maps a tool name to its parameters and whether each is required.
type toolInventory map[string]map[string]bool

func registeredTools(t *testing.T) toolInventory {
	t.Helper()
	cs := connectTestClient(t, &stubToolSet{})
	tools := listTools(t, cs)
	inventory := make(toolInventory, len(tools))
	for name, tool := range tools {
		props, required := schemaProps(t, tool)
		params := make(map[string]bool, len(props))
		for prop := range props {
			params[prop] = false
		}
		for _, prop := range required {
			params[prop] = true
		}
		inventory[name] = params
	}
	return inventory
}

func projectConfigYAMLKeys() []string {
	return yamlFieldKeys(reflect.TypeOf(projectConfigDocument{}))
}

// yamlFieldKeys returns the YAML keys of typ's exported fields. A field without
// a yaml name is keyed by its lowercased field name, which is yaml.v3's default;
// a field tagged "-" has no key.
func yamlFieldKeys(typ reflect.Type) []string {
	keys := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := field.Tag.Get("yaml")
		if j := strings.IndexByte(tag, ','); j >= 0 {
			tag = tag[:j]
		}
		switch tag {
		case "-":
			continue
		case "":
			tag = strings.ToLower(field.Name)
		}
		keys = append(keys, tag)
	}
	return keys
}

func mapNames[V any](m map[string]V) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	return names
}

// compareToolPage returns the differences between mcp-tools.md and the tool
// inventory, each naming the language, the tool and the item.
func compareToolPage(src []byte, lang string, inventory toolInventory) []string {
	rows, err := refpage.FirstTable(src)
	if err != nil {
		return []string{fmt.Sprintf("%s: mcp-tools.md: %v", lang, err)}
	}
	page := make(toolInventory, len(rows))
	var problems []string
	for i, row := range rows {
		if len(row) < 3 {
			problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: row %d has %d columns, want at least 3", lang, i+1, len(row)))
			continue
		}
		name, ok := refpage.CodeSpan(row[0])
		if !ok {
			problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: row %d: tool cell %q is not one code span", lang, i+1, row[0]))
			continue
		}
		if _, dup := page[name]; dup {
			problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: tool %q appears more than once", lang, name))
			continue
		}
		params, err := refpage.Params(row[2], lang)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: row %d: %v", lang, i+1, err))
			continue
		}
		marks := make(map[string]bool, len(params))
		for _, param := range params {
			if _, dup := marks[param.Name]; dup {
				problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: tool %q: parameter %q appears more than once", lang, name, param.Name))
				continue
			}
			marks[param.Name] = param.Required
		}
		page[name] = marks
	}

	missing, extra := refpage.Diff(mapNames(inventory), mapNames(page))
	for _, name := range missing {
		problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: tool %q is missing from the page", lang, name))
	}
	for _, name := range extra {
		problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: tool %q is not registered by the MCP server", lang, name))
	}
	for name, want := range inventory {
		got, ok := page[name]
		if !ok {
			continue
		}
		missingParams, extraParams := refpage.Diff(mapNames(want), mapNames(got))
		for _, param := range missingParams {
			problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: tool %q: parameter %q is missing from the page", lang, name, param))
		}
		for _, param := range extraParams {
			problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: tool %q: parameter %q is not an input of the tool", lang, name, param))
		}
		for param, wantRequired := range want {
			gotRequired, ok := got[param]
			if !ok || gotRequired == wantRequired {
				continue
			}
			if wantRequired {
				problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: tool %q: parameter %q is optional in the page but required in the schema", lang, name, param))
			} else {
				problems = append(problems, fmt.Sprintf("%s: mcp-tools.md: tool %q: parameter %q is required in the page but optional in the schema", lang, name, param))
			}
		}
	}
	sort.Strings(problems)
	return problems
}

// compareProjectKeyPage returns the differences between project-config.md and
// the project configuration keys, each naming the language and the key.
func compareProjectKeyPage(src []byte, lang string, keys []string) []string {
	rows, err := refpage.FirstTable(src)
	if err != nil {
		return []string{fmt.Sprintf("%s: project-config.md: %v", lang, err)}
	}
	page := make([]string, 0, len(rows))
	seen := make(map[string]bool, len(rows))
	var problems []string
	for i, row := range rows {
		key, ok := refpage.CodeSpan(row[0])
		if !ok {
			problems = append(problems, fmt.Sprintf("%s: project-config.md: row %d: key cell %q is not one code span", lang, i+1, row[0]))
			continue
		}
		if seen[key] {
			problems = append(problems, fmt.Sprintf("%s: project-config.md: key %q appears more than once", lang, key))
			continue
		}
		seen[key] = true
		page = append(page, key)
	}
	missing, extra := refpage.Diff(keys, page)
	for _, key := range missing {
		problems = append(problems, fmt.Sprintf("%s: project-config.md: key %q is missing from the page", lang, key))
	}
	for _, key := range extra {
		problems = append(problems, fmt.Sprintf("%s: project-config.md: key %q is not a project configuration key", lang, key))
	}
	sort.Strings(problems)
	return problems
}

// TestReferencePageTools checks that mcp-tools.md matches the registered tools,
// their parameters and each parameter's required mark in both languages.
func TestReferencePageTools(t *testing.T) {
	inventory := registeredTools(t)
	for _, lang := range refpage.Langs {
		for _, problem := range compareToolPage(refpage.Read(t, lang, "mcp-tools"), lang, inventory) {
			t.Error(problem)
		}
	}
}

// TestReferencePageProjectKeys checks that project-config.md lists the project
// configuration keys in both languages.
func TestReferencePageProjectKeys(t *testing.T) {
	keys := projectConfigYAMLKeys()
	for _, lang := range refpage.Langs {
		for _, problem := range compareProjectKeyPage(refpage.Read(t, lang, "project-config"), lang, keys) {
			t.Error(problem)
		}
	}
}

type fixtureTool struct {
	name   string
	params []fixtureParam
}

type fixtureParam struct {
	name     string
	required bool
}

func toolFixturePage(lang string, tools []fixtureTool) []byte {
	vocab, _ := refpage.Words(lang)
	sep := ", "
	header := "| Tool | What it does | Parameters | Config-file permission |\n|---|---|---|---|\n"
	if lang == "zh" {
		sep = "、"
		header = "| 工具 | 作用 | 参数 | 配置文件权限 |\n|---|---|---|---|\n"
	}
	var b strings.Builder
	b.WriteString(header)
	for _, tool := range tools {
		cells := make([]string, 0, len(tool.params))
		for _, param := range tool.params {
			cells = append(cells, fixtureParamCell(lang, vocab, param))
		}
		fmt.Fprintf(&b, "| `%s` | x | %s | None |\n", tool.name, strings.Join(cells, sep))
	}
	return []byte(b.String())
}

func fixtureParamCell(lang string, vocab refpage.Vocabulary, param fixtureParam) string {
	mark := vocab.Optional
	if param.required {
		mark = vocab.Required
	}
	if lang == "zh" {
		return fmt.Sprintf("`%s`（类型，%s）", param.name, mark)
	}
	return fmt.Sprintf("`%s` (type, %s)", param.name, mark)
}

func cloneTools(tools []fixtureTool) []fixtureTool {
	out := make([]fixtureTool, len(tools))
	for i, tool := range tools {
		out[i] = fixtureTool{name: tool.name, params: append([]fixtureParam(nil), tool.params...)}
	}
	return out
}

// wantProblem checks that problems holds one starting with lang's prefix and
// containing every word in want.
func wantProblem(t *testing.T, problems []string, lang string, want ...string) {
	t.Helper()
	for _, problem := range problems {
		if !strings.HasPrefix(problem, lang+": ") {
			continue
		}
		matches := true
		for _, w := range want {
			if !strings.Contains(problem, w) {
				matches = false
				break
			}
		}
		if matches {
			return
		}
	}
	t.Errorf("no %s problem contains %q; problems: %v", lang, want, problems)
}

// TestReferencePageToolsFixtures checks that compareToolPage names each planted
// drift in both languages.
func TestReferencePageToolsFixtures(t *testing.T) {
	inventory := toolInventory{
		"list_agents":  {"topic": false},
		"send_message": {"to": true, "message": true},
	}
	base := []fixtureTool{
		{name: "list_agents", params: []fixtureParam{{name: "topic"}}},
		{name: "send_message", params: []fixtureParam{{name: "to", required: true}, {name: "message", required: true}}},
	}
	cases := []struct {
		name   string
		mutate func([]fixtureTool) []fixtureTool
		want   []string
	}{
		{
			name: "tool row removed",
			mutate: func(tools []fixtureTool) []fixtureTool {
				return tools[1:]
			},
			want: []string{`tool "list_agents" is missing`},
		},
		{
			name: "unknown tool row added",
			mutate: func(tools []fixtureTool) []fixtureTool {
				return append(tools, fixtureTool{name: "bogus_tool", params: []fixtureParam{{name: "extra"}}})
			},
			want: []string{`tool "bogus_tool" is not registered`},
		},
		{
			name: "parameter removed",
			mutate: func(tools []fixtureTool) []fixtureTool {
				tools[0].params = nil
				return tools
			},
			want: []string{`parameter "topic" is missing`},
		},
		{
			name: "unknown parameter added",
			mutate: func(tools []fixtureTool) []fixtureTool {
				tools[0].params = append(tools[0].params, fixtureParam{name: "extra"})
				return tools
			},
			want: []string{`parameter "extra" is not an input`},
		},
		{
			name: "required parameter marked optional",
			mutate: func(tools []fixtureTool) []fixtureTool {
				tools[1].params[0].required = false
				return tools
			},
			want: []string{`parameter "to" is optional in the page but required in the schema`},
		},
		{
			name: "optional parameter marked required",
			mutate: func(tools []fixtureTool) []fixtureTool {
				tools[0].params[0].required = true
				return tools
			},
			want: []string{`parameter "topic" is required in the page but optional in the schema`},
		},
		{
			name: "duplicate tool row",
			mutate: func(tools []fixtureTool) []fixtureTool {
				return append(tools, fixtureTool{name: "list_agents", params: []fixtureParam{{name: "topic"}}})
			},
			want: []string{`tool "list_agents" appears more than once`},
		},
		{
			name: "duplicate parameter",
			mutate: func(tools []fixtureTool) []fixtureTool {
				tools[0].params = append(tools[0].params, fixtureParam{name: "topic"})
				return tools
			},
			want: []string{`parameter "topic" appears more than once`},
		},
	}
	for _, lang := range refpage.Langs {
		if problems := compareToolPage(toolFixturePage(lang, cloneTools(base)), lang, inventory); len(problems) != 0 {
			t.Errorf("%s: unmodified fixture reports %v", lang, problems)
		}
		for _, tc := range cases {
			t.Run(lang+"/"+tc.name, func(t *testing.T) {
				problems := compareToolPage(toolFixturePage(lang, tc.mutate(cloneTools(base))), lang, inventory)
				wantProblem(t, problems, lang, tc.want...)
			})
		}
	}
}

// TestReferencePageProjectConfigYAMLKeys checks that a field without a yaml
// name is keyed by its lowercased field name and that a "-" field has no key.
func TestReferencePageProjectConfigYAMLKeys(t *testing.T) {
	type fixtureDocument struct {
		Topics   []string `yaml:"topics"`
		AutoJoin *bool    `yaml:"auto_join"`
		Path     string
		Renamed  string `yaml:",omitempty"`
		Ignored  string `yaml:"-"`
	}
	got := yamlFieldKeys(reflect.TypeOf(fixtureDocument{}))
	want := []string{"topics", "auto_join", "path", "renamed"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("yamlFieldKeys = %v, want %v", got, want)
	}
}

func projectKeyFixturePage(lang string, keys []string) []byte {
	header := "| Key | Type | Default | Meaning |\n|---|---|---|---|\n"
	if lang == "zh" {
		header = "| 键 | 类型 | 默认值 | 含义 |\n|---|---|---|---|\n"
	}
	var b strings.Builder
	b.WriteString(header)
	for _, key := range keys {
		fmt.Fprintf(&b, "| `%s` | x | x | x |\n", key)
	}
	return []byte(b.String())
}

// TestReferencePageProjectKeyFixtures checks that compareProjectKeyPage names
// each planted drift in both languages.
func TestReferencePageProjectKeyFixtures(t *testing.T) {
	keys := []string{"topics", "auto_join"}
	cases := []struct {
		name string
		page []string
		want string
	}{
		{"key row removed", []string{"auto_join"}, `key "topics" is missing`},
		{"unknown key row added", []string{"topics", "auto_join", "bogus_key"}, `key "bogus_key" is not a project configuration key`},
		{"duplicate key row", []string{"topics", "auto_join", "topics"}, `key "topics" appears more than once`},
	}
	for _, lang := range refpage.Langs {
		if problems := compareProjectKeyPage(projectKeyFixturePage(lang, keys), lang, keys); len(problems) != 0 {
			t.Errorf("%s: unmodified fixture reports %v", lang, problems)
		}
		for _, tc := range cases {
			t.Run(lang+"/"+tc.name, func(t *testing.T) {
				problems := compareProjectKeyPage(projectKeyFixturePage(lang, tc.page), lang, keys)
				wantProblem(t, problems, lang, tc.want)
			})
		}
	}
}
