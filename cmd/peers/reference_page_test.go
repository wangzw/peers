package main

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/wangzw/peers/internal/testkit/refpage"
)

type refProblem struct {
	lang    string
	command string
	item    string
}

func (p refProblem) String() string {
	return fmt.Sprintf("%s: %s: %s", p.lang, p.command, p.item)
}

// TestReferencePageCLI checks that the English and Chinese command reference
// pages agree with the command tree.
func TestReferencePageCLI(t *testing.T) {
	a, _, _, _ := newDispatchApp("dev")
	root := a.newRootCommand()

	for _, lang := range refpage.Langs {
		t.Run(lang, func(t *testing.T) {
			problems := referenceCLIProblems(refpage.Read(t, lang, "cli"), lang, root)
			for _, problem := range problems {
				t.Error(problem)
			}
		})
	}
}

// TestReferencePageCLIFixtures checks that the comparison reports each planted
// disagreement in the English and Chinese fixture pages.
func TestReferencePageCLIFixtures(t *testing.T) {
	for _, lang := range refpage.Langs {
		t.Run(lang, func(t *testing.T) {
			base := referenceFixturePage(lang)
			if problems := referenceCLIProblems([]byte(base), lang, newReferenceFixtureTree()); len(problems) != 0 {
				t.Fatalf("the fixture page should match the fixture tree, got %v", problems)
			}

			cases := []struct {
				name     string
				page     func(string) string
				tree     func(*cobra.Command)
				command  string
				item     string
				wantNone bool
			}{
				{
					name:    "command heading removed",
					page:    func(src string) string { return dropSection(src, "## peers demo build") },
					command: "demo build",
					item:    "no section documents this command",
				},
				{
					name:    "nonexistent command heading added",
					page:    func(src string) string { return src + "\n## peers demo deploy\n\nDeploys the demo.\n" },
					command: "demo deploy",
					item:    "no such command",
				},
				{
					name:    "flag list item removed",
					page:    func(src string) string { return dropLinePrefix(src, "- `--force`") },
					command: "demo build",
					item:    "missing flag --force",
				},
				{
					name:    "nonexistent flag list item added",
					page:    func(src string) string { return src + "- `--dry-run` does nothing.\n" },
					command: "demo build",
					item:    "extra flag --dry-run",
				},
				{
					name: "grouped heading name removed",
					page: func(src string) string {
						return strings.Replace(src, "start, stop and restart", "start and restart", 1)
					},
					command: "demo stop",
					item:    "no section documents this command",
				},
				{
					name:    "root flag heading removed",
					page:    func(src string) string { return dropSection(src, "## peers --version") },
					command: "peers",
					item:    "missing flag --version",
				},
				{
					name: "flag on one command of a grouped heading",
					tree: func(root *cobra.Command) {
						fixtureCommand(root, "demo restart").Flags().Bool("wait", false, "wait for the restart")
					},
					command: "demo restart",
					item:    "missing flag --wait",
				},
				{
					name: "hidden flag is not required",
					tree: func(root *cobra.Command) {
						hideFixtureFlag(fixtureCommand(root, "demo build"), "trace")
					},
					wantNone: true,
				},
				{
					name: "documented hidden flag is not extra",
					tree: func(root *cobra.Command) {
						hideFixtureFlag(fixtureCommand(root, "demo build"), "trace")
					},
					page:     func(src string) string { return src + "- `--trace` records a trace.\n" },
					wantNone: true,
				},
				{
					name:     "section without flag items",
					page:     func(src string) string { return src + "\n## peers demo\n\nRuns the demo.\n" },
					wantNone: true,
				},
				{
					name: "hidden subcommand with flags",
					tree: func(root *cobra.Command) {
						debug := &cobra.Command{Use: "debug", Hidden: true}
						debug.Flags().Bool("verbose", false, "print more")
						fixtureCommand(root, "demo").AddCommand(debug)
					},
					page:     func(src string) string { return src + "\n## peers demo\n\nRuns the demo.\n" },
					wantNone: true,
				},
				{
					name: "command persistent flag",
					tree: func(root *cobra.Command) {
						fixtureCommand(root, "demo build").PersistentFlags().Bool("deep", false, "build deeper")
					},
					command: "demo build",
					item:    "missing flag --deep",
				},
				{
					name: "command with a flag needs its own section",
					tree: func(root *cobra.Command) {
						fixtureCommand(root, "demo").Flags().Bool("verbose", false, "print more")
					},
					command: "demo",
					item:    "no section documents this command",
				},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					tree := newReferenceFixtureTree()
					if tc.tree != nil {
						tc.tree(tree)
					}
					src := base
					if tc.page != nil {
						src = tc.page(src)
					}
					problems := referenceCLIProblems([]byte(src), lang, tree)
					if tc.wantNone {
						if len(problems) != 0 {
							t.Fatalf("got problems %v, want none", problems)
						}
						return
					}
					assertReferenceProblem(t, problems, lang, tc.command, tc.item)
				})
			}
		})
	}
}

func referenceCLIProblems(src []byte, lang string, root *cobra.Command) []refProblem {
	sections := refpage.CommandSections(src, lang)
	commands := commandIndex(root)

	named := make(map[string]bool)
	for _, section := range sections {
		for _, path := range section.Paths {
			if path != "" {
				named[path] = true
			}
		}
	}

	var problems []refProblem
	for _, section := range sections {
		for _, path := range section.Paths {
			if path == "" {
				continue
			}
			if _, ok := commands[path]; !ok {
				problems = append(problems, refProblem{lang, path, "no such command in the command tree"})
			}
		}
	}

	for _, path := range sortedPaths(commands) {
		if commands[path].Hidden {
			continue
		}
		if !commandCovered(path, named, commands) {
			problems = append(problems, refProblem{lang, path, "no section documents this command"})
		}
	}

	return append(problems, flagProblems(sections, lang, root, commands, named)...)
}

func commandIndex(root *cobra.Command) map[string]*cobra.Command {
	index := make(map[string]*cobra.Command)
	prefix := root.CommandPath() + " "
	var walk func(*cobra.Command)
	walk = func(cmd *cobra.Command) {
		for _, child := range cmd.Commands() {
			index[strings.TrimPrefix(child.CommandPath(), prefix)] = child
			walk(child)
		}
	}
	walk(root)
	return index
}

func sortedPaths(commands map[string]*cobra.Command) []string {
	paths := make([]string, 0, len(commands))
	for path := range commands {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func commandCovered(path string, named map[string]bool, commands map[string]*cobra.Command) bool {
	if named[path] {
		return true
	}
	parent := parentPath(path)
	coveredByAncestor := parent != "" && named[parent]
	// A command with visible flags of its own needs a section that compares
	// them: one naming it, or one naming its nearest ancestor below the root,
	// which merges the flags of its unnamed descendants. A section of a
	// descendant covers only commands without such flags.
	if cmd, ok := commands[path]; ok && len(visibleFlagNames(cmd)) > 0 {
		return coveredByAncestor
	}
	if coveredByAncestor {
		return true
	}
	if !hasVisibleChild(path, commands) {
		return false
	}
	prefix := path + " "
	for candidate := range commands {
		if strings.HasPrefix(candidate, prefix) && named[candidate] {
			return true
		}
	}
	return false
}

func parentPath(path string) string {
	if i := strings.LastIndexByte(path, ' '); i >= 0 {
		return path[:i]
	}
	return ""
}

func hasVisibleChild(path string, commands map[string]*cobra.Command) bool {
	for candidate, cmd := range commands {
		if !cmd.Hidden && parentPath(candidate) == path {
			return true
		}
	}
	return false
}

func flagProblems(sections []refpage.CommandSection, lang string, root *cobra.Command, commands map[string]*cobra.Command, named map[string]bool) []refProblem {
	var problems []refProblem
	haveRootFlags := false
	var rootFlags []string

	for _, section := range sections {
		if len(section.Paths) == 1 && section.Paths[0] == "" {
			haveRootFlags = true
			rootFlags = append(rootFlags, section.Flags...)
			continue
		}
		for _, path := range section.Paths {
			cmd, ok := commands[path]
			if !ok {
				continue
			}
			want := availableFlags(cmd, section.Flags)
			for descendant, child := range commands {
				if child.Hidden || named[descendant] || !isDescendant(descendant, path) {
					continue
				}
				want = append(want, availableFlags(child, nil)...)
			}
			problems = append(problems, diffProblems(lang, path, want, section.Flags)...)
		}
	}

	rootPath := root.CommandPath()
	if !haveRootFlags {
		rootFlags = nil
	}
	problems = append(problems, diffProblems(lang, rootPath, availableFlags(root, rootFlags), rootFlags)...)
	return problems
}

func isDescendant(path, ancestor string) bool {
	return strings.HasPrefix(path, ancestor+" ")
}

func diffProblems(lang, command string, want, got []string) []refProblem {
	missing, extra := refpage.Diff(want, got)
	var problems []refProblem
	for _, name := range missing {
		problems = append(problems, refProblem{lang, command, "missing flag --" + name})
	}
	for _, name := range extra {
		problems = append(problems, refProblem{lang, command, "extra flag --" + name})
	}
	return problems
}

func availableFlags(cmd *cobra.Command, documented []string) []string {
	names := visibleFlagNames(cmd)
	local := cmd.LocalFlags()
	for _, name := range documented {
		if containsString(names, name) {
			continue
		}
		if flag := local.Lookup(name); flag != nil && flag.Hidden {
			names = append(names, name)
		}
	}
	return names
}

// visibleFlagNames returns the long names of the visible flags set on cmd
// itself, its persistent flags included and flags inherited from a parent
// excluded.
func visibleFlagNames(cmd *cobra.Command) []string {
	var names []string
	for _, line := range strings.Split(cmd.LocalFlags().FlagUsages(), "\n") {
		spec, _, _ := strings.Cut(strings.TrimLeft(line, " "), "  ")
		for _, field := range strings.Fields(spec) {
			name, ok := strings.CutPrefix(field, "--")
			if !ok {
				continue
			}
			if i := strings.IndexAny(name, "=["); i >= 0 {
				name = name[:i]
			}
			if name != "" && name != "help" {
				names = append(names, name)
			}
			break
		}
	}
	return names
}

func containsString(items []string, item string) bool {
	for _, candidate := range items {
		if candidate == item {
			return true
		}
	}
	return false
}

func assertReferenceProblem(t *testing.T, problems []refProblem, lang, command, item string) {
	t.Helper()
	for _, problem := range problems {
		if problem.lang == lang && problem.command == command && strings.Contains(problem.item, item) {
			return
		}
	}
	t.Fatalf("no problem names command %q and item %q, got %v", command, item, problems)
}

// fixtureCommand returns the command at path in the fixture tree.
func fixtureCommand(root *cobra.Command, path string) *cobra.Command {
	cmd := root
	for _, name := range strings.Fields(path) {
		for _, child := range cmd.Commands() {
			if child.Name() == name {
				cmd = child
				break
			}
		}
	}
	return cmd
}

// hideFixtureFlag adds a hidden flag to cmd.
func hideFixtureFlag(cmd *cobra.Command, name string) {
	cmd.Flags().Bool(name, false, "hidden from the help output")
	if err := cmd.Flags().MarkHidden(name); err != nil {
		panic(err)
	}
}

func newReferenceFixtureTree() *cobra.Command {
	root := &cobra.Command{Use: "peers"}
	root.Flags().Bool("version", false, "print the version and exit")

	demo := &cobra.Command{Use: "demo"}
	for _, name := range []string{"start", "stop", "restart"} {
		cmd := &cobra.Command{Use: name}
		cmd.Flags().Bool("system", false, "select the system level")
		cmd.Flags().Bool("user", false, "select the user level")
		demo.AddCommand(cmd)
	}
	build := &cobra.Command{Use: "build"}
	build.Flags().Bool("force", false, "overwrite the output")
	demo.AddCommand(build)

	root.AddCommand(demo)
	return root
}

func referenceFixturePage(lang string) string {
	if lang == "zh" {
		return `# 命令行

## peers --version

打印版本后退出。

## peers demo start, stop and restart

启动、停止或重启演示。

- ` + "`--system` 与 `--user` 选择级别。" + `

## peers demo build

构建演示。

- ` + "`--force` 覆盖输出。" + `
`
	}
	return `# Command line

## peers --version

Prints the version and exits.

## peers demo start, stop and restart

Starts, stops or restarts the demo.

- ` + "`--system` and `--user` select the level." + `

## peers demo build

Builds the demo.

- ` + "`--force` overwrites the output." + `
`
}

func dropSection(src, heading string) string {
	var out []string
	dropping := false
	for _, line := range strings.Split(src, "\n") {
		if line == heading {
			dropping = true
			continue
		}
		if dropping {
			if !strings.HasPrefix(line, "## ") {
				continue
			}
			dropping = false
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func dropLinePrefix(src, prefix string) string {
	var out []string
	for _, line := range strings.Split(src, "\n") {
		if strings.HasPrefix(line, prefix) {
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}
