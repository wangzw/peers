// Command coverreport merges Go coverage data and reports statement coverage
// per package under internal/ and cmd/peers.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

const usageText = `usage: coverreport -i DIR[,DIR...] [-root DIR] [-o FILE] [-format text|markdown] [-total PCT] [-package PCT]

The status column shows ok or FAIL only for a threshold that is given:
-package sets package rows and -total sets the total row. A row without a
threshold shows -.

  -i DIR[,DIR...]  covdata directories, comma separated (required)
  -root DIR        module root (default ".")
  -o FILE          write the merged profile to this file
  -format FORMAT   output format: text or markdown (default "text")
  -total PCT       total coverage floor in percent
  -package PCT     per-package coverage floor in percent
`

type config struct {
	inputs []string
	root   string
	out    string
	format string
	total  *int // percent scaled by ten
	pkg    *int // percent scaled by ten
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func parseConfig(args []string) (*config, error) {
	fs := flag.NewFlagSet("coverreport", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var inputs stringList
	fs.Var(&inputs, "i", "covdata directories, comma separated (required)")
	root := fs.String("root", ".", "module root")
	out := fs.String("o", "", "write the merged profile to this file")
	format := fs.String("format", "text", "output format: text or markdown; the status column shows ok or FAIL only for a threshold that is given, and - otherwise")
	total := fs.String("total", "", "total coverage floor in percent; gives the total row an ok/FAIL status")
	pkg := fs.String("package", "", "per-package coverage floor in percent; gives each package row an ok/FAIL status")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	cfg := &config{root: *root, out: *out, format: *format}
	for _, v := range inputs {
		for _, dir := range strings.Split(v, ",") {
			if dir == "" {
				return nil, errors.New("-i contains an empty directory")
			}
			cfg.inputs = append(cfg.inputs, dir)
		}
	}
	if len(cfg.inputs) == 0 {
		return nil, errors.New("-i is required")
	}
	if cfg.format != "text" && cfg.format != "markdown" {
		return nil, fmt.Errorf("-format must be text or markdown, not %q", cfg.format)
	}
	// A flag counts as given when it appears on the command line, not when its
	// value is non-empty: an explicit -total "" is a mistake and must fail
	// rather than silently leave the total unenforced.
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })
	if given["total"] {
		v, err := parsePercent(*total)
		if err != nil {
			return nil, fmt.Errorf("-total: %v", err)
		}
		cfg.total = &v
	}
	if given["package"] {
		v, err := parsePercent(*pkg)
		if err != nil {
			return nil, fmt.Errorf("-package: %v", err)
		}
		cfg.pkg = &v
	}
	return cfg, nil
}

// parsePercent parses a percentage between 0 and 100 with at most one
// fractional digit and returns it scaled by ten.
func parsePercent(s string) (int, error) {
	whole, frac := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		whole, frac = s[:i], s[i+1:]
		if len(frac) != 1 {
			return 0, fmt.Errorf("%q: want a decimal between 0 and 100 with at most one fractional digit", s)
		}
	}
	if whole == "" {
		return 0, fmt.Errorf("%q: want a decimal between 0 and 100 with at most one fractional digit", s)
	}
	for _, r := range whole {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("%q: want a decimal between 0 and 100 with at most one fractional digit", s)
		}
	}
	n, err := strconv.Atoi(whole)
	if err != nil {
		return 0, fmt.Errorf("%q: %v", s, err)
	}
	d := 0
	if frac != "" {
		d = int(frac[0] - '0')
	}
	scaled := n*10 + d
	if scaled > 1000 {
		return 0, fmt.Errorf("%q: want a decimal between 0 and 100 with at most one fractional digit", s)
	}
	return scaled, nil
}

type profileBlock struct {
	file      string
	startLine int
	startCol  int
	endLine   int
	endCol    int
	numStmt   int
	count     int64
}

type blockKey struct {
	file      string
	startLine int
	startCol  int
	endLine   int
	endCol    int
}

func (k blockKey) position() string {
	return fmt.Sprintf("%s:%d.%d,%d.%d", k.file, k.startLine, k.startCol, k.endLine, k.endCol)
}

type block struct {
	key     blockKey
	pkg     string
	numStmt int
	count   int64
}

type packageCoverage struct {
	path       string
	statements int
	covered    int
}

type report struct {
	packages []packageCoverage
	total    packageCoverage
	blocks   []block
}

func run(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseConfig(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprint(stderr, usageText)
			return 2
		}
		_, _ = fmt.Fprintf(stderr, "coverreport: %v\n", err)
		return 2
	}
	rep, err := buildReport(cfg)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "coverreport: %v\n", err)
		return 2
	}
	if cfg.out != "" {
		if err := writeProfile(cfg.out, rep.blocks); err != nil {
			_, _ = fmt.Fprintf(stderr, "coverreport: %v\n", err)
			return 2
		}
	}
	var werr error
	if cfg.format == "markdown" {
		werr = writeMarkdown(stdout, rep, cfg)
	} else {
		werr = writeText(stdout, rep, cfg)
	}
	if werr != nil {
		_, _ = fmt.Fprintf(stderr, "coverreport: %v\n", werr)
		return 2
	}
	if belowThreshold(rep, cfg) {
		return 1
	}
	return 0
}

func buildReport(cfg *config) (*report, error) {
	for _, dir := range cfg.inputs {
		if err := validateInputDir(dir); err != nil {
			return nil, err
		}
	}
	modPath, err := modulePath(cfg.root)
	if err != nil {
		return nil, err
	}
	universe, err := listUniverse(cfg.root, modPath)
	if err != nil {
		return nil, err
	}
	text, err := covdataTextfmt(cfg.inputs)
	if err != nil {
		return nil, err
	}
	raw, err := parseProfile(strings.NewReader(text))
	if err != nil {
		return nil, err
	}
	blocks, err := filterBlocks(raw, cfg.root, modPath)
	if err != nil {
		return nil, err
	}
	merged, err := mergeBlocks(blocks)
	if err != nil {
		return nil, err
	}

	stats := map[string]*packageCoverage{}
	for _, b := range merged {
		s := stats[b.pkg]
		if s == nil {
			s = &packageCoverage{path: b.pkg}
			stats[b.pkg] = s
		}
		s.statements += b.numStmt
		if b.count > 0 {
			s.covered += b.numStmt
		}
	}

	var absent []string
	for _, p := range universe {
		if _, ok := stats[p.ImportPath]; ok {
			continue
		}
		n, err := countPackageStatements(p)
		if err != nil {
			return nil, err
		}
		if n > 0 {
			absent = append(absent, fmt.Sprintf("%s: %d statements", p.ImportPath, n))
		}
	}
	if len(absent) > 0 {
		return nil, fmt.Errorf("no coverage data for packages with statements:\n\t%s", strings.Join(absent, "\n\t"))
	}

	rep := &report{blocks: merged}
	for _, s := range stats {
		if s.statements == 0 {
			continue
		}
		rep.packages = append(rep.packages, *s)
		rep.total.statements += s.statements
		rep.total.covered += s.covered
	}
	if rep.total.statements == 0 {
		// A zero denominator makes every threshold pass at 0 >= 0, so a
		// threshold given here would report a meaningless success.
		return nil, errors.New("the merged coverage data contains no statements")
	}
	sort.Slice(rep.packages, func(i, j int) bool { return rep.packages[i].path < rep.packages[j].path })
	return rep, nil
}

func validateInputDir(dir string) error {
	st, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("-i directory %s: %v", dir, err)
	}
	if !st.IsDir() {
		return fmt.Errorf("-i path %s is not a directory", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("-i directory %s: %v", dir, err)
	}
	hasMeta, hasCounters := false, false
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "covmeta.") {
			hasMeta = true
		}
		if strings.HasPrefix(name, "covcounters.") {
			hasCounters = true
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("-i directory %s: %v", dir, err)
		}
		if info.IsDir() {
			continue
		}
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return fmt.Errorf("-i directory %s: unreadable file %s: %v", dir, name, err)
		}
		_ = f.Close()
	}
	if !hasMeta {
		return fmt.Errorf("-i directory %s has no covmeta.* file", dir)
	}
	if !hasCounters {
		return fmt.Errorf("-i directory %s has no covcounters.* file", dir)
	}
	return nil
}

func modulePath(root string) (string, error) {
	out, err := goCommand(root, "list", "-m", "-f={{.Path}}")
	if err != nil {
		return "", fmt.Errorf("go list -m in %s: %v", root, err)
	}
	modPath := strings.TrimSpace(string(out))
	if modPath == "" {
		return "", fmt.Errorf("go list -m in %s returned no module path", root)
	}
	return modPath, nil
}

type listedPackage struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	CgoFiles   []string
	Error      *struct{ Err string }
}

func listUniverse(root, modPath string) ([]listedPackage, error) {
	out, err := goCommand(root, "list", "-e", "-json", "./internal/...", "./cmd/peers")
	if err != nil {
		return nil, fmt.Errorf("go list ./internal/... ./cmd/peers in %s: %v", root, err)
	}
	var pkgs []listedPackage
	dec := json.NewDecoder(bytes.NewReader(out))
	for {
		var p listedPackage
		if err := dec.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decode go list output: %v", err)
		}
		if p.ImportPath == "" || p.Dir == "" || strings.HasPrefix(p.ImportPath, "./") {
			continue
		}
		if isTestkitPath(p.ImportPath, modPath) {
			continue
		}
		pkgs = append(pkgs, p)
	}
	return pkgs, nil
}

func isTestkitPath(importPath, modPath string) bool {
	testkit := modPath + "/internal/testkit"
	return importPath == testkit || strings.HasPrefix(importPath, testkit+"/")
}

func goCommand(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOOS=linux")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, errors.New(msg)
	}
	return stdout.Bytes(), nil
}

func covdataTextfmt(inputs []string) (string, error) {
	tmp, err := os.CreateTemp("", "coverreport-*.txt")
	if err != nil {
		return "", err
	}
	name := tmp.Name()
	_ = tmp.Close()
	defer func() { _ = os.Remove(name) }()

	cmd := exec.Command("go", "tool", "covdata", "textfmt", "-i="+strings.Join(inputs, ","), "-o="+name)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("go tool covdata textfmt: %q", msg)
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return "", fmt.Errorf("read merged profile: %v", err)
	}
	return string(data), nil
}

var profileLineRE = regexp.MustCompile(`^(.+):([0-9]+)\.([0-9]+),([0-9]+)\.([0-9]+) ([0-9]+) ([0-9]+)$`)

func parseProfile(r io.Reader) ([]profileBlock, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var blocks []profileBlock
	first := true
	lineNo := 0
	for sc.Scan() {
		line := sc.Text()
		lineNo++
		if first {
			first = false
			if strings.HasPrefix(line, "mode: ") {
				continue
			}
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		m := profileLineRE.FindStringSubmatch(line)
		if m == nil {
			return nil, fmt.Errorf("malformed profile line %d: %q", lineNo, line)
		}
		nums := make([]int, 6)
		for i := range nums {
			n, err := strconv.Atoi(m[i+2])
			if err != nil {
				return nil, fmt.Errorf("malformed profile line %d: %q: %v", lineNo, line, err)
			}
			nums[i] = n
		}
		blocks = append(blocks, profileBlock{
			file:      m[1],
			startLine: nums[0],
			startCol:  nums[1],
			endLine:   nums[2],
			endCol:    nums[3],
			numStmt:   nums[4],
			count:     int64(nums[5]),
		})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read merged profile: %v", err)
	}
	return blocks, nil
}

func filterBlocks(raw []profileBlock, root, modPath string) ([]block, error) {
	var out []block
	for _, rb := range raw {
		rel, ok := countedRelPath(root, modPath, rb.file)
		if !ok {
			continue
		}
		full := filepath.Join(root, rel)
		if _, err := os.Stat(full); err != nil {
			return nil, fmt.Errorf("profile file %s not found on disk: %v", rb.file, err)
		}
		generated, err := hasGeneratedHeader(full)
		if err != nil {
			return nil, err
		}
		if generated {
			continue
		}
		relSlash := filepath.ToSlash(rel)
		pkg := modPath + "/" + path.Dir(relSlash)
		out = append(out, block{
			key: blockKey{
				file:      rb.file,
				startLine: rb.startLine,
				startCol:  rb.startCol,
				endLine:   rb.endLine,
				endCol:    rb.endCol,
			},
			pkg:     pkg,
			numStmt: rb.numStmt,
			count:   rb.count,
		})
	}
	return out, nil
}

// countedRelPath resolves a profile file name to a path relative to the module
// root and reports whether that path belongs to the denominator.
func countedRelPath(root, modPath, file string) (string, bool) {
	var rel string
	if filepath.IsAbs(file) {
		r, err := filepath.Rel(root, file)
		if err != nil {
			return "", false
		}
		rel = r
	} else if file == modPath {
		return "", false
	} else if strings.HasPrefix(file, modPath+"/") {
		rel = filepath.FromSlash(strings.TrimPrefix(file, modPath+"/"))
	} else {
		rel = filepath.FromSlash(file)
	}
	rel = filepath.Clean(rel)
	if filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	slash := filepath.ToSlash(rel)
	if !countedPath(slash) {
		return "", false
	}
	return rel, true
}

func countedPath(rel string) bool {
	if strings.HasPrefix(rel, "internal/testkit/") {
		return false
	}
	if strings.HasPrefix(rel, "internal/") {
		return true
	}
	return strings.HasPrefix(rel, "cmd/peers/")
}

func mergeBlocks(in []block) ([]block, error) {
	byKey := map[blockKey]int{}
	var out []block
	for _, b := range in {
		if i, ok := byKey[b.key]; ok {
			prev := &out[i]
			if prev.numStmt != b.numStmt {
				return nil, fmt.Errorf("blocks at %s disagree on statement count: %d and %d", b.key.position(), prev.numStmt, b.numStmt)
			}
			if b.count > prev.count {
				prev.count = b.count
			}
			continue
		}
		byKey[b.key] = len(out)
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].key, out[j].key
		if a.file != b.file {
			return a.file < b.file
		}
		if a.startLine != b.startLine {
			return a.startLine < b.startLine
		}
		if a.startCol != b.startCol {
			return a.startCol < b.startCol
		}
		if a.endLine != b.endLine {
			return a.endLine < b.endLine
		}
		return a.endCol < b.endCol
	})
	return out, nil
}

func writeProfile(name string, blocks []block) error {
	var buf bytes.Buffer
	buf.WriteString("mode: atomic\n")
	for _, b := range blocks {
		fmt.Fprintf(&buf, "%s:%d.%d,%d.%d %d %d\n",
			b.key.file, b.key.startLine, b.key.startCol, b.key.endLine, b.key.endCol, b.numStmt, b.count)
	}
	return os.WriteFile(name, buf.Bytes(), 0o644)
}

var generatedHeaderRE = regexp.MustCompile(`^// Code generated .* DO NOT EDIT\.$`)

func hasGeneratedHeader(name string) (bool, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		return false, fmt.Errorf("read %s: %v", name, err)
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, data, parser.PackageClauseOnly)
	if err != nil {
		return false, fmt.Errorf("parse %s: %v", name, err)
	}
	pkgLine := fset.Position(f.Package).Line
	lines := bytes.Split(data, []byte("\n"))
	for i := 0; i < pkgLine-1 && i < len(lines); i++ {
		line := strings.TrimSuffix(string(lines[i]), "\r")
		if generatedHeaderRE.MatchString(line) {
			return true, nil
		}
	}
	return false, nil
}

func countPackageStatements(p listedPackage) (int, error) {
	total := 0
	files := append(append([]string(nil), p.GoFiles...), p.CgoFiles...)
	for _, name := range files {
		full := filepath.Join(p.Dir, name)
		generated, err := hasGeneratedHeader(full)
		if err != nil {
			return 0, err
		}
		if generated {
			continue
		}
		n, err := countFileStatements(full)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func countFileStatements(name string) (int, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %v", name, err)
	}
	c := &statementCounter{}
	ast.Walk(c, f)
	return c.count, nil
}

// statementCounter counts the statements cmd/cover would instrument. The
// traversal and boundary rules mirror src/cmd/cover/cover.go so that a count
// equals the statement total an instrumented build reports.
type statementCounter struct {
	count int
}

func (c *statementCounter) Visit(node ast.Node) ast.Visitor {
	switch n := node.(type) {
	case *ast.FuncDecl:
		if n.Name.Name == "_" || n.Body == nil {
			return nil
		}
		ast.Walk(c, n.Body)
		return nil
	case *ast.FuncLit:
		ast.Walk(c, n.Body)
		return nil
	case *ast.BlockStmt:
		if len(n.List) > 0 {
			switch n.List[0].(type) {
			case *ast.CaseClause:
				for _, s := range n.List {
					clause := s.(*ast.CaseClause)
					c.count += countStatements(clause.Body, clause.End(), false)
				}
				return c
			case *ast.CommClause:
				for _, s := range n.List {
					clause := s.(*ast.CommClause)
					c.count += countStatements(clause.Body, clause.End(), false)
				}
				return c
			}
		}
		c.count += countStatements(n.List, n.Rbrace+1, true)
		return c
	case *ast.IfStmt:
		if n.Init != nil {
			ast.Walk(c, n.Init)
		}
		ast.Walk(c, n.Cond)
		ast.Walk(c, n.Body)
		if n.Else == nil {
			return nil
		}
		switch e := n.Else.(type) {
		case *ast.IfStmt:
			c.count++ // the else-if runs in an implicit block of its own
			ast.Walk(c, e)
		case *ast.BlockStmt:
			ast.Walk(c, e)
		}
		return nil
	case *ast.SelectStmt:
		if n.Body == nil || len(n.Body.List) == 0 {
			return nil
		}
	case *ast.SwitchStmt:
		if n.Body == nil || len(n.Body.List) == 0 {
			if n.Init != nil {
				ast.Walk(c, n.Init)
			}
			if n.Tag != nil {
				ast.Walk(c, n.Tag)
			}
			return nil
		}
	case *ast.TypeSwitchStmt:
		if n.Body == nil || len(n.Body.List) == 0 {
			if n.Init != nil {
				ast.Walk(c, n.Init)
			}
			ast.Walk(c, n.Assign)
			return nil
		}
	}
	return c
}

// countStatements counts the statements in one basic block list the way
// cmd/cover's addCounters does. Statements whose position is at or past the
// block's boundary are excluded, which is how a label split excludes the label
// itself and a bare block excludes the block statement.
func countStatements(list []ast.Stmt, blockEnd token.Pos, extendToClosingBrace bool) int {
	total := 0
	for len(list) > 0 {
		last := 0
		var end token.Pos
		for last = 0; last < len(list); last++ {
			stmt := list[last]
			end = statementBoundary(stmt)
			if !endsBasicSourceBlock(stmt) {
				continue
			}
			if label, ok := stmt.(*ast.LabeledStmt); ok && !isControlStmt(label.Stmt) {
				end = label.Pos()
				total += countBefore(list[:last+1], end)
				list = append([]ast.Stmt{label.Stmt}, list[last+1:]...)
				extendToClosingBrace = false
				goto next
			}
			last++
			extendToClosingBrace = false
			break
		}
		if extendToClosingBrace {
			end = blockEnd
		}
		total += countBefore(list[:last], end)
		list = list[last:]
	next:
	}
	return total
}

func countBefore(list []ast.Stmt, end token.Pos) int {
	n := 0
	for _, s := range list {
		if s.Pos() < end {
			n++
		}
	}
	return n
}

func statementBoundary(s ast.Stmt) token.Pos {
	switch s := s.(type) {
	case *ast.BlockStmt:
		return s.Lbrace
	case *ast.IfStmt:
		if found, pos := hasFuncLiteral(s.Init); found {
			return pos
		}
		if found, pos := hasFuncLiteral(s.Cond); found {
			return pos
		}
		return s.Body.Lbrace
	case *ast.ForStmt:
		if found, pos := hasFuncLiteral(s.Init); found {
			return pos
		}
		if found, pos := hasFuncLiteral(s.Cond); found {
			return pos
		}
		if found, pos := hasFuncLiteral(s.Post); found {
			return pos
		}
		return s.Body.Lbrace
	case *ast.LabeledStmt:
		return statementBoundary(s.Stmt)
	case *ast.RangeStmt:
		if found, pos := hasFuncLiteral(s.X); found {
			return pos
		}
		return s.Body.Lbrace
	case *ast.SwitchStmt:
		if found, pos := hasFuncLiteral(s.Init); found {
			return pos
		}
		if found, pos := hasFuncLiteral(s.Tag); found {
			return pos
		}
		return s.Body.Lbrace
	case *ast.SelectStmt:
		return s.Body.Lbrace
	case *ast.TypeSwitchStmt:
		if found, pos := hasFuncLiteral(s.Init); found {
			return pos
		}
		return s.Body.Lbrace
	}
	if found, pos := hasFuncLiteral(s); found {
		return pos
	}
	return s.End()
}

func endsBasicSourceBlock(s ast.Stmt) bool {
	switch s := s.(type) {
	case *ast.BlockStmt, *ast.BranchStmt, *ast.ForStmt, *ast.IfStmt, *ast.LabeledStmt,
		*ast.RangeStmt, *ast.SwitchStmt, *ast.SelectStmt, *ast.TypeSwitchStmt:
		return true
	case *ast.ExprStmt:
		if call, ok := s.X.(*ast.CallExpr); ok {
			if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "panic" && len(call.Args) == 1 {
				return true
			}
		}
	}
	found, _ := hasFuncLiteral(s)
	return found
}

func isControlStmt(s ast.Stmt) bool {
	switch s.(type) {
	case *ast.ForStmt, *ast.RangeStmt, *ast.SwitchStmt, *ast.SelectStmt, *ast.TypeSwitchStmt:
		return true
	}
	return false
}

func hasFuncLiteral(n ast.Node) (bool, token.Pos) {
	if n == nil {
		return false, token.NoPos
	}
	var lit funcLiteralFinder
	ast.Walk(&lit, n)
	return lit.found, lit.pos
}

type funcLiteralFinder struct {
	found bool
	pos   token.Pos
}

func (f *funcLiteralFinder) Visit(node ast.Node) ast.Visitor {
	if f.found {
		return nil
	}
	if lit, ok := node.(*ast.FuncLit); ok {
		f.found = true
		f.pos = lit.Body.Lbrace
		return nil
	}
	return f
}

func percentString(covered, statements int) string {
	if statements == 0 {
		return "0.0"
	}
	tenths := covered * 1000 / statements
	return fmt.Sprintf("%d.%d", tenths/10, tenths%10)
}

func meetsThreshold(covered, statements, scaledPercent int) bool {
	return covered*1000 >= scaledPercent*statements
}

func packageStatus(p packageCoverage, floor *int) string {
	if floor == nil {
		return "-"
	}
	if meetsThreshold(p.covered, p.statements, *floor) {
		return "ok"
	}
	return "FAIL"
}

func totalStatus(rep *report, floor *int) string {
	if floor == nil {
		return "-"
	}
	if meetsThreshold(rep.total.covered, rep.total.statements, *floor) {
		return "ok"
	}
	return "FAIL"
}

func belowThreshold(rep *report, cfg *config) bool {
	if cfg.total != nil && !meetsThreshold(rep.total.covered, rep.total.statements, *cfg.total) {
		return true
	}
	for _, p := range rep.packages {
		if packageStatus(p, cfg.pkg) == "FAIL" {
			return true
		}
	}
	return false
}

func writeText(w io.Writer, rep *report, cfg *config) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "package\tstatements\tcovered\tpercent\tstatus")
	for _, p := range rep.packages {
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\n", p.path, p.statements, p.covered, percentString(p.covered, p.statements), packageStatus(p, cfg.pkg))
	}
	_, _ = fmt.Fprintf(tw, "total\t%d\t%d\t%s\t%s\n", rep.total.statements, rep.total.covered, percentString(rep.total.covered, rep.total.statements), totalStatus(rep, cfg.total))
	return tw.Flush()
}

func writeMarkdown(w io.Writer, rep *report, cfg *config) error {
	_, _ = fmt.Fprintln(w, "| package | statements | covered | percent | status |")
	_, _ = fmt.Fprintln(w, "| --- | --- | --- | --- | --- |")
	for _, p := range rep.packages {
		_, _ = fmt.Fprintf(w, "| `%s` | %d | %d | %s | %s |\n", p.path, p.statements, p.covered, percentString(p.covered, p.statements), packageStatus(p, cfg.pkg))
	}
	_, _ = fmt.Fprintf(w, "| **total** | %d | %d | %s | %s |\n", rep.total.statements, rep.total.covered, percentString(rep.total.covered, rep.total.statements), totalStatus(rep, cfg.total))
	return nil
}
