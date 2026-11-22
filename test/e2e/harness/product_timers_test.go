package harness

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestMirroredProductTimersMatch keeps the harness's copies of the product's
// unexported timers in step with their definitions. The e2e package cannot read
// an unexported constant from another package, so it repeats the value; a
// product change would otherwise silently leave every derived wait wrong.
func TestMirroredProductTimersMatch(t *testing.T) {
	root, err := repositoryRoot()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		file string
		name string
		want time.Duration
	}{
		{"internal/mcpserver/topics.go", "topicRPCTimeout", topicRPCTimeoutMirror},
		{"internal/mcpserver/updatehint.go", "updateStatusTimeout", updateHintTimeoutMirror},
	} {
		got, ok := sourceDuration(filepath.Join(root, tc.file), tc.name)
		if !ok {
			t.Errorf("%s: no duration constant %s was found", tc.file, tc.name)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: %s = %s, but the harness mirror is %s", tc.file, tc.name, got, tc.want)
		}
	}
}

// sourceDuration returns the value of the named constant in path. It evaluates
// the small duration expressions the product uses: an integer multiplied or
// divided by a time unit, with parentheses.
func sourceDuration(path, name string) (time.Duration, bool) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return 0, false
	}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, ident := range value.Names {
				if ident.Name != name || i >= len(value.Values) {
					continue
				}
				return evalDurationExpr(value.Values[i])
			}
		}
	}
	return 0, false
}

// evalDurationExpr evaluates the duration expression forms the product uses.
func evalDurationExpr(expr ast.Expr) (time.Duration, bool) {
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return evalDurationExpr(e.X)
	case *ast.BasicLit:
		if e.Kind != token.INT {
			return 0, false
		}
		n, err := strconv.ParseInt(e.Value, 0, 64)
		if err != nil {
			return 0, false
		}
		return time.Duration(n), true
	case *ast.SelectorExpr:
		ident, ok := e.X.(*ast.Ident)
		if !ok || ident.Name != "time" {
			return 0, false
		}
		switch e.Sel.Name {
		case "Nanosecond":
			return time.Nanosecond, true
		case "Microsecond":
			return time.Microsecond, true
		case "Millisecond":
			return time.Millisecond, true
		case "Second":
			return time.Second, true
		case "Minute":
			return time.Minute, true
		case "Hour":
			return time.Hour, true
		}
	case *ast.BinaryExpr:
		left, ok := evalDurationExpr(e.X)
		if !ok {
			return 0, false
		}
		right, ok := evalDurationExpr(e.Y)
		if !ok {
			return 0, false
		}
		switch e.Op {
		case token.MUL:
			return left * right, true
		case token.QUO:
			if right == 0 {
				return 0, false
			}
			return left / right, true
		}
	}
	return 0, false
}
