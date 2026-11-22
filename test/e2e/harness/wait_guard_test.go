package harness

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// eventuallyCallExempt exempts one direct or indirect tool call inside a wait
// callback. The comment must stand on the same line as the call or on the line
// directly above it, and the marker must be followed by a reason. A line
// comment and a block comment are both accepted. The reason is not checked
// beyond being present: it is there for the next reader, who needs to know why
// this wait cannot retry with PollCall.
const eventuallyCallExempt = "eventually-call-allow:"

// minWaitCallChecks is the least number of wait and wrapper call sites the walk
// must find under test/e2e. The tree holds roughly 310; a count far below that
// means the walk stopped seeing the tree, not that the tests stopped using
// waits.
const minWaitCallChecks = 100

// TestEventuallyCallbacksUsePollBoundedCalls rejects a tool call made inside
// an Eventually, EventuallyPoll, ConsistentlyPoll or WaitFor callback when the
// call bypasses the wait's bounds. Session.Call bounds one tool call at
// sessionCallBound, sixty seconds, and fails the test on a transport error, so
// a callback that uses it turns one stalled MCP call into a failed wait
// instead of retrying it. A callback makes poll-bounded calls with
// Session.PollCall or Session.PollCallWithin, which the deadline the wait
// passes in bounds and clamps to the time the wait has left.
//
// The callback's direct calls are checked, and so is one level of indirection:
// a call to a function in the same directory whose own body makes a direct
// .Call is rejected too, because the helper hides the call from a guard that
// only reads the callback. The helper is matched whether it is called by name,
// in method form as in w.poll(s), or generically as in poll[int](s). A
// selector call is also matched against the helpers of every directory under
// test/e2e, because a callback reaches another package's helper that way, as
// in s.CallUntilOK. Session.CallErr and Session.CallErrWithin take the same
// sixty-second bound without a direct .Call, so the guard names them too.
//
// A closure held in a variable is followed to its function literal. A callback
// handed to a wrapper function such as registrationEventually is inspected at
// the wrapper's call site: every func-typed parameter the wrapper forwards is
// followed, including through another wrapper, and a wrapper held in a closure
// variable such as drWaitStatus is followed the same way.
//
// The test counts the wait and wrapper calls it checked and fails when the
// count falls below minWaitCallChecks, so a walk that silently stops seeing
// the tree cannot pass.
//
// The guard stops at these known boundaries, which still need a reader:
//   - a wrapper declared in another package under test/e2e is not in the
//     per-directory wrapper index;
//   - a wrapper that stores its callback in a closure variable before passing
//     it to the wait is not followed through both levels;
//   - Consistently is checked separately: it observes a window without
//     retrying, so its callback may not make an unbounded .Call or .CallErr,
//     directly or through one level of a same-directory helper; a callback
//     that needs a tool call uses ConsistentlyPoll and Session.PollCall, which
//     the wait check covers;
//   - closureLiterals and local wrapper names are matched by name within the
//     file or function and do not resolve lexical scope.
//
// A wait that genuinely needs the sixty-second bound exempts the call with
// // eventually-call-allow: <reason> on the call's line or the line above it.
// A wait whose callback is a named function also needs an exemption, because
// the guard cannot see that function's body from the wait call.
//
// The test walks the Go sources under test/e2e rather than the compiled
// packages because most of them carry the e2e build tag, and it lives in the
// harness package so make test runs it.
func TestEventuallyCallbacksUsePollBoundedCalls(t *testing.T) {
	root, err := repositoryRoot()
	if err != nil {
		t.Fatal(err)
	}
	e2eDir := filepath.Join(root, "test", "e2e")

	type sourceFile struct {
		path string
		file *ast.File
	}
	var sources []sourceFile
	fset := token.NewFileSet()
	err = filepath.WalkDir(e2eDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		sources = append(sources, sourceFile{path: path, file: file})
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", e2eDir, err)
	}

	// callFuncs maps a directory to the names of its functions whose body makes
	// a direct .Call. A wait callback that calls such a function is reported,
	// one level deep; the function's own callees are not followed.
	callFuncs := make(map[string]map[string]bool)
	globalCallFuncs := make(map[string]bool)
	filesByDir := make(map[string][]*ast.File)
	for _, src := range sources {
		dir := filepath.Dir(src.path)
		if callFuncs[dir] == nil {
			callFuncs[dir] = make(map[string]bool)
		}
		for _, name := range directCallFuncs(src.file) {
			callFuncs[dir][name] = true
			globalCallFuncs[name] = true
		}
		filesByDir[dir] = append(filesByDir[dir], src.file)
	}
	indexes := make(map[string]*guardIndex)
	for dir, files := range filesByDir {
		indexes[dir] = &guardIndex{
			directCalls:       callFuncs[dir],
			globalDirectCalls: globalCallFuncs,
			wrappers:          wrapperFuncs(files),
		}
	}

	var violations []string
	checked := 0
	for _, src := range sources {
		dir := filepath.Dir(src.path)
		found, checks := waitCallbackViolations(fset, src.file, indexes[dir])
		violations = append(violations, found...)
		checked += checks
		// Consistently is not retried and passes no deadline, so its callback
		// gets its own check: a tool call there must use ConsistentlyPoll with
		// Session.PollCall, not the unbounded Session.Call.
		violations = append(violations, consistentlyCallbackViolations(fset, src.file, indexes[dir], exemptLines(fset, src.file))...)
	}
	if checked < minWaitCallChecks {
		t.Fatalf("checked only %d wait and wrapper call sites, want at least %d; the walk is not seeing test/e2e", checked, minWaitCallChecks)
	}
	t.Logf("checked %d wait and wrapper call sites under test/e2e", checked)

	sort.Strings(violations)
	for _, violation := range violations {
		t.Error(violation)
	}
	if len(violations) > 0 {
		t.Errorf("%d tool calls inside wait callbacks bypass the wait's bounds; call Session.PollCall or PollCallWithin with the callback's deadline, or exempt the call with // %s <reason>", len(violations), eventuallyCallExempt)
	}
}

// repositoryRoot returns the module root, found by walking up from this source
// file to the directory holding go.mod.
func repositoryRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("harness: cannot locate the guard test's source file")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("harness: no go.mod above %s", file)
		}
		dir = parent
	}
}

// directCallFuncs returns the names of the functions declared in file whose
// body makes a call bounded at the session call bound.
func directCallFuncs(file *ast.File) []string {
	var names []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		if bodyCallsCall(fn.Body) {
			names = append(names, fn.Name.Name)
		}
	}
	return names
}

// longCallMethod reports whether a method call is bounded at sessionCallBound,
// sixty seconds. CallErr and CallErrWithin take the same bound and make no
// direct .Call themselves, so a name-only index would miss them.
func longCallMethod(name string) bool {
	switch name {
	case "Call", "CallErr", "CallErrWithin":
		return true
	}
	return false
}

// callFun unwraps a call's function expression through parentheses and type
// arguments, so a generic call such as f[T](...) yields its identifier.
func callFun(fun ast.Expr) ast.Expr {
	for {
		switch f := fun.(type) {
		case *ast.ParenExpr:
			fun = f.X
		case *ast.IndexExpr:
			fun = f.X
		case *ast.IndexListExpr:
			fun = f.X
		default:
			return fun
		}
	}
}

// bodyCallsCall reports whether body makes a call bounded at the session call
// bound, anywhere in the body including a nested function literal. Session's
// method is what the guard is concerned with; a method with one of these names
// on another type is rejected too, which the exemption covers.
func bodyCallsCall(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := callFun(call.Fun).(*ast.SelectorExpr); ok && longCallMethod(sel.Sel.Name) {
			found = true
			return false
		}
		return true
	})
	return found
}

// guardIndex is what the guard knows about the functions in one directory. A
// callback can hide a bounded call behind three shapes: a helper called by name
// or in method form, a closure held in a variable, and a callback forwarded by
// a wrapper function. directCalls and wrappers describe the first and the
// third; closureLiterals describes the second.
type guardIndex struct {
	// directCalls names the functions and methods declared in the directory
	// whose body makes a call bounded at the session call bound, including one
	// inside a nested function literal.
	directCalls map[string]bool
	// globalDirectCalls names every function and method under test/e2e with the
	// same property. A callback reaches another package's helper through a
	// selector, as in s.CallUntilOK, so a selector call is matched against this
	// wider set.
	globalDirectCalls map[string]bool
	// wrappers maps a function or method declared in the directory to every
	// position of a func-typed parameter it forwards to a wait. A callback
	// passed at one of those positions is inspected like a callback written
	// directly at the wait.
	wrappers map[string][]int
}

// wrapperFuncs returns the functions and methods in files that forward a
// func-typed parameter to a wait, mapped to every forwarded position. A
// function that forwards through another wrapper is found by repeating the
// walk until no new position appears.
func wrapperFuncs(files []*ast.File) map[string][]int {
	wrappers := make(map[string][]int)
	var decls []*ast.FuncDecl
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			decls = append(decls, fn)
			if positions, ok := forwardedToWait(fn.Type, fn.Body); ok {
				wrappers[fn.Name.Name] = mergePositions(wrappers[fn.Name.Name], positions)
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for _, fn := range decls {
			positions, ok := forwardedThroughWrapper(fn, wrappers)
			if !ok {
				continue
			}
			before := len(wrappers[fn.Name.Name])
			wrappers[fn.Name.Name] = mergePositions(wrappers[fn.Name.Name], positions)
			if len(wrappers[fn.Name.Name]) != before {
				changed = true
			}
		}
	}
	return wrappers
}

// mergePositions returns the union of two position slices, sorted.
func mergePositions(dst, more []int) []int {
	seen := make(map[int]bool, len(dst)+len(more))
	for _, at := range dst {
		seen[at] = true
	}
	for _, at := range more {
		seen[at] = true
	}
	positions := make([]int, 0, len(seen))
	for at := range seen {
		positions = append(positions, at)
	}
	sort.Ints(positions)
	return positions
}

// forwardedToWait reports every position of a func-typed parameter of fn that
// is referenced inside the callback of a wait call in body. A wrapper usually
// calls the parameter, as registrationEventually does with cond(deadline), but
// it may also pass the parameter itself as the callback.
func forwardedToWait(paramsType *ast.FuncType, body *ast.BlockStmt) ([]int, bool) {
	params := funcParamPositions(paramsType)
	if len(params) == 0 {
		return nil, false
	}
	found := make(map[int]bool)
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if _, ok := waitCallName(call); !ok {
			return true
		}
		last := lastArg(call)
		if last == nil {
			return true
		}
		switch arg := last.(type) {
		case *ast.Ident:
			if at, ok := params[arg.Name]; ok {
				found[at] = true
			}
		case *ast.FuncLit:
			ast.Inspect(arg.Body, func(n ast.Node) bool {
				id, ok := n.(*ast.Ident)
				if !ok {
					return true
				}
				if at, ok := params[id.Name]; ok {
					found[at] = true
				}
				return true
			})
		}
		return true
	})
	return sortedPositions(found), len(found) > 0
}

// forwardedThroughWrapper reports every position of a func-typed parameter of
// fn that fn hands to another wrapper at one of that wrapper's callback
// positions.
func forwardedThroughWrapper(fn *ast.FuncDecl, wrappers map[string][]int) ([]int, bool) {
	return forwardedThroughWrapperBody(fn.Type, fn.Body, wrappers)
}

// forwardedThroughWrapperBody is forwardedThroughWrapper for a function
// declaration and for a function literal held in a closure variable.
func forwardedThroughWrapperBody(paramsType *ast.FuncType, body *ast.BlockStmt, wrappers map[string][]int) ([]int, bool) {
	params := funcParamPositions(paramsType)
	if len(params) == 0 {
		return nil, false
	}
	found := make(map[int]bool)
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := callName(call)
		if !ok {
			return true
		}
		for _, at := range wrappers[name] {
			if at >= len(call.Args) {
				continue
			}
			id, ok := call.Args[at].(*ast.Ident)
			if !ok {
				continue
			}
			if position, ok := params[id.Name]; ok {
				found[position] = true
			}
		}
		return true
	})
	return sortedPositions(found), len(found) > 0
}

// sortedPositions returns the positions in found in order.
func sortedPositions(found map[int]bool) []int {
	positions := make([]int, 0, len(found))
	for at := range found {
		positions = append(positions, at)
	}
	sort.Ints(positions)
	return positions
}

// funcParamPositions maps each func-typed parameter name of ft to its position
// in a call. Parameters grouped in one field, as in "a, b func()", occupy
// consecutive positions.
func funcParamPositions(ft *ast.FuncType) map[string]int {
	params := make(map[string]int)
	if ft == nil || ft.Params == nil {
		return params
	}
	position := 0
	for _, field := range ft.Params.List {
		width := len(field.Names)
		if width == 0 {
			width = 1
		}
		if _, ok := field.Type.(*ast.FuncType); ok {
			for i, name := range field.Names {
				params[name.Name] = position + i
			}
		}
		position += width
	}
	return params
}

// forwardedParamNames returns the names of the func-typed parameters of fn
// that fn forwards to a wait. A wait inside fn that passes one of them as a
// named callback is not reported: the guard checks every caller instead.
func forwardedParamNames(fn *ast.FuncDecl, wrappers map[string][]int) map[string]bool {
	return forwardedParamNamesOf(fn.Type, wrappers[fn.Name.Name])
}

// forwardedParamNamesOf returns the parameter names at positions in ft.
func forwardedParamNamesOf(ft *ast.FuncType, positions []int) map[string]bool {
	names := make(map[string]bool)
	if ft == nil || ft.Params == nil {
		return names
	}
	position := 0
	for _, field := range ft.Params.List {
		width := len(field.Names)
		if width == 0 {
			width = 1
		}
		for _, at := range positions {
			if at >= position && at < position+width {
				for _, name := range field.Names {
					names[name.Name] = true
				}
			}
		}
		position += width
	}
	return names
}

// localWrapperFuncs returns the closure variables in fn that forward a
// func-typed parameter to a wait, mapped to every forwarded position, and the
// wait calls inside those wrappers whose callback is the forwarded parameter.
// Those calls would otherwise be reported as named callbacks; the guard checks
// the wrapper's call sites instead. A name bound to more than one literal, or
// to anything that is not a literal, is left out: the guard cannot tell which
// body a call reaches. Scope is not resolved, so a name is matched for the
// whole function. A wrapper call that hands its parameter to another wrapper is
// left to the named-callback report.
func localWrapperFuncs(fn *ast.FuncDecl) (map[string][]int, map[*ast.CallExpr]bool) {
	bindings := make(map[string][]*ast.FuncLit)
	ambiguous := make(map[string]bool)
	add := func(name string, value ast.Expr) {
		lit, ok := value.(*ast.FuncLit)
		if !ok {
			ambiguous[name] = true
			return
		}
		bindings[name] = append(bindings[name], lit)
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			if len(node.Lhs) != len(node.Rhs) {
				for _, lhs := range node.Lhs {
					if id, ok := lhs.(*ast.Ident); ok {
						ambiguous[id.Name] = true
					}
				}
				return true
			}
			for i, lhs := range node.Lhs {
				if id, ok := lhs.(*ast.Ident); ok {
					add(id.Name, node.Rhs[i])
				}
			}
		case *ast.ValueSpec:
			if len(node.Values) == 0 {
				return true
			}
			if len(node.Names) != len(node.Values) {
				for _, name := range node.Names {
					ambiguous[name.Name] = true
				}
				return true
			}
			for i, name := range node.Names {
				add(name.Name, node.Values[i])
			}
		}
		return true
	})

	type namedLiteral struct {
		name string
		lit  *ast.FuncLit
	}
	var literals []namedLiteral
	wrappers := make(map[string][]int)
	skips := make(map[*ast.CallExpr]bool)
	record := func(name string, lit *ast.FuncLit, positions []int) {
		wrappers[name] = mergePositions(wrappers[name], positions)
		names := forwardedParamNamesOf(lit.Type, positions)
		if len(names) == 0 {
			return
		}
		ast.Inspect(lit.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if _, ok := waitCallName(call); !ok {
				return true
			}
			if id, ok := lastArg(call).(*ast.Ident); ok && names[id.Name] {
				skips[call] = true
			}
			return true
		})
	}
	for name, bound := range bindings {
		if ambiguous[name] || len(bound) != 1 {
			continue
		}
		lit := bound[0]
		literals = append(literals, namedLiteral{name: name, lit: lit})
		if positions, ok := forwardedToWait(lit.Type, lit.Body); ok {
			record(name, lit, positions)
		}
	}
	for changed := true; changed; {
		changed = false
		for _, nl := range literals {
			positions, ok := forwardedThroughWrapperBody(nl.lit.Type, nl.lit.Body, wrappers)
			if !ok {
				continue
			}
			before := len(wrappers[nl.name])
			record(nl.name, nl.lit, positions)
			if len(wrappers[nl.name]) != before {
				changed = true
			}
		}
	}
	return wrappers, skips
}

// closureLiterals maps a variable in file to the function literal assigned to
// it, when every assignment to that name in the file assigns the same literal.
// A callback passed as such a variable is inspected as if the literal had been
// written at the wait. A name assigned anything else is left out, and the
// callback stays a named callback the guard reports.
func closureLiterals(file *ast.File) map[string]*ast.FuncLit {
	literals := make(map[string]*ast.FuncLit)
	ambiguous := make(map[string]bool)
	assign := func(name string, value ast.Expr) {
		lit, ok := value.(*ast.FuncLit)
		if !ok {
			ambiguous[name] = true
			return
		}
		if previous, ok := literals[name]; ok && previous != lit {
			ambiguous[name] = true
			return
		}
		literals[name] = lit
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			if len(node.Lhs) != len(node.Rhs) {
				for _, lhs := range node.Lhs {
					if id, ok := lhs.(*ast.Ident); ok {
						ambiguous[id.Name] = true
					}
				}
				return true
			}
			for i, lhs := range node.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok {
					continue
				}
				assign(id.Name, node.Rhs[i])
			}
		case *ast.ValueSpec:
			if len(node.Values) == 0 {
				return true
			}
			if len(node.Names) != len(node.Values) {
				for _, name := range node.Names {
					ambiguous[name.Name] = true
				}
				return true
			}
			for i, name := range node.Names {
				assign(name.Name, node.Values[i])
			}
		}
		return true
	})
	for name := range ambiguous {
		delete(literals, name)
	}
	return literals
}

// callName returns the name a call invokes, for a plain function, a method or
// a generic instantiation.
func callName(call *ast.CallExpr) (string, bool) {
	switch fun := callFun(call.Fun).(type) {
	case *ast.Ident:
		return fun.Name, true
	case *ast.SelectorExpr:
		return fun.Sel.Name, true
	}
	return "", false
}

// waitCallbackViolations returns one message for every tool call in the
// callback passed to an Eventually, EventuallyPoll or WaitFor call in file, and
// in a callback handed to a wrapper function, that bypasses the wait's bounds.
// index describes the functions the callback can reach, and the returned count
// is the number of wait and wrapper calls the walk checked. A callback held in
// a variable is followed to its literal.
func waitCallbackViolations(fset *token.FileSet, file *ast.File, index *guardIndex) ([]string, int) {
	exempt := exemptLines(fset, file)
	closures := closureLiterals(file)
	var violations []string
	checked := 0
	exemptAt := func(pos token.Pos) bool {
		line := fset.Position(pos).Line
		return exempt[line] || exempt[line-1]
	}

	// inspectCallback reports the calls in one callback body. owner names the
	// wait or the wrapper, for the message. local names the closure wrappers in
	// the enclosing function, and seen guards against a recursive closure.
	var inspectCallback func(owner string, body *ast.BlockStmt, local map[string][]int, seen map[*ast.FuncLit]bool)
	inspectCallback = func(owner string, body *ast.BlockStmt, local map[string][]int, seen map[*ast.FuncLit]bool) {
		ast.Inspect(body, func(n ast.Node) bool {
			inner, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if _, nested := waitCallName(inner); nested {
				// The walk over the file reports a nested wait's callback
				// on its own; do not count its calls twice.
				return false
			}
			if name, ok := callName(inner); ok {
				// The walk over the file inspects a wrapper's callback at the
				// wrapper's call site. Descending into it here as well would
				// report the same call twice.
				if _, ok := local[name]; ok {
					return false
				}
				if _, ok := index.wrappers[name]; ok {
					return false
				}
			}
			if exemptAt(inner.Lparen) {
				return true
			}
			switch fun := callFun(inner.Fun).(type) {
			case *ast.SelectorExpr:
				if longCallMethod(fun.Sel.Name) {
					violations = append(violations, fmt.Sprintf(
						"%s: .%s inside the callback of %s; a wait must use Session.PollCall or PollCallWithin and its deadline, or exempt the call with // %s <reason>",
						fset.Position(fun.Sel.Pos()), fun.Sel.Name, owner, eventuallyCallExempt))
					return true
				}
				if index.globalDirectCalls[fun.Sel.Name] {
					violations = append(violations, fmt.Sprintf(
						"%s: %s is called inside the callback of %s and makes a bounded call in its body; call a deadline-taking variant and Session.PollCall, or exempt the call with // %s <reason>",
						fset.Position(fun.Sel.Pos()), fun.Sel.Name, owner, eventuallyCallExempt))
				}
			case *ast.Ident:
				if index.directCalls[fun.Name] {
					violations = append(violations, fmt.Sprintf(
						"%s: %s is called inside the callback of %s and makes a bounded call in its body; call a deadline-taking variant and Session.PollCall, or exempt the call with // %s <reason>",
						fset.Position(fun.Pos()), fun.Name, owner, eventuallyCallExempt))
					return true
				}
				if lit := closures[fun.Name]; lit != nil && !seen[lit] {
					seen[lit] = true
					inspectCallback(owner, lit.Body, local, seen)
				}
			}
			return true
		})
	}

	// checkCallback follows the callback argument arg of a wait or wrapper
	// call. forwarded names the wrapper's own parameters that are checked at
	// its call sites instead, so they are not reported here.
	checkCallback := func(owner string, arg ast.Expr, forwarded map[string]bool, local map[string][]int) {
		if arg == nil {
			return
		}
		switch value := arg.(type) {
		case *ast.FuncLit:
			inspectCallback(owner, value.Body, local, map[*ast.FuncLit]bool{})
		case *ast.Ident:
			if value.Name == "nil" || forwarded[value.Name] {
				return
			}
			if lit := closures[value.Name]; lit != nil {
				inspectCallback(owner, lit.Body, local, map[*ast.FuncLit]bool{lit: true})
				return
			}
			if exemptAt(value.Pos()) {
				return
			}
			violations = append(violations, fmt.Sprintf(
				"%s: %s takes a named callback, so this guard cannot see whether it makes a tool call; pass a function literal or exempt the wait with // %s <reason>",
				fset.Position(value.Pos()), owner, eventuallyCallExempt))
		default:
			if exemptAt(arg.Pos()) {
				return
			}
			violations = append(violations, fmt.Sprintf(
				"%s: %s takes a callback this guard cannot see through, so it cannot tell whether it makes a tool call; pass a function literal or exempt the wait with // %s <reason>",
				fset.Position(arg.Pos()), owner, eventuallyCallExempt))
		}
	}

	// checkCall reports a wait call's callback, or the callbacks a wrapper
	// forwards at the positions its parameters occupy. local names the closure
	// wrappers in the enclosing function, and skips names the wait calls inside
	// them whose callback is the forwarded parameter.
	checkCall := func(call *ast.CallExpr, forwarded map[string]bool, local map[string][]int, skips map[*ast.CallExpr]bool) {
		if name, ok := waitCallName(call); ok {
			checked++
			if skips[call] {
				// The wrapper hands this parameter to the wait; its call
				// sites are checked instead.
				return
			}
			checkCallback(name, lastArg(call), forwarded, local)
			return
		}
		name, ok := callName(call)
		if !ok {
			return
		}
		positions, ok := local[name]
		if !ok {
			positions, ok = index.wrappers[name]
		}
		if !ok {
			return
		}
		checked++
		for _, at := range positions {
			if at < len(call.Args) {
				checkCallback(name, call.Args[at], forwarded, local)
			}
		}
	}

	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
			forwarded := forwardedParamNames(fn, index.wrappers)
			local, skips := localWrapperFuncs(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					checkCall(call, forwarded, local, skips)
				}
				return true
			})
			continue
		}
		// A wait can also sit in a package-level value. It has no parameters
		// to forward.
		ast.Inspect(decl, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				checkCall(call, nil, nil, nil)
			}
			return true
		})
	}
	return violations, checked
}

// lastArg returns the last argument of call, or nil when it takes none.
func lastArg(call *ast.CallExpr) ast.Expr {
	if len(call.Args) == 0 {
		return nil
	}
	return call.Args[len(call.Args)-1]
}

// waitCallName reports whether call invokes Eventually, EventuallyPoll,
// WaitFor or ConsistentlyPoll, and returns the name for the message.
func waitCallName(call *ast.CallExpr) (string, bool) {
	var name string
	switch fun := callFun(call.Fun).(type) {
	case *ast.Ident:
		name = fun.Name
	case *ast.SelectorExpr:
		name = fun.Sel.Name
	default:
		return "", false
	}
	switch name {
	case "Eventually", "EventuallyPoll", "WaitFor", "ConsistentlyPoll":
		return name, true
	}
	return "", false
}

// consistentlyCallName reports whether call invokes the window-only
// Consistently, whose callback has no deadline and therefore cannot make a
// poll-bounded call. Its callback is checked separately.
func consistentlyCallName(call *ast.CallExpr) bool {
	name, ok := callName(call)
	return ok && name == "Consistently"
}

// consistentlyCallbackViolations reports every unbounded tool call in the
// callback of a Consistently call in file, directly or through one level of a
// same-directory helper. Consistently observes a window instead of retrying, so
// it passes no deadline; a callback that needs a tool call uses
// ConsistentlyPoll and Session.PollCall, which that guard checks. A callback
// that genuinely needs the sixty-second bound exempts the call with the same
// marker. A callback held in a variable is followed to its literal.
//
// index supplies the same direct-call and wrapper knowledge the wait guard
// uses. Without it, a callback that calls a helper such as discListTopics is
// invisible even though the helper's body calls Session.Call.
func consistentlyCallbackViolations(fset *token.FileSet, file *ast.File, index *guardIndex, exempt map[int]bool) []string {
	closures := closureLiterals(file)
	var violations []string
	exemptAt := func(pos token.Pos) bool {
		line := fset.Position(pos).Line
		return exempt[line] || exempt[line-1]
	}
	var inspect func(owner string, body *ast.BlockStmt, seen map[*ast.FuncLit]bool)
	inspect = func(owner string, body *ast.BlockStmt, seen map[*ast.FuncLit]bool) {
		ast.Inspect(body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if _, nested := waitCallName(call); nested || consistentlyCallName(call) {
				// A nested wait or window reports its own callback.
				return false
			}
			if exemptAt(call.Lparen) {
				return true
			}
			switch fun := callFun(call.Fun).(type) {
			case *ast.SelectorExpr:
				if fun.Sel.Name == "Call" || fun.Sel.Name == "CallErr" {
					violations = append(violations, fmt.Sprintf(
						"%s: .%s inside the callback of %s, which passes no deadline; use harness.ConsistentlyPoll with Session.PollCall or PollCallWithin, or exempt the call with // %s <reason>",
						fset.Position(fun.Sel.Pos()), fun.Sel.Name, owner, eventuallyCallExempt))
					return true
				}
				if index != nil && index.globalDirectCalls[fun.Sel.Name] {
					violations = append(violations, fmt.Sprintf(
						"%s: %s is called inside the callback of %s and makes a bounded call in its body; use harness.ConsistentlyPoll and a deadline-taking variant, or exempt the call with // %s <reason>",
						fset.Position(fun.Sel.Pos()), fun.Sel.Name, owner, eventuallyCallExempt))
				}
			case *ast.Ident:
				if index != nil && index.directCalls[fun.Name] {
					violations = append(violations, fmt.Sprintf(
						"%s: %s is called inside the callback of %s and makes a bounded call in its body; use harness.ConsistentlyPoll and a deadline-taking variant, or exempt the call with // %s <reason>",
						fset.Position(fun.Pos()), fun.Name, owner, eventuallyCallExempt))
					return true
				}
				if lit := closures[fun.Name]; lit != nil && !seen[lit] {
					seen[lit] = true
					inspect(owner, lit.Body, seen)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !consistentlyCallName(call) {
			return true
		}
		arg := lastArg(call)
		switch value := arg.(type) {
		case *ast.FuncLit:
			inspect("Consistently", value.Body, map[*ast.FuncLit]bool{})
		case *ast.Ident:
			if lit := closures[value.Name]; lit != nil {
				inspect("Consistently", lit.Body, map[*ast.FuncLit]bool{lit: true})
				return true
			}
			if !exemptAt(value.Pos()) {
				violations = append(violations, fmt.Sprintf(
					"%s: Consistently takes a named callback, so this guard cannot see whether it makes a tool call; pass a function literal or exempt the wait with // %s <reason>",
					fset.Position(value.Pos()), eventuallyCallExempt))
			}
		default:
			if arg != nil && !exemptAt(arg.Pos()) {
				violations = append(violations, fmt.Sprintf(
					"%s: Consistently takes a callback this guard cannot see through, so it cannot tell whether it makes a tool call; pass a function literal or exempt the wait with // %s <reason>",
					fset.Position(arg.Pos()), eventuallyCallExempt))
			}
		}
		return true
	})
	return violations
}

// exemptLines returns the lines carrying an exemption marker with a reason. A
// line comment and a block comment are both accepted; a block comment marks
// every line it covers.
func exemptLines(fset *token.FileSet, file *ast.File) map[int]bool {
	lines := make(map[int]bool)
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if !hasExemptionReason(comment.Text) {
				continue
			}
			start := fset.Position(comment.Pos()).Line
			end := fset.Position(comment.End()).Line
			for line := start; line <= end; line++ {
				lines[line] = true
			}
		}
	}
	return lines
}

// hasExemptionReason reports whether text is an exemption marker followed by a
// reason, in a line comment or a block comment.
func hasExemptionReason(text string) bool {
	text = strings.TrimSpace(text)
	text = strings.TrimPrefix(text, "//")
	if strings.HasPrefix(text, "/*") {
		text = strings.TrimSuffix(strings.TrimPrefix(text, "/*"), "*/")
	}
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, eventuallyCallExempt) {
		return false
	}
	return strings.TrimSpace(strings.TrimPrefix(text, eventuallyCallExempt)) != ""
}

// TestWaitGuardSeesNamedAndIndirectCallbacks covers the two cases the walk must
// not pass silently: a named callback, whose body the guard cannot see from the
// wait call, and a call to a same-package function whose body makes a direct
// .Call. The last case checks that a block comment exempts the call, as the
// marker's documentation says.
func TestWaitGuardSeesNamedAndIndirectCallbacks(t *testing.T) {
	const src = `package p

type S struct{}

func (s S) Call(t any, tool string, args any) {}

func helper(s S) { s.Call(nil, "list_agents", nil) }

func named() (bool, string) { return true, "" }

func f() {
	Eventually(nil, 0, "named", named)
	Eventually(nil, 0, "indirect", func() (bool, string) { helper(S{}); return true, "" })
	Eventually(nil, 0, "exempt", func() (bool, string) {
		helper(S{}) /* eventually-call-allow: helper is a read-only poll */
		return true, ""
	})
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	index := &guardIndex{
		directCalls:       map[string]bool{"helper": true},
		globalDirectCalls: map[string]bool{"helper": true},
		wrappers:          wrapperFuncs([]*ast.File{file}),
	}
	violations, _ := waitCallbackViolations(fset, file, index)
	joined := strings.Join(violations, "\n")
	if len(violations) != 2 {
		t.Fatalf("found %d violations, want 2 (named callback and indirect call):\n%s", len(violations), joined)
	}
	if !strings.Contains(joined, "takes a named callback") {
		t.Errorf("the named callback was not reported:\n%s", joined)
	}
	if !strings.Contains(joined, "helper is called inside") {
		t.Errorf("the indirect call was not reported:\n%s", joined)
	}
}

// TestWaitGuardSeesMethodClosureAndWrapperCallbacks covers the three callback
// shapes the walk must not pass silently: a same-package helper called in
// method form, a closure held in a variable, and a callback handed to a
// wrapper function such as registrationEventually.
func TestWaitGuardSeesMethodClosureAndWrapperCallbacks(t *testing.T) {
	const src = `package p

type S struct{}

func (s S) Call(t any, tool string, args any) {}

type W struct{}

func (w W) poll(s S) { s.Call(nil, "list_agents", nil) }

func wrapper(cond func() (bool, string)) {
	Eventually(nil, 0, "wrapped", func() (bool, string) { return cond() })
}

func inner(cond func() (bool, string)) {
	Eventually(nil, 0, "inner", func() (bool, string) { return cond() })
}

func outer(cond func() (bool, string)) { inner(cond) }

func direct(cond func() (bool, string)) {
	Eventually(nil, 0, "direct", cond)
}

func f(s S, w W) {
	Eventually(nil, 0, "method", func() (bool, string) { w.poll(s); return true, "" })
	cb := func() (bool, string) { s.Call(nil, "list_topics", nil); return true, "" }
	Eventually(nil, 0, "closure", cb)
	wrapper(func() (bool, string) { s.Call(nil, "list_brokers", nil); return true, "" })
	wrapped := func() (bool, string) { s.Call(nil, "list_brokers", nil); return true, "" }
	wrapper(wrapped)
	outer(func() (bool, string) { s.Call(nil, "list_agents", nil); return true, "" })
	direct(func() (bool, string) { s.Call(nil, "list_topics", nil); return true, "" })
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	index := &guardIndex{
		directCalls:       map[string]bool{"poll": true},
		globalDirectCalls: map[string]bool{"poll": true},
		wrappers:          wrapperFuncs([]*ast.File{file}),
	}
	violations, _ := waitCallbackViolations(fset, file, index)
	joined := strings.Join(violations, "\n")
	if len(violations) != 6 {
		t.Errorf("found %d violations, want 6:\n%s", len(violations), joined)
	}
	for _, want := range []string{
		"poll is called inside",
		".Call inside the callback of Eventually",
		".Call inside the callback of wrapper",
		".Call inside the callback of outer",
		".Call inside the callback of direct",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("the guard did not report %q:\n%s", want, joined)
		}
	}
}

// TestWaitGuardSeesClosureWrapperCallbacks covers a wrapper held in a closure
// variable, as drWaitStatus is. The callback it forwards is written at the
// wrapper's call site, so the guard must follow the variable to the function
// literal and inspect the argument at the forwarded position.
func TestWaitGuardSeesClosureWrapperCallbacks(t *testing.T) {
	const src = `package p

type S struct{}

func (s S) Call(t any, tool string, args any) {}

func f(s S) {
	wait := func(t any, want func(int) bool, b int) {
		EventuallyPoll(nil, 0, "x", func(time.Time) (bool, string) { return want(1), "" })
	}
	wait(nil, func(int) bool { s.Call(nil, "a", nil); return true }, 0)
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	violations, _ := waitCallbackViolations(fset, file, newGuardIndex(file))
	joined := strings.Join(violations, "\n")
	if !strings.Contains(joined, ".Call inside the callback of wait") {
		t.Errorf("the guard did not report the closure wrapper callback:\n%s", joined)
	}
}

// TestWaitGuardSeesCallErrAndCallErrWithin covers the helpers that take a long
// bound but make no direct .Call themselves, so the direct-call index does not
// name them, both when the callback calls them and when it calls a helper that
// does.
func TestWaitGuardSeesCallErrAndCallErrWithin(t *testing.T) {
	const src = `package p

type S struct{}

func (s S) CallErr(tool string, args any) (any, error) { return nil, nil }

func (s S) CallErrWithin(bound int, tool string, args any) (any, error) { return nil, nil }

func errHelper(s S) { s.CallErr("list_agents", nil) }

func f(s S) {
	Eventually(nil, 0, "err", func() (bool, string) { s.CallErr("list_agents", nil); return true, "" })
	Eventually(nil, 0, "within", func() (bool, string) { s.CallErrWithin(0, "list_agents", nil); return true, "" })
	Eventually(nil, 0, "helper", func() (bool, string) { errHelper(s); return true, "" })
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	violations, _ := waitCallbackViolations(fset, file, newGuardIndex(file))
	joined := strings.Join(violations, "\n")
	for _, want := range []string{".CallErr inside", ".CallErrWithin inside", "errHelper is called inside"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the guard did not report %q:\n%s", want, joined)
		}
	}
}

// TestWaitGuardSeesClosureVariableCalls covers a closure variable called by
// name inside a callback. The call is not a helper in the direct-call index,
// so the guard must follow the variable to its literal.
func TestWaitGuardSeesClosureVariableCalls(t *testing.T) {
	const src = `package p

type S struct{}

func (s S) Call(t any, tool string, args any) {}

func f(s S) {
	chk := func() { s.Call(nil, "list_agents", nil) }
	Eventually(nil, 0, "closure call", func() (bool, string) { chk(); return true, "" })
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	violations, _ := waitCallbackViolations(fset, file, newGuardIndex(file))
	joined := strings.Join(violations, "\n")
	if !strings.Contains(joined, ".Call inside the callback of Eventually") {
		t.Errorf("the guard did not report the closure variable call:\n%s", joined)
	}
}

// TestWaitGuardReportsWrapperCallbackOnce checks that a wrapper called inside
// another callback is reported once, not once by the callback walk and again
// at the wrapper's call site.
func TestWaitGuardReportsWrapperCallbackOnce(t *testing.T) {
	const src = `package p

type S struct{}

func (s S) Call(t any, tool string, args any) {}

func wrapper(cond func() (bool, string)) {
	Eventually(nil, 0, "wrapped", func() (bool, string) { return cond() })
}

func f(s S) {
	Eventually(nil, 0, "outer", func() (bool, string) {
		wrapper(func() (bool, string) { s.Call(nil, "a", nil); return true, "" })
		return true, ""
	})
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	violations, _ := waitCallbackViolations(fset, file, newGuardIndex(file))
	joined := strings.Join(violations, "\n")
	if len(violations) != 1 {
		t.Fatalf("found %d violations, want one:\n%s", len(violations), joined)
	}
	if !strings.Contains(joined, ".Call inside the callback of wrapper") {
		t.Errorf("the guard did not report the wrapper callback:\n%s", joined)
	}
}

// TestWaitGuardSeesMultipleForwardedParams covers a wrapper with two
// func-typed parameters, both forwarded to waits. The guard must record every
// forwarded position, not only the first one it meets.
func TestWaitGuardSeesMultipleForwardedParams(t *testing.T) {
	const src = `package p

type S struct{}

func (s S) Call(t any, tool string, args any) {}

func two(a, b func() (bool, string)) {
	Eventually(nil, 0, "a", func() (bool, string) { return a() })
	Eventually(nil, 0, "b", func() (bool, string) { return b() })
}

func f(s S) {
	two(func() (bool, string) { return true, "" }, func() (bool, string) { s.Call(nil, "a", nil); return true, "" })
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	violations, _ := waitCallbackViolations(fset, file, newGuardIndex(file))
	joined := strings.Join(violations, "\n")
	if !strings.Contains(joined, ".Call inside the callback of two") {
		t.Errorf("the guard did not report the second forwarded parameter:\n%s", joined)
	}
}

// TestWaitGuardSeesGenericCalls covers a generic wrapper call and a generic
// direct-call helper, whose Fun is an IndexExpr rather than an Ident or a
// SelectorExpr.
func TestWaitGuardSeesGenericCalls(t *testing.T) {
	const src = `package p

type S struct{}

func (s S) Call(t any, tool string, args any) {}

func genericHelper[T any](s S) { s.Call(nil, "list_agents", nil) }

func genericWait[T any](t any, n int, what string, cond func() (bool, string)) {
	Eventually(t, n, what, cond)
}

func f(s S) {
	genericWait[int](nil, 0, "generic", func() (bool, string) { s.Call(nil, "a", nil); return true, "" })
	Eventually(nil, 0, "generic helper", func() (bool, string) { genericHelper[int](s); return true, "" })
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	violations, _ := waitCallbackViolations(fset, file, newGuardIndex(file))
	joined := strings.Join(violations, "\n")
	for _, want := range []string{
		".Call inside the callback of genericWait",
		"genericHelper is called inside",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("the guard did not report %q:\n%s", want, joined)
		}
	}
}

// newGuardIndex builds the index a fixture needs from its own declarations.
func newGuardIndex(file *ast.File) *guardIndex {
	index := &guardIndex{
		directCalls:       make(map[string]bool),
		globalDirectCalls: make(map[string]bool),
		wrappers:          wrapperFuncs([]*ast.File{file}),
	}
	for _, name := range directCallFuncs(file) {
		index.directCalls[name] = true
		index.globalDirectCalls[name] = true
	}
	return index
}

// TestWaitGuardSeesConsistentlyHelperCalls covers a Consistently callback that
// delegates its tool call to a same-directory helper, as the discovery windows
// did with discListTopics. The helper is one level from Session.Call, so the
// direct-call index names it.
func TestWaitGuardSeesConsistentlyHelperCalls(t *testing.T) {
	const src = `package p

type S struct{}

func (s S) Call(t any, tool string, args any) any { return nil }

func discListTopics(s S) (any, string) { return s.Call(nil, "list_topics", nil), "" }

func f(s S) {
	Consistently(nil, 0, "window", func() (bool, string) {
		_, state := discListTopics(s)
		return state == "", state
	})
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing the fixture: %v", err)
	}
	violations := consistentlyCallbackViolations(fset, file, newGuardIndex(file), exemptLines(fset, file))
	joined := strings.Join(violations, "\n")
	if !strings.Contains(joined, "discListTopics is called inside the callback of Consistently") {
		t.Errorf("the guard did not report the helper call:\n%s", joined)
	}
}

// TestExemptionCommentForms checks that the marker accepts the line and block
// comment forms its documentation names, and requires a reason.
func TestExemptionCommentForms(t *testing.T) {
	for _, tc := range []struct {
		comment string
		want    bool
	}{
		{"// eventually-call-allow: read-only poll", true},
		{"/* eventually-call-allow: read-only poll */", true},
		{"// eventually-call-allow:", false},
		{"/* eventually-call-allow: */", false},
		{"// unrelated comment", false},
	} {
		if got := hasExemptionReason(tc.comment); got != tc.want {
			t.Errorf("hasExemptionReason(%q) = %v, want %v", tc.comment, got, tc.want)
		}
	}
}
