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

// TestMirroredProductTimersMatch checks that the harness's copies of the
// product's unexported timer constants match the product source.
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
		{"internal/mcpserver/state.go", "handlerRPCTimeout", handlerRPCTimeoutMirror},
		{"internal/mcpserver/handlers_messaging.go", "listAgentsRPCTimeout", listAgentsRPCTimeoutMirror},
		{"internal/mcpserver/handlers_messaging.go", "sendMessageRPCTimeout", sendMessageRPCTimeoutMirror},
		{"internal/mcpserver/handlers_brokers.go", "connectBrokerRPCTimeout", connectBrokerRPCTimeoutMirror},
		{"internal/mcpserver/handlers_update.go", "updateBrokerRPCTimeout", updateBrokerRPCTimeoutMirror},
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

// TestToolPollBoundsDeriveFromHandlerBound checks that every tool's poll bound
// is its handler bound plus the update hint and margin, and that its wait bound
// is twice its poll bound.
func TestToolPollBoundsDeriveFromHandlerBound(t *testing.T) {
	margin := updateHintTimeoutMirror + pollBoundMargin
	tools := []struct {
		name    string
		handler time.Duration
	}{
		{"list_agents", listAgentsRPCTimeoutMirror},
		{"send_message", sendMessageRPCTimeoutMirror},
		{"connect_broker", connectBrokerRPCTimeoutMirror},
		{"update_broker", updateBrokerRPCTimeoutMirror},
		{"list_topics", topicRPCTimeoutMirror},
		{"join_topic", topicRPCTimeoutMirror},
		{"leave_topic", topicRPCTimeoutMirror},
		{"get_config", handlerRPCTimeoutMirror},
		{"set_config", handlerRPCTimeoutMirror},
		{"set_broker_online", handlerRPCTimeoutMirror},
		{"list_brokers", handlerRPCTimeoutMirror},
		{"disconnect_broker", handlerRPCTimeoutMirror},
		{"create_pairing_key", handlerRPCTimeoutMirror},
	}
	for _, tc := range tools {
		got := ToolPollBound(tc.name)
		want := tc.handler + margin
		if got != want {
			t.Errorf("ToolPollBound(%q) = %s, want the handler bound %s plus the update hint %s and margin %s",
				tc.name, got, tc.handler, updateHintTimeoutMirror, pollBoundMargin)
		}
		if got <= tc.handler+updateHintTimeoutMirror {
			t.Errorf("ToolPollBound(%q) = %s does not exceed its handler %s plus the update hint %s",
				tc.name, got, tc.handler, updateHintTimeoutMirror)
		}
		if wait := ToolWaitBound(tc.name); wait != 2*got {
			t.Errorf("ToolWaitBound(%q) = %s, want two poll bounds %s", tc.name, wait, 2*got)
		}
	}
	if got := ToolPollBound("no_such_tool"); got != ToolCallPollBound {
		t.Errorf("ToolPollBound(%q) = %s, want the fallback %s", "no_such_tool", got, ToolCallPollBound)
	}
	if ListAgentsPollBound <= ToolCallPollBound {
		t.Errorf("ListAgentsPollBound = %s is not above the fallback %s", ListAgentsPollBound, ToolCallPollBound)
	}
}

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
