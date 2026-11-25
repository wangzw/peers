package mcpserver

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// TestUpdateHintUsesFallbackCurrentVersion describes a Broker with no current
// version using this process's version.
func TestUpdateHintUsesFallbackCurrentVersion(t *testing.T) {
	u := &updateHint{
		source: func(context.Context) (updateStatus, error) {
			return updateStatus{AvailableVersion: "0.2.0"}, nil
		},
		fallbackCurrent: "0.1.0",
	}
	handler := u.middleware(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return &mcp.CallToolResult{}, nil
	})
	res, err := handler(context.Background(), toolsCallMethod, nil)
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}
	call, ok := res.(*mcp.CallToolResult)
	if !ok || len(call.Content) == 0 {
		t.Fatalf("result = %+v, want the hint appended", res)
	}
	text, ok := call.Content[len(call.Content)-1].(*mcp.TextContent)
	if !ok || !strings.Contains(text.Text, "this Broker runs 0.1.0") {
		t.Fatalf("hint = %+v, want it to use the fallback current version", call.Content)
	}
}

// TestUpdateHintRecoversFromPanic logs a panicking source and keeps the tool
// result.
func TestUpdateHintRecoversFromPanic(t *testing.T) {
	handler := &recordHandler{}
	u := &updateHint{
		source: func(context.Context) (updateStatus, error) {
			panic("boom")
		},
		log: slog.New(handler),
	}
	next := func(context.Context, string, mcp.Request) (mcp.Result, error) {
		return &mcp.CallToolResult{}, nil
	}
	res, err := u.middleware(next)(context.Background(), toolsCallMethod, nil)
	if err != nil {
		t.Fatalf("middleware: %v", err)
	}
	call, ok := res.(*mcp.CallToolResult)
	if !ok || len(call.Content) != 0 {
		t.Fatalf("result = %+v, want the tool result without a hint", res)
	}
	want := "panic in the update check"
	for _, message := range handler.messages() {
		if message == want {
			return
		}
	}
	t.Fatalf("log messages = %v, want %q", handler.messages(), want)
}
