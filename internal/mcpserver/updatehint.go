package mcpserver

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	toolsCallMethod = "tools/call"
	// updateStatusTimeout bounds one UpdateStatus query; a timeout or error only
	// drops the hint.
	updateStatusTimeout   = 3 * time.Second
	updateRecheckInterval = 10 * time.Minute
)

type updateStatus struct {
	CurrentVersion   string
	AvailableVersion string
	ReleaseURL       string
}

type updateStatusSource func(ctx context.Context) (updateStatus, error)

type updateHint struct {
	source          updateStatusSource
	timeout         time.Duration
	log             *slog.Logger
	fallbackCurrent string
	// A nil now means time.Now.
	now func() time.Time

	// mu protects the fields below; a query runs outside the lock.
	mu              sync.Mutex
	checking        bool
	queried         bool
	lastQuery       time.Time
	notifiedVersion string
}

// middleware appends the notice after the wrapped handler returns, so the result
// is never replaced; the query runs before the result is returned, can delay it
// by up to updateStatusTimeout, and an error or timeout only drops the hint.
func (u *updateHint) middleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		res, err := next(ctx, method, req)
		if method != toolsCallMethod || err != nil || res == nil {
			return res, err
		}
		call, ok := res.(*mcp.CallToolResult)
		if !ok {
			return res, err
		}
		if text, ok := u.take(ctx); ok {
			call.Content = append(call.Content, &mcp.TextContent{Text: text})
		}
		return res, err
	}
}

// take returns the notice to append, if any. Only an answer from the Broker
// counts as a query, so any error leaves the process unchecked and the next
// tool call retries.
func (u *updateHint) take(ctx context.Context) (string, bool) {
	now := u.now
	if now == nil {
		now = time.Now
	}

	u.mu.Lock()
	at := now()
	if u.checking || (u.queried && at.Sub(u.lastQuery) < updateRecheckInterval) {
		u.mu.Unlock()
		return "", false
	}
	u.checking = true
	u.mu.Unlock()

	st, err := u.query(ctx)

	u.mu.Lock()
	u.checking = false
	if err == nil {
		u.queried = true
		u.lastQuery = at
	}
	if err != nil {
		u.mu.Unlock()
		if u.log != nil {
			u.log.Debug("the update check was skipped", "error", err)
		}
		return "", false
	}
	if st.AvailableVersion == "" {
		u.mu.Unlock()
		return "", false
	}
	if st.AvailableVersion == u.notifiedVersion {
		u.mu.Unlock()
		return "", false
	}
	u.notifiedVersion = st.AvailableVersion
	current := st.CurrentVersion
	if current == "" {
		current = u.fallbackCurrent
	}
	u.mu.Unlock()
	return hintText(st.AvailableVersion, current), true
}

// query runs the source with the query timeout; a panic in the source becomes
// an error rather than ending the MCP server.
func (u *updateHint) query(ctx context.Context) (st updateStatus, err error) {
	timeout := u.timeout
	if timeout <= 0 {
		timeout = updateStatusTimeout
	}
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("the update check panicked: %v", r)
			if u.log != nil {
				u.log.Error("panic in the update check", "panic", r)
			}
		}
	}()
	return u.source(qctx)
}

func hintText(available, current string) string {
	return fmt.Sprintf("peers %s is available (this Broker runs %s). "+
		"To upgrade, the user can ask you to call update_broker or run `peers update` in a terminal.",
		available, current)
}
