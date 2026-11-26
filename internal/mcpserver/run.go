package mcpserver

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// Options configures Run. Zero values select the production behaviour; the
// unexported fields are test seams set only by tests in this package.
type Options struct {
	// Version is reported as the MCP implementation version; "" means "dev".
	Version string
	// Transport carries the MCP protocol; nil means a stdio transport.
	Transport mcp.Transport
	// Logger receives every log line; nil means a JSON handler on os.Stderr.
	Logger *slog.Logger
	// LocalProtocolVersion is sent in Hello; 0 means localpb.ProtocolVersion.
	LocalProtocolVersion uint32

	getenv       func(string) string
	getwd        func() (string, error)
	userSocket   string
	systemSocket string
	readOrigin   originReader
	after        func(time.Duration) <-chan time.Time
	probe        func(context.Context, string) probeInfo
	updateStatus updateStatusSource
}

// Run serves the MCP server "peer" until ctx is done or the client closes the
// transport. The MCP handshake completes immediately; the Broker connection is
// made in the background, and a Broker failure never makes Run return. When ctx
// is cancelled, Run cancels the loop, closes the registration stream's send
// side, and waits for both before returning. Run installs no signal handler:
// cmd cancels ctx on SIGINT and SIGTERM. Run always returns nil: a closed
// transport or a cancelled ctx is a normal shutdown; an error from the MCP
// server itself is only logged, and Broker connection failures show up in the
// status phase.
func Run(ctx context.Context, opts Options) error {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}

	version := opts.Version
	if version == "" {
		version = "dev"
	}
	protoVersion := opts.LocalProtocolVersion
	if protoVersion == 0 {
		protoVersion = localpb.ProtocolVersion
	}

	getenv := opts.getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	getwd := opts.getwd
	if getwd == nil {
		getwd = os.Getwd
	}

	dir, err := getwd()
	haveDir := err == nil
	if err != nil {
		log.Error("cannot determine the working directory; no default topic will be joined", "error", err)
	}

	first := &localpb.RegisterRequest{
		InboxSocketPath: getenv("CLAUDE_CODE_MESSAGING_SOCKET"),
		ClaudeConfigDir: getenv("CLAUDE_CONFIG_DIR"),
	}

	st := &statusStore{}
	mem := &topicMemory{}

	readOrigin := opts.readOrigin
	if readOrigin == nil {
		readOrigin = gitOrigin
	}
	session := &sessionTopicList{compute: func(ctx context.Context) defaultLookup {
		if !haveDir {
			return defaultLookup{settled: true}
		}
		return lookupDefaultTopics(ctx, dir, readOrigin, log)
	}}
	defaults := func(ctx context.Context, autoJoin bool) defaultLookup {
		res := session.lookup(ctx)
		res.topics = automaticTopics(res, autoJoin)
		return res
	}
	restore := &restorer{mem: mem, defaults: defaults, log: log}

	h := &handlers{st: st, topics: mem, log: log, sessionTopics: session.get}
	server := newMCPServer(version, h, log)

	// The update hint appends the available version to a tool result and never
	// changes the result otherwise; a failed query only costs the hint.
	updateStatus := opts.updateStatus
	if updateStatus == nil {
		updateStatus = brokerUpdateStatus(st)
	}
	server.AddReceivingMiddleware((&updateHint{
		source:          updateStatus,
		timeout:         updateStatusTimeout,
		log:             log,
		fallbackCurrent: version,
	}).middleware)

	after := opts.after
	if after == nil {
		after = time.After
	}
	userSockets := userSocketCandidates(os.Geteuid())
	if opts.userSocket != "" {
		userSockets = []string{opts.userSocket}
	}
	systemSocket := opts.systemSocket
	if systemSocket == "" {
		systemSocket = systemSocketPath()
	}
	probe := opts.probe
	if probe == nil {
		probe = inspectSocket
	}

	sel := &selector{
		getenv:       getenv,
		userSocket:   userSockets[0],
		systemSocket: systemSocket,
		probe:        inspectSocket,
		after:        after,
		lockHeld:     accountLockHeld,
	}
	if len(userSockets) > 1 {
		sel.altUserSocket = userSockets[1]
	}

	loopCtx, stopLoop := context.WithCancel(ctx)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		runLoop(loopCtx, loopDeps{
			selectTarget: sel.Find,
			probe:        probe,
			dial:         dialLocal,
			hello:        hello,
			waitNotReady: waitNotReady,
			version:      protoVersion,
			register:     runRegistration,
			first:        first,
			st:           st,
			onComplete:   restore.onComplete,
			after:        after,
			log:          log,
		})
	}()

	transport := opts.Transport
	if transport == nil {
		transport = &mcp.StdioTransport{}
	}

	if err := server.Run(ctx, transport); err != nil {
		log.Debug("the MCP server stopped", "error", err)
	}

	// Wait for the connection loop so the Register stream is closed before Run
	// returns.
	stopLoop()
	<-loopDone
	return nil
}
