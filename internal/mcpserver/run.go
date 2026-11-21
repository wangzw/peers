package mcpserver

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// Options configures Run.
//
// Zero values select the production behaviour. The unexported fields are test
// seams set only by tests in this package.
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
	// probe replaces the loop's raw socket probe; nil means probeSocket. The
	// loop uses it to tell a user Broker whose socket is gone from one
	// whose socket still answers.
	probe func(context.Context, string) probeResult
	// updateStatus replaces the Broker query made by the update hint; nil means
	// brokerUpdateStatus. Tests use it to avoid a Broker.
	updateStatus updateStatusSource
}

// Run serves the MCP server "peer" until ctx is done or the client closes the
// transport. The MCP handshake completes immediately: connecting to the Broker
// happens in the background, before the first tool call rather than before the
// handshake.
//
// When ctx is cancelled, Run cancels the background loop and waits for it to
// return; the registration stream closes its send side before Run returns. Run
// installs no signal handler: cmd cancels ctx on SIGINT and SIGTERM. A Broker
// failure never makes Run return.
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

	// The working directory is read once. Without it there is no place to look
	// for a project configuration or a git origin, so no default topic.
	dir, err := getwd()
	haveDir := err == nil
	if err != nil {
		log.Error("cannot determine the working directory; no default topic will be joined", "error", err)
	}

	// The two environment variables that make up the first Register frame are
	// read once; every re-registration reuses the same values.
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
	// The session's default topics are read once, on the first registration,
	// and the same result serves both the automatic join and nameless topic
	// calls. A later edit to the project configuration or the origin therefore
	// cannot make a nameless leave miss a topic this process joined
	// automatically. Leaving some of them does not remove them from the list.
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

	// The update hint decorates a tool result with the version the Broker
	// reports as available. It queries at most once per updateRecheckInterval,
	// never repeats a version, and never changes the result on its own: a
	// failed, missing or unresponsive Broker only costs the hint.
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
		probe = probeSocket
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

	// A normal shutdown is not an error: the client may close the transport and
	// ctx may be cancelled.
	if err := server.Run(ctx, transport); err != nil {
		log.Debug("the MCP server stopped", "error", err)
	}

	// Wait for the connection loop so that the Register stream has been closed
	// before Run returns.
	stopLoop()
	<-loopDone
	return nil
}
