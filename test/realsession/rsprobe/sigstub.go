package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func init() {
	register("sigstub", cmdSigstub)
}

type sigstubEvent struct {
	Event         string  `json:"event"`
	PID           int     `json:"pid,omitempty"`
	PPID          int     `json:"ppid,omitempty"`
	MessagingSock *string `json:"messaging_socket,omitempty"`
	Sig           string  `json:"sig,omitempty"`
	TMonNs        int64   `json:"t_mono_ns"`
}

// eventLog appends complete JSON lines without buffering, so a parent reading
// the file mid-run sees every event; its mutex keeps events from different
// goroutines from interleaving.
type eventLog struct {
	mu sync.Mutex
	f  *os.File
	t0 time.Time
}

func (l *eventLog) write(ev sigstubEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err = l.f.Write(b)
	return err
}

func (l *eventLog) sinceStart() int64 {
	return time.Since(l.t0).Nanoseconds()
}

func cmdSigstub(args []string) error {
	fs := flag.NewFlagSet("sigstub", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	logPath := fs.String("log", "", "path to the JSON-lines event log")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *logPath == "" {
		return errors.New("sigstub requires --log")
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return serveSigstub(*logPath)
}

// serveSigstub logs every catchable termination signal and never returns
// normally; the probe ends it with SIGKILL.
func serveSigstub(logPath string) error {
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	log := &eventLog{f: f, t0: time.Now()}

	// Install the handlers before announcing the start, so a probe that signals
	// as soon as the start line appears does not hit the default action.
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(sigCh)

	sock := os.Getenv("CLAUDE_CODE_MESSAGING_SOCKET")
	if err := log.write(sigstubEvent{
		Event:         "start",
		PID:           os.Getpid(),
		PPID:          os.Getppid(),
		MessagingSock: &sock,
	}); err != nil {
		return err
	}

	go func() {
		for sig := range sigCh {
			_ = log.write(sigstubEvent{
				Event:  "signal",
				Sig:    signalName(sig),
				TMonNs: log.sinceStart(),
			})
		}
	}()

	server := mcp.NewServer(&mcp.Implementation{Name: "sigstub", Version: "0"}, nil)
	mcp.AddTool[any, any](
		server,
		&mcp.Tool{Name: "ping", Description: "Reply with pong."},
		func(context.Context, *mcp.CallToolRequest, any) (*mcp.CallToolResult, any, error) {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "pong"}},
			}, nil, nil
		},
	)

	// Run returns when stdin closes or the transport fails; the stub stays alive
	// afterwards to see whether Claude Code signals once the session is gone.
	_ = server.Run(context.Background(), &mcp.StdioTransport{})
	if err := log.write(sigstubEvent{
		Event:  "stdin-eof",
		TMonNs: log.sinceStart(),
	}); err != nil {
		return err
	}

	select {}
}

func signalName(sig os.Signal) string {
	switch sig {
	case syscall.SIGINT:
		return "SIGINT"
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGHUP:
		return "SIGHUP"
	default:
		return sig.String()
	}
}
