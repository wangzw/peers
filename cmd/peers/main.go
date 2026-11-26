// Command peers is the single entry point for the Broker process, the MCP
// server and the terminal commands.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
)

// version is set at build time with -ldflags "-X main.version=<version>".
var version = "dev"

func main() {
	ctx, stop := newSignalContext(context.Background())
	a := &app{
		stdin:   os.Stdin,
		stdout:  os.Stdout,
		stderr:  os.Stderr,
		version: version,
		deps:    realDeps(version),
	}
	code := runGuarded(os.Stderr, func() int { return a.run(ctx, os.Args[1:]) })
	stop()
	os.Exit(code)
}

// runGuarded runs one command line and turns a panic into exitFailure. Left to
// the Go runtime, a panic exits with status 2, which the update contract
// reserves for "this binary has no post-update command".
func runGuarded(stderr io.Writer, fn func() int) (code int) {
	defer func() {
		if r := recover(); r != nil {
			_, _ = fmt.Fprintf(stderr, "peers: internal error: %v\n", r)
			code = exitFailure
		}
	}()
	return fn()
}
