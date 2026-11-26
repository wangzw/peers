//go:build peers_testenv

package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

// signalRestoreDone receives one value when signalRestoreHook fires. It is
// buffered and sent to rather than closed, so a test binary with more than one
// signal context cannot panic.
var signalRestoreDone = make(chan struct{}, 1)

func init() {
	signalRestoreHook = func() {
		select {
		case signalRestoreDone <- struct{}{}:
		default:
		}
	}

	addTestInternalCommands = func(internal *cobra.Command, a *app) {
		internal.AddCommand(&cobra.Command{
			Use:    "signal-block",
			Short:  "Block until a signal cancels the context",
			Hidden: true,
			RunE: func(cmd *cobra.Command, _ []string) error {
				_, _ = fmt.Fprintln(a.stderr, "ready")
				<-cmd.Context().Done()
				// newSignalContext restores the default handler from another
				// goroutine, so wait for the hook: otherwise the second signal can
				// race the restore and be swallowed by the notifier.
				<-signalRestoreDone
				_, _ = fmt.Fprintln(a.stderr, "cancelled")
				<-time.After(time.Hour)
				return nil
			},
		})
	}
}
