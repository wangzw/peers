//go:build peers_genman

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

func init() {
	addGenManCommand = addGenManCommandImpl
}

// manPageDate is fixed rather than taken from the clock, so regenerating an
// unchanged tree produces byte-identical pages for gen-check to compare.
var manPageDate = time.Date(2026, time.November, 21, 0, 0, 0, 0, time.UTC)

func addGenManCommandImpl(root *cobra.Command, _ *app) {
	root.AddCommand(&cobra.Command{
		Use:    "gen-man <dir>",
		Short:  "Generate the man pages for the command tree",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return generateManPages(root, args[0])
		},
	})
}

// The stale-page removal deletes every .1 file in dir, so dir must hold only
// generated pages.
func generateManPages(root *cobra.Command, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", dir, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("reading %s: %w", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".1") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return fmt.Errorf("removing stale page %s: %w", entry.Name(), err)
		}
	}
	header := &doc.GenManHeader{
		Title:   "PEERS",
		Section: "1",
		Date:    &manPageDate,
	}
	if err := doc.GenManTree(root, header, dir); err != nil {
		return fmt.Errorf("generating man pages: %w", err)
	}
	return nil
}
