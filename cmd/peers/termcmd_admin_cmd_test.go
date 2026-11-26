package main

import (
	"testing"
)

// TestAdminCommandsRegistered checks that the broker and key nouns are direct
// children of the root command.
func TestAdminCommandsRegistered(t *testing.T) {
	a, _, _ := newTermcmdApp(&termcmdStub{})
	root := a.newRootCommand()
	for _, name := range []string{"broker", "key"} {
		cmd, _, err := root.Find([]string{name})
		if err != nil || cmd.Name() != name {
			t.Errorf("root command has no %q command", name)
		}
	}
}
