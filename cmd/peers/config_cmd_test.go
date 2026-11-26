package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/wangzw/peers/internal/broker"
)

func configCalls(s *termcmdStub) int {
	return len(s.configGetCalls) + len(s.configSetCalls)
}

// TestConfigKeyCompletion checks that key completion offers the configuration
// keys, filtered by the typed word, only for the first positional argument.
func TestConfigKeyCompletion(t *testing.T) {
	all := broker.ConfigKeys()
	if len(all) == 0 {
		t.Fatal("broker.ConfigKeys returned no keys")
	}
	if !slices.Contains(all, "log.level") {
		t.Fatalf("broker.ConfigKeys does not contain log.level: %v", all)
	}
	prefix := all[0][:strings.IndexByte(all[0], '.')+1]
	var prefixed []string
	for _, name := range all {
		if strings.HasPrefix(name, prefix) {
			prefixed = append(prefixed, name)
		}
	}

	cases := []struct {
		name       string
		args       []string
		toComplete string
		want       []string
	}{
		{"every key", nil, "", all},
		{"typed prefix", nil, prefix, prefixed},
		{"known key", nil, "log.level", []string{"log.level"}},
		{"after the key", []string{all[0]}, "", nil},
		{"after a key and a value", []string{all[0], "x"}, "", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, directive := completeConfigKey(nil, tc.args, tc.toComplete)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("completeConfigKey(%q, %q) = %v, want %v", tc.args, tc.toComplete, got, tc.want)
			}
			if directive != cobra.ShellCompDirectiveNoFileComp {
				t.Fatalf("directive = %v, want %v", directive, cobra.ShellCompDirectiveNoFileComp)
			}
		})
	}
}

// TestConfigCompletionEndToEnd checks that the hidden __complete command offers
// log.level for config get.
func TestConfigCompletionEndToEnd(t *testing.T) {
	a, stdout, _ := newTermcmdApp(&termcmdStub{})

	if got := a.run(context.Background(), []string{"__complete", "config", "get", ""}); got != exitOK {
		t.Fatalf("run(__complete config get) = %d, want %d", got, exitOK)
	}
	if !strings.Contains(stdout.String(), "log.level") {
		t.Fatalf("stdout = %q, want it to offer log.level", stdout.String())
	}
}

// TestConfigCommandsAreWired checks that config is under the local group with
// its three verbs and key completion on the two key-taking verbs.
func TestConfigCommandsAreWired(t *testing.T) {
	a, _, _ := newTermcmdApp(&termcmdStub{})
	root := a.newRootCommand()

	configCmd, _, err := root.Find([]string{"config"})
	if err != nil {
		t.Fatalf("find config: %v", err)
	}
	if configCmd.Name() != "config" {
		t.Fatalf("root command reached %q, want config", configCmd.CommandPath())
	}
	if configCmd.GroupID != groupLocal {
		t.Errorf("config group = %q, want %q", configCmd.GroupID, groupLocal)
	}
	for _, name := range []string{"list", "get", "set"} {
		if _, _, err := configCmd.Find([]string{name}); err != nil {
			t.Errorf("config %s: %v", name, err)
		}
	}
	for _, name := range []string{"get", "set"} {
		cmd, _, err := configCmd.Find([]string{name})
		if err != nil {
			t.Fatalf("config %s: %v", name, err)
		}
		if cmd.ValidArgsFunction == nil {
			t.Errorf("config %s has no key completion", name)
		}
	}
}

// TestConfigGetCallsRunner checks that config list and config get reach the
// runner with the expected key.
func TestConfigGetCallsRunner(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantKey string
	}{
		{"all items", []string{"config", "list"}, ""},
		{"one item", []string{"config", "get", "log.level"}, "log.level"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &termcmdStub{}
			a, stdout, stderr := newTermcmdApp(stub)

			if got := a.run(context.Background(), tc.args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", tc.args, got, exitOK, stderr.String())
			}
			if len(stub.configGetCalls) != 1 {
				t.Fatalf("runConfigGet called %d times, want 1", len(stub.configGetCalls))
			}
			call := stub.configGetCalls[0]
			if call.key != tc.wantKey {
				t.Fatalf("runConfigGet key = %q, want %q", call.key, tc.wantKey)
			}
			if call.out != stdout {
				t.Fatalf("runConfigGet got writer %T, want app.stdout", call.out)
			}
			if stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("stdout = %q, stderr = %q, want both empty", stdout.String(), stderr.String())
			}
		})
	}
}

// TestConfigSetCallsRunner checks that config set passes the key and value to
// the runner.
func TestConfigSetCallsRunner(t *testing.T) {
	stub := &termcmdStub{}
	a, stdout, stderr := newTermcmdApp(stub)

	if got := a.run(context.Background(), []string{"config", "set", "log.level", "debug"}); got != exitOK {
		t.Fatalf("run(config set) = %d, want %d (stderr %q)", got, exitOK, stderr.String())
	}
	if len(stub.configSetCalls) != 1 {
		t.Fatalf("runConfigSet called %d times, want 1", len(stub.configSetCalls))
	}
	call := stub.configSetCalls[0]
	if call.key != "log.level" || call.value != "debug" {
		t.Fatalf("runConfigSet got (%q, %q), want (log.level, debug)", call.key, call.value)
	}
	if call.out != stdout {
		t.Fatalf("runConfigSet got writer %T, want app.stdout", call.out)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("stdout = %q, stderr = %q, want both empty", stdout.String(), stderr.String())
	}
}

// TestConfigCommandErrorsExitOne checks that a runtime error on a config command
// writes one line and exits 1.
func TestConfigCommandErrorsExitOne(t *testing.T) {
	errDenied := errors.New("permission_denied: not allowed")
	cases := []struct {
		name    string
		args    []string
		command string
		prepare func(*termcmdStub)
	}{
		{
			name: "config list", args: []string{"config", "list"}, command: "config list",
			prepare: func(s *termcmdStub) { s.configGetErr = errDenied },
		},
		{
			name: "config get", args: []string{"config", "get", "broker.alias"}, command: "config get",
			prepare: func(s *termcmdStub) { s.configGetErr = errDenied },
		},
		{
			name: "config set", args: []string{"config", "set", "k", "v"}, command: "config set",
			prepare: func(s *termcmdStub) { s.configSetErr = errDenied },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &termcmdStub{}
			tc.prepare(stub)
			a, stdout, stderr := newTermcmdApp(stub)

			if got := a.run(context.Background(), tc.args); got != exitFailure {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitFailure)
			}
			if want := "peers " + tc.command + ": permission_denied: not allowed\n"; stderr.String() != want {
				t.Fatalf("stderr = %q, want %q", stderr.String(), want)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

// TestConfigUsageErrors checks the usage diagnostic for each malformed config
// invocation.
func TestConfigUsageErrors(t *testing.T) {
	cases := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "config without verb", args: []string{"config"}},
		{
			name:       "config unknown verb",
			args:       []string{"config", "bogus"},
			wantStderr: usageDiagnostic("peers config", `unknown command "bogus" for "peers config"`),
		},
		{
			name:       "config list extra argument",
			args:       []string{"config", "list", "x"},
			wantStderr: usageDiagnostic("peers config list", `unexpected argument "x"`),
		},
		{
			name:       "config get unknown flag",
			args:       []string{"config", "get", "--bogus"},
			wantStderr: usageDiagnostic("peers config get", "unknown flag: --bogus"),
		},
		{
			name:       "config get without key",
			args:       []string{"config", "get"},
			wantStderr: usageDiagnostic("peers config get", "accepts 1 arg(s), received 0"),
		},
		{
			name:       "config get extra argument",
			args:       []string{"config", "get", "a", "b"},
			wantStderr: usageDiagnostic("peers config get", "accepts 1 arg(s), received 2"),
		},
		{
			name:       "config set missing value",
			args:       []string{"config", "set", "key"},
			wantStderr: usageDiagnostic("peers config set", "accepts 2 arg(s), received 1"),
		},
		{
			name:       "config set extra argument",
			args:       []string{"config", "set", "k", "v", "x"},
			wantStderr: usageDiagnostic("peers config set", "accepts 2 arg(s), received 3"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &termcmdStub{}
			a, stdout, stderr := newTermcmdApp(stub)

			if got := a.run(context.Background(), tc.args); got != exitUsage {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitUsage)
			}
			if calls := configCalls(stub); calls != 0 {
				t.Fatalf("dependencies called %d times, want 0", calls)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if tc.wantStderr != "" {
				if got := stderr.String(); got != tc.wantStderr {
					t.Fatalf("stderr = %q, want %q", got, tc.wantStderr)
				}
				return
			}
			if !strings.Contains(stderr.String(), "Usage:") || !strings.Contains(stderr.String(), "peers config") {
				t.Fatalf("stderr = %q, want the config usage", stderr.String())
			}
		})
	}
}

// TestConfigHelp checks that each config command prints its usage on -h or
// --help.
func TestConfigHelp(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		wantUsage string
	}{
		{"config -h", []string{"config", "-h"}, "peers config"},
		{"config --help", []string{"config", "--help"}, "peers config"},
		{"config list -h", []string{"config", "list", "-h"}, "peers config list"},
		{"config list --help", []string{"config", "list", "--help"}, "peers config list"},
		{"config get -h", []string{"config", "get", "-h"}, "peers config get <key>"},
		{"config get after key", []string{"config", "get", "broker.alias", "--help"}, "peers config get <key>"},
		{"config set --help", []string{"config", "set", "--help"}, "peers config set <key> <value>"},
		{"config set after value", []string{"config", "set", "k", "v", "-h"}, "peers config set <key> <value>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &termcmdStub{}
			a, stdout, stderr := newTermcmdApp(stub)

			if got := a.run(context.Background(), tc.args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", tc.args, got, exitOK, stderr.String())
			}
			if calls := configCalls(stub); calls != 0 {
				t.Fatalf("dependencies called %d times, want 0", calls)
			}
			if !strings.Contains(stdout.String(), tc.wantUsage) {
				t.Fatalf("stdout = %q, want it to contain %q", stdout.String(), tc.wantUsage)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}
