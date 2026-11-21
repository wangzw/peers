package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/service"
)

// setServiceSeams installs the option and operation seams for one test. A nil
// function leaves that seam unchanged.
func setServiceSeams(
	t *testing.T,
	options func(bool) (service.Options, error),
	install func(service.Options) (service.Result, error),
	uninstall func(service.Options) (service.Result, error),
) {
	t.Helper()
	previousOptions, previousInstall, previousUninstall := defaultOptionsFn, installFn, uninstallFn
	if options != nil {
		defaultOptionsFn = options
	}
	if install != nil {
		installFn = install
	}
	if uninstall != nil {
		uninstallFn = uninstall
	}
	// The stub's deps.euid reports a non-root account, so the tests check the
	// default level independently of who runs them.
	t.Cleanup(func() {
		defaultOptionsFn = previousOptions
		installFn = previousInstall
		uninstallFn = previousUninstall
	})
}

func TestServiceInstallMapsSystemFlag(t *testing.T) {
	cases := []struct {
		args       []string
		euid       int
		wantSystem bool
	}{
		{[]string{"service", "install"}, 1000, false},
		{[]string{"service", "install", "--user"}, 1000, false},
		{[]string{"service", "install", "--system"}, 1000, true},
		{[]string{"service", "install"}, 0, true},
		{[]string{"service", "install", "--user"}, 0, false},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var (
				optionsSystem bool
				optionsCalls  int
				installed     service.Options
				installCalls  int
			)
			setServiceSeams(t,
				func(system bool) (service.Options, error) {
					optionsCalls++
					optionsSystem = system
					return service.Options{System: system, Exe: "/usr/local/bin/peers"}, nil
				},
				func(opts service.Options) (service.Result, error) {
					installCalls++
					installed = opts
					if opts.System {
						return service.Result{
							Binary:     "/usr/local/bin/peers",
							ConfigPath: "/etc/peers/config.yaml",
							ConfigNew:  true,
							RunLog:     "journalctl -u peers.service",
							AuditLog:   "/var/log/peers/audit.log",
						}, nil
					}
					return service.Result{
						Binary:     "/usr/local/bin/peers",
						ConfigPath: "/home/alice/.peers/config.yaml",
						ConfigNew:  true,
						RunLog:     "journalctl --user -u peers.service",
						AuditLog:   "/home/alice/.peers/audit.log",
					}, nil
				},
				nil,
			)

			a, stdout, stderr, _ := newDispatchApp("dev")
			a.deps.euid = func() int { return tc.euid }
			if got := a.run(context.Background(), tc.args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", tc.args, got, exitOK, stderr.String())
			}
			if optionsCalls != 1 || installCalls != 1 {
				t.Fatalf("option calls = %d, install calls = %d, want 1 each", optionsCalls, installCalls)
			}
			if optionsSystem != tc.wantSystem || installed.System != tc.wantSystem {
				t.Fatalf("system = %v (options) / %v (install), want %v", optionsSystem, installed.System, tc.wantSystem)
			}
			level := "user service, starts at login"
			if tc.wantSystem {
				level = "system service, starts at boot"
			}
			if want := "peers dev installed (" + level + ")\n"; !strings.HasPrefix(stdout.String(), want) {
				t.Fatalf("stdout = %q, want it to start with %q", stdout.String(), want)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestServiceInstallMapsAdminGroup(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"service", "install", "--system"}, ""},
		{[]string{"service", "install", "--system", "--admin-group", "broker-ops"}, "broker-ops"},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var installed service.Options
			setServiceSeams(t,
				func(system bool) (service.Options, error) {
					return service.Options{System: system, Exe: "/usr/local/bin/peers"}, nil
				},
				func(opts service.Options) (service.Result, error) {
					installed = opts
					return service.Result{UnitPath: "/x/peers.service"}, nil
				},
				nil,
			)

			a, _, stderr, _ := newDispatchApp("dev")
			if got := a.run(context.Background(), tc.args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", tc.args, got, exitOK, stderr.String())
			}
			if installed.AdminGroup != tc.want {
				t.Errorf("install AdminGroup = %q, want %q", installed.AdminGroup, tc.want)
			}
		})
	}
}

// TestServiceInstallPrintsTheSummary checks the summary printed after a
// successful install: level and start time, then the paths, the commands and
// the configuration state. The administration-group hint follows a kept
// system configuration.
func TestServiceInstallPrintsTheSummary(t *testing.T) {
	cases := []struct {
		name   string
		system bool
		result service.Result
		want   string
	}{
		{
			name: "user generated configuration",
			result: service.Result{
				Binary:     "/usr/local/bin/peers",
				ConfigPath: "/home/alice/.peers/config.yaml",
				ConfigNew:  true,
				RunLog:     "journalctl --user -u peers.service",
				AuditLog:   "/home/alice/.peers/audit.log",
			},
			want: "peers 0.1.2 installed (user service, starts at login)\n" +
				"  binary:      /usr/local/bin/peers\n" +
				"  config:      /home/alice/.peers/config.yaml (generated)\n" +
				"  run log:     journalctl --user -u peers.service\n" +
				"  audit log:   /home/alice/.peers/audit.log\n" +
				"  start:       peers service start\n" +
				"  stop:        peers service stop\n" +
				"  restart:     peers service restart\n" +
				"  uninstall:   peers uninstall\n",
		},
		{
			name: "user kept configuration",
			result: service.Result{
				Binary:     "/usr/local/bin/peers",
				ConfigPath: "/home/alice/.peers/config.yaml",
				RunLog:     "journalctl --user -u peers.service",
				AuditLog:   "/home/alice/.peers/audit.log",
			},
			want: "peers 0.1.2 installed (user service, starts at login)\n" +
				"  binary:      /usr/local/bin/peers\n" +
				"  config:      /home/alice/.peers/config.yaml (kept)\n" +
				"  run log:     journalctl --user -u peers.service\n" +
				"  audit log:   /home/alice/.peers/audit.log\n" +
				"  start:       peers service start\n" +
				"  stop:        peers service stop\n" +
				"  restart:     peers service restart\n" +
				"  uninstall:   peers uninstall\n",
		},
		{
			name:   "system generated configuration",
			system: true,
			result: service.Result{
				Binary:            "/usr/local/bin/peers",
				ConfigPath:        "/etc/peers/config.yaml",
				ConfigNew:         true,
				AdminGroup:        "peers",
				AdminGroupCreated: true,
				RunLog:            "journalctl -u peers.service",
				AuditLog:          "/var/log/peers/audit.log",
			},
			want: "peers 0.1.2 installed (system service, starts at boot)\n" +
				"  binary:      /usr/local/bin/peers\n" +
				"  config:      /etc/peers/config.yaml (generated)\n" +
				"  admin group: peers (created)\n" +
				"  run log:     journalctl -u peers.service\n" +
				"  audit log:   /var/log/peers/audit.log\n" +
				"  start:       sudo peers service start\n" +
				"  stop:        sudo peers service stop\n" +
				"  restart:     sudo peers service restart\n" +
				"  uninstall:   peers uninstall\n",
		},
		{
			name:   "system reused group with members",
			system: true,
			result: service.Result{
				Binary:                   "/usr/local/bin/peers",
				ConfigPath:               "/etc/peers/config.yaml",
				ConfigNew:                true,
				AdminGroup:               "peers",
				AdminGroupMembers:        []string{"alice", "bob"},
				AdminGroupMembersQueried: true,
				RunLog:                   "journalctl -u peers.service",
				AuditLog:                 "/var/log/peers/audit.log",
			},
			want: "peers 0.1.2 installed (system service, starts at boot)\n" +
				"  binary:      /usr/local/bin/peers\n" +
				"  config:      /etc/peers/config.yaml (generated)\n" +
				"  admin group: peers (existing, members: alice, bob)\n" +
				"  run log:     journalctl -u peers.service\n" +
				"  audit log:   /var/log/peers/audit.log\n" +
				"  start:       sudo peers service start\n" +
				"  stop:        sudo peers service stop\n" +
				"  restart:     sudo peers service restart\n" +
				"  uninstall:   peers uninstall\n",
		},
		{
			name:   "system reused group with no members",
			system: true,
			result: service.Result{
				Binary:                   "/usr/local/bin/peers",
				ConfigPath:               "/etc/peers/config.yaml",
				ConfigNew:                true,
				AdminGroup:               "peers",
				AdminGroupMembersQueried: true,
				RunLog:                   "journalctl -u peers.service",
				AuditLog:                 "/var/log/peers/audit.log",
			},
			want: "peers 0.1.2 installed (system service, starts at boot)\n" +
				"  binary:      /usr/local/bin/peers\n" +
				"  config:      /etc/peers/config.yaml (generated)\n" +
				"  admin group: peers (existing, no members listed)\n" +
				"  run log:     journalctl -u peers.service\n" +
				"  audit log:   /var/log/peers/audit.log\n" +
				"  start:       sudo peers service start\n" +
				"  stop:        sudo peers service stop\n" +
				"  restart:     sudo peers service restart\n" +
				"  uninstall:   peers uninstall\n",
		},
		{
			name:   "system kept configuration",
			system: true,
			result: service.Result{
				Binary:     "/usr/local/bin/peers",
				ConfigPath: "/etc/peers/config.yaml",
				AdminGroup: "peers",
				RunLog:     "journalctl -u peers.service",
				AuditLog:   "/var/log/peers/audit.log",
			},
			want: "peers 0.1.2 installed (system service, starts at boot)\n" +
				"  binary:      /usr/local/bin/peers\n" +
				"  config:      /etc/peers/config.yaml (kept)\n" +
				"  admin group: peers (existing)\n" +
				"  run log:     journalctl -u peers.service\n" +
				"  audit log:   /var/log/peers/audit.log\n" +
				"  start:       sudo peers service start\n" +
				"  stop:        sudo peers service stop\n" +
				"  restart:     sudo peers service restart\n" +
				"  uninstall:   peers uninstall\n" +
				"To delegate administration, run: chgrp peers /etc/peers/config.yaml && chmod 0660 /etc/peers/config.yaml\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setServiceSeams(t,
				func(bool) (service.Options, error) { return service.Options{System: tc.system}, nil },
				func(service.Options) (service.Result, error) { return tc.result, nil },
				nil,
			)

			args := []string{"service", "install"}
			if tc.system {
				args = append(args, "--system")
			}
			a, stdout, stderr, _ := newDispatchApp("0.1.2")
			if got := a.run(context.Background(), args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", args, got, exitOK, stderr.String())
			}
			if stdout.String() != tc.want {
				t.Fatalf("stdout = %q, want %q", stdout.String(), tc.want)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

// TestServiceInstallReportsDegraded checks that a service installed and running
// with a degraded Broker still prints the summary, writes the two degraded
// lines to standard error, and exits with exitServiceDegraded.
func TestServiceInstallReportsDegraded(t *testing.T) {
	result := service.Result{
		Binary:     "/usr/local/bin/peers",
		ConfigPath: "/home/alice/.peers/config.yaml",
		ConfigNew:  true,
		RunLog:     "journalctl --user -u peers.service",
		AuditLog:   "/home/alice/.peers/audit.log",
	}
	summary := "peers 0.1.2 installed (user service, starts at login)\n" +
		"  binary:      /usr/local/bin/peers\n" +
		"  config:      /home/alice/.peers/config.yaml (generated)\n" +
		"  run log:     journalctl --user -u peers.service\n" +
		"  audit log:   /home/alice/.peers/audit.log\n" +
		"  start:       peers service start\n" +
		"  stop:        peers service stop\n" +
		"  restart:     peers service restart\n" +
		"  uninstall:   peers uninstall\n"

	cases := []struct {
		name  string
		state service.DegradedState
		want  string
	}{
		{
			name: "local only",
			state: service.DegradedState{
				Level:  service.DegradedLocalOnly,
				Reason: "listen tcp :17443: bind: address already in use",
				Fix:    "change broker.listen to another port",
			},
			want: "Broker: degraded (local only): listen tcp :17443: bind: address already in use\n" +
				"Fix: change broker.listen to another port\n",
		},
		{
			name: "diagnostics only",
			state: service.DegradedState{
				Level:  service.DegradedDiagnosticsOnly,
				Reason: "the data directory is not writable",
				Fix:    "fix the permissions of ~/.peers",
			},
			want: "Broker: degraded (diagnostics only): the data directory is not writable\n" +
				"Fix: fix the permissions of ~/.peers\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setServiceSeams(t,
				func(bool) (service.Options, error) { return service.Options{}, nil },
				func(service.Options) (service.Result, error) {
					return result, &service.DegradedError{State: tc.state}
				},
				nil,
			)

			a, stdout, stderr, _ := newDispatchApp("0.1.2")
			if got := a.run(context.Background(), []string{"service", "install"}); got != exitServiceDegraded {
				t.Fatalf("run(service install) = %d, want %d", got, exitServiceDegraded)
			}
			if stdout.String() != summary {
				t.Fatalf("stdout = %q, want the summary %q", stdout.String(), summary)
			}
			if stderr.String() != tc.want {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.want)
			}
		})
	}
}

func TestServiceUninstall(t *testing.T) {
	cases := []struct {
		name    string
		result  service.Result
		system  bool
		want    string
		wantArg bool
	}{
		{
			name:   "removed",
			result: service.Result{Removed: true},
			want:   "uninstalled peers.service\n",
		},
		{
			name:   "removed from a launchd domain",
			result: service.Result{Removed: true, Domain: "user/501"},
			want:   "uninstalled io.github.wangzw.peers from user/501\n",
		},
		{
			name:   "system",
			result: service.Result{Removed: true},
			system: true,
			want:   "uninstalled peers.service\n",
		},
		{
			name:   "not installed",
			result: service.Result{},
			want:   "The peers service is not installed.\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				gotSystem     bool
				uninstallCall int
			)
			setServiceSeams(t,
				func(system bool) (service.Options, error) {
					gotSystem = system
					return service.Options{System: system}, nil
				},
				nil,
				func(opts service.Options) (service.Result, error) {
					uninstallCall++
					return tc.result, nil
				},
			)

			args := []string{"service", "remove"}
			if tc.system {
				args = append(args, "--system")
			}
			a, stdout, stderr, _ := newDispatchApp("dev")
			if got := a.run(context.Background(), args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", args, got, exitOK, stderr.String())
			}
			if uninstallCall != 1 {
				t.Fatalf("uninstall calls = %d, want 1", uninstallCall)
			}
			if gotSystem != tc.system {
				t.Fatalf("system = %v, want %v", gotSystem, tc.system)
			}
			if stdout.String() != tc.want {
				t.Fatalf("stdout = %q, want %q", stdout.String(), tc.want)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestServiceReportsErrors(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		var installCalls int
		setServiceSeams(t,
			func(bool) (service.Options, error) { return service.Options{}, errors.New("no executable") },
			func(service.Options) (service.Result, error) {
				installCalls++
				return service.Result{}, nil
			},
			nil,
		)

		a, stdout, stderr, _ := newDispatchApp("dev")
		if got := a.run(context.Background(), []string{"service", "install"}); got != exitFailure {
			t.Fatalf("run(service install) = %d, want %d", got, exitFailure)
		}
		if installCalls != 0 {
			t.Fatalf("install calls = %d, want 0", installCalls)
		}
		if want := "peers service install: no executable\n"; stderr.String() != want {
			t.Fatalf("stderr = %q, want %q", stderr.String(), want)
		}
		if stdout.Len() != 0 {
			t.Fatalf("stdout = %q, want empty", stdout.String())
		}
	})

	t.Run("install", func(t *testing.T) {
		setServiceSeams(t,
			func(bool) (service.Options, error) { return service.Options{}, nil },
			func(service.Options) (service.Result, error) {
				return service.Result{}, errors.New("systemctl failed")
			},
			nil,
		)

		a, stdout, stderr, _ := newDispatchApp("dev")
		if got := a.run(context.Background(), []string{"service", "install", "--system"}); got != exitFailure {
			t.Fatalf("run(service install --system) = %d, want %d", got, exitFailure)
		}
		if want := "peers service install: systemctl failed\n"; stderr.String() != want {
			t.Fatalf("stderr = %q, want %q", stderr.String(), want)
		}
		if stdout.Len() != 0 {
			t.Fatalf("stdout = %q, want no success line after a failure", stdout.String())
		}
	})

	t.Run("remove", func(t *testing.T) {
		setServiceSeams(t,
			func(bool) (service.Options, error) { return service.Options{}, nil },
			nil,
			func(service.Options) (service.Result, error) {
				return service.Result{}, errors.New("systemctl failed")
			},
		)

		a, stdout, stderr, _ := newDispatchApp("dev")
		if got := a.run(context.Background(), []string{"service", "remove"}); got != exitFailure {
			t.Fatalf("run(service remove) = %d, want %d", got, exitFailure)
		}
		if want := "peers service remove: systemctl failed\n"; stderr.String() != want {
			t.Fatalf("stderr = %q, want %q", stderr.String(), want)
		}
		if stdout.Len() != 0 {
			t.Fatalf("stdout = %q, want empty", stdout.String())
		}
	})
}

func TestServiceUsageErrors(t *testing.T) {
	cases := []struct {
		args []string
		// want is a substring of stderr unless wantStderr is set, which then
		// has to match stderr exactly.
		want       string
		wantStderr string
	}{
		{args: []string{"service"}, want: "Usage:"},
		{
			args:       []string{"service", "bogus"},
			wantStderr: usageDiagnostic("peers service", `unknown command "bogus" for "peers service"`),
		},
		{
			args:       []string{"service", "install", "extra"},
			wantStderr: usageDiagnostic("peers service install", `unexpected argument "extra"`),
		},
		{
			args:       []string{"service", "install", "--bogus"},
			wantStderr: usageDiagnostic("peers service install", "unknown flag: --bogus"),
		},
		{
			args:       []string{"service", "install", "--admin-group", "ops"},
			wantStderr: usageDiagnostic("peers service install", "--admin-group requires --system"),
		},
		{
			args:       []string{"service", "install", "--system", "--admin-group", "-ops"},
			wantStderr: usageDiagnostic("peers service install", `invalid --admin-group "-ops": a group name must start with a lowercase letter or underscore and contain only lowercase letters, digits, underscores and hyphens`),
		},
		{
			args:       []string{"service", "install", "--system", "--admin-group", "Ops"},
			wantStderr: usageDiagnostic("peers service install", `invalid --admin-group "Ops": a group name must start with a lowercase letter or underscore and contain only lowercase letters, digits, underscores and hyphens`),
		},
		{
			args:       []string{"service", "install", "--system", "--user"},
			wantStderr: usageDiagnostic("peers service install", "--system and --user are mutually exclusive"),
		},
		{
			args:       []string{"service", "remove", "extra"},
			wantStderr: usageDiagnostic("peers service remove", `unexpected argument "extra"`),
		},
		{
			args:       []string{"service", "remove", "--bogus"},
			wantStderr: usageDiagnostic("peers service remove", "unknown flag: --bogus"),
		},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var calls int
			setServiceSeams(t,
				func(bool) (service.Options, error) {
					t.Fatal("default options ran for a usage error")
					return service.Options{}, nil
				},
				func(service.Options) (service.Result, error) {
					calls++
					return service.Result{}, nil
				},
				func(service.Options) (service.Result, error) {
					calls++
					return service.Result{}, nil
				},
			)

			a, stdout, stderr, _ := newDispatchApp("dev")
			if got := a.run(context.Background(), tc.args); got != exitUsage {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", tc.args, got, exitUsage, stderr.String())
			}
			if calls != 0 {
				t.Fatalf("operation calls = %d, want 0", calls)
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
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), tc.want)
			}
		})
	}
}

func TestServiceHelpPrintsUsage(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"service", "--help"}, "peers service"},
		{[]string{"service", "install", "--help"}, "peers service install"},
		{[]string{"service", "remove", "--help"}, "peers service remove"},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			setServiceSeams(t,
				func(bool) (service.Options, error) {
					t.Fatal("default options ran for --help")
					return service.Options{}, nil
				},
				func(service.Options) (service.Result, error) {
					t.Fatal("install ran for --help")
					return service.Result{}, nil
				},
				func(service.Options) (service.Result, error) {
					t.Fatal("remove ran for --help")
					return service.Result{}, nil
				},
			)

			a, stdout, stderr, _ := newDispatchApp("dev")
			if got := a.run(context.Background(), tc.args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitOK)
			}
			if !strings.Contains(stdout.String(), "Usage:") || !strings.Contains(stdout.String(), tc.want) {
				t.Fatalf("stdout = %q, want the usage naming %q", stdout.String(), tc.want)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}
