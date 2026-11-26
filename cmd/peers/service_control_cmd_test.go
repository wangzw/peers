package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/service"
)

type controlSeams struct {
	options   func(bool) (service.Options, error)
	start     func(service.Options) (service.Result, error)
	stop      func(service.Options) (service.Result, error)
	restart   func(service.Options) (service.Result, error)
	installed func(service.Options) (bool, error)
}

func setControlSeams(t *testing.T, s controlSeams) {
	t.Helper()
	previousOptions, previousStart, previousStop := defaultOptionsFn, startFn, stopFn
	previousRestart, previousInstalled := restartFn, installedFn
	if s.options != nil {
		defaultOptionsFn = s.options
	}
	if s.start != nil {
		startFn = s.start
	}
	if s.stop != nil {
		stopFn = s.stop
	}
	if s.restart != nil {
		restartFn = s.restart
	}
	if s.installed != nil {
		installedFn = s.installed
	}
	t.Cleanup(func() {
		defaultOptionsFn, startFn, stopFn = previousOptions, previousStart, previousStop
		restartFn, installedFn = previousRestart, previousInstalled
	})
}

// TestServiceControlMapsVerbAndLevel checks that each control verb calls its
// operation at the selected level.
func TestServiceControlMapsVerbAndLevel(t *testing.T) {
	cases := []struct {
		command  string
		args     []string
		euid     int
		wantPast string
		wantSys  bool
	}{
		{"start", nil, 1000, "started", false},
		{"stop", nil, 1000, "stopped", false},
		{"restart", nil, 1000, "restarted", false},
		{"start", nil, 0, "started", true},
		{"stop", nil, 0, "stopped", true},
		{"restart", nil, 0, "restarted", true},
		{"start", []string{"--system"}, 1000, "started", true},
		{"stop", []string{"--user"}, 0, "stopped", false},
		{"restart", []string{"--system"}, 1000, "restarted", true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s %v euid=%d", tc.command, tc.args, tc.euid), func(t *testing.T) {
			var (
				optionsSystem bool
				optionsCalls  int
				calls         []string
			)
			operation := func(name string) func(service.Options) (service.Result, error) {
				return func(opts service.Options) (service.Result, error) {
					calls = append(calls, name)
					if opts.System != tc.wantSys {
						t.Errorf("%s got System=%v, want %v", name, opts.System, tc.wantSys)
					}
					return service.Result{}, nil
				}
			}
			setControlSeams(t, controlSeams{
				options: func(system bool) (service.Options, error) {
					optionsCalls++
					optionsSystem = system
					return service.Options{System: system}, nil
				},
				start:   operation("start"),
				stop:    operation("stop"),
				restart: operation("restart"),
			})

			a, stdout, stderr, stub := newDispatchApp("dev")
			stub.euid = func() int { return tc.euid }
			args := append([]string{"service", tc.command}, tc.args...)
			if got := a.run(context.Background(), args); got != exitOK {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", args, got, exitOK, stderr.String())
			}
			if optionsCalls != 1 {
				t.Fatalf("options calls = %d, want 1", optionsCalls)
			}
			if optionsSystem != tc.wantSys {
				t.Errorf("options system = %v, want %v", optionsSystem, tc.wantSys)
			}
			if len(calls) != 1 || calls[0] != tc.command {
				t.Errorf("operations called = %v, want [%s]", calls, tc.command)
			}
			level := "user"
			if tc.wantSys {
				level = "system"
			}
			if want := fmt.Sprintf("The %s Broker service has %s.\n", level, tc.wantPast); stdout.String() != want {
				t.Errorf("stdout = %q, want %q", stdout.String(), want)
			}
			if stderr.Len() != 0 {
				t.Errorf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

// TestServiceControlRejectsArguments checks that conflicting level flags and
// positional arguments are usage errors.
func TestServiceControlRejectsArguments(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"service", "start", "--system", "--user"}, "mutually exclusive"},
		{[]string{"service", "stop", "--user", "--system"}, "mutually exclusive"},
		{[]string{"service", "restart", "extra"}, `unexpected argument "extra"`},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			setControlSeams(t, controlSeams{
				options: func(bool) (service.Options, error) {
					t.Fatal("default options ran for a usage error")
					return service.Options{}, nil
				},
				start: func(service.Options) (service.Result, error) {
					t.Fatal("start ran for a usage error")
					return service.Result{}, nil
				},
				stop: func(service.Options) (service.Result, error) {
					t.Fatal("stop ran for a usage error")
					return service.Result{}, nil
				},
				restart: func(service.Options) (service.Result, error) {
					t.Fatal("restart ran for a usage error")
					return service.Result{}, nil
				},
			})

			a, stdout, stderr, _ := newDispatchApp("dev")
			if got := a.run(context.Background(), tc.args); got != exitUsage {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitUsage)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tc.want)
			}
		})
	}
}

// TestServiceControlHelp checks that --help prints each control command's usage.
func TestServiceControlHelp(t *testing.T) {
	for _, command := range []string{"start", "stop", "restart"} {
		t.Run(command, func(t *testing.T) {
			setControlSeams(t, controlSeams{
				options: func(bool) (service.Options, error) {
					t.Fatal("default options ran for --help")
					return service.Options{}, nil
				},
			})
			a, stdout, stderr, _ := newDispatchApp("dev")
			if got := a.run(context.Background(), []string{"service", command, "--help"}); got != exitOK {
				t.Fatalf("run(service %s --help) = %d, want %d", command, got, exitOK)
			}
			if !strings.Contains(stdout.String(), "peers service "+command) {
				t.Errorf("stdout = %q, want the %s usage", stdout.String(), command)
			}
			if stderr.Len() != 0 {
				t.Errorf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

// TestServiceControlNotInstalled checks how an uninstalled service is reported
// for each account and level.
func TestServiceControlNotInstalled(t *testing.T) {
	notInstalled := fmt.Errorf("%w (user service)", service.ErrNotInstalled)

	t.Run("only the user service is installed", func(t *testing.T) {
		var systems []bool
		setControlSeams(t, controlSeams{
			options: func(system bool) (service.Options, error) {
				systems = append(systems, system)
				return service.Options{System: system}, nil
			},
			start: func(service.Options) (service.Result, error) { return service.Result{}, notInstalled },
			installed: func(service.Options) (bool, error) {
				return false, nil
			},
		})

		a, stdout, stderr, _ := newDispatchApp("dev")
		if got := a.run(context.Background(), []string{"service", "start"}); got != exitFailure {
			t.Fatalf("run(service start) = %d, want %d", got, exitFailure)
		}
		if stdout.Len() != 0 {
			t.Errorf("stdout = %q, want empty", stdout.String())
		}
		want := "peers service start: no user service is installed; install it with `peers service install`\n"
		if stderr.String() != want {
			t.Errorf("stderr = %q, want %q", stderr.String(), want)
		}
		if len(systems) != 2 || systems[0] != false || systems[1] != true {
			t.Errorf("options systems = %v, want [false true]", systems)
		}
	})

	t.Run("only the system service is installed", func(t *testing.T) {
		setControlSeams(t, controlSeams{
			options: func(system bool) (service.Options, error) {
				return service.Options{System: system}, nil
			},
			start: func(service.Options) (service.Result, error) { return service.Result{}, notInstalled },
			installed: func(opts service.Options) (bool, error) {
				return opts.System, nil
			},
		})

		a, _, stderr, _ := newDispatchApp("dev")
		if got := a.run(context.Background(), []string{"service", "start"}); got != exitFailure {
			t.Fatalf("run(service start) = %d, want %d", got, exitFailure)
		}
		want := "peers service start: no user service is installed, but the system service is; run `sudo peers service start --system`\n"
		if stderr.String() != want {
			t.Errorf("stderr = %q, want %q", stderr.String(), want)
		}
	})

	t.Run("root on the system installation", func(t *testing.T) {
		var systems []bool
		setControlSeams(t, controlSeams{
			options: func(system bool) (service.Options, error) {
				systems = append(systems, system)
				return service.Options{System: system}, nil
			},
			start: func(service.Options) (service.Result, error) {
				return service.Result{}, fmt.Errorf("%w (system service)", service.ErrNotInstalled)
			},
			installed: func(service.Options) (bool, error) {
				t.Fatal("the system installed check ran for root")
				return false, nil
			},
		})

		a, _, stderr, stub := newDispatchApp("dev")
		stub.euid = func() int { return 0 }
		if got := a.run(context.Background(), []string{"service", "start"}); got != exitFailure {
			t.Fatalf("run(service start) = %d, want %d", got, exitFailure)
		}
		if len(systems) != 1 || systems[0] != true {
			t.Errorf("options systems = %v, want [true]", systems)
		}
		for _, want := range []string{"no system service is installed", "sudo peers service install --system"} {
			if !strings.Contains(stderr.String(), want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
			}
		}
	})
}

// TestServiceControlDegraded checks the output and exit code of a control
// command that ends in a degraded run.
func TestServiceControlDegraded(t *testing.T) {
	cases := []struct {
		name  string
		level service.DegradedLevel
		want  string
	}{
		{"local_only", service.DegradedLocalOnly, "local only"},
		{"diagnostics_only", service.DegradedDiagnosticsOnly, "diagnostics only"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := service.DegradedState{
				Level:  tc.level,
				Step:   "listen",
				Reason: "listen :17443: address already in use",
				Fix:    "change broker.listen",
			}
			setControlSeams(t, controlSeams{
				options: func(bool) (service.Options, error) { return service.Options{}, nil },
				start: func(service.Options) (service.Result, error) {
					return service.Result{Domain: "gui/501"}, &service.DegradedError{State: state}
				},
			})

			a, stdout, stderr, _ := newDispatchApp("dev")
			if got := a.run(context.Background(), []string{"service", "start"}); got != exitServiceDegraded {
				t.Fatalf("run(service start) = %d, want %d", got, exitServiceDegraded)
			}
			if want := "The user Broker service has started.\n"; stdout.String() != want {
				t.Errorf("stdout = %q, want %q", stdout.String(), want)
			}
			want := fmt.Sprintf("Broker: degraded (%s): %s\nFix: %s\n", tc.want, state.Reason, state.Fix)
			if stderr.String() != want {
				t.Errorf("stderr = %q, want %q", stderr.String(), want)
			}
		})
	}
}

// TestServiceControlFailure checks that a failed operation reaches standard
// error with the failure exit code.
func TestServiceControlFailure(t *testing.T) {
	setControlSeams(t, controlSeams{
		options: func(bool) (service.Options, error) { return service.Options{}, nil },
		stop: func(service.Options) (service.Result, error) {
			return service.Result{}, errors.New("systemctl failed")
		},
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"service", "stop"}); got != exitFailure {
		t.Fatalf("run(service stop) = %d, want %d", got, exitFailure)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty after a failure", stdout.String())
	}
	if want := "peers service stop: systemctl failed\n"; stderr.String() != want {
		t.Errorf("stderr = %q, want %q", stderr.String(), want)
	}
}

// TestServiceControlDefaultOptionsError checks that a failure to build the
// options stops the command before the operation runs.
func TestServiceControlDefaultOptionsError(t *testing.T) {
	var calls int
	setControlSeams(t, controlSeams{
		options: func(bool) (service.Options, error) { return service.Options{}, errors.New("no executable") },
		restart: func(service.Options) (service.Result, error) {
			calls++
			return service.Result{}, nil
		},
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"service", "restart"}); got != exitFailure {
		t.Fatalf("run(service restart) = %d, want %d", got, exitFailure)
	}
	if calls != 0 {
		t.Errorf("restart calls = %d, want 0", calls)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
	if want := "peers service restart: no executable\n"; stderr.String() != want {
		t.Errorf("stderr = %q, want %q", stderr.String(), want)
	}
}
