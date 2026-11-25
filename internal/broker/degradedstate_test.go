package broker

import (
	"errors"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

// TestDegradedFixHints checks the repair hint for each listen and data failure,
// with and without sudo for the system form.
func TestDegradedFixHints(t *testing.T) {
	paths := config.PathSet{
		RuntimeDir: "/run/peers",
		ConfigFile: "/etc/peers/config.yaml",
		DataDir:    "/var/lib/peers",
		AuditLog:   "/var/log/peers/audit.log",
	}
	listenErr := errors.New("address already in use")
	lowPortErr := config.ErrLowPortDenied

	cases := []struct {
		name string
		f    *startupFailure
		want string
	}{
		{
			name: "listen user",
			f: &startupFailure{
				step: "listen", addr: "127.0.0.1:17443", cause: listenErr,
				err:   errors.Join(ErrListenUnavailable, listenErr),
				paths: paths,
			},
			want: "peers config set broker.listen <free address>, or edit broker.listen in /etc/peers/config.yaml",
		},
		{
			name: "listen system",
			f: &startupFailure{
				step: "listen", addr: "127.0.0.1:17443", cause: listenErr, system: true,
				err:   errors.Join(ErrListenUnavailable, listenErr),
				paths: paths,
			},
			want: "sudo peers config set broker.listen <free address>, or edit broker.listen in /etc/peers/config.yaml with sudo",
		},
		{
			name: "low port user",
			f: &startupFailure{
				step: "listen", addr: "127.0.0.1:443", cause: lowPortErr,
				err:   errors.Join(ErrListenUnavailable, lowPortErr),
				paths: paths,
			},
			want: "chmod g-w /etc/peers/config.yaml, or set broker.listen to a port at or above 1024",
		},
		{
			name: "low port system",
			f: &startupFailure{
				step: "listen", addr: "127.0.0.1:443", cause: lowPortErr, system: true,
				err:   errors.Join(ErrListenUnavailable, lowPortErr),
				paths: paths,
			},
			want: "sudo chmod g-w /etc/peers/config.yaml, or set broker.listen to a port at or above 1024",
		},
		{
			name: "data directory user",
			f: &startupFailure{
				step: "identity", err: wrapDataUnavailable(dataKindIdentity, errors.New("permission denied")),
				paths: paths,
			},
			want: "check the owner and permissions of /var/lib/peers, then run peers service restart; otherwise the Broker retries every minute",
		},
		{
			name: "data directory system",
			f: &startupFailure{
				step: "identity", err: wrapDataUnavailable(dataKindIdentity, errors.New("permission denied")),
				paths: paths, system: true,
			},
			want: "check the owner and permissions of /var/lib/peers, then run sudo peers service restart; otherwise the Broker retries every minute",
		},
		{
			name: "audit log user",
			f: &startupFailure{
				step: "audit log",
				err: &config.DataUnavailableError{
					Paths: paths,
					Err:   errors.New("cannot open /var/log/peers/audit.log: permission denied"),
				},
			},
			want: "check the owner and permissions of /var/log/peers/audit.log, then run peers service restart; otherwise the Broker retries every minute",
		},
		{
			name: "audit log system",
			f: &startupFailure{
				step: "audit log", system: true,
				err: &config.DataUnavailableError{
					Paths: paths,
					Err:   errors.New("cannot open /var/log/peers/audit.log: permission denied"),
				},
			},
			want: "check the owner and permissions of /var/log/peers/audit.log, then run sudo peers service restart; otherwise the Broker retries every minute",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := degradedStateFor(tc.f)
			if got.GetFix() != tc.want {
				t.Fatalf("fix = %q, want %q", got.GetFix(), tc.want)
			}
		})
	}
}

// TestDegradedConfigReasonNamesKeyOnly checks that a configuration failure's
// degraded reason names the key and the failure class, never the value.
func TestDegradedConfigReasonNamesKeyOnly(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "invalid value",
			err:  errors.Join(ErrInvalidConfig, config.ErrInvalidValue, errors.New(`config: invalid value: broker.advertise: host "bad!host" contains an invalid character`)),
			want: "the configuration value for broker.advertise is invalid",
		},
		{
			name: "parse error",
			err:  errors.Join(ErrInvalidConfig, errors.New("config file parse error: yaml: line 1: bad!value")),
			want: "the configuration file cannot be parsed",
		},
		{
			name: "invalid combination",
			err:  errors.Join(ErrInvalidConfig, config.ErrInvalidCombination, errors.New("config: invalid listen/advertise combination")),
			want: "the configuration has an invalid combination of values",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := degradedStateFor(&startupFailure{step: "config", err: tc.err})
			if state.GetReason() != tc.want {
				t.Fatalf("reason = %q, want %q", state.GetReason(), tc.want)
			}
			if strings.Contains(state.GetReason(), "bad!") {
				t.Fatalf("reason %q contains a configuration value", state.GetReason())
			}
		})
	}
}

// TestDegradedStateLevels checks which failure becomes which degraded level.
func TestDegradedStateLevels(t *testing.T) {
	listen := degradedStateFor(&startupFailure{
		step: "listen", addr: "127.0.0.1:17443", cause: errors.New("in use"),
		err: errors.Join(ErrListenUnavailable, errors.New("in use")),
	})
	if listen.GetLevel() != localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY {
		t.Errorf("listen level = %v, want local_only", listen.GetLevel())
	}
	data := degradedStateFor(&startupFailure{step: "identity", err: wrapDataUnavailable(dataKindIdentity, errors.New("permission denied"))})
	if data.GetLevel() != localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY {
		t.Errorf("data level = %v, want diagnostics_only", data.GetLevel())
	}
	if data.GetStep() != "identity" {
		t.Errorf("data step = %q, want identity", data.GetStep())
	}
}
