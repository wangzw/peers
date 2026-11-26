package main

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/wangzw/peers/internal/broker/update"
)

type stubStatusCall struct {
	system   bool
	explicit bool
	stdout   io.Writer
}

type stubAuditCall struct {
	system   bool
	explicit bool
	stdout   io.Writer
	limit    uint32
}

type stubDeps struct {
	euid              func() int
	runBroker         func(ctx context.Context, o brokerOptions) error
	classifyBrokerErr func(err error) startFailure
	runMCP            func(ctx context.Context) error
	runStatus         func(ctx context.Context, system, explicit bool, stdout io.Writer) error
	runAudit          func(ctx context.Context, system, explicit bool, stdout io.Writer, limit uint32) error

	updateAPIBase        func() string
	updateLatest         func(ctx context.Context, client *http.Client, api string) (update.Release, error)
	updateNewer          func(current, candidate string) bool
	updateApply          func(ctx context.Context, o update.ApplyOptions) error
	applyUpdateViaBroker func(ctx context.Context) (brokerUpdateResult, error)
	brokerUpdateStatus   func(ctx context.Context) (brokerUpdateStatus, error)
	restartService       func(ctx context.Context) error

	brokerErr      error
	classifyResult startFailure
	mcpErr         error
	statusErr      error
	auditErr       error

	latestRelease   update.Release
	latestErr       error
	newerResult     bool
	applyErr        error
	brokerUpdate    brokerUpdateResult
	brokerUpdateErr error
	brokerStatus    brokerUpdateStatus
	brokerStatusErr error

	brokerCalls         []brokerOptions
	mcpCalls            int
	statusCalls         []stubStatusCall
	auditCalls          []stubAuditCall
	latestCalls         int
	latestAPI           string
	latestClient        *http.Client
	newerCalls          [][2]string
	applyCalls          []update.ApplyOptions
	brokerApplyCalls    int
	brokerStatusCalls   int
	restartServiceCalls int
}

func (s *stubDeps) deps() deps {
	return deps{
		euid: func() int {
			if s.euid != nil {
				return s.euid()
			}
			// A non-root account keeps the default-level tests independent of
			// the account running them.
			return 1000
		},
		runBroker: func(ctx context.Context, o brokerOptions) error {
			s.brokerCalls = append(s.brokerCalls, o)
			if s.runBroker != nil {
				return s.runBroker(ctx, o)
			}
			return s.brokerErr
		},
		classifyBrokerErr: func(err error) startFailure {
			if s.classifyBrokerErr != nil {
				return s.classifyBrokerErr(err)
			}
			return s.classifyResult
		},
		runMCP: func(ctx context.Context) error {
			s.mcpCalls++
			if s.runMCP != nil {
				return s.runMCP(ctx)
			}
			return s.mcpErr
		},
		runStatus: func(ctx context.Context, system, explicit bool, stdout io.Writer) error {
			s.statusCalls = append(s.statusCalls, stubStatusCall{system: system, explicit: explicit, stdout: stdout})
			if s.runStatus != nil {
				return s.runStatus(ctx, system, explicit, stdout)
			}
			return s.statusErr
		},
		runAudit: func(ctx context.Context, system, explicit bool, stdout io.Writer, limit uint32) error {
			s.auditCalls = append(s.auditCalls, stubAuditCall{system: system, explicit: explicit, stdout: stdout, limit: limit})
			if s.runAudit != nil {
				return s.runAudit(ctx, system, explicit, stdout, limit)
			}
			return s.auditErr
		},
		peerNames: func(context.Context) ([]string, error) { return nil, nil },
		updateAPIBase: func() string {
			if s.updateAPIBase != nil {
				return s.updateAPIBase()
			}
			return "https://api.example"
		},
		updateLatest: func(ctx context.Context, client *http.Client, api string) (update.Release, error) {
			s.latestCalls++
			s.latestAPI = api
			s.latestClient = client
			if s.updateLatest != nil {
				return s.updateLatest(ctx, client, api)
			}
			return s.latestRelease, s.latestErr
		},
		updateNewer: func(current, candidate string) bool {
			s.newerCalls = append(s.newerCalls, [2]string{current, candidate})
			if s.updateNewer != nil {
				return s.updateNewer(current, candidate)
			}
			return s.newerResult
		},
		updateApply: func(ctx context.Context, o update.ApplyOptions) error {
			s.applyCalls = append(s.applyCalls, o)
			if s.updateApply != nil {
				return s.updateApply(ctx, o)
			}
			return s.applyErr
		},
		applyUpdateViaBroker: func(ctx context.Context) (brokerUpdateResult, error) {
			s.brokerApplyCalls++
			if s.applyUpdateViaBroker != nil {
				return s.applyUpdateViaBroker(ctx)
			}
			return s.brokerUpdate, s.brokerUpdateErr
		},
		brokerUpdateStatus: func(ctx context.Context) (brokerUpdateStatus, error) {
			s.brokerStatusCalls++
			if s.brokerUpdateStatus != nil {
				return s.brokerUpdateStatus(ctx)
			}
			return s.brokerStatus, s.brokerStatusErr
		},
		restartService: func(ctx context.Context) error {
			s.restartServiceCalls++
			if s.restartService != nil {
				return s.restartService(ctx)
			}
			return nil
		},
	}
}

// TestExitCodeForIsDistinctPerStartupFailure checks that each startup failure
// maps to a dedicated exit code.
func TestExitCodeForIsDistinctPerStartupFailure(t *testing.T) {
	failures := []startFailure{
		failAlreadyRunning,
		failListenUnavailable,
		failInvalidConfig,
		failInSession,
		failNotRoot,
		failRuntimeDir,
	}
	seen := make(map[int]startFailure, len(failures))
	for _, f := range failures {
		code := exitCodeFor(f)
		if code == 0 || code == 1 || code == 2 {
			t.Errorf("exitCodeFor(%v) = %d, want a dedicated startup failure code", f, code)
		}
		if prev, ok := seen[code]; ok {
			t.Errorf("exitCodeFor(%v) and exitCodeFor(%v) both return %d", f, prev, code)
		}
		seen[code] = f
	}
}

// TestExitCodeForTable checks exitCodeFor's mapping for every failure,
// including an unclassified value.
func TestExitCodeForTable(t *testing.T) {
	tests := []struct {
		failure startFailure
		want    int
	}{
		{failOther, 1},
		{failAlreadyRunning, 3},
		{failListenUnavailable, 4},
		{failInvalidConfig, 5},
		{failInSession, 6},
		{failNotRoot, 7},
		{failRuntimeDir, 8},
		{failInvalidListen, 2},
		{startFailure(99), 1},
	}
	for _, tt := range tests {
		if got := exitCodeFor(tt.failure); got != tt.want {
			t.Errorf("exitCodeFor(%v) = %d, want %d", tt.failure, got, tt.want)
		}
	}
}

// TestExitServiceDegraded checks that exitServiceDegraded is 3, shared only
// with exitAlreadyRunning, and distinct from the generic and usage codes.
func TestExitServiceDegraded(t *testing.T) {
	if exitServiceDegraded != 3 {
		t.Fatalf("exitServiceDegraded = %d, want 3", exitServiceDegraded)
	}
	for _, other := range []struct {
		name string
		code int
	}{
		{"exitFailure", exitFailure},
		{"exitUsage", exitUsage},
	} {
		if exitServiceDegraded == other.code {
			t.Errorf("exitServiceDegraded shares %d with %s", other.code, other.name)
		}
	}
	if exitServiceDegraded != exitAlreadyRunning {
		t.Errorf("exitServiceDegraded = %d, want the documented shared value %d of exitAlreadyRunning", exitServiceDegraded, exitAlreadyRunning)
	}
	for f := failOther; f <= failInvalidListen; f++ {
		if got := exitCodeFor(f); got == exitServiceDegraded && f != failAlreadyRunning {
			t.Errorf("exitCodeFor(%d) = %d, want only failAlreadyRunning to share %d", f, got, exitServiceDegraded)
		}
	}
}
