package broker

import (
	"context"
	"strings"
	"time"

	"golang.org/x/mod/semver"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/broker/update"
	"github.com/wangzw/peers/internal/config"
)

// The periodic update check. A Broker that runs a formal release waits
// updateCheckInitialDelay after startup, checks once, and then checks every
// update.check_interval. update.check is read when a check runs and the
// interval when the next check is scheduled, so a change takes effect at the
// next period.
const (
	updateCheckDefaultInterval = 24 * time.Hour
	updateCheckMinInterval     = time.Hour
	updateCheckRequestTimeout  = 30 * time.Second
)

// startUpdateChecks schedules the first check. Run calls it after the Broker is
// ready, and only when the running version is a formal release.
func (b *Broker) startUpdateChecks(ctx context.Context) {
	// Turning update.check off clears the result already held, so status and
	// the session hint stop reporting a release the Broker no longer checks
	// for before the next period would run.
	b.deps.Config.Subscribe("update.check", func(_, new config.Value) {
		if new.Kind == config.KindBool && !new.Bool {
			b.setAvailableUpdate("", "")
		}
	})
	b.scheduleUpdateCheck(ctx, updateCheckInitialDelay())
	go func() {
		<-ctx.Done()
		b.updateMu.Lock()
		if b.updateTimer != nil {
			b.updateTimer.Stop()
			b.updateTimer = nil
		}
		b.updateMu.Unlock()
	}()
}

// scheduleUpdateCheck arms the next check. The callback runs on the injected
// clock, so tests advance time instead of sleeping.
func (b *Broker) scheduleUpdateCheck(ctx context.Context, d time.Duration) {
	if ctx.Err() != nil {
		return
	}
	timer := b.deps.Clock.AfterFunc(d, func() { b.updateCheckTick(ctx) })
	b.updateMu.Lock()
	b.updateTimer = timer
	b.updateMu.Unlock()
}

// updateCheckTick performs one check and schedules the next period. A disabled
// check still schedules the next one, so turning update.check back on takes
// effect without a restart.
func (b *Broker) updateCheckTick(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	b.checkForUpdate(ctx)
	b.scheduleUpdateCheck(ctx, b.updateCheckInterval())
}

// checkForUpdate looks up the latest release and keeps the result in memory.
// A failure only logs at debug level: it must not affect other functions and
// is not written to the audit log.
func (b *Broker) checkForUpdate(ctx context.Context) {
	current := b.deps.Version
	if !isReleaseVersion(current) {
		return
	}
	if !b.updateCheckEnabled() {
		return
	}
	latest := b.deps.LatestRelease
	if latest == nil {
		latest = update.Latest
	}
	checkCtx, cancel := context.WithTimeout(ctx, updateCheckRequestTimeout)
	defer cancel()
	release, err := latest(checkCtx, b.deps.UpdateHTTPClient, b.deps.UpdateAPI)
	if err != nil {
		b.log.Debug(EvUpdateCheckFailed, "error", err.Error())
		return
	}
	if !update.Newer(current, release.Version) {
		b.setAvailableUpdate("", "")
		return
	}
	b.setAvailableUpdate(release.Version, release.URL)
}

// updateCheckEnabled reads update.check. A missing or unreadable value keeps
// the check on, which matches the key's default.
func (b *Broker) updateCheckEnabled() bool {
	v, err := b.deps.Config.Get("update.check")
	if err != nil || v.Kind != config.KindBool {
		return true
	}
	return v.Bool
}

// updateCheckInterval reads update.check_interval. A missing or out-of-range
// value falls back to the default.
func (b *Broker) updateCheckInterval() time.Duration {
	v, err := b.deps.Config.Get("update.check_interval")
	if err != nil || v.Kind != config.KindDuration || v.Duration < updateCheckMinInterval {
		return updateCheckDefaultInterval
	}
	return v.Duration
}

func (b *Broker) setAvailableUpdate(version, url string) {
	b.updateMu.Lock()
	defer b.updateMu.Unlock()
	b.updateAvailable = version
	b.updateReleaseURL = url
}

func (b *Broker) availableUpdate() (version, url string) {
	// The gate makes the response match the current setting even in the
	// window between the value changing and the subscriber clearing it.
	if !b.updateCheckEnabled() {
		return "", ""
	}
	b.updateMu.Lock()
	defer b.updateMu.Unlock()
	return b.updateAvailable, b.updateReleaseURL
}

// UpdateStatus reports the running version and the newer release the periodic
// check found, with the release page. It needs no agent identity and no
// configuration-file permission. It answers empty availability for a
// non-release build, which never checks.
func (b *Broker) UpdateStatus(context.Context, *localpb.UpdateStatusRequest) (*localpb.UpdateStatusResponse, error) {
	current := b.deps.Version
	available, releaseURL := b.availableUpdate()
	if !isReleaseVersion(current) {
		available, releaseURL = "", ""
	}
	return &localpb.UpdateStatusResponse{
		CurrentVersion:   current,
		AvailableVersion: available,
		ReleaseUrl:       releaseURL,
	}, nil
}

// isReleaseVersion reports whether v is a formal release version written
// without a leading v: three numeric components, no prerelease and no build
// metadata. "dev", "e2e" and "0.1.0-SNAPSHOT" are not formal releases.
func isReleaseVersion(v string) bool {
	if v == "" || strings.HasPrefix(v, "v") {
		return false
	}
	tag := "v" + v
	if !semver.IsValid(tag) || semver.Canonical(tag) != tag {
		return false
	}
	return semver.Prerelease(tag) == ""
}
