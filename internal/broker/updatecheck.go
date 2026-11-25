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

// Timing of the periodic update check.
const (
	updateCheckDefaultInterval = 24 * time.Hour
	updateCheckMinInterval     = time.Hour
	updateCheckRequestTimeout  = 30 * time.Second
)

// Called only after Run is ready and only for a formal release, so a
// non-release build never arms a check.
func (b *Broker) startUpdateChecks(ctx context.Context) {
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

func (b *Broker) scheduleUpdateCheck(ctx context.Context, d time.Duration) {
	if ctx.Err() != nil {
		return
	}
	timer := b.deps.Clock.AfterFunc(d, func() { b.updateCheckTick(ctx) })
	b.updateMu.Lock()
	b.updateTimer = timer
	b.updateMu.Unlock()
}

// updateCheckTick runs one check and schedules the next period even when the
// check is disabled, so re-enabling takes effect without a restart.
func (b *Broker) updateCheckTick(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	b.checkForUpdate(ctx)
	b.scheduleUpdateCheck(ctx, b.updateCheckInterval())
}

func (b *Broker) checkForUpdate(ctx context.Context) {
	current := b.deps.Version
	if !isReleaseVersion(current) {
		return
	}
	if !b.updateCheckEnabled() {
		return
	}
	release, err := b.lookupLatestRelease(ctx)
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

func (b *Broker) lookupLatestRelease(ctx context.Context) (update.Release, error) {
	latest := b.deps.LatestRelease
	if latest == nil {
		latest = update.Latest
	}
	lookupCtx, cancel := context.WithTimeout(ctx, updateCheckRequestTimeout)
	defer cancel()
	return latest(lookupCtx, b.deps.UpdateHTTPClient, b.deps.UpdateAPI)
}

func (b *Broker) updateCheckEnabled() bool {
	v, err := b.deps.Config.Get("update.check")
	if err != nil || v.Kind != config.KindBool {
		return true
	}
	return v.Bool
}

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

// UpdateStatus reports the running version, the newer release the periodic
// check found and its release page. It needs no agent identity and no
// configuration-file permission, and answers empty availability for a
// non-release build.
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

// isReleaseVersion reports whether v is a formal release: three numeric
// components without a leading v, no prerelease and no build metadata.
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
