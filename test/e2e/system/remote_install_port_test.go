//go:build e2e

package e2e

import "testing"

// TestRemoteInstallServeTwoSuites checks that two fake release services started
// on one host take distinct kernel-assigned ports instead of colliding.
func TestRemoteInstallServeTwoSuites(t *testing.T) {
	remoteInstallServe(t, remoteInstallReleaseOptions{})
	remoteInstallServe(t, remoteInstallReleaseOptions{})
}
