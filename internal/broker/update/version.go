package update

import (
	"strings"

	"golang.org/x/mod/semver"
)

// Newer reports whether candidate is newer than current. Both must be formal
// release versions; a development, test or snapshot build never reports an
// update, and a prerelease candidate is never offered.
func Newer(current, candidate string) bool {
	if !isReleaseVersion(current) || !isReleaseVersion(candidate) {
		return false
	}
	return semver.Compare(semverOf(candidate), semverOf(current)) > 0
}

func isReleaseVersion(version string) bool {
	v := semverOf(version)
	return semver.IsValid(v) && semver.Prerelease(v) == "" && semver.Canonical(v) == v
}

func semverOf(version string) string {
	return "v" + strings.TrimPrefix(version, "v")
}

// IsReleaseVersion reports whether version is a formal release version, with or
// without a leading v. It rejects development, test and snapshot builds, short
// forms such as "0.1", and build metadata such as "0.1.0+build".
func IsReleaseVersion(version string) bool {
	return isReleaseVersion(version)
}
