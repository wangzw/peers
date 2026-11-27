package resolve

import (
	"sort"
	"strconv"
	"strings"
)

// version is the numeric part of a vX.Y.Z release version.
type version struct {
	major, minor, patch int
}

// parseVersion accepts exactly vX.Y.Z, with no prerelease suffix and no
// leading zero in a number.
func parseVersion(s string) (version, bool) {
	if !strings.HasPrefix(s, "v") {
		return version{}, false
	}
	parts := strings.Split(s[1:], ".")
	if len(parts) != 3 {
		return version{}, false
	}
	nums := [3]int{}
	for i, part := range parts {
		if part == "" || (len(part) > 1 && part[0] == '0') {
			return version{}, false
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return version{}, false
			}
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return version{}, false
		}
		nums[i] = n
	}
	return version{nums[0], nums[1], nums[2]}, true
}

// compare returns a negative number, zero or a positive number as v is below,
// equal to or above other.
func (v version) compare(other version) int {
	switch {
	case v.major != other.major:
		return v.major - other.major
	case v.minor != other.minor:
		return v.minor - other.minor
	default:
		return v.patch - other.patch
	}
}

// release is one version of the site together with where its content comes
// from.
type release struct {
	name        string
	ver         version
	commit      string
	workingTree bool
}

// sortReleases orders releases by semantic version, lowest first.
func sortReleases(releases []release) {
	sort.Slice(releases, func(i, j int) bool {
		return releases[i].ver.compare(releases[j].ver) < 0
	})
}
