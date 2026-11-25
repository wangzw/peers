package localpb

import "testing"

// TestDegradedLevelNames checks the tool-result and terminal names of each
// degraded level, including unset and unknown values.
func TestDegradedLevelNames(t *testing.T) {
	cases := []struct {
		level DegradedLevel
		name  string
		label string
	}{
		{DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY, "local_only", "local only"},
		{DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY, "diagnostics_only", "diagnostics only"},
		{DegradedLevel_DEGRADED_LEVEL_UNSPECIFIED, "unspecified", "unspecified"},
		{DegradedLevel(99), "unspecified", "unspecified"},
	}
	for _, tc := range cases {
		if got := DegradedLevelName(tc.level); got != tc.name {
			t.Errorf("DegradedLevelName(%v) = %q, want %q", tc.level, got, tc.name)
		}
		if got := DegradedLevelLabel(tc.level); got != tc.label {
			t.Errorf("DegradedLevelLabel(%v) = %q, want %q", tc.level, got, tc.label)
		}
	}
}
