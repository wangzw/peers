package gitenv

import "testing"

// TestEnvDropsRedirectingVariables checks that every variable which would point
// git elsewhere is gone and an unrelated git variable stays.
func TestEnvDropsRedirectingVariables(t *testing.T) {
	planted := []string{
		"GIT_DIR",
		"GIT_WORK_TREE",
		"GIT_INDEX_FILE",
		"GIT_OBJECT_DIRECTORY",
		"GIT_ALTERNATE_OBJECT_DIRECTORIES",
		"GIT_COMMON_DIR",
		"GIT_NAMESPACE",
		"GIT_CEILING_DIRECTORIES",
		"GIT_DISCOVERY_ACROSS_FILESYSTEM",
		"GIT_CONFIG",
		"GIT_CONFIG_GLOBAL",
		"GIT_CONFIG_SYSTEM",
		"GIT_CONFIG_COUNT",
		"GIT_CONFIG_PARAMETERS",
		"GIT_CONFIG_KEY_0",
		"GIT_CONFIG_VALUE_0",
	}
	for _, key := range planted {
		t.Setenv(key, "planted")
	}
	t.Setenv("GIT_AUTHOR_NAME", "kept")

	seen := make(map[string]string)
	for _, entry := range Env() {
		seen[Name(entry)] = entry
	}
	for _, key := range planted {
		if _, ok := seen[key]; ok {
			t.Errorf("Env still holds %s", key)
		}
	}
	if got := seen["GIT_AUTHOR_NAME"]; got != "GIT_AUTHOR_NAME=kept" {
		t.Errorf("Env holds %q for GIT_AUTHOR_NAME, want it kept", got)
	}
}
