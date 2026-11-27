// Package gitenv supplies the environment for the git commands of the
// versioned-site build: the caller's environment without the variables that
// would point git at another repository, index or configuration.
package gitenv

import (
	"os"
	"strings"
)

// variables are dropped because they redirect git away from the repository it
// was given on its command line.
var variables = map[string]bool{
	"GIT_DIR":                          true,
	"GIT_WORK_TREE":                    true,
	"GIT_INDEX_FILE":                   true,
	"GIT_OBJECT_DIRECTORY":             true,
	"GIT_ALTERNATE_OBJECT_DIRECTORIES": true,
	"GIT_COMMON_DIR":                   true,
	"GIT_NAMESPACE":                    true,
	"GIT_CEILING_DIRECTORIES":          true,
	"GIT_DISCOVERY_ACROSS_FILESYSTEM":  true,
	"GIT_CONFIG":                       true,
	"GIT_CONFIG_GLOBAL":                true,
	"GIT_CONFIG_SYSTEM":                true,
	"GIT_CONFIG_COUNT":                 true,
	"GIT_CONFIG_PARAMETERS":            true,
}

// IsVariable reports whether key names a variable to drop.
func IsVariable(key string) bool {
	if variables[key] {
		return true
	}
	return strings.HasPrefix(key, "GIT_CONFIG_KEY_") || strings.HasPrefix(key, "GIT_CONFIG_VALUE_")
}

// Env returns the caller's environment without the redirecting variables.
func Env() []string {
	var env []string
	for _, entry := range os.Environ() {
		if IsVariable(Name(entry)) {
			continue
		}
		env = append(env, entry)
	}
	return env
}

// Name returns the variable name of one NAME=value entry.
func Name(entry string) string {
	if i := strings.IndexByte(entry, '='); i >= 0 {
		return entry[:i]
	}
	return entry
}
