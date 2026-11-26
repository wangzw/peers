//go:build e2e

package e2e

import (
	"os"
	"testing"

	"github.com/wangzw/peers/test/e2e/syscontainer"
)

// TestMain forwards every test to a container through syscontainer.RunPair, or
// runs the body of the role the environment selects.
func TestMain(m *testing.M) {
	switch os.Getenv(registerClientRoleEnv) {
	case registerClientRole:
		os.Exit(runRegisterClientRole())
	case listBrokersRole:
		os.Exit(runListBrokersRole())
	case forgeAccessCheckRole:
		os.Exit(runForgeAccessCheckRole())
	case statusClientRole:
		os.Exit(runStatusClientRole())
	case updateStatusClientRole:
		os.Exit(runUpdateStatusClientRole())
	}
	os.Exit(syscontainer.RunPair(m, e2ePairOptions))
}
