package broker

import "testing"

// TestPackageConfigSatisfiesConfigSource checks at compile time that
// PackageConfig implements ConfigSource.
func TestPackageConfigSatisfiesConfigSource(t *testing.T) {
	var _ = ConfigSource(PackageConfig())
}
