package broker

import (
	"fmt"

	"github.com/wangzw/peers/internal/config"
)

// listenProbeCheck returns the broker.listen validator. An unchanged value is
// accepted without probing because the running link may hold the address;
// any other value is probed, which binds and immediately releases it. The
// returned function calls no config function, so it is safe under the config
// lock.
func listenProbeCheck(probe func(string) error) func(old, new config.Value) error {
	return func(old, new config.Value) error {
		if old.String == new.String {
			return nil
		}
		if err := probe(new.String); err != nil {
			return fmt.Errorf("address %q is unavailable: %v", new.String, err)
		}
		return nil
	}
}
