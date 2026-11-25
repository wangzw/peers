package broker

import (
	"fmt"

	"github.com/wangzw/peers/internal/config"
)

// aliasConflictCheck returns the validation function for broker.alias. The
// returned function runs under the config lock, so it may call only
// AliasConflict, which takes just the pairing lock; any other pairing query
// could deadlock against a concurrent config access.
func aliasConflictCheck(p OwnAliasSync) func(old, new config.Value) error {
	return func(_, new config.Value) error {
		if new.String == "" {
			return nil
		}
		if p.AliasConflict(new.String) {
			return fmt.Errorf("alias %q is already the effective alias of a peer Broker; choose another alias or unpair that peer first", new.String)
		}
		return nil
	}
}

// wireOwnAlias keeps pairing's own-alias cache in step with broker.alias.
// Subscribing before the first read keeps a change from being lost, and
// pushing until two reads agree keeps a concurrent rename from leaving the
// cached value stale.
func wireOwnAlias(cfg ConfigSource, p OwnAliasSync) error {
	cfg.Subscribe("broker.alias", func(_, new config.Value) {
		p.SetOwnAlias(new.String)
	})
	cfg.Validate("broker.alias", aliasConflictCheck(p))
	cur, err := cfg.Get("broker.alias")
	if err != nil {
		return fmt.Errorf("reading broker.alias: %w", err)
	}
	for {
		p.SetOwnAlias(cur.String)
		next, err := cfg.Get("broker.alias")
		if err != nil {
			return fmt.Errorf("reading broker.alias: %w", err)
		}
		if next.String == cur.String {
			return nil
		}
		cur = next
	}
}
