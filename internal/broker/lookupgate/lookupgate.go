// Package lookupgate bounds the account database lookups, and the getent
// processes behind them, that run at once in one process.
//
// The broker package resolves caller names and the helper package looks up the
// account a permission check must run as. Both used to keep their own gate, so
// one process could run twice as many getent processes as either limit allowed.
// They share this gate instead: the bound is on the whole process, not on one
// caller of the account database.
package lookupgate

import (
	"context"
	"sync"
)

// Concurrency is the most account database lookups one process runs at once.
// A uid without an account entry, such as a subuid mapped by a rootless
// container, would otherwise start one getent process per connection or check.
const Concurrency = 4

var slots = make(chan struct{}, Concurrency)

// Acquire takes one of the process-wide lookup slots, waiting until ctx is
// done. The returned function returns the slot and may be called more than
// once; only the first call has an effect.
func Acquire(ctx context.Context) (func(), error) {
	select {
	case slots <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-slots }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
