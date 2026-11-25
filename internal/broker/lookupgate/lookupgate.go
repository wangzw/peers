// Package lookupgate bounds the account database lookups, and the getent
// processes behind them, that run at once in one process. The broker and
// helper packages share this gate.
package lookupgate

import (
	"context"
	"sync"
)

// Concurrency is the most account database lookups one process runs at once.
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
