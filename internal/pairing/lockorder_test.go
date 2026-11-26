package pairing

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// lockedAliasConfig models config's lock around broker.alias: the validator runs
// under the lock and the subscriber after it is released.
type lockedAliasConfig struct {
	mu        sync.Mutex
	alias     string
	validator func(newAlias string) error
	onCommit  func(newAlias string)
}

func (c *lockedAliasConfig) set(alias string) error {
	c.mu.Lock()
	if c.validator != nil {
		if err := c.validator(alias); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	c.alias = alias
	c.mu.Unlock()
	if c.onCommit != nil {
		c.onCommit(alias)
	}
	return nil
}

func (c *lockedAliasConfig) get() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alias, nil
}

const (
	// lockOrderRounds must stay large enough that the two sides overlap on every
	// run, which is what exposes an inverted lock order.
	lockOrderRounds = 100
	roundSamples    = 20
	// stallFactor gives the no-progress window as a multiple of the slower measured round and must leave room for the scheduler and the other side's critical section, so do not shrink it.
	stallFactor = 100
	// minStallWindow floors the window at what the scheduler and timer can
	// resolve; do not shrink it.
	minStallWindow = time.Second
	// maxStallWindow caps the window so an abnormally slow measurement still
	// fails within an acceptable time.
	maxStallWindow = 30 * time.Second
)

// TestAliasValidationKeepsTheConfigPairingLockOrder checks that the broker.alias
// validator holding config's lock and concurrent handshake commits holding the
// pairing lock complete without deadlock.
func TestAliasValidationKeepsTheConfigPairingLockOrder(t *testing.T) {
	// The rule is config lock then pairing lock, never the reverse: pairing must
	// not call config while its table lock is held.
	cfg := &lockedAliasConfig{alias: "self"}
	s := newHSSettings(t)
	s.alias = "self"
	s.aliasFunc = cfg.get // reading the own alias takes config's lock
	p := hsNewPairing(t, s, &hsTransport{}, newHSClock())
	p.SetOwnAlias("self")

	cfg.validator = func(newAlias string) error {
		if p.AliasConflict(newAlias) {
			return fmt.Errorf("alias %q conflicts with a peer", newAlias)
		}
		if _, ok := p.ResolveAlias(newAlias); ok {
			return fmt.Errorf("alias %q resolves to a peer", newAlias)
		}
		return nil
	}

	type peerKey struct {
		fp    Fingerprint
		pub   []byte
		alias string
	}

	// The no-progress window is derived from measured rounds, so load that only
	// slows each round cannot fail the test.
	measureKeys := make([]peerKey, roundSamples)
	for i := range measureKeys {
		fp, pub := peerTestKey(t)
		measureKeys[i] = peerKey{fp: fp, pub: pub, alias: fmt.Sprintf("measure-%d", i)}
	}
	var measureErr error
	configRound := meanRoundDuration(roundSamples, func() {
		if err := cfg.set("measure-own"); err != nil && measureErr == nil {
			measureErr = err
		}
	})
	if measureErr != nil {
		t.Fatalf("measuring the validator round: %v", measureErr)
	}
	next := 0
	pairingRound := meanRoundDuration(roundSamples, func() {
		k := measureKeys[next]
		next++
		token, ok := p.peers.reserveAlias(k.alias, k.fp)
		if !ok {
			if measureErr == nil {
				measureErr = fmt.Errorf("reserve %q failed", k.alias)
			}
			return
		}
		entry := peerTestEntry(k.fp, k.pub, k.alias, RoleAcceptor)
		if _, err := commitHandshakeForTest(p, entry, token); err != nil && measureErr == nil {
			measureErr = fmt.Errorf("commit %q: %w", k.alias, err)
		}
	})
	if measureErr != nil {
		t.Fatalf("measuring the commit round: %v", measureErr)
	}

	slowest := configRound
	if pairingRound > slowest {
		slowest = pairingRound
	}
	stallWindow := stallFactor * slowest
	if stallWindow < minStallWindow {
		stallWindow = minStallWindow
	}
	if stallWindow > maxStallWindow {
		stallWindow = maxStallWindow
	}
	t.Logf("one uncontended round: config=%s pairing=%s; failing only after %s without progress (%d× the slower round, clamped to [%s, %s])",
		configRound, pairingRound, stallWindow, stallFactor, minStallWindow, maxStallWindow)

	keys := make([]peerKey, lockOrderRounds)
	for i := range keys {
		fp, pub := peerTestKey(t)
		keys[i] = peerKey{fp: fp, pub: pub, alias: fmt.Sprintf("peer-%d", i)}
	}

	var configRounds, pairingRounds atomic.Int64
	start := make(chan struct{})
	errs := make(chan error, 2)

	go func() {
		<-start
		for i := 0; i < lockOrderRounds; i++ {
			if err := cfg.set(fmt.Sprintf("own-%d", i)); err != nil {
				errs <- fmt.Errorf("validator round %d: %w", i, err)
				return
			}
			configRounds.Add(1)
		}
		errs <- nil
	}()

	go func() {
		<-start
		for i := 0; i < lockOrderRounds; i++ {
			token, ok := p.peers.reserveAlias(keys[i].alias, keys[i].fp)
			if !ok {
				errs <- fmt.Errorf("reserve %q failed", keys[i].alias)
				return
			}
			entry := peerTestEntry(keys[i].fp, keys[i].pub, keys[i].alias, RoleAcceptor)
			if _, err := commitHandshakeForTest(p, entry, token); err != nil {
				errs <- fmt.Errorf("commit %q: %w", keys[i].alias, err)
				return
			}
			pairingRounds.Add(1)
		}
		errs <- nil
	}()

	checkEvery := stallWindow / 10
	if checkEvery < 10*time.Millisecond {
		checkEvery = 10 * time.Millisecond
	}
	ticker := time.NewTicker(checkEvery)
	defer ticker.Stop()

	close(start)
	lastConfig, lastPairing := configRounds.Load(), pairingRounds.Load()
	lastProgress := time.Now()
	finished := 0
	for finished < 2 {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("validator or commit failed: %v", err)
			}
			finished++
		case <-ticker.C:
			doneConfig, donePairing := configRounds.Load(), pairingRounds.Load()
			if doneConfig != lastConfig || donePairing != lastPairing {
				lastConfig, lastPairing = doneConfig, donePairing
				lastProgress = time.Now()
				continue
			}
			if since := time.Since(lastProgress); since < stallWindow {
				continue
			}
			t.Fatalf("no round completed for %s (config completed %d/%d rounds, pairing completed %d/%d rounds; one uncontended round measured config=%s pairing=%s, so the window is %d× the slower round clamped to [%s, %s]): config and pairing locks are inverted\nall goroutine stacks:\n%s",
				time.Since(lastProgress).Round(time.Millisecond), doneConfig, lockOrderRounds, donePairing, lockOrderRounds,
				configRound, pairingRound, stallFactor, minStallWindow, maxStallWindow, allGoroutineStacks())
		}
	}
}

func meanRoundDuration(n int, op func()) time.Duration {
	start := time.Now()
	for i := 0; i < n; i++ {
		op()
	}
	return time.Since(start) / time.Duration(n)
}

func allGoroutineStacks() []byte {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}
