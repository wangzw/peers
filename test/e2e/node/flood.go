package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

// floodDialBound bounds one dial of the connection flood.
const floodDialBound = 2 * time.Second

// floodDialGap is how long a worker waits after a failed dial before trying
// again.
const floodDialGap = 20 * time.Millisecond

// floodRedialGap is how long a worker waits after the listener closes its
// connection before dialing again. Without it an evicted connection redials at
// once and the flood busy-spins against the listener. The first dial is not
// delayed, so the sources still fill the global limit and force evictions.
const floodRedialGap = 10 * time.Millisecond

// FloodSpec describes a flood of bare TCP connections a node holds against a
// listener. Each of the Sources distinct loopback source addresses holds
// PerSource connections at once; a worker redials as soon as the listener
// closes its connection.
type FloodSpec struct {
	// Addr is the listener in host:port form.
	Addr string `json:"addr"`
	// Sources is the number of distinct source addresses, 127.0.0.2 upward.
	Sources int `json:"sources"`
	// PerSource is the number of connections each source holds at once.
	PerSource int `json:"per_source"`
}

// FloodStats is one reading of a running flood.
type FloodStats struct {
	// Dialed is the number of completed dials.
	Dialed int64 `json:"dialed"`
	// Open is the number of connections the flood still holds open.
	Open int64 `json:"open"`
	// MaxOpen is the largest number of connections held open at once.
	MaxOpen int64 `json:"max_open"`
	// Closed is the number of connections the listener closed, not counting
	// those the flood closed itself when it stopped.
	Closed int64 `json:"closed"`
}

// floodRun holds bare connections and redials them when the listener closes
// one. Binding each worker to a loopback address makes every source a distinct
// /32, so a test can fill the per-source and global limits from one node.
type floodRun struct {
	spec   FloodSpec
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]struct{}

	dialed  atomic.Int64
	open    atomic.Int64
	maxOpen atomic.Int64
	closed  atomic.Int64
}

// FloodStart starts a flood and returns once its workers are running. Only one
// flood runs per node at a time.
func (a *Agent) FloodStart(spec FloodSpec) error {
	var err error
	_ = Guard(func() { err = a.floodStart(spec) })
	return err
}

// floodStart implements FloodStart. It checks the request before starting any
// goroutine, so a bad request leaves the agent unchanged.
func (a *Agent) floodStart(spec FloodSpec) error {
	if spec.Addr == "" {
		return errors.New("flood needs an address")
	}
	if spec.Sources < 1 || spec.Sources > 200 {
		return fmt.Errorf("flood sources = %d, want between 1 and 200", spec.Sources)
	}
	if spec.PerSource < 1 || spec.PerSource > 1024 {
		return fmt.Errorf("flood per_source = %d, want between 1 and 1024", spec.PerSource)
	}
	a.mu.Lock()
	if a.flood != nil {
		a.mu.Unlock()
		return errors.New("a flood is already running")
	}
	run := &floodRun{spec: spec, conns: map[net.Conn]struct{}{}}
	a.flood = run
	a.mu.Unlock()

	run.start()
	return nil
}

// start launches PerSource workers for each source address.
func (f *floodRun) start() {
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	for source := 0; source < f.spec.Sources; source++ {
		ip := netip.AddrFrom4([4]byte{127, 0, 0, byte(source + 2)})
		local := netip.AddrPortFrom(ip, 0)
		for i := 0; i < f.spec.PerSource; i++ {
			f.wg.Add(1)
			go f.worker(ctx, local)
		}
	}
}

// worker holds one connection slot: it dials, waits for the listener or the
// stop to close the connection, then dials again.
func (f *floodRun) worker(ctx context.Context, local netip.AddrPort) {
	defer f.wg.Done()
	dialer := net.Dialer{Timeout: floodDialBound, LocalAddr: net.TCPAddrFromAddrPort(local)}
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := dialer.DialContext(ctx, "tcp", f.spec.Addr)
		if err != nil {
			select {
			case <-time.After(floodDialGap):
			case <-ctx.Done():
				return
			}
			continue
		}
		f.dialed.Add(1)
		f.raiseMax(f.open.Add(1))

		// Register under the lock, but only if the flood is still running.
		// stop closes every connection in the map, so a connection that
		// completes its dial after stop began would otherwise sit open until
		// the listener's first-frame deadline, and stop would wait for it.
		f.mu.Lock()
		if ctx.Err() != nil {
			f.mu.Unlock()
			_ = conn.Close()
			f.open.Add(-1)
			return
		}
		f.conns[conn] = struct{}{}
		f.mu.Unlock()

		// Read only: the listener closes the connection when it evicts it or
		// the first-frame deadline passes, and a write would start a TLS
		// handshake the flood does not want.
		buffer := make([]byte, 1)
		_, readErr := conn.Read(buffer)

		f.mu.Lock()
		delete(f.conns, conn)
		f.mu.Unlock()
		_ = conn.Close()
		f.open.Add(-1)
		// A close the flood caused by stopping is not the listener closing
		// the connection.
		if readErr != nil && ctx.Err() == nil {
			f.closed.Add(1)
		}

		// Pause before the next dial so an evicted connection does not redial
		// in a tight loop.
		select {
		case <-time.After(floodRedialGap):
		case <-ctx.Done():
			return
		}
	}
}

// raiseMax records v as the high-water mark of open connections.
func (f *floodRun) raiseMax(v int64) {
	for {
		cur := f.maxOpen.Load()
		if v <= cur || f.maxOpen.CompareAndSwap(cur, v) {
			return
		}
	}
}

// Stats returns one reading of the flood.
func (f *floodRun) stats() FloodStats {
	return FloodStats{
		Dialed:  f.dialed.Load(),
		Open:    f.open.Load(),
		MaxOpen: f.maxOpen.Load(),
		Closed:  f.closed.Load(),
	}
}

// stop cancels the workers and closes every connection they hold, then waits
// for them to exit.
func (f *floodRun) stop() {
	f.cancel()
	f.mu.Lock()
	for conn := range f.conns {
		_ = conn.Close()
	}
	f.mu.Unlock()
	f.wg.Wait()
}

// FloodStats reads the running flood's counters.
func (a *Agent) FloodStats() (FloodStats, error) {
	var stats FloodStats
	var err error
	_ = Guard(func() {
		a.mu.Lock()
		run := a.flood
		a.mu.Unlock()
		if run == nil {
			err = errors.New("no flood is running")
			return
		}
		stats = run.stats()
	})
	return stats, err
}

// FloodStop stops the running flood and waits for its workers to exit.
func (a *Agent) FloodStop() error {
	var err error
	_ = Guard(func() { err = a.floodStop() })
	return err
}

// floodStop implements FloodStop.
func (a *Agent) floodStop() error {
	a.mu.Lock()
	run := a.flood
	a.flood = nil
	a.mu.Unlock()
	if run == nil {
		return errors.New("no flood is running")
	}
	run.stop()
	return nil
}
