package harness

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
)

// The routed topologies pin every IPv4 subnet inside 198.18.0.0/15. Docker's
// default bridge (172.17.0.0/16) and allocation pool (172.16.0.0/12,
// 192.168.0.0/16) avoid it, and this host's k8s networks (10.12.72.0/24,
// 10.244.0.0/16, 172.17.0.0/16, 10.96.0.0/12) must never be captured by a bridge
// route.
const (
	// routedSubnetSkip reserves the first eight /24s for the compose files' defaults, so a manual "docker compose up" never takes a block the allocator hands out.
	routedSubnetSkip   = 8
	routedSubnetStride = 3
	routedSubnetCount  = 512
	// routedSubnetLimit stops the routed allocator before the last 32 /24s, which are reserved for the system container network.
	routedSubnetLimit = routedSubnetCount - 32
	// subnetStateDir holds one file per reserved subnet, away from the runtime roots so the residue scan does not mistake it for a root.
	subnetStateDir = "/tmp/.ape2e-subnets"
	subnetLockName = ".lock"
)

const (
	// subnetPrefixFilePrefix marks a reservation file that names a prefix rather than a /24 index; the two kinds share the directory and its lock but not an address space.
	subnetPrefixFilePrefix = "prefix-"
)

var (
	subnetMu sync.Mutex
	// defaultSubnetStore is the shared state the exported allocators use.
	defaultSubnetStore = subnetStore{dir: subnetStateDir}
)

// subnetStore is the reservation directory one allocator family uses; it is a value so a test can point it at a temporary directory.
type subnetStore struct {
	dir string
}

// RoutedSubnetPrefixes returns every /24 the routed-topology allocator can hand
// out.
func RoutedSubnetPrefixes() []netip.Prefix {
	out := make([]netip.Prefix, 0, routedSubnetLimit-routedSubnetSkip)
	for first := routedSubnetSkip; first+2 < routedSubnetLimit; first += routedSubnetStride {
		out = append(out, routedSubnetPrefix(first), routedSubnetPrefix(first+1), routedSubnetPrefix(first+2))
	}
	return out
}

// allocRoutedSubnets reserves three consecutive /24s in 198.18.0.0/15 for this
// test. The reservation is cross-process: a second suite running at the same
// time sees the live reservation and takes the next block. A reservation whose
// owner is gone is reclaimed, so a killed run does not block the next one.
func allocRoutedSubnets(t *testing.T) [3]string {
	t.Helper()
	o := currentOwner()
	indices, err := defaultSubnetStore.reserveBlock(o)
	if err != nil {
		t.Fatalf("harness: reserving routed subnets: %v", err)
	}
	t.Cleanup(func() { defaultSubnetStore.releaseBlock(indices, o) })
	return [3]string{routedSubnet(indices[0]), routedSubnet(indices[1]), routedSubnet(indices[2])}
}

// ReserveSubnetIndex reserves one /24 index in [first, last] for the current
// test process and returns it; the caller's t.Cleanup releases it. The
// reservation is cross-process, and an index whose owner is gone is reclaimed.
// It returns an error when the range is outside 198.18.0.0/15 or holds no free
// index.
func ReserveSubnetIndex(t testing.TB, first, last int) (int, error) {
	t.Helper()
	if first < 0 || last >= routedSubnetCount || first > last {
		return 0, fmt.Errorf("subnet index range [%d, %d] is outside 198.18.0.0/15", first, last)
	}
	o := currentOwner()
	index, err := defaultSubnetStore.reserveIndex(o, first, last)
	if err != nil {
		return 0, err
	}
	t.Cleanup(func() { defaultSubnetStore.releaseIndex(index, o) })
	return index, nil
}

// ReserveSubnetPrefix reserves one prefix from pool for the current test process
// and returns it; the caller's t.Cleanup releases it. The scan and the
// reservation happen under one cross-process lock, and a reservation whose owner
// is gone is reclaimed. When every prefix is held by a live process the error
// names the pool range, each holder and how to clear the reservations.
func ReserveSubnetPrefix(t testing.TB, pool []netip.Prefix) (netip.Prefix, error) {
	t.Helper()
	o := currentOwner()
	prefix, err := defaultSubnetStore.reservePrefix(o, pool)
	if err != nil {
		return netip.Prefix{}, err
	}
	t.Cleanup(func() { defaultSubnetStore.releasePrefix(prefix, o) })
	return prefix, nil
}

func (s subnetStore) reserveIndex(o owner, first, last int) (int, error) {
	subnetMu.Lock()
	defer subnetMu.Unlock()

	unlock, err := s.lock()
	if err != nil {
		return 0, err
	}
	defer unlock()

	reserved, err := s.scan()
	if err != nil {
		return 0, err
	}
	for index := first; index <= last; index++ {
		if reserved.routed[index] {
			continue
		}
		if err := os.WriteFile(s.routedPath(index), []byte(o.String()), 0o600); err != nil {
			return 0, err
		}
		return index, nil
	}
	return 0, fmt.Errorf("no free /24 left in [%d, %d]", first, last)
}

func (s subnetStore) reserveBlock(o owner) ([3]int, error) {
	subnetMu.Lock()
	defer subnetMu.Unlock()

	unlock, err := s.lock()
	if err != nil {
		return [3]int{}, err
	}
	defer unlock()

	reserved, err := s.scan()
	if err != nil {
		return [3]int{}, err
	}
	for index := routedSubnetSkip; index+2 < routedSubnetLimit; index += routedSubnetStride {
		if reserved.routed[index] || reserved.routed[index+1] || reserved.routed[index+2] {
			continue
		}
		for i := index; i < index+3; i++ {
			if err := os.WriteFile(s.routedPath(i), []byte(o.String()), 0o600); err != nil {
				return [3]int{}, err
			}
		}
		return [3]int{index, index + 1, index + 2}, nil
	}
	return [3]int{}, fmt.Errorf("no free /24 block left in 198.18.0.0/15")
}

func (s subnetStore) reservePrefix(o owner, pool []netip.Prefix) (netip.Prefix, error) {
	if len(pool) == 0 {
		return netip.Prefix{}, errors.New("the subnet pool is empty")
	}
	subnetMu.Lock()
	defer subnetMu.Unlock()

	unlock, err := s.lock()
	if err != nil {
		return netip.Prefix{}, err
	}
	defer unlock()

	reserved, err := s.scan()
	if err != nil {
		return netip.Prefix{}, err
	}
	for _, candidate := range pool {
		prefix := candidate.Masked()
		if _, taken := reserved.prefixes[prefix.String()]; taken {
			continue
		}
		if err := os.WriteFile(s.prefixPath(prefix), []byte(o.String()), 0o600); err != nil {
			return netip.Prefix{}, err
		}
		return prefix, nil
	}
	return netip.Prefix{}, poolExhaustedError(pool, reserved.prefixes)
}

type subnetReservations struct {
	routed   map[int]bool
	prefixes map[string]owner
}

// scan reads the state directory, removes every reservation whose owner is gone,
// and returns the live ones; a file whose name it does not recognize is left
// alone, since it may belong to a newer allocator.
func (s subnetStore) scan() (subnetReservations, error) {
	reserved := subnetReservations{
		routed:   map[int]bool{},
		prefixes: map[string]owner{},
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return reserved, nil
		}
		return reserved, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == subnetLockName {
			continue
		}
		index, routedIndex := strconv.Atoi(name)
		prefix, prefixed := parseSubnetPrefixName(name)
		if routedIndex != nil && !prefixed {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, name))
		if err != nil {
			continue
		}
		o, ok := parseOwnerValue(string(data))
		if !ok || !o.alive() {
			_ = os.Remove(filepath.Join(s.dir, name))
			continue
		}
		if routedIndex == nil {
			reserved.routed[index] = true
		} else {
			reserved.prefixes[prefix.String()] = o
		}
	}
	return reserved, nil
}

func poolExhaustedError(pool []netip.Prefix, holders map[string]owner) error {
	first, last := pool[0].Masked(), pool[0].Masked()
	for _, candidate := range pool[1:] {
		prefix := candidate.Masked()
		if prefix.Addr().Less(first.Addr()) {
			first = prefix
		}
		if last.Addr().Less(prefix.Addr()) {
			last = prefix
		}
	}

	byOwner := map[owner][]string{}
	for prefix, o := range holders {
		byOwner[o] = append(byOwner[o], prefix)
	}
	owners := make([]owner, 0, len(byOwner))
	for o := range byOwner {
		owners = append(owners, o)
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i].String() < owners[j].String() })

	var b strings.Builder
	fmt.Fprintf(&b, "no free subnet in %s through %s (%d slots): every slot is reserved by a live test process",
		first, last, len(pool))
	for _, o := range owners {
		prefixes := byOwner[o]
		sort.Strings(prefixes)
		fmt.Fprintf(&b, "\n  %s holds %d: %s", o.String(), len(prefixes), summarizePrefixes(prefixes))
	}
	fmt.Fprintf(&b, "\nthe reservations are files under %s; a reservation whose process is gone is reclaimed on the next allocation, so stop the processes listed above or wait for them to finish and allocate again",
		subnetStateDir)
	return errors.New(b.String())
}

func summarizePrefixes(prefixes []string) string {
	const shown = 3
	if len(prefixes) <= shown {
		return strings.Join(prefixes, ", ")
	}
	return fmt.Sprintf("%s, ... (%d more)", strings.Join(prefixes[:shown], ", "), len(prefixes)-shown)
}

func cleanSubnetReservations(logf func(format string, args ...any)) {
	if _, err := os.Stat(subnetStateDir); err != nil {
		return
	}
	unlock, err := defaultSubnetStore.lock()
	if err != nil {
		logf("cleanup: locking subnet reservations: %v", err)
		return
	}
	defer unlock()
	if _, err := defaultSubnetStore.scan(); err != nil {
		logf("cleanup: reading subnet reservations: %v", err)
	}
}

func (s subnetStore) releaseBlock(indices [3]int, o owner) {
	subnetMu.Lock()
	defer subnetMu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return
	}
	defer unlock()
	for _, index := range indices {
		s.releaseFile(strconv.Itoa(index), o)
	}
}

func (s subnetStore) releaseIndex(index int, o owner) {
	subnetMu.Lock()
	defer subnetMu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return
	}
	defer unlock()
	s.releaseFile(strconv.Itoa(index), o)
}

func (s subnetStore) releasePrefix(prefix netip.Prefix, o owner) {
	subnetMu.Lock()
	defer subnetMu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return
	}
	defer unlock()
	s.releaseFile(subnetPrefixFileName(prefix), o)
}

// releaseFile removes the reservation file when it still names o, so a file
// re-reserved by another owner is left alone.
func (s subnetStore) releaseFile(name string, o owner) {
	data, err := os.ReadFile(filepath.Join(s.dir, name))
	if err != nil {
		return
	}
	if got, ok := parseOwnerValue(string(data)); ok && got == o {
		_ = os.Remove(filepath.Join(s.dir, name))
	}
}

func (s subnetStore) lock() (func(), error) {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(s.dir, subnetLockName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

func routedSubnet(index int) string {
	return routedSubnetPrefix(index).String()
}

func routedSubnetPrefix(index int) netip.Prefix {
	return netip.PrefixFrom(
		netip.AddrFrom4([4]byte{198, byte(18 + index/256), byte(index % 256), 0}),
		24,
	)
}

func (s subnetStore) routedPath(index int) string {
	return filepath.Join(s.dir, strconv.Itoa(index))
}

func (s subnetStore) prefixPath(prefix netip.Prefix) string {
	return filepath.Join(s.dir, subnetPrefixFileName(prefix))
}

func subnetPrefixFileName(prefix netip.Prefix) string {
	return subnetPrefixFilePrefix + strings.ReplaceAll(prefix.Masked().String(), "/", "_")
}

func parseSubnetPrefixName(name string) (netip.Prefix, bool) {
	if !strings.HasPrefix(name, subnetPrefixFilePrefix) {
		return netip.Prefix{}, false
	}
	rest := strings.TrimPrefix(name, subnetPrefixFilePrefix)
	slash := strings.LastIndex(rest, "_")
	if slash <= 0 || slash == len(rest)-1 {
		return netip.Prefix{}, false
	}
	prefix, err := netip.ParsePrefix(rest[:slash] + "/" + rest[slash+1:])
	if err != nil {
		return netip.Prefix{}, false
	}
	return prefix, true
}
