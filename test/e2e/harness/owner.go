package harness

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/wangzw/peers/internal/procsig"
)

// owner identifies one incarnation of a test process: its PID, the kernel's start
// time for that PID, and its PID namespace inode. A reused PID has a different
// start time, and a PID from another namespace is not comparable.
type owner struct {
	pid   int
	start uint64
	ns    uint64
}

// ownerNamespacePrefix marks the PID namespace field of a name's owner marker, spelled out so it cannot be confused with the slug that follows.
const ownerNamespacePrefix = "pidns"

// OwnerLabel is the docker label that carries a project's owner marker on its
// containers, networks and volumes. A project started from a compose file keeps
// its readable name and is identified through this label, which CleanResidue
// reads before the project name.
const OwnerLabel = "peers.e2e.owner"

// OwnerMarker returns this process's owner marker as a label value, in the same
// "ape2e-<pid>-<start>-pidns<inode>" form a project name carries.
func OwnerMarker() string {
	return "ape2e-" + currentOwner().String()
}

// String formats the marker names carry: "<pid>-<start>-pidns<inode>"; a platform
// without PID namespaces records zero, which a cleanup treats as unknown.
func (o owner) String() string {
	return fmt.Sprintf("%d-%d-%s%d", o.pid, o.start, ownerNamespacePrefix, o.ns)
}

var (
	currentOwnerOnce sync.Once
	currentOwnerVal  owner
)

func currentOwner() owner {
	currentOwnerOnce.Do(func() {
		pid := os.Getpid()
		start, err := processStartTime(pid)
		if err != nil {
			start = 0
		}
		ns, err := processNamespace(pid)
		if err != nil {
			ns = 0
		}
		currentOwnerVal = owner{pid: pid, start: start, ns: ns}
	})
	return currentOwnerVal
}

// parseOwner reads the marker from a name built as "ape2e-<pid>-<start>-pidns<inode>...";
// the fields after the marker do not matter, and a name without the marker returns
// false.
func parseOwner(name string) (owner, bool) {
	fields := strings.Split(name, "-")
	if len(fields) < 3 || fields[0] != "ape2e" {
		return owner{}, false
	}
	o, ok := makeOwner(fields[1], fields[2])
	if !ok {
		return owner{}, false
	}
	if len(fields) > 3 {
		o.ns = parseNamespaceField(fields[3])
	}
	return o, true
}

func parseOwnerValue(text string) (owner, bool) {
	fields := strings.Split(strings.TrimSpace(text), "-")
	if len(fields) != 2 && len(fields) != 3 {
		return owner{}, false
	}
	o, ok := makeOwner(fields[0], fields[1])
	if !ok {
		return owner{}, false
	}
	if len(fields) == 3 {
		o.ns = parseNamespaceField(fields[2])
	}
	return o, true
}

func parseNamespaceField(field string) uint64 {
	if !strings.HasPrefix(field, ownerNamespacePrefix) {
		return 0
	}
	ns, err := strconv.ParseUint(field[len(ownerNamespacePrefix):], 10, 64)
	if err != nil {
		return 0
	}
	return ns
}

func makeOwner(pidText, startText string) (owner, bool) {
	pid, err := strconv.Atoi(pidText)
	if err != nil || pid <= 0 {
		return owner{}, false
	}
	start, err := strconv.ParseUint(startText, 10, 64)
	if err != nil {
		return owner{}, false
	}
	return owner{pid: pid, start: start}, true
}

// alive reports whether the process that carries this owner still runs; a marker
// from another PID namespace, or one whose /proc entry cannot be read for a reason
// other than "no such process", is treated as alive so a cleanup never removes
// another suite's resources.
func (o owner) alive() bool {
	return o.aliveIn(currentOwner().ns, processStartTime, processExists)
}

// aliveIn reports whether the owner is alive as observed from selfNS, with the
// start-time reader and existence check passed in so the decision can be tested
// without the system. A different or absent namespace on either side is kept; in
// the same namespace only a confirmed ESRCH or ENOENT, or a changed start time,
// proves the process gone.
func (o owner) aliveIn(selfNS uint64, startTime func(int) (uint64, error), exists func(int) bool) bool {
	if o.pid <= 0 {
		return false
	}
	if o.ns != 0 && selfNS != 0 && o.ns != selfNS {
		return true
	}
	if o.ns == 0 && selfNS != 0 {
		return true
	}
	if o.ns != 0 && selfNS == 0 {
		return true
	}
	if o.start == 0 {
		return exists(o.pid)
	}
	start, err := startTime(o.pid)
	switch {
	case err == nil:
		return start == o.start
	case errors.Is(err, syscall.ESRCH), errors.Is(err, os.ErrNotExist):
		return false
	case errors.Is(err, errors.ErrUnsupported):
		// The platform cannot report a start time, as on a system without /proc;
		// the PID alone decides.
		return exists(o.pid)
	default:
		return true
	}
}

func ownerGone(name string) bool {
	o, ok := parseOwner(name)
	return ok && !o.alive()
}

func processExists(pid int) bool {
	return procsig.Alive(pid)
}
