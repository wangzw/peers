package harness

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestFindFreePortSkipsReserved checks that findFreePort skips ports marked taken.
func TestFindFreePortSkipsReserved(t *testing.T) {
	taken := map[int]bool{portScanFirst: true, portScanFirst + 1: true}
	got, err := findFreePort(taken)
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	if got <= portScanFirst+1 {
		t.Fatalf("findFreePort returned %d, want a port above the reserved ones", got)
	}
}

// TestPortScanRanges checks the scan order: the ports below the ephemeral
// range come first, and an unknown or unusable low bound leaves one full
// range.
func TestPortScanRanges(t *testing.T) {
	cases := []struct {
		name string
		low  int
		want []portRange
	}{
		{"typical linux", 32768, []portRange{{portScanFirst, 32767}, {32768, portScanLast}}},
		{"low at the floor", portScanFirst, []portRange{{portScanFirst, portScanLast}}},
		{"unknown low", 0, []portRange{{portScanFirst, portScanLast}}},
		{"low above the range", portScanLast + 1, []portRange{{portScanFirst, portScanLast}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := portScanRanges(tc.low); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("portScanRanges(%d) = %v, want %v", tc.low, got, tc.want)
			}
		})
	}
}

// TestReadPortReservationsReclaimsDeadOwners checks that a live owner's reservation is honored and a dead owner's file removed, using ports below the scan range that a concurrent suite cannot take.
func TestReadPortReservationsReclaimsDeadOwners(t *testing.T) {
	const livePort, deadPort = 15001, 15002

	live := currentOwner()
	dead := owner{pid: 999999999, start: 1, ns: live.ns}

	unlock, err := lockPortState()
	if err != nil {
		t.Fatalf("locking port reservations: %v", err)
	}
	defer unlock()
	t.Cleanup(func() {
		// A second test process may use the same low ports; take the lock so
		// this cleanup cannot remove its reservation between its write and
		// its read.
		if cleanupUnlock, err := lockPortState(); err == nil {
			defer cleanupUnlock()
		}
		_ = os.Remove(portStatePath(livePort))
		_ = os.Remove(portStatePath(deadPort))
	})

	if err := os.WriteFile(portStatePath(livePort), []byte(live.String()), 0o600); err != nil {
		t.Fatalf("writing the live reservation: %v", err)
	}
	if err := os.WriteFile(portStatePath(deadPort), []byte(dead.String()), 0o600); err != nil {
		t.Fatalf("writing the dead reservation: %v", err)
	}

	taken, err := readPortReservations()
	if err != nil {
		t.Fatalf("reading port reservations: %v", err)
	}
	if !taken[livePort] {
		t.Errorf("port %d of a live owner is not reserved", livePort)
	}
	if taken[deadPort] {
		t.Errorf("port %d of a dead owner is reserved", deadPort)
	}
	if _, err := os.Stat(portStatePath(deadPort)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a dead owner's reservation file was kept: %v", err)
	}
}

// TestReservePortReleases checks that two reservations differ and that release
// removes their files.
func TestReservePortReleases(t *testing.T) {
	o := currentOwner()
	firstAddr, firstPort, err := reservePort(o)
	if err != nil {
		t.Fatalf("reserving the first port: %v", err)
	}
	secondAddr, secondPort, err := reservePort(o)
	if err != nil {
		releasePorts([]int{firstPort}, o)
		t.Fatalf("reserving the second port: %v", err)
	}
	if firstAddr == secondAddr {
		t.Errorf("two reservations returned the same address %s", firstAddr)
	}

	releasePorts([]int{firstPort, secondPort}, o)
	for _, port := range []int{firstPort, secondPort} {
		if _, err := os.Stat(portStatePath(port)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("port %d is still reserved after release: %v", port, err)
		}
	}
}

const portChildEnv = "APE2E_PORT_CHILD"

// TestReservePortCrossProcess checks that two child processes allocating at the same time receive different ports.
func TestReservePortCrossProcess(t *testing.T) {
	if os.Getenv(portChildEnv) == "1" {
		addr, port, err := reservePort(currentOwner())
		if err != nil {
			t.Fatalf("reserving a port: %v", err)
		}
		fmt.Printf("PORT=%s\n", addr)
		// Hold the reservation until the other child has allocated.
		time.Sleep(2 * time.Second)
		releasePorts([]int{port}, currentOwner())
		return
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}
	type child struct {
		cmd *exec.Cmd
		out *bytes.Buffer
	}
	children := make([]*child, 2)
	for i := range children {
		out := &bytes.Buffer{}
		cmd := exec.Command(exe, "-test.run=^TestReservePortCrossProcess$")
		cmd.Env = append(os.Environ(), portChildEnv+"=1")
		cmd.Stdout = out
		cmd.Stderr = out
		if err := cmd.Start(); err != nil {
			t.Fatalf("starting child %d: %v", i, err)
		}
		children[i] = &child{cmd: cmd, out: out}
	}

	addrs := make([]string, len(children))
	for i, c := range children {
		if err := c.cmd.Wait(); err != nil {
			t.Fatalf("child %d: %v\n%s", i, err, c.out.String())
		}
		addrs[i] = portChildAddress(t, c.out.String())
	}
	if addrs[0] == addrs[1] {
		t.Fatalf("two processes reserved the same address %s", addrs[0])
	}
}

// portChildAddress returns the address a child printed.
func portChildAddress(t *testing.T, output string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "PORT=") {
			return strings.TrimPrefix(line, "PORT=")
		}
	}
	t.Fatalf("child printed no PORT= line:\n%s", output)
	return ""
}
