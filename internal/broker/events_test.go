package broker

import "testing"

// TestEventLaneMergesByDirectoryAndPID checks that a duplicate (dir, pid) is
// kept once and a pid 0 event replaces every queued pid event of its directory.
func TestEventLaneMergesByDirectoryAndPID(t *testing.T) {
	var lane eventLane
	lane.add(registryEvent{uid: 1, dir: "/a", pid: 10})
	lane.add(registryEvent{uid: 1, dir: "/a", pid: 11})
	lane.add(registryEvent{uid: 1, dir: "/a", pid: 10}) // duplicate
	lane.add(registryEvent{uid: 1, dir: "/b", pid: 12})

	if got := len(lane.items); got != 3 {
		t.Fatalf("after pid events, %d items queued, want 3", got)
	}

	// pid 0 covers both queued pids of /a and leaves /b alone.
	lane.add(registryEvent{uid: 1, dir: "/a", pid: 0})
	if got := len(lane.items); got != 2 {
		t.Fatalf("after a pid 0 event, %d items queued, want 2", got)
	}
	for _, item := range lane.items {
		if item.dir == "/a" && item.pid != 0 {
			t.Fatalf("a pid event of /a survived its pid 0 event: %+v", item)
		}
	}

	// A later pid event of the same directory is covered by the queued pid 0.
	lane.add(registryEvent{uid: 1, dir: "/a", pid: 7})
	if got := len(lane.items); got != 2 {
		t.Fatalf("after a pid event covered by pid 0, %d items queued, want 2", got)
	}
}
