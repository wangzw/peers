package harness

import (
	"slices"
	"testing"
)

// TestOwnerMarkerIsANameMarker checks that the label value uses the same "ape2e-<pid>-<start>-pidns<inode>" form as a project name.
func TestOwnerMarkerIsANameMarker(t *testing.T) {
	marker := OwnerMarker()
	got, ok := parseOwner(marker)
	if !ok {
		t.Fatalf("parseOwner(%q) did not find an owner marker", marker)
	}
	if got != currentOwner() {
		t.Fatalf("parseOwner(%q) = %+v, want %+v", marker, got, currentOwner())
	}
	if ownerGone(marker) {
		t.Fatalf("ownerGone(%q) = true; the owner is this live process", marker)
	}
}

// TestDeadDockerIDsKeepsLiveOwners checks that the cleanup removes only resources whose owner is gone, with the owner label deciding when the project name carries no marker.
func TestDeadDockerIDsKeepsLiveOwners(t *testing.T) {
	live := OwnerMarker()
	dead := "ape2e-" + (owner{pid: 999999999, start: 1, ns: currentOwner().ns}).String()
	resources := []dockerResource{
		// A project from a compose file: the name has no marker and the label names the owner.
		{id: "compose-live", name: "compose-live_plain_1", project: "ape2e-t0001-abcdef", owner: live},
		{id: "compose-dead", name: "compose-dead_plain_1", project: "ape2e-t0001-abcdef", owner: dead},
		// Harness projects: the name carries the marker and there is no label.
		{id: "harness-live", project: live},
		{id: "harness-dead", project: dead},
		// The label wins over a project name that parses the other way.
		{id: "label-live-name-dead", project: dead, owner: live},
		// A resource without a marker has an unknown owner and is left alone.
		{id: "unknown", project: "ape2e-legacy-abcdef"},
	}

	got := deadDockerIDs(resources)
	want := []string{"compose-dead", "harness-dead"}
	if !slices.Equal(got, want) {
		t.Fatalf("deadDockerIDs = %v, want %v", got, want)
	}
}

// TestParseDockerResourcesReadsOwnerLabel checks the listing parser keeps the
// owner label, including a volume line whose name field is empty and a line
// with an empty owner label.
func TestParseDockerResourcesReadsOwnerLabel(t *testing.T) {
	const marker = "ape2e-123-456-pidns789"
	resources := parseDockerResources([]string{
		"abc|proj_plain_1|proj|" + marker,
		"def|proj_plain_1|proj|",
		"pair-vol||proj|" + marker,
	})
	if len(resources) != 3 {
		t.Fatalf("parseDockerResources returned %d resources, want 3", len(resources))
	}
	if got := resources[0]; got.id != "abc" || got.name != "proj_plain_1" || got.project != "proj" || got.owner != marker {
		t.Errorf("container resource = %+v, want id/name/project/owner filled", got)
	}
	if got := resources[1]; got.owner != "" || got.project != "proj" {
		t.Errorf("resource without an owner label = %+v, want an empty owner and the project kept", got)
	}
	if got := resources[2]; got.id != "pair-vol" || got.name != "pair-vol" || got.project != "proj" || got.owner != marker {
		t.Errorf("volume resource = %+v, want the name as id and name, with project and owner", got)
	}
}
