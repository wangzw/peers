//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/wangzw/peers/test/e2e/harness"
)

// TestInfraSingleNode starts one container node and checks the properties the
// remaining container scenarios rely on: the peers binary, the account ids,
// an owned control directory, root commands, and an internal network without a
// default route.
func TestInfraSingleNode(t *testing.T) {
	c := harness.Up(t, harness.TopoSingle)
	node := c.Node("hostA")

	version := node.Exec(t, []string{"peers", "--version"}, nil)
	if version.ExitCode != 0 {
		t.Fatalf("peers --version exited %d: %s", version.ExitCode, version.Stderr)
	}
	if got := strings.TrimSpace(version.Stdout); got != "peers e2e" {
		t.Fatalf("peers --version printed %q, want %q", got, "peers e2e")
	}

	// The peer account carries the host uid, so files it writes under the
	// bind-mounted control directory belong to the test user.
	wantUID := os.Getuid()
	if wantUID == 0 {
		wantUID = 1000
	}
	uid := node.Exec(t, []string{"id", "-u"}, nil)
	if got := strings.TrimSpace(uid.Stdout); got != strconv.Itoa(wantUID) {
		t.Fatalf("peer account uid = %q, want %d", got, wantUID)
	}

	if got := strings.TrimSpace(c.Root(t, "hostA", "id", "-u", "other")); got != "2000" {
		t.Fatalf("other account uid = %q, want 2000", got)
	}

	logPath := filepath.Join(c.CTLDir(t, "hostA"), "node.log")
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat %s: %v", logPath, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s: no syscall.Stat_t", logPath)
	}
	if int(stat.Uid) != wantUID {
		t.Fatalf("%s is owned by uid %d, want the host uid %d", logPath, stat.Uid, wantUID)
	}

	c.Root(t, "hostA", "iptables", "-L")

	if got := strings.TrimSpace(c.Root(t, "hostA", "ip", "route", "show", "default")); got != "" {
		t.Fatalf("internal network has a default route: %q", got)
	}
}
