package broker

import (
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/config"
)

// TestRunLogsRuntimeDirFallback covers the startup notice: when the preferred
// runtime directory belongs to another account and the fallback is used, the
// run log names the taken path, its owner and the fallback path.
func TestRunLogsRuntimeDirFallback(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.cfg.runtimeFallback = &config.RuntimeDirFallback{
			TakenPath: "/tmp/peers-1000",
			OwnerUID:  1001,
		}
	})
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	logs := rt.logs.String()
	for _, want := range []string{EvRuntimeDirFallback, `"taken":"/tmp/peers-1000"`, `"owner_uid":1001`, `"path":"` + rt.dir + `"`} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs = %q, want it to contain %q", logs, want)
		}
	}
}
