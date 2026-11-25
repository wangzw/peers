package broker

import (
	"context"
	"testing"
)

// TestRunSetsListenOverride checks that Run copies RunOptions.Listen into
// Deps.ListenOverride after Load.
func TestRunSetsListenOverride(t *testing.T) {
	cases := []struct {
		name   string
		listen string
	}{
		{name: "given", listen: "127.0.0.1:0"},
		{name: "empty", listen: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(chan string, 1)
			rt := newRunTest(t, func(rt *runTest) {
				rt.opts.Listen = tc.listen
				rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "capture", fn: func(_ context.Context, _ ConfigSource, deps *Deps, _ *Assembly) ([]Component, error) {
					seen <- deps.ListenOverride
					return nil, nil
				}}}
			})
			rt.start()
			if got := rt.readyLine(); got != "ok\n" {
				t.Fatalf("readiness line = %q", got)
			}

			select {
			case got := <-seen:
				if got != tc.listen {
					t.Errorf("Deps.ListenOverride = %q, want %q", got, tc.listen)
				}
			default:
				t.Fatal("the assembler did not see the Deps")
			}
		})
	}
}
