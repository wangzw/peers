package main

import (
	"bytes"
	"errors"
	"sort"
	"strings"
	"testing"
)

// TestDispatch checks command dispatch, usage output and exit codes.
func TestDispatch(t *testing.T) {
	var calls []string
	register("x", func(args []string) error {
		calls = append(calls, "x:"+strings.Join(args, ","))
		return nil
	})
	register("x y", func(args []string) error {
		calls = append(calls, "x y:"+strings.Join(args, ","))
		return nil
	})
	register("fail", func([]string) error {
		return errors.New("boom")
	})
	t.Cleanup(func() {
		delete(commands, "x")
		delete(commands, "x y")
		delete(commands, "fail")
	})

	var stderr bytes.Buffer

	if code := dispatch([]string{"x", "y", "1"}, &stderr); code != 0 {
		t.Fatalf("dispatch(x y 1) = %d, want 0 (stderr %q)", code, stderr.String())
	}
	if len(calls) != 1 || calls[0] != "x y:1" {
		t.Fatalf("calls = %q, want [x y:1]", calls)
	}

	calls = nil
	if code := dispatch([]string{"x", "z"}, &stderr); code != 0 {
		t.Fatalf("dispatch(x z) = %d, want 0 (stderr %q)", code, stderr.String())
	}
	if len(calls) != 1 || calls[0] != "x:z" {
		t.Fatalf("calls = %q, want [x:z]", calls)
	}

	stderr.Reset()
	if code := dispatch([]string{"nope"}, &stderr); code != 2 {
		t.Fatalf("dispatch(nope) = %d, want 2", code)
	}
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	var want strings.Builder
	want.WriteString("usage: rsprobe <command> [args]; commands:\n")
	for _, name := range names {
		want.WriteString("  " + name + "\n")
	}
	if got := stderr.String(); got != want.String() {
		t.Fatalf("usage = %q, want %q", got, want.String())
	}

	stderr.Reset()
	if code := dispatch([]string{"fail"}, &stderr); code != 1 {
		t.Fatalf("dispatch(fail) = %d, want 1", code)
	}
	if got, want := stderr.String(), "rsprobe: boom\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
}
