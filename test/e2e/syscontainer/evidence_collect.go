//go:build e2e

package syscontainer

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// systemdEvidenceBound bounds the whole evidence collection; it starts after
// systemdReadyBound is given up and does not extend it. Every diagnostic command
// shares this one bound, so a hung command exhausts it and the commands after it
// leave only their timeout error text.
const systemdEvidenceBound = 10 * time.Second

// logBootEvidence writes the state of the systemd container to the test log
// before the deferred teardown removes it.
func (c *container) logBootEvidence(service string) {
	c.t.Helper()
	c.t.Logf("syscontainer: %s", c.bootEvidence(service))
}

// bootEvidence collects the state that explains why systemd did not become
// ready; every command's error text becomes part of the evidence, so a
// container that is already gone still leaves a record.
func (c *container) bootEvidence(service string) string {
	ctx, cancel := context.WithTimeout(context.Background(), systemdEvidenceBound)
	defer cancel()
	run := func(args ...string) string { return c.docker(ctx, args...) }
	return collectBootEvidence(c.project, c.composeFile, service, run, readHostMemory, readHostPids)
}

// docker runs one docker command under ctx and returns its combined output; the
// error text is part of the result, because a command that failed is evidence
// too.
func (c *container) docker(ctx context.Context, args ...string) string {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = c.env
	out, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if err == nil {
		return text
	}
	if text == "" {
		return err.Error()
	}
	return fmt.Sprintf("%v\n%s", err, text)
}

func readHostMemory() string {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return err.Error()
	}
	return headLines(strings.TrimSpace(string(data)), 3)
}

// readHostPids reports the host pids accounting a process-limit kill would show
// against; the root cgroup has no pids files on a systemd host, so the system
// slice and the kernel limit are read too.
func readHostPids() string {
	var b strings.Builder
	for _, path := range []string{
		"/proc/sys/kernel/pid_max",
		"/proc/loadavg",
		"/sys/fs/cgroup/pids.current",
		"/sys/fs/cgroup/pids.max",
		"/sys/fs/cgroup/pids.events",
		"/sys/fs/cgroup/system.slice/pids.current",
		"/sys/fs/cgroup/system.slice/pids.max",
		"/sys/fs/cgroup/system.slice/pids.events",
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(&b, "%s: %v\n", path, err)
			continue
		}
		fmt.Fprintf(&b, "%s: %s\n", path, strings.TrimSpace(string(data)))
	}
	return strings.TrimSpace(b.String())
}
