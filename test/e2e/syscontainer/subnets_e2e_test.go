//go:build e2e

package syscontainer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// subnetChildEnv marks the child copy of this test binary that allocates one
// system subnet and holds it while the parent reads the other child's answer.
const subnetChildEnv = "PEERS_E2E_SUBNET_CHILD"

// TestAllocSystemSubnetCrossProcess has two copies of the test binary reserve a
// subnet each and requires the two answers to differ.
func TestAllocSystemSubnetCrossProcess(t *testing.T) {
	if os.Getenv(subnetChildEnv) == "1" {
		subnet := allocSystemSubnet(t)
		fmt.Printf("SUBNET %s\n", subnet)
		// Hold the reservation until the parent closes stdin, so the two
		// reservations overlap for exactly as long as the parent needs.
		_, _ = io.Copy(io.Discard, os.Stdin)
		return
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}

	type child struct {
		cmd     *exec.Cmd
		stdin   io.WriteCloser
		scanner *bufio.Scanner
		stderr  *strings.Builder
	}
	start := func() *child {
		cmd := exec.Command(exe, "-test.run", "^TestAllocSystemSubnetCrossProcess$", "-test.v")
		cmd.Env = append(os.Environ(), subnetChildEnv+"=1")
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("child stdout: %v", err)
		}
		stdin, err := cmd.StdinPipe()
		if err != nil {
			t.Fatalf("child stdin: %v", err)
		}
		var stderr strings.Builder
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("starting child: %v", err)
		}
		t.Cleanup(func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		})
		return &child{cmd: cmd, stdin: stdin, scanner: bufio.NewScanner(stdout), stderr: &stderr}
	}

	first := start()
	second := start()

	subnetOf := func(c *child, label string) string {
		result := make(chan string, 1)
		go func() {
			for c.scanner.Scan() {
				line := c.scanner.Text()
				if strings.HasPrefix(line, "SUBNET ") {
					result <- strings.TrimSpace(strings.TrimPrefix(line, "SUBNET "))
					return
				}
			}
			result <- ""
		}()
		select {
		case subnet := <-result:
			if subnet == "" {
				t.Errorf("%s did not report a subnet; stderr:\n%s", label, c.stderr.String())
			}
			return subnet
		case <-time.After(20 * time.Second):
			t.Fatalf("%s did not report a subnet within 20s; stderr:\n%s", label, c.stderr.String())
			return ""
		}
	}

	one := subnetOf(first, "first child")
	two := subnetOf(second, "second child")

	// Release the children now that both answers are read; each child exits
	// when its stdin reaches EOF, which also runs its cleanup and frees the
	// reservation.
	for _, c := range []*child{first, second} {
		if err := c.stdin.Close(); err != nil {
			t.Errorf("closing child stdin: %v", err)
		}
	}
	for _, c := range []*child{first, second} {
		if err := c.cmd.Wait(); err != nil {
			t.Errorf("child exit: %v; stderr:\n%s", err, c.stderr.String())
		}
	}

	if one == "" || two == "" {
		return
	}
	if one == two {
		t.Fatalf("two processes both allocated %s; the system subnet reservation is not cross-process", one)
	}
}
