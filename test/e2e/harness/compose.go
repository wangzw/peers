package harness

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/ctl"
	"gopkg.in/yaml.v3"
)

// agentSocketBound bounds the wait for a node agent's control socket after
// compose up returns. Compose starts the entrypoint as soon as the container
// runs; binding a Unix socket is the first thing the node agent does, so the
// project-wide three-second bound is enough. No product timer governs a
// test-only process.
const agentSocketBound = 3 * time.Second

// Topology describes one compose project: which file to start, which services
// it has, which network each service is addressed on, and what to do once the
// containers are running. The fault work item adds its topologies with the
// same fields.
type Topology struct {
	// file is the file name under compose/ that describes the project.
	file string
	// nodes are the compose service names, in start order.
	nodes []string
	// networks names the network each node is addressed on.
	networks map[string]string
	// env adds compose variables on top of E2E_IMAGE and CTL_<NODE>.
	env map[string]string
	// envFunc adds compose variables computed for this project. It runs on
	// every Up, so a topology whose values must differ per project, such as
	// the IPv4 subnets of the routed topologies or the randomly chosen IPv6
	// subnets, does not share them between two concurrent projects in one
	// test process.
	envFunc func(t *testing.T) map[string]string
	// init runs once every node agent answers, before Up returns.
	init func(t *testing.T, c *Cluster)
}

// TopoSingle is one host on one internal network.
var TopoSingle = Topology{
	file:     "single.yaml",
	nodes:    []string{"hostA"},
	networks: map[string]string{"hostA": "net"},
}

// Cluster is one running compose project. A test drives its nodes through the
// same Node and Session types as a local node.
type Cluster struct {
	t           *testing.T
	project     string
	composePath string
	env         []string
	image       string

	ctlDirs map[string]string
	order   []string

	mu           sync.Mutex
	nodes        map[string]*Node
	clients      map[string]*ctl.Client
	containerIDs map[string]string

	downOnce sync.Once
}

// Up starts the compose project described by topo and returns it once every
// node agent answers. The project is torn down when the test ends.
func Up(t *testing.T, topo Topology) *Cluster {
	t.Helper()
	if len(topo.nodes) == 0 {
		t.Fatalf("harness: topology %q has no nodes", topo.file)
	}

	image := Image(t)
	project := projectName(t.Name())
	dir := t.TempDir()
	source, err := composeSourcePath(topo.file)
	if err != nil {
		t.Fatalf("harness: locating compose file %s: %v", topo.file, err)
	}
	composeData, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("harness: reading compose file %s: %v", topo.file, err)
	}
	services, err := composeServices(composeData)
	if err != nil {
		t.Fatalf("harness: parsing compose file %s: %v", topo.file, err)
	}
	composePath := filepath.Join(dir, "compose.yaml")
	if err := os.WriteFile(composePath, composeData, 0o644); err != nil {
		t.Fatalf("harness: writing %s: %v", composePath, err)
	}

	c := &Cluster{
		t:            t,
		project:      project,
		composePath:  composePath,
		image:        image,
		ctlDirs:      map[string]string{},
		order:        append([]string(nil), topo.nodes...),
		nodes:        map[string]*Node{},
		clients:      map[string]*ctl.Client{},
		containerIDs: map[string]string{},
	}

	// Each node gets a host directory mounted at /ctl. It is a t.TempDir, so
	// the host user owns it and the container's peer account, which carries
	// the same uid, can write there. The directories are created once, not per
	// up attempt, so the mount paths stay stable across a retry.
	for _, node := range topo.nodes {
		c.ctlDirs[node] = t.TempDir()
	}
	// Compose substitutes the variables of every declared service, including
	// one a profile keeps stopped, so a service the topology does not start
	// still needs a control-directory variable.
	for _, service := range services {
		if _, ok := c.ctlDirs[service]; ok {
			continue
		}
		c.ctlDirs[service] = t.TempDir()
	}

	// Register the teardown after the temporary directories so it runs before
	// their cleanup removes the mounted directories.
	t.Cleanup(c.cleanup)

	// A block reserved for this project can still overlap a network an older
	// run left behind. Each attempt asks envFunc for a fresh block; the failed
	// block stays reserved until the test ends, so the next attempt cannot
	// pick it again.
	const upAttempts = 8
	var out []byte
	for attempt := 1; ; attempt++ {
		c.env = c.buildEnv(t, topo)
		up := c.composeCommand("up", "-d", "--wait", "--wait-timeout", "60")
		var err error
		out, err = up.CombinedOutput()
		if err == nil {
			break
		}
		if attempt < upAttempts && subnetOverlap(out) {
			_, _ = c.composeCommand("down", "-v", "--remove-orphans", "--timeout", "1").CombinedOutput()
			continue
		}
		t.Fatalf("harness: starting compose project %s: %v\n%s", project, err, strings.TrimSpace(string(out)))
	}

	for _, node := range topo.nodes {
		node := node
		socket := filepath.Join(c.ctlDirs[node], "ctl.sock")
		client := ctl.Dial(socket)
		c.clients[node] = client
		Eventually(t, agentSocketBound, "node agent "+node, func() (bool, string) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := client.Procs(ctx, ctl.ProcsRequest{Kind: "all"}); err != nil {
				return false, err.Error()
			}
			return true, ""
		})
	}

	if topo.init != nil {
		topo.init(t, c)
	}
	return c
}

// buildEnv builds the compose environment for one up attempt. The topology's
// envFunc runs on every call, so a routed topology gets a fresh subnet block
// after an overlap.
func (c *Cluster) buildEnv(t *testing.T, topo Topology) []string {
	env := []string{"E2E_IMAGE=" + c.image}
	names := make([]string, 0, len(c.ctlDirs))
	for name := range c.ctlDirs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		env = append(env, "CTL_"+strings.ToUpper(name)+"="+c.ctlDirs[name])
	}
	for name, value := range topo.env {
		env = append(env, name+"="+value)
	}
	if topo.envFunc != nil {
		for name, value := range topo.envFunc(t) {
			env = append(env, name+"="+value)
		}
	}
	return append(os.Environ(), env...)
}

// subnetOverlap reports whether a failed compose up was refused because its
// subnet pool overlaps another live project's.
func subnetOverlap(out []byte) bool {
	return strings.Contains(strings.ToLower(string(out)), "overlap")
}

// composeServices returns the service names a compose file declares, sorted.
// Compose parses and substitutes every declared service even when a profile
// keeps it stopped, so Up needs the full list and not only the started nodes.
func composeServices(data []byte) ([]string, error) {
	var doc struct {
		Services map[string]any `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(doc.Services))
	for name := range doc.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Project returns the compose project name.
func (c *Cluster) Project() string {
	return c.project
}

// Node returns the node named name. The same Node is returned on every call.
func (c *Cluster) Node(name string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.nodes[name]; ok {
		return n
	}
	dir, ok := c.ctlDirs[name]
	if !ok {
		c.t.Fatalf("harness: cluster %s has no node %q", c.project, name)
		return nil
	}
	n := &Node{impl: &remoteNode{
		t:       c.t,
		name:    name,
		socket:  filepath.Join(dir, "ctl.sock"),
		client:  c.clients[name],
		homeDir: "/home/peer",
	}}
	c.nodes[name] = n
	return n
}

// CTLDir returns the host directory mounted at /ctl on node.
func (c *Cluster) CTLDir(t testing.TB, node string) string {
	t.Helper()
	dir, ok := c.ctlDirs[node]
	if !ok {
		t.Fatalf("harness: cluster %s has no node %q", c.project, node)
	}
	return dir
}

// Addr returns the IPv4 address of node on network.
func (c *Cluster) Addr(node, network string) string {
	return c.address(node, network, "IPAddress")
}

// Addr6 returns the global IPv6 address of node on network.
func (c *Cluster) Addr6(node, network string) string {
	return c.address(node, network, "GlobalIPv6Address")
}

// Gateway returns the IPv4 gateway Docker gave node on network. On a routed
// network this is the host-side bridge address, the one a container could use
// to reach a service listening on the test host.
func (c *Cluster) Gateway(node, network string) string {
	return c.address(node, network, "Gateway")
}

// Networks returns the compose networks node is attached to, without the
// project prefix and sorted by name. The compose file declares the attachments,
// but a node such as the router is attached to several, so a caller that must
// cover every one reads them back from the running container.
func (c *Cluster) Networks(node string) []string {
	format := `{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}`
	cmd := exec.Command("docker", "inspect", "--format", format, c.containerID(node))
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		c.t.Fatalf("harness: inspecting the networks of node %s: %v\n%s", node, err, strings.TrimSpace(string(out)))
		return nil
	}
	prefix := c.project + "_"
	names := make([]string, 0, 4)
	for _, name := range strings.Fields(string(out)) {
		names = append(names, strings.TrimPrefix(name, prefix))
	}
	sort.Strings(names)
	return names
}

// address reads one address field from docker inspect.
func (c *Cluster) address(node, network, field string) string {
	format := fmt.Sprintf("{{with index .NetworkSettings.Networks %q}}{{.%s}}{{end}}", c.project+"_"+network, field)
	cmd := exec.Command("docker", "inspect", "--format", format, c.containerID(node))
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		c.t.Fatalf("harness: inspecting node %s on network %s: %v\n%s", node, network, err, strings.TrimSpace(string(out)))
		return ""
	}
	addr := strings.TrimSpace(string(out))
	if addr == "" {
		c.t.Fatalf("harness: node %s has no %s on network %s", node, field, network)
	}
	return addr
}

// Root runs argv as root in node and returns its standard output. A non-zero
// exit fails the test with the command output.
func (c *Cluster) Root(t testing.TB, node string, argv ...string) string {
	t.Helper()
	out, err := c.RootOK(t, node, argv...)
	if err != nil {
		t.Fatalf("harness: running %v as root in %s: %v", argv, node, err)
	}
	return out
}

// RootOK is Root that returns the error instead of failing. The returned
// string carries standard output and, on failure, standard error as well.
func (c *Cluster) RootOK(t testing.TB, node string, argv ...string) (string, error) {
	t.Helper()
	if len(argv) == 0 {
		return "", fmt.Errorf("harness: Root needs a command")
	}
	args := append([]string{"exec", "-T", "-u", "root", node}, argv...)
	cmd := c.composeCommand(args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%w: %s", err, strings.TrimSpace(stdout.String()+stderr.String()))
	}
	return stdout.String(), nil
}

// Down tears the project down. It is idempotent and also runs from the test
// cleanup.
func (c *Cluster) Down() {
	c.downOnce.Do(func() {
		cmd := c.composeCommand("down", "-v", "--remove-orphans", "--timeout", "1")
		if out, err := cmd.CombinedOutput(); err != nil {
			c.t.Errorf("harness: tearing down compose project %s: %v\n%s", c.project, err, strings.TrimSpace(string(out)))
		}
	})
}

// composeCommand builds one docker compose invocation for this project.
func (c *Cluster) composeCommand(args ...string) *exec.Cmd {
	argv := append([]string{"compose", "-p", c.project, "-f", c.composePath}, args...)
	cmd := exec.Command("docker", argv...)
	cmd.Env = c.env
	return cmd
}

// containerID returns the docker container id of node.
func (c *Cluster) containerID(node string) string {
	c.mu.Lock()
	id, ok := c.containerIDs[node]
	c.mu.Unlock()
	if ok {
		return id
	}

	cmd := c.composeCommand("ps", "-q", node)
	out, err := cmd.CombinedOutput()
	if err != nil {
		c.t.Fatalf("harness: finding container for node %s: %v\n%s", node, err, strings.TrimSpace(string(out)))
		return ""
	}
	id = strings.TrimSpace(string(out))
	if i := strings.IndexByte(id, '\n'); i >= 0 {
		id = id[:i]
	}
	if id == "" {
		c.t.Fatalf("harness: node %s is not running", node)
		return ""
	}
	c.mu.Lock()
	c.containerIDs[node] = id
	c.mu.Unlock()
	return id
}

// cleanup closes the control clients, collects artifacts when the test failed,
// and tears the project down.
func (c *Cluster) cleanup() {
	if c.t.Failed() {
		c.collectArtifacts()
	}
	c.mu.Lock()
	clients := c.clients
	c.clients = nil
	c.mu.Unlock()
	for _, client := range clients {
		client.Close()
	}
	c.Down()
}

// collectArtifacts writes the failing project's state to E2E_ARTIFACTS, or logs
// the tail of each log when that variable is unset.
func (c *Cluster) collectArtifacts() {
	dest := os.Getenv("E2E_ARTIFACTS")
	if dest == "" {
		for _, node := range c.order {
			for _, name := range c.nodeLogNames(node) {
				if data, err := os.ReadFile(filepath.Join(c.ctlDirs[node], name)); err == nil {
					c.t.Logf("harness: node %s, %s (tail):\n%s", node, name, tail(string(redactSecrets(data)), 50))
				}
			}
		}
		out, _ := c.composeCommand("logs", "--no-color", "--tail", "50").CombinedOutput()
		c.t.Logf("harness: compose logs (tail):\n%s", out)
		return
	}

	dir := filepath.Join(dest, c.project)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		c.t.Errorf("harness: creating artifact directory %s: %v", dir, err)
		return
	}
	for _, node := range c.order {
		for _, name := range c.nodeLogNames(node) {
			source := filepath.Join(c.ctlDirs[node], name)
			data, err := os.ReadFile(source)
			if err != nil {
				continue
			}
			writeArtifact(c.t, dir, node+"-"+name, data)
		}
		for _, command := range [][]string{
			{"ip", "addr"},
			{"ip", "route"},
			{"iptables-save"},
		} {
			out, _ := c.RootOK(c.t, node, command...)
			name := node + "-" + strings.Join(command, "-") + ".txt"
			writeArtifact(c.t, dir, name, []byte(out))
		}
	}
	out, _ := c.composeCommand("logs", "--no-color").CombinedOutput()
	writeArtifact(c.t, dir, "compose.log", out)
}

// nodeLogNames lists the files a node agent and its Brokers write under /ctl.
func (c *Cluster) nodeLogNames(node string) []string {
	entries, err := os.ReadDir(c.ctlDirs[node])
	if err != nil {
		return nil
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		names = append(names, entry.Name())
	}
	return names
}

// tail returns the last n lines of text.
func tail(text string, n int) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

var (
	projectNameMu   sync.Mutex
	projectNameLast string
	projectNameSeq  atomic.Uint64
)

// projectName derives a compose project name from a test name:
// ape2e-<owner pid>-<owner start time>-pidns<owner namespace>-<slug, at most 32
// characters>-<six hex digits>. The owner marker lets a cleanup tell a live
// suite's project from one a killed suite left behind, and the start time keeps
// a reused PID from protecting stale residue. The namespace keeps a cleanup
// from mistaking another PID namespace's PID for one of its own. Two calls never
// return the same name.
func projectName(testName string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(testName) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	slug := strings.Trim(b.String(), "-")
	if len(slug) > 32 {
		slug = strings.Trim(slug[:32], "-")
	}
	if slug == "" {
		slug = "e2e"
	}

	projectNameMu.Lock()
	defer projectNameMu.Unlock()
	for {
		name := "ape2e-" + currentOwner().String() + "-" + slug + "-" + randomHex6()
		if name != projectNameLast {
			projectNameLast = name
			return name
		}
	}
}

// randomHex6 returns six random hexadecimal digits. The fallback keeps two
// consecutive names different even if the system random source fails, which
// the test asserts.
func randomHex6() string {
	var b [3]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("%06x", projectNameSeq.Add(1)&0xffffff)
}
