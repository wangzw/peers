//go:build e2e

package e2e

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	cteWaitBound = harness.EventTimeout
	// cteReloadDebounce mirrors the configuration watcher's debounce, the quiet
	// period it waits before reloading a changed file; cteReloadBound is that
	// debounce plus the fallback.
	cteReloadDebounce      = 100 * time.Millisecond
	cteReloadBound         = cteReloadDebounce + cteWaitBound
	cteHotReloadAlias      = "e2e-hot-reload"
	cteAdvertiseExtraValue = "[192.0.2.10:17443]"
)

type cteKey struct {
	name string
	kind string
}

// cteKeys is the complete key set, copied here so the acceptance check stays
// independent of the implementation that generates the template: a new or
// missing key fails this test.
var cteKeys = []cteKey{
	{"broker.alias", "string"},
	{"broker.public", "bool"},
	{"broker.listen", "string"},
	{"broker.offline", "bool"},
	{"broker.advertise", "string"},
	{"broker.advertise_extra", "list"},
	{"broker.address_check_interval", "duration"},
	{"broker.local_socket_group", "string"},
	{"broker.local_socket_mode", "string"},
	{"pairing.static_key", "secret"},
	{"pairing.max_failures", "int"},
	{"pairing.max_attempts_per_minute", "int"},
	{"topics.auto_join", "bool"},
	{"directory.reconcile_interval", "duration"},
	{"directory.max_records", "int"},
	{"directory.max_records_per_link", "int"},
	{"directory.max_topics_per_publisher", "int"},
	{"send.link_retries", "int"},
	{"send.link_backoff", "duration"},
	{"send.request_timeout", "duration"},
	{"send.inbox_retries", "int"},
	{"send.inbox_backoff", "duration"},
	{"send.inbox_timeout", "duration"},
	{"send.dedupe_window", "duration"},
	{"link.dial_timeout", "duration"},
	{"link.reconnect_min", "duration"},
	{"link.reconnect_max", "duration"},
	{"link.keepalive_interval", "duration"},
	{"link.keepalive_timeout", "duration"},
	{"log.level", "string"},
	{"update.check", "bool"},
	{"update.check_interval", "duration"},
}

var cteCommentedKeys = map[string]bool{
	"broker.alias":              true,
	"broker.advertise":          true,
	"broker.advertise_extra":    true,
	"broker.local_socket_group": true,
	"pairing.static_key":        true,
}

type cteLevel struct {
	name       string
	configPath string
	install    func(*testing.T)
	mainPID    func(*testing.T) int
}

// TestREQBrokerConfig040_SystemInstallWritesTemplate installs the system
// service into a container whose process 1 is systemd and checks the
// configuration file the installation generates.
func TestREQBrokerConfig040_SystemInstallWritesTemplate(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	if _, err := os.Stat(systemConfigPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s exists before the installation: %v", systemConfigPath, err)
	}

	installSystemService(t)
	syscontainer.WaitFor(t, "the system Broker socket "+systemSocket, func() bool {
		_, err := os.Stat(systemSocket)
		return err == nil
	})

	session := syscontainer.NewSession(t, "root", "config-template-system")
	cteWaitConnected(t, session)

	cteRunChecks(t, session, cteLevel{
		name:       "system",
		configPath: systemConfigPath,
		install:    func(t *testing.T) { installSystemService(t) },
		mainPID:    cteSystemMainPID,
	})
}

// TestREQBrokerConfig040_UserInstallWritesTemplate installs the user
// service as the peer account and checks the configuration file the
// installation generates in the account's home.
func TestREQBrokerConfig040_UserInstallWritesTemplate(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	peerUID := syscontainer.AccountUID(t, "peer")
	uid := strconv.FormatUint(uint64(peerUID), 10)
	userEnv := []string{
		"XDG_RUNTIME_DIR=/run/user/" + uid,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + uid + "/bus",
	}
	configPath := cteUserConfigPath(t)

	if _, err := os.Stat(configPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s exists before the installation: %v", configPath, err)
	}

	mustRun(t, "root", nil, "loginctl", "enable-linger", "peer")
	mustRun(t, "root", nil, "systemctl", "start", "user@"+uid+".service")
	waitUserManager(t, peerUID)

	install := func(t *testing.T) {
		res := runWithEnv(t, "peer", userEnv, releaseBinary, "service", "install")
		if res.ExitCode != 0 {
			t.Fatalf("peers service install as peer: exit %d\nstdout: %s\nstderr: %s",
				res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
	}
	install(t)

	socket := syscontainer.UserSocketPath(t, "peer")
	syscontainer.WaitFor(t, "the peer Broker socket "+socket, func() bool {
		_, err := os.Stat(socket)
		return err == nil
	})

	session := syscontainer.NewSession(t, "peer", "config-template-user")
	cteWaitConnected(t, session)

	cteRunChecks(t, session, cteLevel{
		name:       "user",
		configPath: configPath,
		install:    install,
		mainPID:    func(t *testing.T) int { return cteUserMainPID(t, userEnv) },
	})
}

func cteRunChecks(t *testing.T, session *syscontainer.Session, lvl cteLevel) {
	t.Helper()

	content := readFile(t, lvl.configPath)
	t.Logf("the %s configuration generated at %s:\n%s", lvl.name, lvl.configPath, content)

	cteAssertTemplate(t, content, lvl.name)
	cteAssertValuesMatchFile(t, session, content, lvl.name)
	cteExerciseHotReload(t, session, lvl)
	cteExerciseSetConfig(t, session, lvl)
	cteExerciseReinstall(t, lvl)
}

func cteAssertTemplate(t *testing.T, content, level string) {
	t.Helper()

	values, counts := cteParseDocument(t, content)
	wanted := make(map[string]bool, len(cteKeys))
	for _, key := range cteKeys {
		wanted[key.name] = true
	}
	failed := false
	// These checks fail on a key that the template gained, so a new
	// configuration key cannot slip through this test unnoticed.
	for name := range counts {
		if !wanted[name] {
			t.Errorf("%s configuration: the file holds the effective key %s, which cteKeys does not list", level, name)
			failed = true
		}
	}
	for name := range cteCommentedKeysInFile(content) {
		if !wanted[name] {
			t.Errorf("%s configuration: the file holds the commented key %s, which cteKeys does not list", level, name)
			failed = true
		}
	}
	for _, key := range cteKeys {
		activeCount := counts[key.name]
		commentCount := cteCountCommentedLines(content, key.name)
		if total := activeCount + commentCount; total != 1 {
			t.Errorf("%s configuration: %s appears %d times (%d active, %d commented), want exactly once",
				level, key.name, total, activeCount, commentCount)
			failed = true
			continue
		}
		if wantCommented := cteCommentedKeys[key.name]; (commentCount == 1) != wantCommented {
			t.Errorf("%s configuration: %s is %s, want it %s",
				level, key.name,
				cteDescribeForm(commentCount == 1),
				cteDescribeForm(wantCommented))
			failed = true
		}
		if activeCount == 1 {
			if _, ok := values[key.name]; !ok {
				t.Errorf("%s configuration: %s has no readable value", level, key.name)
				failed = true
			}
		}
	}
	if failed {
		t.FailNow()
	}
}

func cteDescribeForm(commented bool) string {
	if commented {
		return "a commented placeholder"
	}
	return "an effective value"
}

func cteAssertValuesMatchFile(t *testing.T, session *syscontainer.Session, content, level string) {
	t.Helper()

	values, _ := cteParseDocument(t, content)
	reported := cteGetConfigAll(t, session)
	wanted := make(map[string]bool, len(cteKeys))
	for _, key := range cteKeys {
		wanted[key.name] = true
	}
	failed := false
	// get_config must report exactly the key set; the loop below only looks up
	// the expected keys.
	for name := range reported {
		if !wanted[name] {
			t.Errorf("get_config on the %s Broker returned %s, which cteKeys does not list", level, name)
			failed = true
		}
	}
	for name := range wanted {
		if _, ok := reported[name]; !ok {
			t.Errorf("get_config on the %s Broker returned no %s entry", level, name)
			failed = true
		}
	}
	for _, key := range cteKeys {
		if cteCommentedKeys[key.name] {
			continue
		}
		fileValue, ok := values[key.name]
		if !ok {
			t.Errorf("%s configuration: %s has no effective value", level, key.name)
			failed = true
			continue
		}
		configValue, ok := reported[key.name]
		if !ok {
			t.Errorf("get_config on the %s Broker returned no %s entry", level, key.name)
			failed = true
			continue
		}
		if !cteSameValue(key.kind, fileValue, configValue) {
			t.Errorf("%s configuration: %s is written as %q but get_config reports %q",
				level, key.name, fileValue, configValue)
			failed = true
		}
	}
	if failed {
		t.FailNow()
	}
}

func cteExerciseHotReload(t *testing.T, session *syscontainer.Session, lvl cteLevel) {
	t.Helper()

	before := readFile(t, lvl.configPath)
	index, indent := cteFindCommentedLine(t, before, "broker.alias")
	pidBefore := lvl.mainPID(t)

	lines := strings.Split(before, "\n")
	lines[index] = indent + "alias: " + cteHotReloadAlias
	after := strings.Join(lines, "\n")
	cteWriteFile(t, lvl.configPath, after)

	if got := cteCountCommentedLines(after, "broker.alias"); got != 0 {
		t.Fatalf("the manual edit left %d commented broker.alias lines", got)
	}
	values, counts := cteParseDocument(t, after)
	if counts["broker.alias"] != 1 || values["broker.alias"] != cteHotReloadAlias {
		t.Fatalf("after uncommenting broker.alias: %d effective lines, value %q; want one line with %q",
			counts["broker.alias"], values["broker.alias"], cteHotReloadAlias)
	}

	harness.EventuallyPoll(t, cteReloadBound, "broker.alias to take the hand-written value "+cteHotReloadAlias, func(deadline time.Time) (bool, string) {
		text, isError, err := session.PollCall(t, deadline, "get_config", map[string]any{"key": "broker.alias"})
		if err != nil {
			return false, err.Error()
		}
		if isError {
			return false, text
		}
		got, err := cteDecodeOne(t, text)
		if err != nil {
			return false, err.Error()
		}
		return got == cteHotReloadAlias, fmt.Sprintf("get_config broker.alias = %q", got)
	})

	if pidAfter := lvl.mainPID(t); pidAfter != pidBefore {
		t.Errorf("the %s Broker restarted while the configuration was reloaded: MainPID %d -> %d",
			lvl.name, pidBefore, pidAfter)
	}
}

func cteExerciseSetConfig(t *testing.T, session *syscontainer.Session, lvl cteLevel) {
	t.Helper()

	before := readFile(t, lvl.configPath)
	index, indent := cteFindCommentedLine(t, before, "broker.advertise_extra")

	text, isError := session.Call(t, "set_config", map[string]any{
		"key":   "broker.advertise_extra",
		"value": cteAdvertiseExtraValue,
	})
	if isError {
		t.Fatalf("set_config broker.advertise_extra: %s", text)
	}

	var after string
	harness.Eventually(t, cteWaitBound, "the file to carry the effective broker.advertise_extra line", func() (bool, string) {
		data, err := os.ReadFile(lvl.configPath)
		if err != nil {
			return false, err.Error()
		}
		lines := strings.Split(string(data), "\n")
		if index >= len(lines) {
			return false, fmt.Sprintf("the file has %d lines, want a line at index %d", len(lines), index)
		}
		line := strings.TrimSpace(lines[index])
		if strings.HasPrefix(line, "#") {
			return false, "the placeholder is still commented: " + line
		}
		if !strings.HasPrefix(line, "advertise_extra:") {
			return false, "the replaced line is " + line
		}
		after = string(data)
		return true, ""
	})

	beforeLines := strings.Split(before, "\n")
	afterLines := strings.Split(after, "\n")
	if len(beforeLines) != len(afterLines) {
		t.Fatalf("set_config changed the line count from %d to %d", len(beforeLines), len(afterLines))
	}
	for i := range beforeLines {
		if i == index {
			continue
		}
		if beforeLines[i] != afterLines[i] {
			t.Errorf("set_config changed line %d from %q to %q", i+1, beforeLines[i], afterLines[i])
		}
	}
	if !strings.HasPrefix(afterLines[index], indent+"advertise_extra:") {
		t.Errorf("the replaced line %q does not keep the placeholder's indentation %q and key",
			afterLines[index], indent)
	}
	if got := cteCountCommentedLines(after, "broker.advertise_extra"); got != 0 {
		t.Errorf("the file keeps %d commented broker.advertise_extra lines after set_config", got)
	}
	values, counts := cteParseDocument(t, after)
	if counts["broker.advertise_extra"] != 1 {
		t.Errorf("the file has %d effective broker.advertise_extra lines after set_config, want one",
			counts["broker.advertise_extra"])
	}
	if values["broker.advertise_extra"] != cteAdvertiseExtraValue {
		t.Errorf("the file writes broker.advertise_extra as %q, want %q",
			values["broker.advertise_extra"], cteAdvertiseExtraValue)
	}
	if got := cteGetConfigOne(t, session, "broker.advertise_extra"); got != cteAdvertiseExtraValue {
		t.Errorf("get_config broker.advertise_extra = %q, want %q", got, cteAdvertiseExtraValue)
	}
}

func cteExerciseReinstall(t *testing.T, lvl cteLevel) {
	t.Helper()

	before := readFile(t, lvl.configPath)
	lvl.install(t)
	after := readFile(t, lvl.configPath)
	if after != before {
		t.Errorf("installing the %s service again changed the existing configuration\nbefore:\n%s\nafter:\n%s",
			lvl.name, before, after)
	}
}

func cteParseDocument(t *testing.T, content string) (map[string]string, map[string]int) {
	t.Helper()

	values := make(map[string]string)
	counts := make(map[string]int)

	var document yaml.Node
	if err := yaml.Unmarshal([]byte(content), &document); err != nil {
		t.Fatalf("parsing the configuration file: %v\n%s", err, content)
	}
	if len(document.Content) == 0 {
		return values, counts
	}
	root := document.Content[0]
	if root.Kind == yaml.ScalarNode && root.Tag == "!!null" {
		return values, counts
	}
	if root.Kind != yaml.MappingNode {
		t.Fatalf("the configuration file is not a YAML mapping: %s", content)
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		section := root.Content[i]
		body := root.Content[i+1]
		if section.Kind != yaml.ScalarNode || body.Kind != yaml.MappingNode {
			t.Fatalf("the configuration file has an unexpected shape under %q", section.Value)
		}
		for j := 0; j+1 < len(body.Content); j += 2 {
			leaf := body.Content[j]
			key := section.Value + "." + leaf.Value
			counts[key]++
			values[key] = cteNodeText(body.Content[j+1])
		}
	}
	return values, counts
}

func cteNodeText(node *yaml.Node) string {
	if node.Kind != yaml.SequenceNode {
		return node.Value
	}
	parts := make([]string, 0, len(node.Content))
	for _, item := range node.Content {
		parts = append(parts, item.Value)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// cteCommentedKeysInFile returns every key the file holds as a commented
// placeholder, keyed as section.leaf. Only a leaf made of lowercase letters,
// digits and underscores counts, so a description comment is not read as a key.
func cteCommentedKeysInFile(content string) map[string]bool {
	keys := make(map[string]bool)
	current := ""
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !cteIndented(line) {
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if colon := strings.IndexByte(trimmed, ':'); colon >= 0 {
				current = trimmed[:colon]
			}
			continue
		}
		if current == "" || !strings.HasPrefix(trimmed, "#") {
			continue
		}
		body := strings.TrimSpace(strings.TrimPrefix(trimmed, "#"))
		colon := strings.IndexByte(body, ':')
		if colon <= 0 {
			continue
		}
		leaf := body[:colon]
		if !cteIsKeyLeaf(leaf) {
			continue
		}
		keys[current+"."+leaf] = true
	}
	return keys
}

func cteIsKeyLeaf(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

func cteCountCommentedLines(content, key string) int {
	section, leaf := cteSplitKey(key)
	current := ""
	count := 0
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !cteIndented(line) {
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if colon := strings.IndexByte(trimmed, ':'); colon >= 0 {
				current = trimmed[:colon]
			}
			continue
		}
		if current != section || !strings.HasPrefix(trimmed, "#") {
			continue
		}
		body := strings.TrimSpace(strings.TrimPrefix(trimmed, "#"))
		if strings.HasPrefix(body, leaf+":") {
			count++
		}
	}
	return count
}

func cteFindCommentedLine(t *testing.T, content, key string) (int, string) {
	t.Helper()

	section, leaf := cteSplitKey(key)
	current := ""
	for index, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !cteIndented(line) {
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if colon := strings.IndexByte(trimmed, ':'); colon >= 0 {
				current = trimmed[:colon]
			}
			continue
		}
		if current != section || !strings.HasPrefix(trimmed, "#") {
			continue
		}
		body := strings.TrimSpace(strings.TrimPrefix(trimmed, "#"))
		if strings.HasPrefix(body, leaf+":") {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			return index, indent
		}
	}
	t.Fatalf("the configuration has no commented %s placeholder:\n%s", key, content)
	return 0, ""
}

func cteIndented(line string) bool {
	return strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t")
}

func cteSplitKey(key string) (section, leaf string) {
	if dot := strings.IndexByte(key, '.'); dot >= 0 {
		return key[:dot], key[dot+1:]
	}
	return "", key
}

// cteSameValue compares a value written to the file with the value get_config
// rendered. Durations and integers are compared by value so a different but
// equivalent spelling is not a mismatch.
func cteSameValue(kind, fileValue, configValue string) bool {
	switch kind {
	case "bool":
		return strings.EqualFold(strings.TrimSpace(fileValue), strings.TrimSpace(configValue))
	case "int":
		fileInt, fileErr := strconv.ParseInt(strings.TrimSpace(fileValue), 10, 64)
		configInt, configErr := strconv.ParseInt(strings.TrimSpace(configValue), 10, 64)
		return fileErr == nil && configErr == nil && fileInt == configInt
	case "duration":
		fileDuration, fileErr := time.ParseDuration(strings.TrimSpace(fileValue))
		configDuration, configErr := time.ParseDuration(strings.TrimSpace(configValue))
		return fileErr == nil && configErr == nil && fileDuration == configDuration
	default:
		return fileValue == configValue
	}
}

func cteWriteFile(t *testing.T, path, content string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), info.Mode().Perm()); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

func cteGetConfigAll(t *testing.T, session *syscontainer.Session) map[string]string {
	t.Helper()

	text, isError := session.Call(t, "get_config", map[string]any{})
	if isError {
		t.Fatalf("get_config: %s", text)
	}
	var out struct {
		Entries []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decoding get_config result %q: %v", text, err)
	}
	values := make(map[string]string, len(out.Entries))
	for _, entry := range out.Entries {
		values[entry.Key] = entry.Value
	}
	return values
}

func cteGetConfigOne(t *testing.T, session *syscontainer.Session, key string) string {
	t.Helper()

	text, isError := session.Call(t, "get_config", map[string]any{"key": key})
	if isError {
		t.Fatalf("get_config %s: %s", key, text)
	}
	value, err := cteDecodeOne(t, text)
	if err != nil {
		t.Fatalf("decoding get_config %s result %q: %v", key, text, err)
	}
	return value
}

func cteDecodeOne(t *testing.T, text string) (string, error) {
	t.Helper()

	var out struct {
		Entries []struct {
			Value string `json:"value"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		return "", err
	}
	if len(out.Entries) != 1 {
		return "", fmt.Errorf("the answer holds %d entries, want one", len(out.Entries))
	}
	return out.Entries[0].Value, nil
}

func cteWaitConnected(t *testing.T, session *syscontainer.Session) {
	t.Helper()

	harness.EventuallyPoll(t, cteWaitBound, "the session of "+session.Account+" to reach the Broker", func(deadline time.Time) (bool, string) {
		text, isError, err := session.PollCall(t, deadline, "get_config", map[string]any{"key": "broker.listen"})
		if err != nil {
			return false, err.Error()
		}
		return !isError, text
	})
}

func cteSystemMainPID(t *testing.T) int {
	t.Helper()

	res := runCommandAs(t, "root", "systemctl", "show", "--property", "MainPID", "--value", "peers.service")
	if res.ExitCode != 0 {
		t.Fatalf("systemctl show MainPID: exit %d\nstderr: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	return cteParsePID(t, res.Stdout)
}

func cteUserMainPID(t *testing.T, userEnv []string) int {
	t.Helper()

	res := runWithEnv(t, "peer", userEnv, "systemctl", "--user", "show", "--property", "MainPID", "--value", "peers.service")
	if res.ExitCode != 0 {
		t.Fatalf("systemctl --user show MainPID: exit %d\nstderr: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	return cteParsePID(t, res.Stdout)
}

func cteParsePID(t *testing.T, text string) int {
	t.Helper()

	pid, err := strconv.Atoi(strings.TrimSpace(text))
	if err != nil || pid <= 0 {
		t.Fatalf("MainPID %q is not a running process id: %v", strings.TrimSpace(text), err)
	}
	return pid
}

func cteUserConfigPath(t *testing.T) string {
	t.Helper()

	u, err := user.Lookup("peer")
	if err != nil {
		t.Fatalf("looking up account peer: %v", err)
	}
	return filepath.Join(u.HomeDir, ".peers", "config.yaml")
}
