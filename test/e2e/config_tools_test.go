//go:build e2e

package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
)

// The bounds below follow the project rule: a wait that no product timer
// governs is the three-second test allowance; a wait that a timer governs is
// that timer plus the allowance.
const (
	// ctWaitBound bounds a wait that no product timer governs.
	ctWaitBound = harness.EventTimeout

	// ctRelinkBound is reconnect_max + dial_timeout plus the harness.EventTimeout
	// margin.
	ctRelinkBound = harness.FastReconnectMax + harness.FastDialTimeout + harness.EventTimeout

	// ctReloadBound is the configuration file watcher's debounce (100ms) plus
	// the harness.EventTimeout margin.
	ctReloadBound = 100*time.Millisecond + harness.EventTimeout
)

// ctStaticKey names two legal static keys. The shortest legal length is 16
// characters.
const (
	ctStaticKeyOne = "static-key-00001"
	ctStaticKeyTwo = "static-key-00002"
	ctStaticKeyThr = "static-key-00003"
)

// TestConfigToolAnnotations covers REQ-broker-config-004. A real peers
// MCP session lists its tools and the read-only hint must be set on exactly the
// tools with no side effects. A tool that sends a message, changes Broker
// state, or writes the configuration file must not carry the hint. A missing
// annotations block, or a readOnlyHint of false, both count as not read-only.
func TestConfigToolAnnotations(t *testing.T) {
	host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-annotations"})
	s := host.StartSession(t, harness.SessionSpec{Name: "annotations", StartMCP: true})
	defer s.End(t)

	expected := map[string]bool{
		"list_agents":        true,
		"send_message":       false,
		"list_topics":        true,
		"join_topic":         false,
		"leave_topic":        false,
		"list_brokers":       true,
		"connect_broker":     false,
		"disconnect_broker":  false,
		"create_pairing_key": false,
		"get_config":         true,
		"set_config":         false,
		"set_broker_online":  false,
		"update_broker":      false,
	}

	tools := s.Tools(t)
	if len(tools) != len(expected) {
		t.Fatalf("the MCP server exposes %d tools %v, want %d", len(tools), toolNames(tools), len(expected))
	}
	seen := make(map[string]bool, len(tools))
	for _, tool := range tools {
		// A tool without an annotations block is not read-only; the log records
		// which shape the real session returned.
		shape := "no annotations block"
		if tool.Annotations != nil {
			shape = fmt.Sprintf("readOnlyHint = %v", tool.Annotations.ReadOnlyHint)
		}
		t.Logf("tools/list: %s: %s", tool.Name, shape)

		wantReadOnly, known := expected[tool.Name]
		if !known {
			t.Errorf("unexpected tool %q", tool.Name)
			continue
		}
		seen[tool.Name] = true
		if got := tool.ReadOnly(); got != wantReadOnly {
			t.Errorf("%s: %s, want readOnlyHint %v", tool.Name, shape, wantReadOnly)
		}
	}
	for name := range expected {
		if !seen[name] {
			t.Errorf("the MCP server does not expose %q", name)
		}
	}
}

// toolNames returns the names of tools in listing order.
func toolNames(tools []node.ToolInfo) []string {
	names := make([]string, len(tools))
	for i, tool := range tools {
		names[i] = tool.Name
	}
	return names
}

// TestConfigTools covers the single-host configuration requirements: reading
// the running configuration, changing it through the tools, the audit records
// a change produces, hot reload of the configuration file, and the values that
// the command line or the startup checks refuse.
func TestConfigTools(t *testing.T) {
	t.Run("ReadReflectsBehavior", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-read"})
		listen := host.FreeAddr(t)
		start := ctStartBroker(t, host, harness.BrokerSpec{
			ConfigYAML: ctConfig("broker:\n  listen: " + listen + "\n"),
		})
		listen = start.Address(listen)

		s := host.StartSession(t, harness.SessionSpec{
			Name:     "reader",
			StartMCP: true,
			Workdir:  topicsOrigin(t, "https://github.com/org/repo"),
		})
		defer s.End(t)
		out := registrationWaitRegistered(t, s, "reader")
		alias := registrationAlias(t, out.Self)

		if got := ctGetConfig(t, host, "broker.alias"); got != alias {
			t.Fatalf("get_config broker.alias = %q, want the address alias %q", got, alias)
		}
		if got := ctGetConfig(t, host, "broker.listen"); got != listen {
			t.Fatalf("get_config broker.listen = %q, want %q", got, listen)
		}
		ctMustDial(t, listen, "the configured listen address")
		if got := ctGetConfig(t, host, "topics.auto_join"); got != "true" {
			t.Fatalf("get_config topics.auto_join = %q, want true", got)
		}
		harness.EventuallyPoll(t, ctWaitBound, "the default topic is joined", func(deadline time.Time) (bool, string) {
			topics, state := topicsListPoll(t, deadline, s)
			if state != "" {
				return false, state
			}
			for _, topic := range topics.Joined {
				if topic == "github.com/org/repo" {
					return true, ""
				}
			}
			return false, fmt.Sprintf("joined topics are %v", topics.Joined)
		})
	})

	t.Run("SetAliasIsAudited", func(t *testing.T) {
		b := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-alias-b"})
		a := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-alias-a"})
		addrB := b.FreeAddr(t)
		startB := ctStartBroker(t, b, harness.BrokerSpec{
			ConfigYAML: ctConfig("broker:\n  alias: config-alias-b\n  listen: " + addrB + "\n"),
		})
		addrB = startB.Address(addrB)
		ctStartBroker(t, a, harness.BrokerSpec{
			ConfigYAML: ctConfig("broker:\n  alias: config-alias-a\n  listen: " + a.FreeAddr(t) + "\n"),
		})

		opB := ctOperator(t, b)
		defer opB.End(t)
		opA := ctOperator(t, a)
		defer opA.End(t)

		key := alCreateKey(t, opB)
		alConnect(t, opA, addrB, key.Key).MustOK(t)
		aliasB := ctGetConfig(t, b, "broker.alias")
		aliasA := ctGetConfig(t, a, "broker.alias")
		alWaitConnected(t, a, aliasB, ctRelinkBound)
		alWaitConnected(t, b, aliasA, ctRelinkBound)

		b1 := ctSession(t, b, "b1")
		defer b1.End(t)
		a1 := ctSession(t, a, "a1")
		defer a1.End(t)
		topicsJoin(t, b1, "t")
		topicsJoin(t, a1, "t")
		selfB := alSelfAddress(t, b1, "b1")
		alWaitAgentSuffix(t, a1, "b1", "@"+aliasB, ctRelinkBound)

		beforeCall := time.Now()
		out := ctDecodeSetConfig(t, ctSetConfig(t, b1, "broker.alias", "build-box"))
		afterCall := time.Now()
		since := beforeCall
		if out.Key != "broker.alias" || out.Old != aliasB || out.New != "build-box" {
			t.Fatalf("set_config result = %+v, want old %q and new build-box", out, aliasB)
		}
		ctWaitConfigValue(t, b, "broker.alias", "build-box", ctWaitBound)

		events := ctAudit(t, b, since, 0)
		if len(events) == 0 {
			t.Fatal("the alias change wrote no audit record")
		}
		last := events[len(events)-1]
		at := last.GetTime().AsTime()
		if at.Before(beforeCall) || at.After(afterCall) {
			t.Fatalf("the audit record's time %v is outside the call window [%v, %v]", at, beforeCall, afterCall)
		}
		if last.GetEvent() != localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_SET {
			t.Fatalf("the latest audit record is %s, want config_set", ctAuditKindName(last.GetEvent()))
		}
		if last.GetKey() != "broker.alias" {
			t.Fatalf("the audit record names key %q, want broker.alias", last.GetKey())
		}
		if got := ctRenderValue(last.GetOldValue()); got != aliasB {
			t.Fatalf("the audit record's old value is %q, want %q", got, aliasB)
		}
		if got := ctRenderValue(last.GetNewValue()); got != "build-box" {
			t.Fatalf("the audit record's new value is %q, want build-box", got)
		}
		if last.GetActor() != selfB {
			t.Fatalf("the audit record's actor is %q, want the agent address %q", last.GetActor(), selfB)
		}
		if got, want := last.GetAccount(), ctAccountName(); got != want {
			t.Fatalf("the audit record's account is %q, want %q", got, want)
		}

		// The peer sees the new alias on the running agent.
		alWaitAgentSuffix(t, a1, "b1", "@build-box", ctWaitBound)
	})

	t.Run("AliasTooLong", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-alias-long"})
		listen := host.FreeAddr(t)
		ctStartBroker(t, host, harness.BrokerSpec{
			ConfigYAML: ctConfig("broker:\n  listen: " + listen + "\n"),
		})
		op := ctOperator(t, host)
		defer op.End(t)

		original := ctGetConfig(t, host, "broker.alias")
		tooLong := strings.Repeat("a", 25)
		r := ctSetConfig(t, op, "broker.alias", tooLong)
		r.MustCode(t, "invalid_argument")
		if !strings.Contains(r.Text, "longer than 24 characters") {
			t.Fatalf("the refusal %q does not say the alias is longer than 24 characters", r.Text)
		}
		if got := ctGetConfig(t, host, "broker.alias"); got != original {
			t.Fatalf("the rejected alias changed the value to %q, want %q", got, original)
		}

		legal := strings.Repeat("b", 24)
		out := ctDecodeSetConfig(t, ctSetConfig(t, op, "broker.alias", legal))
		if out.New != legal {
			t.Fatalf("set_config reports new value %q, want %q", out.New, legal)
		}
		if got := ctGetConfig(t, host, "broker.alias"); got != legal {
			t.Fatalf("get_config broker.alias = %q, want %q", got, legal)
		}
	})

	t.Run("TerminalAuditCommand", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-audit-cmd"})
		listen := host.FreeAddr(t)
		ctStartBroker(t, host, harness.BrokerSpec{
			ConfigYAML: ctConfig("broker:\n  listen: " + listen + "\n"),
		})
		op := ctOperator(t, host)
		defer op.End(t)

		// Two different event kinds, so the record-for-record comparison below
		// distinguishes the events instead of matching one name twice.
		alCreateKey(t, op)
		ctDecodeSetConfig(t, ctSetConfig(t, op, "broker.alias", "audit-box"))

		events := ctAudit(t, host, time.Time{}, 0)
		if len(events) < 2 {
			t.Fatalf("the audit log has %d records, want at least the two just written", len(events))
		}
		res := host.Exec(t, []string{ctBinary(t), "service", "audit"}, nil)
		if res.ExitCode != 0 {
			t.Fatalf("peers service audit exited %d: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
		}
		lines := ctNonEmptyLines(res.Stdout)
		if len(lines) != len(events) {
			t.Fatalf("peers service audit printed %d lines for %d audit records:\n%s", len(lines), len(events), res.Stdout)
		}
		for i, event := range events {
			name := ctAuditKindName(event.GetEvent())
			if !strings.Contains(lines[i], name) {
				t.Fatalf("audit line %d is %q, want it to name %s", i+1, lines[i], name)
			}
		}

		newest := ctAudit(t, host, time.Time{}, 1)
		if len(newest) != 1 {
			t.Fatalf("the limited audit read returned %d records, want 1", len(newest))
		}
		res = host.Exec(t, []string{ctBinary(t), "service", "audit", "--limit", "1"}, nil)
		if res.ExitCode != 0 {
			t.Fatalf("peers service audit --limit 1 exited %d: %s", res.ExitCode, strings.TrimSpace(res.Stderr))
		}
		limited := ctNonEmptyLines(res.Stdout)
		if len(limited) != 1 {
			t.Fatalf("peers service audit --limit 1 printed %d lines:\n%s", len(limited), res.Stdout)
		}
		if name := ctAuditKindName(newest[0].GetEvent()); !strings.Contains(limited[0], name) {
			t.Fatalf("the limited line is %q, want the newest record %s", limited[0], name)
		}
	})

	t.Run("FileEditHotReload", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-reload"})
		listen := host.FreeAddr(t)
		start := ctStartBroker(t, host, harness.BrokerSpec{
			ConfigYAML: ctConfig("broker:\n  listen: " + listen + "\n"),
		})
		listen = start.Address(listen)

		aliasBefore := ctGetConfig(t, host, "broker.alias")
		failuresBefore := ctGetConfig(t, host, "pairing.max_failures")
		since := time.Now()
		host.WriteConfig(t, ctConfig(
			"broker:\n  listen: "+listen+"\n  alias: reloaded\n",
			"pairing:\n  max_failures: -1\n",
		))
		ctWaitConfigValue(t, host, "broker.alias", "reloaded", ctReloadBound)
		if got := ctGetConfig(t, host, "pairing.max_failures"); got != failuresBefore {
			t.Fatalf("an invalid pairing.max_failures changed the value to %q, want %q", got, failuresBefore)
		}

		events := ctAudit(t, host, since, 0)
		var changed []*localpb.AuditEvent
		for _, event := range events {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_FILE_CHANGED {
				changed = append(changed, event)
			}
			if event.GetKey() == "pairing.max_failures" {
				t.Fatalf("the rejected pairing.max_failures was audited: %s", event.GetDetail())
			}
		}
		if len(changed) != 1 {
			t.Fatalf("the reload audited %d configuration file changes, want only the alias", len(changed))
		}
		if changed[0].GetKey() != "broker.alias" {
			t.Fatalf("the reload audited key %q, want broker.alias", changed[0].GetKey())
		}
		if got := ctRenderValue(changed[0].GetOldValue()); got != aliasBefore {
			t.Fatalf("the audited old alias is %q, want %q", got, aliasBefore)
		}
		if got := ctRenderValue(changed[0].GetNewValue()); got != "reloaded" {
			t.Fatalf("the audited new alias is %q, want reloaded", got)
		}
		if changed[0].GetActor() != "" || changed[0].GetAccount() != "" {
			t.Fatalf("a file change names actor %q and account %q, want both empty",
				changed[0].GetActor(), changed[0].GetAccount())
		}

		// The same edit under --listen leaves the listener alone: the file's
		// listen value is ignored and only the alias is audited.
		listenHost := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-reload-listen"})
		p1 := listenHost.FreeAddr(t)
		p2 := listenHost.FreeAddr(t)
		listenStart := ctStartBroker(t, listenHost, harness.BrokerSpec{
			Args:       []string{"--listen", p1},
			ConfigYAML: ctConfig("broker:\n  listen: " + p1 + "\n"),
		})
		p1 = listenStart.Address(p1)
		listenSince := time.Now()
		listenHost.WriteConfig(t, ctConfig("broker:\n  listen: "+p2+"\n  alias: after\n"))
		ctWaitConfigValue(t, listenHost, "broker.alias", "after", ctReloadBound)
		if got := ctGetConfig(t, listenHost, "broker.listen"); got != p1 {
			t.Fatalf("get_config broker.listen = %q, want the --listen value %q", got, p1)
		}
		ctMustDial(t, p1, "the listener given by --listen")

		var listenChanged []*localpb.AuditEvent
		for _, event := range ctAudit(t, listenHost, listenSince, 0) {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_FILE_CHANGED {
				listenChanged = append(listenChanged, event)
			}
		}
		if len(listenChanged) != 1 || listenChanged[0].GetKey() != "broker.alias" {
			keys := make([]string, len(listenChanged))
			for i, event := range listenChanged {
				keys[i] = event.GetKey()
			}
			t.Fatalf("the reload under --listen audited keys %v, want only broker.alias", keys)
		}
	})

	// REQ-broker-config-032: the configuration file path refuses the
	// listen/advertise combination the tools refuse, and keeps both values.
	t.Run("FileEditRejectsCombination", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-file-combination"})
		listen := host.FreeAddr(t)
		start := ctStartBroker(t, host, harness.BrokerSpec{
			ConfigYAML: ctConfig("broker:\n  listen: " + listen + "\n"),
		})
		listen = start.Address(listen)

		advertiseBefore := ctGetConfig(t, host, "broker.advertise")
		since := time.Now()
		host.WriteConfig(t, ctConfig(
			"broker:\n  listen: "+listen+"\n  advertise: \":18443\"\n  alias: combination\n",
		))
		ctWaitConfigValue(t, host, "broker.alias", "combination", ctReloadBound)
		if got := ctGetConfig(t, host, "broker.advertise"); got != advertiseBefore {
			t.Fatalf("the refused file change set broker.advertise to %q, want %q", got, advertiseBefore)
		}
		if got := ctGetConfig(t, host, "broker.listen"); got != listen {
			t.Fatalf("the refused file change moved broker.listen to %q, want %q", got, listen)
		}
		ctMustDial(t, listen, "the original listener after the refused file change")

		for _, event := range ctAudit(t, host, since, 0) {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_FILE_CHANGED &&
				event.GetKey() == "broker.advertise" {
				t.Fatalf("the refused broker.advertise was audited: %s", event.GetDetail())
			}
		}
	})

	// REQ-broker-config-032: with no listen address, an advertise address or
	// an extra address is refused through both the tools and the file.
	t.Run("EmptyListenRejectsAddresses", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-no-listen"})
		ctStartBroker(t, host, harness.BrokerSpec{
			ConfigYAML: ctConfig("broker:\n  listen: \"\"\n"),
		})
		op := ctOperator(t, host)
		defer op.End(t)

		beforeHash := ctConfigHash(t, host)
		ctSetConfig(t, op, "broker.advertise", ":18443").MustCode(t, "invalid_argument")
		if got := ctGetConfig(t, host, "broker.advertise"); got != "" {
			t.Fatalf("the refused advertise changed the value to %q, want empty", got)
		}
		ctSetConfig(t, op, "broker.advertise_extra", "[b.vpn.example]").MustCode(t, "invalid_argument")
		if got := ctGetConfig(t, host, "broker.advertise_extra"); got != "[]" {
			t.Fatalf("the refused extra address changed the value to %q, want []", got)
		}
		if afterHash := ctConfigHash(t, host); afterHash != beforeHash {
			t.Fatalf("a refused change rewrote the configuration file: %s -> %s", beforeHash, afterHash)
		}

		since := time.Now()
		host.WriteConfig(t, ctConfig(
			"broker:\n  listen: \"\"\n  advertise: \":18443\"\n  alias: no-listen\n",
		))
		ctWaitConfigValue(t, host, "broker.alias", "no-listen", ctReloadBound)
		if got := ctGetConfig(t, host, "broker.advertise"); got != "" {
			t.Fatalf("the refused file advertise changed the value to %q, want empty", got)
		}
		if got := ctGetConfig(t, host, "broker.listen"); got != "" {
			t.Fatalf("the refused file change moved broker.listen to %q, want empty", got)
		}

		host.WriteConfig(t, ctConfig(
			"broker:\n  listen: \"\"\n  advertise_extra: [b.vpn.example]\n  alias: no-listen-extra\n",
		))
		ctWaitConfigValue(t, host, "broker.alias", "no-listen-extra", ctReloadBound)
		if got := ctGetConfig(t, host, "broker.advertise_extra"); got != "[]" {
			t.Fatalf("the refused file extra address changed the value to %q, want []", got)
		}

		for _, event := range ctAudit(t, host, since, 0) {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_FILE_CHANGED &&
				(event.GetKey() == "broker.advertise" || event.GetKey() == "broker.advertise_extra") {
				t.Fatalf("the refused %s was audited: %s", event.GetKey(), event.GetDetail())
			}
		}
	})

	// REQ-broker-config-036: the configuration file path refuses an eighth
	// extra address and keeps the seven that were running.
	t.Run("FileEditRejectsAddressOverflow", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-file-overflow"})
		listen := host.FreeAddr(t)
		seven := []string{
			"a1.example", "a2.example", "a3.example", "a4.example",
			"a5.example", "a6.example", "a7.example",
		}
		start := ctStartBroker(t, host, harness.BrokerSpec{
			ConfigYAML: ctConfig(
				"broker:\n  listen: " + listen + "\n  advertise_extra: [" + strings.Join(seven, ", ") + "]\n",
			),
		})
		listen = start.Address(listen)

		extraBefore := ctGetConfig(t, host, "broker.advertise_extra")
		since := time.Now()
		eight := append(append([]string(nil), seven...), "a8.example")
		host.WriteConfig(t, ctConfig(
			"broker:\n  listen: "+listen+"\n  advertise_extra: ["+strings.Join(eight, ", ")+"]\n  alias: overflow\n",
		))
		ctWaitConfigValue(t, host, "broker.alias", "overflow", ctReloadBound)
		if got := ctGetConfig(t, host, "broker.advertise_extra"); got != extraBefore {
			t.Fatalf("the refused file change set broker.advertise_extra to %q, want %q", got, extraBefore)
		}
		ctMustDial(t, listen, "the original listener after the refused file change")

		for _, event := range ctAudit(t, host, since, 0) {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_FILE_CHANGED &&
				event.GetKey() == "broker.advertise_extra" {
				t.Fatalf("the refused broker.advertise_extra was audited: %s", event.GetDetail())
			}
		}
	})

	t.Run("ListenFlagBlocksSet", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-listen-flag"})
		p1 := host.FreeAddr(t)
		p2 := host.FreeAddr(t)
		start := ctStartBroker(t, host, harness.BrokerSpec{
			Args:       []string{"--listen", p1},
			ConfigYAML: ctConfig("broker:\n  listen: " + p1 + "\n"),
		})
		p1 = start.Address(p1)
		// A later start reads the configuration as written, so write the
		// address the Broker actually used back into it.
		config := ctConfig("broker:\n  listen: " + p1 + "\n")
		op := ctOperator(t, host)
		defer op.End(t)

		before := ctConfigHash(t, host)
		r := ctSetConfig(t, op, "broker.listen", p2)
		r.MustCode(t, "invalid_argument")
		if !strings.Contains(r.Text, "--listen") {
			t.Fatalf("the refusal %q does not name --listen", r.Text)
		}
		if got := ctGetConfig(t, host, "broker.listen"); got != p1 {
			t.Fatalf("get_config broker.listen = %q, want %q", got, p1)
		}
		if after := ctConfigHash(t, host); after != before {
			t.Fatalf("the rejected change rewrote the configuration file: %s -> %s", before, after)
		}
		ctMustDial(t, p1, "the listener given by --listen")

		// Without the flag the same change succeeds and the Broker rebinds.
		op.End(t)
		if code := host.StopBroker(t, syscall.SIGTERM); code != 0 {
			t.Fatalf("the Broker exited %d on SIGTERM, want 0", code)
		}
		ctStartBroker(t, host, harness.BrokerSpec{ConfigYAML: config})
		op = ctOperator(t, host)
		defer op.End(t)
		ctDecodeSetConfig(t, ctSetConfig(t, op, "broker.listen", p2))
		if got := ctGetConfig(t, host, "broker.listen"); got != p2 {
			t.Fatalf("get_config broker.listen = %q, want %q", got, p2)
		}
		harness.Eventually(t, ctWaitBound, "the Broker listens on the new address", func() (bool, string) {
			if ctDial(p2) {
				return true, ""
			}
			return false, "the new address does not accept connections yet"
		})
	})

	t.Run("UnbindableListen", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-unbindable"})
		p1 := host.FreeAddr(t)
		occupied, p2 := ctOccupyAddr(t, host)
		defer func() { _ = occupied.Close() }()

		start := ctStartBroker(t, host, harness.BrokerSpec{
			ConfigYAML: ctConfig("broker:\n  listen: " + p1 + "\n"),
		})
		// The retry may rewrite the FreeAddr address in the configuration
		// file. p2 is held by this test and never appears there, so the retry
		// never moves it.
		p1 = start.Address(p1)
		op := ctOperator(t, host)
		defer op.End(t)

		r := ctSetConfig(t, op, "broker.listen", p2)
		r.MustCode(t, "invalid_argument")
		if !strings.Contains(r.Text, "is unavailable") {
			t.Fatalf("the refusal %q does not say the address is unavailable", r.Text)
		}
		if got := ctGetConfig(t, host, "broker.listen"); got != p1 {
			t.Fatalf("get_config broker.listen = %q, want %q", got, p1)
		}
		ctMustDial(t, p1, "the original listener after the refused tool change")

		// The configuration file path rejects the same address.
		since := time.Now()
		host.WriteConfig(t, ctConfig("broker:\n  listen: "+p2+"\n  alias: moved\n"))
		ctWaitConfigValue(t, host, "broker.alias", "moved", ctReloadBound)
		if got := ctGetConfig(t, host, "broker.listen"); got != p1 {
			t.Fatalf("the file change moved the listener to %q, want %q", got, p1)
		}
		ctMustDial(t, p1, "the original listener after the refused file change")

		var changed []*localpb.AuditEvent
		for _, event := range ctAudit(t, host, since, 0) {
			if event.GetEvent() == localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_FILE_CHANGED {
				changed = append(changed, event)
			}
		}
		if len(changed) != 1 || changed[0].GetKey() != "broker.alias" {
			keys := make([]string, len(changed))
			for i, event := range changed {
				keys[i] = event.GetKey()
			}
			t.Fatalf("the refused file change audited keys %v, want only broker.alias", keys)
		}
	})

	t.Run("RuntimeAddressRulesThroughTools", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-address-rules"})
		listen := host.FreeAddr(t)
		ctStartBroker(t, host, harness.BrokerSpec{
			ConfigYAML: ctConfig("broker:\n  listen: " + listen + "\n"),
		})
		op := ctOperator(t, host)
		defer op.End(t)

		// A specific listener with a port-only advertise is an invalid
		// combination; the running Broker refuses it and keeps its value.
		advertiseBefore := ctGetConfig(t, host, "broker.advertise")
		ctSetConfig(t, op, "broker.advertise", ":18443").MustCode(t, "invalid_argument")
		if got := ctGetConfig(t, host, "broker.advertise"); got != advertiseBefore {
			t.Fatalf("the rejected advertise changed the value to %q, want %q", got, advertiseBefore)
		}

		// Seven extra addresses plus the listener fill the eight-address list;
		// an eighth extra address overflows it.
		extraHost := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-address-limit"})
		extraListen := extraHost.FreeAddr(t)
		seven := []string{
			"a1.example", "a2.example", "a3.example", "a4.example",
			"a5.example", "a6.example", "a7.example",
		}
		ctStartBroker(t, extraHost, harness.BrokerSpec{
			ConfigYAML: ctConfig(
				"broker:\n  listen: " + extraListen + "\n  advertise_extra: [" + strings.Join(seven, ", ") + "]\n",
			),
		})
		extraOp := ctOperator(t, extraHost)
		defer extraOp.End(t)

		extraBefore := ctGetConfig(t, extraHost, "broker.advertise_extra")
		eight := append(append([]string(nil), seven...), "a8.example")
		ctSetConfig(t, extraOp, "broker.advertise_extra", "["+strings.Join(eight, ", ")+"]").
			MustCode(t, "invalid_argument")
		if got := ctGetConfig(t, extraHost, "broker.advertise_extra"); got != extraBefore {
			t.Fatalf("the rejected extra list changed the value to %q, want %q", got, extraBefore)
		}
	})

	t.Run("StaticKeyThroughTools", func(t *testing.T) {
		host := harness.NewLocalHost(t, harness.LocalOptions{Name: "config-static-key"})
		listen := host.FreeAddr(t)
		ctStartBroker(t, host, harness.BrokerSpec{
			ConfigYAML: ctConfig(
				"broker:\n  listen: "+listen+"\n",
				fmt.Sprintf("pairing:\n  static_key: %q\n", ctStaticKeyOne),
			),
		})
		op := ctOperator(t, host)
		defer op.End(t)

		// Reading the configuration through the tool only says the key is set.
		read := op.Call(t, "get_config", map[string]any{"key": "pairing.static_key"})
		read.MustOK(t)
		readOut, err := harness.Decode[mcpserver.GetConfigOut](string(read.Structured))
		if err != nil {
			t.Fatalf("decoding get_config: %v", err)
		}
		if len(readOut.Entries) != 1 || readOut.Entries[0].Value != "(set)" {
			t.Fatalf("get_config pairing.static_key = %+v, want one (set) entry", readOut.Entries)
		}
		if strings.Contains(string(read.Structured), ctStaticKeyOne) {
			t.Fatalf("get_config returned the static key value: %s", read.Structured)
		}
		all := op.Call(t, "get_config", map[string]any{})
		all.MustOK(t)
		if strings.Contains(string(all.Structured), ctStaticKeyOne) {
			t.Fatalf("get_config returned the static key value in the full listing: %s", all.Structured)
		}

		// Setting, changing and deleting the key are all refused before the
		// file is touched.
		before := ctConfigHash(t, host)
		for _, value := range []string{ctStaticKeyTwo, ctStaticKeyThr, ""} {
			r := ctSetConfig(t, op, "pairing.static_key", value)
			r.MustCode(t, "invalid_argument")
			if !strings.Contains(r.Text, "can only be set in the configuration file") {
				t.Fatalf("the refusal %q does not say the key can only be set in the file", r.Text)
			}
			if value != "" && strings.Contains(r.Text, value) {
				t.Fatalf("the refusal %q contains the attempted value", r.Text)
			}
		}
		if after := ctConfigHash(t, host); after != before {
			t.Fatalf("a refused static key change rewrote the configuration file: %s -> %s", before, after)
		}
		if got := ctGetConfig(t, host, "pairing.static_key"); got != "(set)" {
			t.Fatalf("pairing.static_key reads %q after the refusals, want (set)", got)
		}
	})
}

// ctConfig merges the shortened timers with the given fragments.
func ctConfig(parts ...string) string {
	return harness.ConfigYAML(append([]string{harness.FastTimers}, parts...)...)
}

// ctBinary builds the tagged product binary once per test process.
func ctBinary(t *testing.T) string {
	t.Helper()
	return harness.BuildBinary(t, runtime.GOOS, runtime.GOARCH, "peers_testenv")
}

// ctStartBroker starts a Broker and fails when it does not come up. It returns
// the start result so callers can read the address the Broker actually used.
func ctStartBroker(t *testing.T, n *harness.Node, spec harness.BrokerSpec) harness.BrokerStart {
	t.Helper()
	start := n.StartBroker(t, spec)
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
	return start
}

// ctOccupyAddr binds an address from n.FreeAddr and returns the listener that
// holds it. The port can be taken between the FreeAddr probe and this bind, so
// the helper retries with a fresh address. The returned listener, not the
// harness retry, owns the port.
func ctOccupyAddr(t *testing.T, n *harness.Node) (net.Listener, string) {
	t.Helper()
	for attempt := 0; attempt < 10; attempt++ {
		addr := n.FreeAddr(t)
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln, addr
		}
	}
	t.Fatal("occupying a FreeAddr address failed 10 times")
	return nil, ""
}

// ctOperator starts an MCP server process without an inbox socket. It never
// registers as an agent but can call the configuration tools.
func ctOperator(t *testing.T, n *harness.Node) *harness.Session {
	t.Helper()
	s := n.StartSession(t, harness.SessionSpec{Name: "operator", StartMCP: true, NoSocketEnv: true})
	harness.WaitConnected(t, s, ctWaitBound)
	return s
}

// ctSession starts one registered session.
func ctSession(t *testing.T, n *harness.Node, name string) *harness.Session {
	t.Helper()
	s := n.StartSession(t, harness.SessionSpec{Name: name, StartMCP: true})
	registrationWaitRegistered(t, s, name)
	return s
}

// ctSetConfig calls set_config and returns the raw result.
func ctSetConfig(t *testing.T, s *harness.Session, key, value string) harness.Result {
	t.Helper()
	return s.Call(t, "set_config", map[string]any{"key": key, "value": value})
}

// ctDecodeSetConfig asserts the change succeeded and decodes its result.
func ctDecodeSetConfig(t *testing.T, r harness.Result) mcpserver.SetConfigOut {
	t.Helper()
	r.MustOK(t)
	out, err := harness.Decode[mcpserver.SetConfigOut](string(r.Structured))
	if err != nil {
		t.Fatalf("decoding set_config: %v", err)
	}
	return out
}

// ctGetConfig returns the rendered text of one configuration key.
func ctGetConfig(t *testing.T, n *harness.Node, key string) string {
	t.Helper()
	resp, detail := n.Oracle().GetConfig(t, key)
	if detail != nil {
		t.Fatalf("get_config %s: %s", key, detail.GetMessage())
	}
	entries := resp.GetEntries()
	if len(entries) != 1 {
		t.Fatalf("get_config %s returned %d entries, want 1", key, len(entries))
	}
	return ctRenderValue(entries[0].GetValue())
}

// ctWaitConfigValue waits until one configuration key renders as want.
func ctWaitConfigValue(t *testing.T, n *harness.Node, key, want string, bound time.Duration) {
	t.Helper()
	harness.Eventually(t, bound, "the value of "+key+" is "+want, func() (bool, string) {
		resp, detail := n.Oracle().GetConfig(t, key)
		if detail != nil {
			return false, detail.GetMessage()
		}
		entries := resp.GetEntries()
		if len(entries) != 1 {
			return false, fmt.Sprintf("%d entries", len(entries))
		}
		if got := ctRenderValue(entries[0].GetValue()); got != want {
			return false, "value is " + got
		}
		return true, ""
	})
}

// ctRenderValue renders a configuration value the way the tools write it.
func ctRenderValue(v *localpb.ConfigValue) string {
	if v == nil {
		return ""
	}
	switch x := v.GetValue().(type) {
	case *localpb.ConfigValue_StringValue:
		return x.StringValue
	case *localpb.ConfigValue_BoolValue:
		if x.BoolValue {
			return "true"
		}
		return "false"
	case *localpb.ConfigValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *localpb.ConfigValue_DurationValue:
		if x.DurationValue == nil {
			return ""
		}
		return x.DurationValue.AsDuration().String()
	case *localpb.ConfigValue_StringListValue:
		if x.StringListValue == nil {
			return "[]"
		}
		return "[" + strings.Join(x.StringListValue.GetValues(), ", ") + "]"
	case *localpb.ConfigValue_SecretIsSet:
		if x.SecretIsSet {
			return "(set)"
		}
		return "(not set)"
	default:
		return ""
	}
}

// ctAudit reads the audit stream to its end.
func ctAudit(t *testing.T, n *harness.Node, since time.Time, limit uint32) []*localpb.AuditEvent {
	t.Helper()
	events, detail := n.Oracle().Audit(t, since, limit)
	if detail != nil {
		t.Fatalf("audit: %s", detail.GetMessage())
	}
	return events
}

// ctAuditKindName renders an audit event kind the way the terminal does.
func ctAuditKindName(k localpb.AuditEventKind) string {
	if name, ok := localpb.AuditEventKind_name[int32(k)]; ok {
		return strings.ToLower(strings.TrimPrefix(name, "AUDIT_EVENT_KIND_"))
	}
	return strconv.Itoa(int(k))
}

// ctConfigHash returns the SHA-256 of the account's configuration file.
func ctConfigHash(t *testing.T, n *harness.Node) string {
	t.Helper()
	sum := sha256.Sum256(n.ReadConfig(t))
	return hex.EncodeToString(sum[:])
}

// ctDial reports whether addr accepts a TCP connection within the wait bound.
func ctDial(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, ctWaitBound)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// ctMustDial fails when addr does not accept a TCP connection.
func ctMustDial(t *testing.T, addr, what string) {
	t.Helper()
	if !ctDial(addr) {
		t.Fatalf("%s: dialing %s did not connect", what, addr)
	}
}

// ctAccountName returns the account name the Broker records for this process's
// user, matching broker.usernameFor.
func ctAccountName() string {
	id := strconv.FormatUint(uint64(os.Geteuid()), 10)
	if u, err := user.LookupId(id); err == nil && u.Username != "" {
		return u.Username
	}
	return id
}

// ctNonEmptyLines splits s into its non-blank lines.
func ctNonEmptyLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}
