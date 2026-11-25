package service

import (
	"fmt"
	"path/filepath"
	"strings"
	"unicode"
)

const (
	// Label is the launchd label shared by the per-user agent and the system
	// daemon.
	Label = "io.github.wangzw.peers"
	// UnitName is the systemd unit name shared by the user and system units.
	UnitName = "peers.service"
)

const systemdUserTemplate = `[Unit]
Description=peers Broker for this account
Documentation=https://github.com/wangzw/peers

[Service]
Type=simple
ExecStart=%s service run --user --allow-degraded
Restart=always
RestartSec=5
UMask=0077

[Install]
WantedBy=default.target
`

const systemdSystemTemplate = `[Unit]
Description=peers system Broker
Documentation=https://github.com/wangzw/peers
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
ExecStart=%s service run --system --allow-degraded
Restart=always
RestartSec=5
UMask=0022
NoNewPrivileges=yes
# Keep a runaway Broker or its helpers from exhausting the host's PIDs.
TasksMax=8192
# Keep connections and helper pipes from exhausting the host's descriptors.
LimitNOFILE=65536
# The Broker reaches every served account's session registry and inbox
# sockets through per-account helpers, so it must see the real /home, /tmp
# and /run. Do not add ProtectHome, PrivateTmp or ProtectSystem.

[Install]
WantedBy=multi-user.target
`

const launchAgentTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>io.github.wangzw.peers</string>
	<key>ProgramArguments</key>
	<array>
		<string>%s</string>
		<string>service</string>
		<string>run</string>
		<string>--user</string>
		<string>--allow-degraded</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<true/>
	<key>ProcessType</key>
	<string>Background</string>
	<key>Umask</key>
	<integer>63</integer>
	<key>StandardOutPath</key>
	<string>%s</string>
	<key>StandardErrorPath</key>
	<string>%s</string>
</dict>
</plist>
`

const launchDaemonTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>io.github.wangzw.peers</string>
	<key>ProgramArguments</key>
	<array>
		<string>%s</string>
		<string>service</string>
		<string>run</string>
		<string>--system</string>
		<string>--allow-degraded</string>
	</array>
	<key>UserName</key>
	<string>root</string>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<true/>
	<key>Umask</key>
	<integer>18</integer>
	<key>StandardOutPath</key>
	<string>/var/log/peers/broker.log</string>
	<key>StandardErrorPath</key>
	<string>/var/log/peers/broker.log</string>
</dict>
</plist>
`

// SystemdUserUnit returns the systemd user unit that starts the Broker when
// the account logs in.
func SystemdUserUnit(exe string) (string, error) {
	exec, err := systemdExecStart(exe)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(systemdUserTemplate, exec), nil
}

// SystemdSystemUnit returns the systemd system unit that starts the system
// Broker when the machine boots.
func SystemdSystemUnit(exe string) (string, error) {
	exec, err := systemdExecStart(exe)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(systemdSystemTemplate, exec), nil
}

// LaunchAgentPlist returns the launchd agent that starts the Broker when the
// account logs in. Standard output and standard error go to logPath.
func LaunchAgentPlist(exe, logPath string) (string, error) {
	if err := validatePath("executable", exe); err != nil {
		return "", err
	}
	if err := validatePath("log path", logPath); err != nil {
		return "", err
	}
	escapedExe := escapeXML(exe)
	escapedLog := escapeXML(logPath)
	return fmt.Sprintf(launchAgentTemplate, escapedExe, escapedLog, escapedLog), nil
}

// LaunchDaemonPlist returns the launchd daemon that starts the system Broker
// when the machine boots.
func LaunchDaemonPlist(exe string) (string, error) {
	if err := validatePath("executable", exe); err != nil {
		return "", err
	}
	return fmt.Sprintf(launchDaemonTemplate, escapeXML(exe)), nil
}

func validatePath(kind, path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("cannot write the %s %q: the path must be absolute", kind, path)
	}
	for _, r := range path {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("cannot write the %s: the path contains a control character", kind)
		}
	}
	return nil
}

// systemdExecStart renders path for an ExecStart= line, doubling percent signs
// and dollar signs and quoting a path that needs it.
func systemdExecStart(path string) (string, error) {
	if err := validatePath("executable", path); err != nil {
		return "", err
	}
	exec := strings.ReplaceAll(path, "%", "%%")
	exec = strings.ReplaceAll(exec, "$", "$$")
	if strings.ContainsAny(path, "\"'\\;") || strings.ContainsFunc(path, unicode.IsSpace) {
		exec = strings.ReplaceAll(exec, `\`, `\\`)
		exec = strings.ReplaceAll(exec, `"`, `\"`)
		return `"` + exec + `"`, nil
	}
	return exec, nil
}

// escapeXML escapes the five XML metacharacters in a single pass, so the
// ampersands it introduces are not escaped again.
func escapeXML(s string) string {
	return strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	).Replace(s)
}
