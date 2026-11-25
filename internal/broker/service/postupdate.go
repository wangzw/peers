package service

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/procsig"
)

const (
	// LaunchdHelperLogName is the file in the level's service log directory
	// that receives the detached helper's output.
	LaunchdHelperLogName = "post-update.log"

	launchdHelperWaitBound = 60 * time.Second

	processExitPollInterval = 100 * time.Millisecond
)

// ExecutableID identifies an executable by the device and inode of its file.
type ExecutableID struct {
	// Dev is the device number of the file.
	Dev uint64
	// Ino is the inode number of the file.
	Ino uint64
}

type savedUnit struct {
	exists  bool
	content []byte
	mode    fs.FileMode
}

// PostUpdate rewrites the service definition of opts' level in this binary's
// format without restarting the Broker; only the main unit file or plist is
// replaced, so systemd drop-ins and a plist's EnvironmentVariables survive.
// Linux reloads synchronously and macOS starts a detached helper. A failed
// reload restores the main unit file and reports the restore in the error.
func PostUpdate(opts Options) error {
	if err := checkPlatform(opts, "update the service definition"); err != nil {
		return err
	}

	path, err := unitPath(opts)
	if err != nil {
		return err
	}
	// Resolve the launchd domain before writing, so a missing domain fails
	// without leaving a half-applied definition behind.
	domain := ""
	if opts.GOOS == "darwin" {
		domain, err = launchdDomain(opts)
		if err != nil {
			return err
		}
	}

	saved, err := saveUnit(opts, path)
	if err != nil {
		return err
	}
	if _, err := writeUnit(opts); err != nil {
		return err
	}

	if opts.GOOS == "linux" {
		var res Result
		if err := runSystemctlCommand(opts, &res, "daemon-reload"); err != nil {
			return restoreAfterFailure(opts, path, saved, err)
		}
		return nil
	}
	if err := startLaunchdReload(opts, domain, path); err != nil {
		return restoreAfterFailure(opts, path, saved, err)
	}
	return nil
}

func restoreAfterFailure(opts Options, path string, saved savedUnit, cause error) error {
	if err := restoreUnit(opts, path, saved); err != nil {
		auditUnitRestoreFailure(opts, err)
		return fmt.Errorf("%w; %v", cause, err)
	}
	return fmt.Errorf("%w; the previous service definition was restored", cause)
}

// SavedUnit is the main unit file or plist as it was before a post-update run
// rewrote it. Content and Mode are meaningful only when Exists is true.
type SavedUnit struct {
	// Exists reports whether there was a file to save.
	Exists bool
	// Content holds the file's bytes.
	Content []byte
	// Mode is the file's permission bits.
	Mode fs.FileMode
}

// SaveMainUnit reads the main unit file or plist of opts' level before the
// post-update step rewrites it. A missing file is not an error: restoring it
// then removes the file the step wrote.
func SaveMainUnit(opts Options) (SavedUnit, error) {
	path, err := unitPath(opts)
	if err != nil {
		return SavedUnit{}, err
	}
	saved, err := saveUnit(opts, path)
	if err != nil {
		return SavedUnit{}, err
	}
	return SavedUnit{Exists: saved.exists, Content: saved.content, Mode: saved.mode}, nil
}

// RestoreMainUnit puts the main unit file or plist back as SaveMainUnit read
// it and, on Linux, reloads the unit database again. A file that did not exist
// before is removed.
func RestoreMainUnit(opts Options, saved SavedUnit) error {
	path, err := unitPath(opts)
	if err != nil {
		return err
	}
	return restoreUnit(opts, path, savedUnit{exists: saved.Exists, content: saved.Content, mode: saved.Mode})
}

func saveUnit(opts Options, path string) (savedUnit, error) {
	fsPath := rooted(opts, path)
	info, err := os.Stat(fsPath)
	if errors.Is(err, fs.ErrNotExist) {
		return savedUnit{}, nil
	}
	if err != nil {
		return savedUnit{}, fmt.Errorf("cannot inspect %s before rewriting it: %w", path, err)
	}
	content, err := os.ReadFile(fsPath)
	if err != nil {
		return savedUnit{}, fmt.Errorf("cannot read %s before rewriting it: %w", path, err)
	}
	return savedUnit{exists: true, content: content, mode: info.Mode().Perm()}, nil
}

func restoreUnit(opts Options, path string, saved savedUnit) error {
	fsPath := rooted(opts, path)
	if saved.exists {
		if err := writeFileAtomic(fsPath, string(saved.content), saved.mode); err != nil {
			return fmt.Errorf("the service definition could not be restored: %w", err)
		}
	} else if err := os.Remove(fsPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("the service definition could not be restored: %w", err)
	}
	if opts.GOOS == "linux" {
		var res Result
		if err := runSystemctlCommand(opts, &res, "daemon-reload"); err != nil {
			return fmt.Errorf("the service definition was restored but the service manager did not reload it: %w", err)
		}
	}
	return nil
}

func auditUnitRestoreFailure(opts Options, cause error) {
	_ = config.AppendAudit(rooted(opts, auditLogPath(opts)), config.Event{
		Event:  config.EventUpdateFailed,
		Detail: cause.Error(),
	})
}

func startLaunchdReload(opts Options, domain, plistPath string) error {
	callerPID := os.Getppid()
	if opts.PPID != nil {
		callerPID = opts.PPID()
	}
	exeID, err := executableID(rooted(opts, opts.Exe))
	if err != nil {
		return fmt.Errorf("cannot identify the binary that will reload the service: %w", err)
	}
	logPath, err := launchdHelperLogPath(opts)
	if err != nil {
		return err
	}
	if err := checkLogDir(opts, filepath.Dir(rooted(opts, logPath))); err != nil {
		return err
	}
	logFile, err := os.OpenFile(rooted(opts, logPath), os.O_CREATE|os.O_WRONLY|os.O_APPEND|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("cannot open the post-update log %s: %w", logPath, err)
	}
	stdin, err := os.OpenFile(os.DevNull, os.O_RDONLY, 0)
	if err != nil {
		_ = logFile.Close()
		return fmt.Errorf("cannot open %s for the post-update helper: %w", os.DevNull, err)
	}

	cmd := exec.Command(opts.Exe, "internal", "launchd-reload",
		"--level", levelName(opts.System),
		"--domain", domain,
		"--plist", plistPath,
		"--pid", strconv.Itoa(callerPID),
		"--exe-dev", strconv.FormatUint(exeID.Dev, 10),
		"--exe-ino", strconv.FormatUint(exeID.Ino, 10),
		"--audit", auditLogPath(opts),
	)
	cmd.Stdin = stdin
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = detachedSysProcAttr()

	var startErr error
	if start := opts.StartDetached; start != nil {
		startErr = start(cmd)
	} else {
		// The helper's own pid is not used here: --pid above is the Broker's.
		_, startErr = startDetachedProcess(cmd)
	}
	logCloseErr := logFile.Close()
	stdinCloseErr := stdin.Close()
	if startErr != nil {
		return fmt.Errorf("cannot start the post-update helper: %w", startErr)
	}
	if logCloseErr != nil {
		return fmt.Errorf("cannot close the post-update log %s: %w", logPath, logCloseErr)
	}
	if stdinCloseErr != nil {
		return fmt.Errorf("cannot close %s for the post-update helper: %w", os.DevNull, stdinCloseErr)
	}
	return nil
}

func checkLogDir(opts Options, dir string) error {
	if opts.Owner == nil {
		return fmt.Errorf("cannot inspect the post-update log directory %s", dir)
	}
	uid, mode, err := opts.Owner(dir)
	if err != nil {
		return fmt.Errorf("cannot inspect the post-update log directory %s: %w", dir, err)
	}
	if !mode.IsDir() {
		return fmt.Errorf("cannot use the post-update log directory %s: it is not a directory", dir)
	}
	if want := uint32(opts.EUID); uid != want {
		return fmt.Errorf("cannot use the post-update log directory %s: it is owned by uid %d, want %d", dir, uid, want)
	}
	if mode.Perm()&0o022 != 0 {
		return fmt.Errorf("cannot use the post-update log directory %s: it is writable by group or others (%#o)", dir, mode.Perm())
	}
	return nil
}

// Release sets cmd.Process.Pid to -1, so the pid must be read first and
// returned: kill(-1, sig) signals every process the account may signal.
func startDetachedProcess(cmd *exec.Cmd) (int, error) {
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	if err := cmd.Process.Release(); err != nil {
		return 0, err
	}
	return pid, nil
}

func launchdHelperLogPath(opts Options) (string, error) {
	switch {
	case opts.GOOS != "darwin":
		return "", fmt.Errorf("cannot find a launchd post-update log on %q", opts.GOOS)
	case opts.System:
		return filepath.Join(systemLogDir, LaunchdHelperLogName), nil
	case opts.Home == "":
		return "", errors.New("cannot place the post-update log: the user installation needs a home directory")
	default:
		return filepath.Join(opts.Home, "Library", "Logs", "peers", LaunchdHelperLogName), nil
	}
}

// RelaunchOptions returns the Options the detached launchd reload helper needs:
// it runs as the current account, only calls launchctl, and logs to standard
// error, which is the post-update log.
func RelaunchOptions(system bool) Options {
	return Options{
		System: system,
		GOOS:   "darwin",
		EUID:   os.Geteuid(),
		UID:    os.Getuid(),
		Run: func(name string, args ...string) ([]byte, error) {
			return exec.Command(name, args...).CombinedOutput()
		},
		Logf: func(format string, args ...any) {
			_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
		},
	}
}

// LaunchdReload is the deferred half of a macOS post-update: it waits for the
// Broker that answered the update to exit, then boots the label out and
// bootstraps the rewritten plist. The wait matters because bootout stops the
// job: reloading before the response is written would tear the Broker down
// while the caller is still waiting, and a child in the job would be stopped
// with it. The update caller already has its success response, so a failure is
// also recorded in the audit log and on standard error, which is the
// post-update log.
//
// It gives up without reloading when the Broker does not exit within
// launchdHelperWaitBound, or when the plist no longer starts the binary that
// started this helper: the update may have rolled back, and bootout would stop
// the Broker still serving the old version. callerPID at or below 1 means no
// Broker is left to wait for, so the reload proceeds at once and pid 1 is never
// signalled.
func LaunchdReload(opts Options, domain, plistPath string, callerPID int, expected ExecutableID, auditPath string) error {
	if callerPID > 1 {
		if err := waitProcessExit(opts, callerPID, launchdHelperWaitBound); err != nil {
			return abandonLaunchdReload(opts, auditPath, fmt.Sprintf(
				"the Broker process %d did not exit within %s, so the service definition was not reloaded: %v",
				callerPID, launchdHelperWaitBound, err))
		}
	}
	if err := plistRunsBinary(opts, plistPath, expected); err != nil {
		return abandonLaunchdReload(opts, auditPath,
			fmt.Sprintf("the service definition was not reloaded: %v", err))
	}
	if err := reloadLaunchd(opts, domain, plistPath); err != nil {
		auditLaunchdFailure(auditPath, "reloading the service definition after the update failed: "+err.Error())
		return err
	}
	return nil
}

func abandonLaunchdReload(opts Options, auditPath, detail string) error {
	logf(opts, "post-update helper: %s", detail)
	auditLaunchdFailure(auditPath, detail)
	return errors.New(detail)
}

func auditLaunchdFailure(auditPath, detail string) {
	if auditPath == "" {
		return
	}
	_ = config.AppendAudit(auditPath, config.Event{
		Event:  config.EventUpdateFailed,
		Detail: "second phase: " + detail,
	})
}

func executableID(path string) (ExecutableID, error) {
	info, err := os.Stat(path)
	if err != nil {
		return ExecutableID{}, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ExecutableID{}, fmt.Errorf("no file identity for %s", path)
	}
	return ExecutableID{Dev: uint64(st.Dev), Ino: st.Ino}, nil
}

func plistRunsBinary(opts Options, plistPath string, expected ExecutableID) error {
	data, err := os.ReadFile(rooted(opts, plistPath))
	if err != nil {
		return fmt.Errorf("cannot read %s: %w", plistPath, err)
	}
	program, err := launchdProgramPath(data)
	if err != nil {
		return fmt.Errorf("cannot read %s: %w", plistPath, err)
	}
	got, err := executableID(rooted(opts, program))
	if err != nil {
		return fmt.Errorf("cannot inspect the executable %q named by %s: %w", program, plistPath, err)
	}
	if got != expected {
		return fmt.Errorf("the service definition starts %s, which is not the binary that started this helper", program)
	}
	return nil
}

func launchdProgramPath(data []byte) (string, error) {
	dec := xml.NewDecoder(bytes.NewReader(data))
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		start, ok := tok.(xml.StartElement)
		if !ok || start.Name.Local != "key" {
			continue
		}
		var key string
		if err := dec.DecodeElement(&key, &start); err != nil {
			return "", err
		}
		if key != "ProgramArguments" {
			continue
		}
		array, err := nextStartElement(dec)
		if err != nil {
			return "", fmt.Errorf("ProgramArguments is not an array: %w", err)
		}
		if array.Name.Local != "array" {
			return "", errors.New("ProgramArguments is not an array")
		}
		first, err := nextStartElement(dec)
		if err != nil {
			return "", fmt.Errorf("ProgramArguments has no first entry: %w", err)
		}
		if first.Name.Local != "string" {
			return "", errors.New("ProgramArguments has no first entry")
		}
		var program string
		if err := dec.DecodeElement(&program, &first); err != nil {
			return "", err
		}
		return program, nil
	}
	return "", errors.New("no ProgramArguments in the plist")
}

// The decoder reports the whitespace between two elements as character data,
// so a caller that wants the element after a key cannot take the very next
// token.
func nextStartElement(dec *xml.Decoder) (xml.StartElement, error) {
	for {
		tok, err := dec.Token()
		if err != nil {
			return xml.StartElement{}, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			return t, nil
		case xml.EndElement:
			return xml.StartElement{}, fmt.Errorf("unexpected closing element </%s>", t.Name.Local)
		}
	}
}

type plistEnvironmentEntry struct {
	Name  string
	Value string
}

func plistEnvironment(path string) ([]plistEnvironmentEntry, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := parsePlistEnvironment(data)
	if err != nil {
		return nil, nil
	}
	return entries, nil
}

func parsePlistEnvironment(data []byte) ([]plistEnvironmentEntry, error) {
	dec := xml.NewDecoder(bytes.NewReader(data))
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		start, ok := tok.(xml.StartElement)
		if !ok || start.Name.Local != "key" {
			continue
		}
		var key string
		if err := dec.DecodeElement(&key, &start); err != nil {
			return nil, err
		}
		if key != "EnvironmentVariables" {
			continue
		}
		dict, err := nextStartElement(dec)
		if err != nil {
			return nil, fmt.Errorf("EnvironmentVariables is not a dictionary: %w", err)
		}
		if dict.Name.Local != "dict" {
			return nil, errors.New("EnvironmentVariables is not a dictionary")
		}
		entries, err := parsePlistStringDict(dec)
		if err != nil {
			return nil, err
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		return entries, nil
	}
}

func parsePlistStringDict(dec *xml.Decoder) ([]plistEnvironmentEntry, error) {
	var (
		entries []plistEnvironmentEntry
		name    string
		hasName bool
	)
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "key":
				if err := dec.DecodeElement(&name, &t); err != nil {
					return nil, err
				}
				hasName = true
			case "string":
				var value string
				if err := dec.DecodeElement(&value, &t); err != nil {
					return nil, err
				}
				if hasName {
					entries = append(entries, plistEnvironmentEntry{Name: name, Value: value})
					hasName = false
				}
			default:
				if err := dec.Skip(); err != nil {
					return nil, err
				}
				hasName = false
			}
		case xml.EndElement:
			if t.Name.Local == "dict" {
				return entries, nil
			}
		}
	}
}

// A generated entry wins over a kept entry of the same name, so a variable
// this version sets cannot be shadowed by an older value.
func withPlistEnvironment(content string, kept []plistEnvironmentEntry) string {
	generated, err := parsePlistEnvironment([]byte(content))
	if err != nil {
		generated = nil
	}
	merged := mergePlistEnvironment(generated, kept)
	if len(merged) == 0 {
		return content
	}
	if len(generated) > 0 {
		content = removePlistEnvironment(content)
	}
	const documentEnd = "</dict>\n</plist>"
	if !strings.Contains(content, documentEnd) {
		return content
	}
	var block strings.Builder
	block.WriteString("\t<key>EnvironmentVariables</key>\n\t<dict>\n")
	for _, entry := range merged {
		fmt.Fprintf(&block, "\t\t<key>%s</key>\n\t\t<string>%s</string>\n", escapeXML(entry.Name), escapeXML(entry.Value))
	}
	block.WriteString("\t</dict>\n")
	return strings.Replace(content, documentEnd, block.String()+documentEnd, 1)
}

func mergePlistEnvironment(generated, kept []plistEnvironmentEntry) []plistEnvironmentEntry {
	seen := make(map[string]bool, len(generated)+len(kept))
	merged := make([]plistEnvironmentEntry, 0, len(generated)+len(kept))
	for _, list := range [][]plistEnvironmentEntry{generated, kept} {
		for _, entry := range list {
			if seen[entry.Name] {
				continue
			}
			seen[entry.Name] = true
			merged = append(merged, entry)
		}
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Name < merged[j].Name })
	return merged
}

func removePlistEnvironment(content string) string {
	dec := xml.NewDecoder(strings.NewReader(content))
	for {
		start := dec.InputOffset()
		tok, err := dec.Token()
		if err != nil {
			return content
		}
		element, ok := tok.(xml.StartElement)
		if !ok || element.Name.Local != "key" {
			continue
		}
		var key string
		if err := dec.DecodeElement(&key, &element); err != nil {
			return content
		}
		if key != "EnvironmentVariables" {
			continue
		}
		dict, err := nextStartElement(dec)
		if err != nil || dict.Name.Local != "dict" {
			return content
		}
		if err := dec.Skip(); err != nil {
			return content
		}
		end := dec.InputOffset()
		prefix := strings.TrimRight(content[:start], " \t")
		return prefix + content[end:]
	}
}

// A bootout that reports the job was not loaded (exit status 3) is not a
// failure. It waits for launchd's asynchronous teardown before bootstrapping,
// and bootstrapService retries launchd's transient exit status 5.
func reloadLaunchd(opts Options, domain, plistPath string) error {
	var res Result
	if err := bootoutService(opts, domain, &res); err != nil {
		return err
	}
	budget := stopBound(opts)
	if err := waitServiceUnloaded(opts, domain, time.Now().Add(budget), budget, unloadAdvice(opts, domain, "peers internal post-update")); err != nil {
		return err
	}

	cmdline := "launchctl bootstrap " + domain + " " + plistPath
	out, err := bootstrapService(opts, domain, plistPath)
	if err != nil {
		return commandError("cannot run "+cmdline, out, err)
	}
	return nil
}

func waitProcessExit(opts Options, pid int, bound time.Duration) error {
	if opts.WaitProcessExit != nil {
		return opts.WaitProcessExit(pid, bound)
	}
	return waitProcessGone(pid, bound)
}

func waitProcessGone(pid int, bound time.Duration) error {
	deadline := time.Now().Add(bound)
	for {
		if processGone(pid) {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("process %d is still running after %s", pid, bound)
		}
		time.Sleep(processExitPollInterval)
	}
}

func processGone(pid int) bool {
	return !procsig.Alive(pid)
}

func logf(opts Options, format string, args ...any) {
	if opts.Logf != nil {
		opts.Logf(format, args...)
	}
}

func levelName(system bool) string {
	if system {
		return "system"
	}
	return "user"
}
