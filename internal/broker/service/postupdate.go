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

	// launchdHelperWaitBound bounds how long the detached helper waits for the
	// Broker that answered the update to exit. The response is written before
	// that exit, so the wait only covers the Broker's graceful shutdown. When
	// the bound expires the helper gives up: the update may have been rolled
	// back, and booting the service out would stop a Broker that is serving.
	launchdHelperWaitBound = 60 * time.Second

	// processExitPollInterval separates two checks of whether the caller is
	// still running.
	processExitPollInterval = 100 * time.Millisecond
)

// ExecutableID identifies an executable by the device and inode of its file.
// The post-update step records the new binary's identity before it starts the
// detached helper. A rolled-back update leaves a different file at the same
// path, and the helper compares the plist's executable against this record
// before it boots the service out.
type ExecutableID struct {
	Dev uint64
	Ino uint64
}

// savedUnit is the main unit file or plist as it was before this run rewrote
// it. exists is false when there was no file to keep.
type savedUnit struct {
	exists  bool
	content []byte
	mode    fs.FileMode
}

// PostUpdate rewrites the service definition of opts' level in this binary's
// format. Only the main unit file or plist is replaced, atomically; systemd
// drop-ins are left in place, so a definition that an operator extended keeps
// its extensions. A macOS plist keeps the account's EnvironmentVariables,
// which launchd offers no drop-in mechanism for. Linux then reloads the unit
// database synchronously with daemon-reload, which does not stop the running
// Broker. macOS writes the plist and starts a detached helper instead: bootout
// would stop the job that runs the Broker and this process, so the helper
// performs bootout and bootstrap after the Broker exits. The Broker is not
// restarted: restarting is the update flow's job.
//
// A failed reload restores the main unit file this run replaced, so the binary
// rollback that follows does not leave the service manager holding the next
// version's definition. The restore is reported in the returned error; when it
// itself fails, the error says so separately and an update_failed record names
// the problem.
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

// restoreAfterFailure puts the main unit file back and lets the caller roll the
// binary back as well. The original error is kept first; a successful restore
// is stated, and a failed one is named as such, recorded in the audit log and
// returned together with the original cause.
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
	Exists  bool
	Content []byte
	Mode    fs.FileMode
}

// SaveMainUnit reads the main unit file or plist of opts' level before the
// post-update step rewrites it. A missing file is not an error: restoring it
// then removes the file the step wrote. The old binary calls this so it can
// undo a rewrite the step's own restore never reaches, because the step was
// killed by its bound or by a canceled context.
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
// it and, on Linux, reloads the unit database again. macOS needs nothing more:
// the detached reload helper compares the plist's executable against the new
// binary and abandons the reload when a rollback changed it.
func RestoreMainUnit(opts Options, saved SavedUnit) error {
	path, err := unitPath(opts)
	if err != nil {
		return err
	}
	return restoreUnit(opts, path, savedUnit{exists: saved.Exists, content: saved.Content, mode: saved.Mode})
}

// saveUnit reads the main unit file or plist before this run replaces it.
// A missing file is not an error: restoring it then means removing the file
// this run wrote.
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

// restoreUnit writes the saved definition back and, on Linux, reloads the unit
// database again so systemd forgets the next version's format too. A file this
// run created is removed instead. Both failures are reported as a restore
// failure: in the first case the disk still holds the failed run's definition,
// in the second the disk holds the old one but the service manager does not.
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

// auditUnitRestoreFailure records a restore failure. The update fails either
// way; the record is what tells an operator, after the caller's error scrolled
// away, that the disk and the service manager may disagree about the unit.
func auditUnitRestoreFailure(opts Options, cause error) {
	_ = config.AppendAudit(rooted(opts, auditLogPath(opts)), config.Event{
		Event:  config.EventUpdateFailed,
		Detail: cause.Error(),
	})
}

// startLaunchdReload starts the helper that reloads the rewritten plist after
// the Broker exits. The helper is detached with setsid into a new session and
// does not inherit the Broker's standard streams, so the launchd job's bootout
// cannot stop it. Its standard input is an explicitly opened /dev/null and its
// output goes to the level's service log directory.
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

// checkLogDir verifies the directory that holds the post-update log before the
// helper opens a file in it. The log is opened with O_NOFOLLOW, so a symlink at
// the log path cannot redirect the write; the directory must still belong to
// the account running the helper and not be writable by group or others, or
// another account could replace the log between two appends.
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

// startDetachedProcess starts cmd, reads the child's pid, then releases the
// process so the caller can exit without waiting for the helper and the helper
// is reparented to launchd. Release sets cmd.Process.Pid to -1, so the pid is
// read first and returned; a caller that reads cmd.Process.Pid after Release
// gets -1, and kill(-1, sig) would signal every process the account may signal.
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

// launchdHelperLogPath returns the production path of the helper's log. The
// service definition's own write already created the directory.
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

// RelaunchOptions fills the Options the detached launchd reload helper needs.
// It runs as the same account as the service and only calls launchctl, so it
// reads neither the configuration nor the account home; its standard error is
// already the post-update log file.
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

// LaunchdReload is the deferred half of a macOS post-update. The helper process
// starts it after the plist was rewritten; it waits for the Broker that
// answered the update to exit, then boots the label out and bootstraps the
// rewritten plist. The wait matters because bootout stops the job: reloading
// before the response is written would tear the Broker down while the caller is
// still waiting, and a child in the job would be stopped with it.
//
// Two conditions make the helper give up instead of reloading. A Broker that
// does not exit within the bound may have rolled the update back, and a plist
// whose executable is no longer the binary that started this helper belongs to
// a rolled-back update. Booting the service out then would stop the Broker that
// is serving the old version, so both cases are logged and recorded in the
// audit log instead.
//
// A failure here comes after the caller received the success response, so it is
// recorded in the audit log and on standard error, which is the post-update
// log, instead of being returned to a caller.
//
// callerPID must name the Broker, not init. A pid at or below 1 means there is
// no Broker left to wait for: the helper was already reparented to init, so
// os.Getppid returned 1, and waiting on init would only time out. Pid 1 is
// never signalled either way; procsig.SendSignal refuses it.
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

// abandonLaunchdReload records why the helper left the service loaded and
// returns the same reason. The helper's standard error is the post-update log.
func abandonLaunchdReload(opts Options, auditPath, detail string) error {
	logf(opts, "post-update helper: %s", detail)
	auditLaunchdFailure(auditPath, detail)
	return errors.New(detail)
}

// auditLaunchdFailure records a second-phase failure. The Detail marks the
// phase, because the same event name is also written by the Broker when the
// first phase fails.
func auditLaunchdFailure(auditPath, detail string) {
	if auditPath == "" {
		return
	}
	_ = config.AppendAudit(auditPath, config.Event{
		Event:  config.EventUpdateFailed,
		Detail: "second phase: " + detail,
	})
}

// executableID records the device and inode of path, following symbolic links.
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

// plistRunsBinary reports whether the first ProgramArguments entry of the
// rewritten plist is the file expected identifies. The executable's inode
// changes when an update is rolled back, because the previous binary is renamed
// over the path.
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

// launchdProgramPath returns the first entry of the plist's ProgramArguments
// array.
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

// nextStartElement returns the next start element, skipping character data,
// comments and directives. A decoder reports the whitespace between two
// elements as a character-data token, so a caller that wants the element after
// a key cannot take the very next token. An end element before a start element
// is an error: the caller required a child.
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

// plistEnvironmentEntry is one EnvironmentVariables entry kept from an
// existing plist.
type plistEnvironmentEntry struct {
	Name  string
	Value string
}

// plistEnvironment reads the EnvironmentVariables of the plist at path, sorted
// by name. A missing plist has none; a plist written by this package has no
// EnvironmentVariables key.
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
		// The file is not a plist, or is not parseable. launchd cannot load such
		// a file either, and this run replaces it, so there is nothing to keep.
		return nil, nil
	}
	return entries, nil
}

// parsePlistEnvironment returns the string-valued entries of the top-level
// EnvironmentVariables dictionary, sorted by name. Other value types are
// skipped: launchd passes only strings to the process environment.
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

// parsePlistStringDict reads key/string pairs from the dictionary whose start
// element was just consumed. Other value types are skipped, and the matching
// end element ends the dictionary.
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

// withPlistEnvironment writes one EnvironmentVariables dictionary into a
// rendered plist: the entries this binary generated, plus the entries kept from
// the plist it replaces. A generated entry wins over a kept entry of the same
// name, so a variable this version sets cannot be shadowed by an older value,
// and the two are never written as separate dictionaries with the same key.
func withPlistEnvironment(content string, kept []plistEnvironmentEntry) string {
	generated, err := parsePlistEnvironment([]byte(content))
	if err != nil {
		// The content is rendered by this package; a parse failure leaves only
		// the kept entries to write.
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

// mergePlistEnvironment combines generated entries with kept ones, generated
// taking precedence, sorted by name so a rewrite is deterministic.
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

// removePlistEnvironment removes the top-level EnvironmentVariables key and its
// dictionary from content, keeping the rest of the document unchanged. Only the
// package's own rendered plist reaches this function, so the dictionary is the
// one the template wrote.
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

// reloadLaunchd replaces the loaded job with the rewritten plist. A bootout
// that reports the job was not loaded (exit status 3) is not a failure. After
// the bootout, it waits for launchd to finish the asynchronous teardown before
// bootstrapping, and bootstrapService retries launchd's transient exit status 5
// within its own bound, as a restart does.
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

// waitProcessExit waits until pid is gone. The injected function keeps the
// helper's deferral testable without a real process.
func waitProcessExit(opts Options, pid int, bound time.Duration) error {
	if opts.WaitProcessExit != nil {
		return opts.WaitProcessExit(pid, bound)
	}
	return waitProcessGone(pid, bound)
}

// waitProcessGone polls until pid no longer exists or bound elapses.
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

// processGone reports whether pid no longer exists. A permission error still
// means the process exists, which procsig.Alive accounts for. Every caller
// passes a pid above 1: LaunchdReload returns early when the caller is init.
func processGone(pid int) bool {
	return !procsig.Alive(pid)
}

// logf writes one helper log line when the caller supplied a logger.
func logf(opts Options, format string, args ...any) {
	if opts.Logf != nil {
		opts.Logf(format, args...)
	}
}

// levelName is the --level value of the service level.
func levelName(system bool) string {
	if system {
		return "system"
	}
	return "user"
}
