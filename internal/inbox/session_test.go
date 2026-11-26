package inbox

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestREQAgentMessaging001_LookupFindsRecord checks that Lookup returns the record's fields and takes the pid from the file name, not the content.
func TestREQAgentMessaging001_LookupFindsRecord(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	aWriteRecord(t, dir, 5001, map[string]any{
		"pid":                 9999, // ignored: the file name is authoritative
		"name":                "api-worker",
		"status":              "busy",
		"messagingSocketPath": socket,
	})

	s, err := Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if s.PID != 5001 {
		t.Fatalf("PID = %d, want 5001 from the file name", s.PID)
	}
	if s.Name != "api-worker" || s.Status != "busy" || s.SocketPath != socket {
		t.Fatalf("record fields not returned verbatim: %+v", s)
	}
	if s.ConfigDir != dir {
		t.Fatalf("ConfigDir = %q, want %q", s.ConfigDir, dir)
	}
}

// TestREQAgentMessaging005_RereadPicksUpRename checks that Reread returns a renamed record and leaves other fields intact.
func TestREQAgentMessaging005_RereadPicksUpRename(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	aWriteRecord(t, dir, 5002, map[string]any{
		"name":                "api-worker",
		"messagingSocketPath": socket,
	})
	before, err := Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup before rename: %v", err)
	}

	aWriteRecord(t, dir, 5002, map[string]any{
		"name":                "api-v2",
		"messagingSocketPath": socket,
	})
	after, err := Reread(before)
	if err != nil {
		t.Fatalf("Reread after rename: %v", err)
	}
	if after.Name != "api-v2" {
		t.Fatalf("Reread Name = %q, want api-v2", after.Name)
	}
	if after.PID != 5002 || after.SocketPath != socket || after.ConfigDir != dir {
		t.Fatalf("Reread changed unrelated fields: %+v", after)
	}
}

// TestREQAgentMessaging010_StatusVerbatim checks that Lookup and Reread return the status verbatim, including unrecognized and empty values.
func TestREQAgentMessaging010_StatusVerbatim(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name   string
		status string
	}{
		{"idle", "idle"},
		{"busy", "busy"},
		{"waiting", "waiting"},
		{"shell", "shell"},
		{"unrecognized", "paused"},
		{"missing", ""},
	}
	for i, tc := range cases {
		pid := 5100 + i
		socket := filepath.Join(dir, "status-"+strconv.Itoa(i)+".sock")
		if tc.status == "" {
			aWriteRaw(t, dir, pid, []byte(`{"name":"`+tc.name+`","messagingSocketPath":`+strconv.Quote(socket)+`}`))
		} else {
			aWriteRecord(t, dir, pid, map[string]any{
				"name":                tc.name,
				"status":              tc.status,
				"messagingSocketPath": socket,
			})
		}
		s, err := Lookup(dir, socket)
		if err != nil {
			t.Fatalf("Lookup %s: %v", tc.name, err)
		}
		if s.Status != tc.status {
			t.Fatalf("Lookup Status = %q, want %q", s.Status, tc.status)
		}
		again, err := Reread(s)
		if err != nil {
			t.Fatalf("Reread %s: %v", tc.name, err)
		}
		if again.Status != tc.status {
			t.Fatalf("Reread Status = %q, want %q", again.Status, tc.status)
		}
	}
}

// TestREQAgentMessaging033_RereadAfterSessionIDChange checks that a session id change leaves the agent identity unchanged.
func TestREQAgentMessaging033_RereadAfterSessionIDChange(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	aWriteRecord(t, dir, 5201, map[string]any{
		"name":                "api-worker",
		"status":              "idle",
		"messagingSocketPath": socket,
		"sessionId":           "11111111-1111-4111-8111-111111111111",
	})
	before, err := Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup before session id change: %v", err)
	}

	aWriteRecord(t, dir, 5201, map[string]any{
		"name":                "api-worker",
		"status":              "idle",
		"messagingSocketPath": socket,
		"sessionId":           "22222222-2222-4222-8222-222222222222",
	})
	after, err := Reread(before)
	if err != nil {
		t.Fatalf("Reread after session id change: %v", err)
	}
	if after.Name != "api-worker" || after.PID != 5201 || after.SocketPath != socket || after.ConfigDir != dir {
		t.Fatalf("session id change altered the Agent identity: %+v", after)
	}
	again, err := Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup after session id change: %v", err)
	}
	if again.Name != "api-worker" || again.PID != 5201 {
		t.Fatalf("Lookup changed across a session id change: %+v", again)
	}
}

// TestREQAgentMessaging036_HeadlessRecord checks that headless and IDE records are read regardless of kind and entrypoint.
func TestREQAgentMessaging036_HeadlessRecord(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		pid        int
		name       string
		kind       string
		entrypoint string
	}{
		{5301, "headless-agent", "headless", "cli"},
		{5302, "ide-agent", "interactive", "vscode"},
	}
	for _, tc := range cases {
		socket := filepath.Join(dir, tc.name+".sock")
		aWriteRecord(t, dir, tc.pid, map[string]any{
			"name":                tc.name,
			"kind":                tc.kind,
			"entrypoint":          tc.entrypoint,
			"messagingSocketPath": socket,
		})
		s, err := Lookup(dir, socket)
		if err != nil {
			t.Fatalf("Lookup %s: %v", tc.name, err)
		}
		if s.Name != tc.name || s.PID != tc.pid {
			t.Fatalf("record filtered by kind or entrypoint: %+v", s)
		}
		if _, err := Reread(s); err != nil {
			t.Fatalf("Reread %s: %v", tc.name, err)
		}
	}
}

// TestLookup_NewestMtimeWins checks that the newest modification time wins and equal times favour the larger pid.
func TestLookup_NewestMtimeWins(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "shared.sock")
	olderPID := aWriteRecord(t, dir, 5401, map[string]any{
		"name":                "older",
		"messagingSocketPath": socket,
	})
	newerPID := aWriteRecord(t, dir, 5402, map[string]any{
		"name":                "newer",
		"messagingSocketPath": socket,
	})
	aSetModTime(t, newerPID, time.Unix(1000, 0))
	aSetModTime(t, olderPID, time.Unix(2000, 0))

	s, err := Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if s.PID != 5401 || s.Name != "older" {
		t.Fatalf("newest modification time did not win: %+v", s)
	}

	aSetModTime(t, olderPID, time.Unix(2000, 0))
	aSetModTime(t, newerPID, time.Unix(2000, 0))
	s, err = Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup with equal modification times: %v", err)
	}
	if s.PID != 5402 {
		t.Fatalf("equal modification times chose PID %d, want the larger 5402", s.PID)
	}
}

// TestLookup_SkipsPartialAndBadFiles checks that Lookup skips half-written, empty, non-canonical, symlinked, oversized and directory records.
func TestLookup_SkipsPartialAndBadFiles(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	good := aWriteRecord(t, dir, 5501, map[string]any{
		"name":                "good",
		"messagingSocketPath": socket,
	})
	sessions := aSessionsDir(dir)

	aWriteRaw(t, dir, 5502, []byte(`{"pid":5502,`))
	aWriteRaw(t, dir, 5503, []byte{})
	aWriteFile(t, filepath.Join(sessions, "notes.txt"), []byte("unrelated"))
	aWriteFile(t, filepath.Join(sessions, "007.json"), []byte(
		`{"name":"noncanonical","messagingSocketPath":`+strconv.Quote(socket)+`}`))
	if err := os.Symlink(good, filepath.Join(sessions, "5504.json")); err != nil {
		t.Fatalf("creating symlink record: %v", err)
	}
	aWriteFile(t, filepath.Join(sessions, "5505.json"), []byte(strings.Repeat("x", maxRecordSize+1)))
	if err := os.Mkdir(filepath.Join(sessions, "5506.json"), 0o700); err != nil {
		t.Fatalf("creating directory record: %v", err)
	}

	s, err := Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup alongside bad records: %v", err)
	}
	if s.PID != 5501 || s.Name != "good" {
		t.Fatalf("Lookup returned a bad record: %+v", s)
	}
}

// aCreateFIFO creates a FIFO record file: opening it would block until a writer appears, so Lookup and Reread must not open it.
func aCreateFIFO(t *testing.T, path string) {
	t.Helper()
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("creating FIFO %s: %v", path, err)
	}
}

func aCreateSocketFile(t *testing.T, path string) {
	t.Helper()
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatalf("creating Unix socket file %s: %v", path, err)
	}
	ln.SetUnlinkOnClose(false)
	if err := ln.Close(); err != nil {
		t.Fatalf("closing Unix socket %s: %v", path, err)
	}
}

// TestLookup_SkipsFIFOAndSocketFiles checks that Lookup skips FIFO and socket records without blocking.
func TestLookup_SkipsFIFOAndSocketFiles(t *testing.T) {
	// The socket record needs a path inside macOS's ~104-byte Unix socket limit.
	dir := bShortTempDir(t)
	socket := filepath.Join(dir, "inbox.sock")
	aWriteRecord(t, dir, 5507, map[string]any{
		"name":                "good",
		"messagingSocketPath": socket,
	})
	sessions := aSessionsDir(dir)
	aCreateFIFO(t, filepath.Join(sessions, "5508.json"))
	aCreateSocketFile(t, filepath.Join(sessions, "5509.json"))

	type lookupResult struct {
		s   Session
		err error
	}
	done := make(chan lookupResult, 1)
	go func() {
		s, err := Lookup(dir, socket)
		done <- lookupResult{s: s, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Lookup alongside a FIFO and a socket record: %v", got.err)
		}
		if got.s.PID != 5507 || got.s.Name != "good" {
			t.Fatalf("Lookup returned a non-regular record: %+v", got.s)
		}
	case <-time.After(aWaitTimeout):
		t.Fatal("Lookup blocked on a non-regular record")
	}
}

// TestReread_NonRegularAndOversizedRecordsAreNoRecord checks that Reread reports ErrNoRecord for symlink, FIFO, socket, directory and oversized records.
func TestReread_NonRegularAndOversizedRecordsAreNoRecord(t *testing.T) {
	cases := []struct {
		name  string
		pid   int
		build func(t *testing.T, configDir, path string)
	}{
		{"symlink", 5801, func(t *testing.T, configDir, path string) {
			target := aWriteRecord(t, configDir, 5890, map[string]any{"name": "target"})
			if err := os.Symlink(target, path); err != nil {
				t.Fatalf("creating symlink record: %v", err)
			}
		}},
		{"fifo", 5802, func(t *testing.T, configDir, path string) { aCreateFIFO(t, path) }},
		{"socket", 5803, func(t *testing.T, configDir, path string) { aCreateSocketFile(t, path) }},
		{"directory", 5804, func(t *testing.T, configDir, path string) {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatalf("creating directory record: %v", err)
			}
		}},
		{"oversized", 5805, func(t *testing.T, configDir, path string) {
			aWriteFile(t, path, []byte(strings.Repeat("x", maxRecordSize+1)))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Keep the directory under /tmp so a Unix socket record fits macOS's
			// path limit.
			dir := bShortTempDir(t)
			if err := os.MkdirAll(aSessionsDir(dir), 0o700); err != nil {
				t.Fatalf("creating sessions directory: %v", err)
			}
			tc.build(t, dir, filepath.Join(aSessionsDir(dir), strconv.Itoa(tc.pid)+".json"))

			type rereadResult struct {
				s   Session
				err error
			}
			done := make(chan rereadResult, 1)
			go func() {
				s, err := Reread(Session{ConfigDir: dir, PID: tc.pid})
				done <- rereadResult{s: s, err: err}
			}()
			select {
			case got := <-done:
				if !errors.Is(got.err, ErrNoRecord) {
					t.Fatalf("Reread of a %s record error = %v, want ErrNoRecord", tc.name, got.err)
				}
				if got.s != (Session{}) {
					t.Fatalf("Reread returned a non-zero Session with ErrNoRecord: %+v", got.s)
				}
			case <-time.After(aWaitTimeout):
				t.Fatalf("Reread blocked on a %s record", tc.name)
			}
		})
	}
}

// TestLookup_LenientFields checks that missing and mistyped fields are read as empty rather than making the record invalid.
func TestLookup_LenientFields(t *testing.T) {
	dir := t.TempDir()

	missingSocket := filepath.Join(dir, "missing.sock")
	aWriteRaw(t, dir, 5601, []byte(`{"messagingSocketPath":`+strconv.Quote(missingSocket)+`}`))
	s, err := Lookup(dir, missingSocket)
	if err != nil {
		t.Fatalf("Lookup a record with missing fields: %v", err)
	}
	if s.PID != 5601 || s.Name != "" || s.Status != "" || s.PeerProtocol != 0 {
		t.Fatalf("missing fields were not read as empty: %+v", s)
	}

	wrongSocket := filepath.Join(dir, "wrong.sock")
	aWriteRaw(t, dir, 5602, []byte(
		`{"name":42,"status":true,"messagingSocketPath":`+strconv.Quote(wrongSocket)+`,"extra":{"nested":[1,2]}}`))
	s, err = Lookup(dir, wrongSocket)
	if err != nil {
		t.Fatalf("Lookup a record with mistyped fields: %v", err)
	}
	if s.Name != "" || s.Status != "" || s.SocketPath != wrongSocket || s.PeerProtocol != 0 {
		t.Fatalf("mistyped fields were not read as empty: %+v", s)
	}
}

// TestLookup_SocketPathComparedVerbatim checks that the socket path is compared as a plain string without normalization.
func TestLookup_SocketPathComparedVerbatim(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	aWriteRecord(t, dir, 5701, map[string]any{
		"name":                "agent",
		"messagingSocketPath": socket,
	})

	s, err := Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup with the exact socket path: %v", err)
	}
	if s.PID != 5701 {
		t.Fatalf("Lookup with the exact socket path returned %+v", s)
	}

	cwd, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("resolving working directory: %v", err)
	}
	relative, err := filepath.Rel(cwd, socket)
	if err != nil {
		t.Fatalf("making the socket path relative: %v", err)
	}
	for _, spelling := range []string{
		relative,
		socket + string(filepath.Separator),
		dir + string(filepath.Separator) + "." + string(filepath.Separator) + "inbox.sock",
	} {
		if _, err := Lookup(dir, spelling); !errors.Is(err, ErrNoRecord) {
			t.Fatalf("Lookup with socket path %q error = %v, want ErrNoRecord", spelling, err)
		}
	}
}

// TestReread_NoRecordAndPartial checks the ErrNoRecord and ErrPartial results and recovery when a record is removed, half-written or emptied.
func TestReread_NoRecordAndPartial(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	path := aWriteRecord(t, dir, 5801, map[string]any{
		"name":                "agent",
		"messagingSocketPath": socket,
	})
	s, err := Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("removing record: %v", err)
	}
	removed, err := Reread(s)
	if !errors.Is(err, ErrNoRecord) {
		t.Fatalf("Reread of a removed record error = %v, want ErrNoRecord", err)
	}
	if removed != (Session{}) {
		t.Fatalf("Reread returned a non-zero Session with ErrNoRecord: %+v", removed)
	}

	aWriteRaw(t, dir, 5801, []byte(`{"pid":5801,`))
	partial, err := Reread(s)
	if !errors.Is(err, ErrPartial) {
		t.Fatalf("Reread of a half-written record error = %v, want ErrPartial", err)
	}
	if partial != (Session{}) {
		t.Fatalf("Reread returned a non-zero Session with ErrPartial: %+v", partial)
	}

	aWriteRaw(t, dir, 5801, []byte{})
	empty, err := Reread(s)
	if !errors.Is(err, ErrPartial) {
		t.Fatalf("Reread of an empty record error = %v, want ErrPartial", err)
	}
	if empty != (Session{}) {
		t.Fatalf("Reread returned a non-zero Session with ErrPartial: %+v", empty)
	}

	aWriteRecord(t, dir, 5801, map[string]any{
		"name":                "recovered",
		"messagingSocketPath": socket,
	})
	recovered, err := Reread(s)
	if err != nil {
		t.Fatalf("Reread of a recovered record: %v", err)
	}
	if recovered.Name != "recovered" {
		t.Fatalf("Reread Name = %q, want recovered", recovered.Name)
	}

	if _, err := Reread(Session{ConfigDir: dir, PID: 0}); !errors.Is(err, ErrNoRecord) {
		t.Fatalf("Reread of a non-positive PID error = %v, want ErrNoRecord", err)
	}
}

// TestSessionsPathIsRegularFile checks that Lookup and Reread report ErrNoRecord when the sessions path is a regular file.
func TestSessionsPathIsRegularFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(aSessionsDir(dir), []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("writing a regular file at the sessions path: %v", err)
	}
	if _, err := Lookup(dir, "/tmp/inbox.sock"); !errors.Is(err, ErrNoRecord) {
		t.Fatalf("Lookup with a regular file at the sessions path error = %v, want ErrNoRecord", err)
	}
	if _, err := Reread(Session{ConfigDir: dir, PID: 6001}); !errors.Is(err, ErrNoRecord) {
		t.Fatalf("Reread with a regular file at the sessions path error = %v, want ErrNoRecord", err)
	}
}

// TestReread_DoesNotCompareSocketPath checks that Reread returns a rewritten record as it stands, including a changed, first-bound or unbound socket path.
func TestReread_DoesNotCompareSocketPath(t *testing.T) {
	dir := t.TempDir()
	aWriteRecord(t, dir, 5901, map[string]any{
		"name":                "old-session",
		"messagingSocketPath": "/tmp/old-inbox.sock",
	})
	old, err := Lookup(dir, "/tmp/old-inbox.sock")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	aWriteRecord(t, dir, 5901, map[string]any{
		"name":                "new-session",
		"messagingSocketPath": "/tmp/new-inbox.sock",
	})
	reused, err := Reread(old)
	if err != nil {
		t.Fatalf("Reread after the socket path changed: %v", err)
	}
	if reused.Name != "new-session" || reused.SocketPath != "/tmp/new-inbox.sock" {
		t.Fatalf("Reread did not return the rewritten record verbatim: %+v", reused)
	}

	aWriteRecord(t, dir, 5902, map[string]any{
		"name":                "binding",
		"messagingSocketPath": "",
	})
	unbound := Session{ConfigDir: dir, PID: 5902}
	aWriteRecord(t, dir, 5902, map[string]any{
		"name":                "binding",
		"messagingSocketPath": "/tmp/bound-inbox.sock",
	})
	bound, err := Reread(unbound)
	if err != nil {
		t.Fatalf("Reread after the first bind: %v", err)
	}
	if bound.SocketPath != "/tmp/bound-inbox.sock" {
		t.Fatalf("first bind was not reported: %+v", bound)
	}

	aWriteRecord(t, dir, 5903, map[string]any{
		"name":                "unbinding",
		"messagingSocketPath": "/tmp/unbinding-inbox.sock",
	})
	wasBound, err := Lookup(dir, "/tmp/unbinding-inbox.sock")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	aWriteRecord(t, dir, 5903, map[string]any{
		"name":                "unbinding",
		"messagingSocketPath": "",
	})
	nowUnbound, err := Reread(wasBound)
	if err != nil {
		t.Fatalf("Reread after unbinding: %v", err)
	}
	if nowUnbound.SocketPath != "" {
		t.Fatalf("unbinding was not reported: %+v", nowUnbound)
	}
}

// TestParseRecordPeerProtocol checks that only an integer between 1 and MaxInt32 yields a protocol version.
func TestParseRecordPeerProtocol(t *testing.T) {
	cases := []struct {
		name   string
		record string
		want   int
	}{
		{"one", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":1}`, 1},
		{"two", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":2}`, 2},
		{"missing", `{"name":"n","messagingSocketPath":"/s.sock"}`, 0},
		{"string", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":"1"}`, 0},
		{"fraction", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":1.5}`, 0},
		{"zero", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":0}`, 0},
		{"negative", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":-1}`, 0},
		{"max-int32", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":2147483647}`, 2147483647},
		{"max-int32-plus-one", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":2147483648}`, 0},
		{"too-large", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":1e12}`, 0},
		{"null", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":null}`, 0},
		{"object", `{"name":"n","messagingSocketPath":"/s.sock","peerProtocol":{}}`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := parseRecord([]byte(tc.record), 1)
			if err != nil {
				t.Fatalf("parseRecord: %v", err)
			}
			if s.PeerProtocol != tc.want {
				t.Fatalf("PeerProtocol = %d, want %d", s.PeerProtocol, tc.want)
			}
			if s.Name != "n" || s.SocketPath != "/s.sock" {
				t.Fatalf("peerProtocol changed other fields: %+v", s)
			}
		})
	}
}

// TestLookup_ReturnsPeerProtocol checks that Lookup and Reread carry peerProtocol to the caller.
func TestLookup_ReturnsPeerProtocol(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "inbox.sock")
	aWriteRaw(t, dir, 5003, []byte(
		`{"name":"a","messagingSocketPath":`+strconv.Quote(socket)+`,"peerProtocol":1}`))
	s, err := Lookup(dir, socket)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if s.PeerProtocol != 1 {
		t.Fatalf("Lookup PeerProtocol = %d, want 1", s.PeerProtocol)
	}

	aWriteRaw(t, dir, 5003, []byte(
		`{"name":"a","messagingSocketPath":`+strconv.Quote(socket)+`,"peerProtocol":3}`))
	next, err := Reread(s)
	if err != nil {
		t.Fatalf("Reread: %v", err)
	}
	if next.PeerProtocol != 3 {
		t.Fatalf("Reread PeerProtocol = %d, want 3", next.PeerProtocol)
	}
}

// TestParseRecordName checks that only canonical positive-integer <pid>.json names are accepted.
func TestParseRecordName(t *testing.T) {
	cases := []struct {
		name string
		pid  int
		ok   bool
	}{
		{"1.json", 1, true},
		{"4201.json", 4201, true},
		{"007.json", 0, false},
		{"0.json", 0, false},
		{".json", 0, false},
		{"x.json", 0, false},
		{"123", 0, false},
		{"123.JSON", 0, false},
		{"123.json.bak", 0, false},
	}
	for _, tc := range cases {
		pid, ok := parseRecordName(tc.name)
		if ok != tc.ok || pid != tc.pid {
			t.Fatalf("parseRecordName(%q) = (%d, %v), want (%d, %v)", tc.name, pid, ok, tc.pid, tc.ok)
		}
	}
}
