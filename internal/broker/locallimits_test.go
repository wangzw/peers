package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/accountdb"
	"github.com/wangzw/peers/internal/broker/localpb"
)

// Event names of the local interface limits. They are spelled out so the
// tests still compile against the version that has no such events.
const (
	connectionLimitEventName = "local_connection_limit_reached"
	registerLimitEventName   = "register_limit_reached"
)

// withLocalLimits lowers the local interface limits for one test. It must run
// before newTestBroker so the Broker is built with them.
func withLocalLimits(t *testing.T, conns, streams, regs int) {
	t.Helper()
	withLocalTotalLimit(t, conns, streams, regs, maxLocalConnectionsTotal)
}

// withLocalTotalLimit lowers the per-account and global connection limits for
// one test. It must run before newTestBroker so the Broker is built with them.
func withLocalTotalLimit(t *testing.T, conns, streams, regs, total int) {
	t.Helper()
	oldConns, oldStreams, oldRegs, oldTotal := localConnLimit, localStreamLimit, localRegisterLimit, localTotalConnLimit
	localConnLimit, localStreamLimit, localRegisterLimit, localTotalConnLimit = conns, streams, regs, total
	t.Cleanup(func() {
		localConnLimit, localStreamLimit, localRegisterLimit, localTotalConnLimit = oldConns, oldStreams, oldRegs, oldTotal
	})
}

// limitLogRecords returns the runtime log records whose event is name.
func limitLogRecords(t *testing.T, tb *testBroker, name string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, rec := range tb.logLines(t) {
		if rec["event"] == name {
			out = append(out, rec)
		}
	}
	return out
}

// count returns the slots uid holds; it is a test helper.
func (l *accountLimit) count(uid uint32) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.counts[uid]
}

// TestAccountLimitCountsPerAccount pins the accounting under the connection and
// registration limits: slots are per account key, a refusal is logged at most
// once per account per minute, and the log is written again after a minute.
func TestAccountLimitCountsPerAccount(t *testing.T) {
	var logs syncBuffer
	l := newAccountLimit(1, connectionLimitEventName, NewLogger(&logs, new(slog.LevelVar)))
	now := time.Unix(1000, 0)
	l.now = func() time.Time { return now }

	if !l.acquire(1000, 1000) {
		t.Fatal("uid 1000 was refused its first slot")
	}
	if !l.acquire(1001, 1001) {
		t.Fatal("uid 1001 was refused a slot while uid 1000 held one")
	}
	for i := 0; i < 3; i++ {
		if l.acquire(1000, 1000) {
			t.Fatal("uid 1000 got a slot past the limit")
		}
	}
	if got := l.count(1000); got != 1 {
		t.Fatalf("uid 1000 holds %d slots, want 1", got)
	}
	if got := l.count(1001); got != 1 {
		t.Fatalf("uid 1001 holds %d slots, want 1", got)
	}

	// Releasing the only held slot frees it, and after the log interval has
	// passed a later refusal is recorded again.
	l.release(1000)
	now = now.Add(overLimitLogInterval)
	if !l.acquire(1000, 1000) {
		t.Fatal("uid 1000 was refused a slot after releasing one")
	}
	if l.acquire(1000, 1000) {
		t.Fatal("uid 1000 got a slot past the limit after reacquiring")
	}

	records := 0
	for _, rec := range decodeLogLines(t, logs.String()) {
		if rec["event"] == connectionLimitEventName {
			records++
		}
	}
	if records != 2 {
		t.Fatalf("%s records = %d, want 2 (one per minute)", connectionLimitEventName, records)
	}
}

// TestAccountLimitLogsOncePerMinute checks that a retrying client cannot flood
// the runtime log: one hundred refusals within a minute write one record.
func TestAccountLimitLogsOncePerMinute(t *testing.T) {
	var logs syncBuffer
	l := newAccountLimit(1, connectionLimitEventName, NewLogger(&logs, new(slog.LevelVar)))
	now := time.Unix(1000, 0)
	l.now = func() time.Time { return now }

	if !l.acquire(1000, 1000) {
		t.Fatal("uid 1000 was refused its first slot")
	}
	for i := 0; i < 100; i++ {
		if l.acquire(1000, 1000) {
			t.Fatal("uid 1000 got a slot past the limit")
		}
	}
	records := limitLogRecordsText(t, logs.String(), connectionLimitEventName)
	if records != 1 {
		t.Fatalf("%s records = %d, want 1 within one minute", connectionLimitEventName, records)
	}
}

// limitLogRecordsText counts the records of one event in a raw log.
func limitLogRecordsText(t *testing.T, raw, name string) int {
	t.Helper()
	n := 0
	for _, rec := range decodeLogLines(t, raw) {
		if rec["event"] == name {
			n++
		}
	}
	return n
}

// TestLocalConnectionLimitRefusesExtraConnection pins that a third connection
// from one account is refused during the handshake when the account is limited
// to two, and that the refusal is logged once per over-limit episode.
func TestLocalConnectionLimitRefusesExtraConnection(t *testing.T) {
	withLocalLimits(t, 2, maxLocalStreams, maxLocalRegistrations)
	tb := newTestBroker(t)
	uid := uint32(os.Geteuid())

	first := newCredClient(t, tb.sock)
	second := newCredClient(t, tb.sock)
	if err := callHello(t, first); err != nil {
		t.Fatalf("first connection: %v", err)
	}
	if err := callHello(t, second); err != nil {
		t.Fatalf("second connection: %v", err)
	}

	// Two attempts past the limit: the log must record the episode once.
	for i := 0; i < 2; i++ {
		cc := newCredClient(t, tb.sock)
		err := callHello(t, cc)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("extra connection %d: err = %v (code %v), want the handshake refused",
				i+1, err, status.Code(err))
		}
	}

	records := limitLogRecords(t, tb, connectionLimitEventName)
	if len(records) != 1 {
		t.Fatalf("%s records = %d, want 1 for one over-limit episode", connectionLimitEventName, len(records))
	}
	if got := records[0]["level"]; got != "WARN" {
		t.Errorf("log level = %v, want WARN", got)
	}
	if got := records[0]["uid"]; got != float64(uid) {
		t.Errorf("log uid = %v, want %d", got, uid)
	}
	if got := records[0]["limit"]; got != float64(2) {
		t.Errorf("log limit = %v, want 2", got)
	}
}

// TestLocalConnectionSlotReturnsOnClose pins that a closed connection returns
// its account's slot, so another connection is accepted afterwards.
func TestLocalConnectionSlotReturnsOnClose(t *testing.T) {
	withLocalLimits(t, 1, maxLocalStreams, maxLocalRegistrations)
	tb := newTestBroker(t)

	first := newCredClient(t, tb.sock)
	if err := callHello(t, first); err != nil {
		t.Fatalf("first connection: %v", err)
	}

	second := newCredClient(t, tb.sock)
	if err := callHello(t, second); status.Code(err) != codes.Unavailable {
		t.Fatalf("second connection: err = %v (code %v), want the handshake refused", err, status.Code(err))
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close the first connection: %v", err)
	}
	// The server releases the slot when it notices the close; poll by dialing
	// until a connection is accepted again.
	waitForReg(t, "the connection slot to return", func() bool {
		cc := newCredClient(t, tb.sock)
		if err := callHello(t, cc); err != nil {
			_ = cc.Close()
			return false
		}
		return true
	})
}

// TestLocalStreamLimitHoldsExtraStreams pins the per-connection stream limit:
// a second stream on a connection limited to one waits for the first to end.
func TestLocalStreamLimitHoldsExtraStreams(t *testing.T) {
	withLocalLimits(t, maxLocalConnections, 1, maxLocalRegistrations)
	tb := newTestBroker(t)
	// Warm the connection up so the server's stream limit is in effect before
	// the streams below open.
	if err := callHello(t, tb.conn); err != nil {
		t.Fatalf("warm up: %v", err)
	}

	held := startRegistrationAt(t, tb.client, "", "")
	if got := held.problem(3 * time.Second); got != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING {
		t.Fatalf("held registration reason = %v, want socket_path_missing", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := tb.client.Register(ctx); err == nil {
		t.Fatal("a second concurrent stream opened on a connection limited to one")
	} else if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("second stream: err = %v (code %v), want it held until the deadline", err, status.Code(err))
	}

	// Ending the held stream returns the slot.
	held.cancel()
	nextCtx, nextCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer nextCancel()
	next, err := tb.client.Register(nextCtx)
	if err != nil {
		t.Fatalf("a stream after the held one ended: %v", err)
	}
	_ = next.CloseSend()
}

// TestRegisterLimitRefusesExtraStream pins the per-account registration
// limit: a third open Register stream from one account is refused with
// quota_exceeded, and the refusal is logged once.
func TestRegisterLimitRefusesExtraStream(t *testing.T) {
	withLocalLimits(t, maxLocalConnections, maxLocalStreams, 2)
	tb := newTestBroker(t)
	if err := callHello(t, tb.conn); err != nil {
		t.Fatalf("warm up: %v", err)
	}

	firstConn := newCredClient(t, tb.sock)
	first := startRegistrationAt(t, localpb.NewLocalClient(firstConn), "", "")
	if got := first.problem(3 * time.Second); got != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING {
		t.Fatalf("first registration reason = %v, want socket_path_missing", got)
	}
	// The second registration uses another connection: the limit is per
	// account, not per connection.
	secondConn := newCredClient(t, tb.sock)
	secondClient := localpb.NewLocalClient(secondConn)
	second := startRegistrationAt(t, secondClient, "", "")
	if got := second.problem(3 * time.Second); got != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING {
		t.Fatalf("second registration reason = %v, want socket_path_missing", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := secondClient.Register(ctx)
	if err != nil {
		t.Fatalf("open the third registration: %v", err)
	}
	if err := stream.Send(&localpb.RegisterRequest{}); err != nil {
		t.Fatalf("send the third registration frame: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("third registration: err = %v (code %v), want ResourceExhausted", err, status.Code(err))
	}
	var detail *localpb.ErrorDetail
	st, _ := status.FromError(err)
	for _, d := range st.Details() {
		if ed, ok := d.(*localpb.ErrorDetail); ok {
			detail = ed
		}
	}
	if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED {
		t.Fatalf("error code = %v, want quota_exceeded", detail.GetCode())
	}
	want := fmt.Sprintf("cannot register: account %s already has 2 open registrations",
		usernameFor(uint32(os.Geteuid())))
	if got := detail.GetMessage(); got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}

	records := limitLogRecords(t, tb, registerLimitEventName)
	if len(records) != 1 {
		t.Fatalf("%s records = %d, want 1 for one over-limit episode", registerLimitEventName, len(records))
	}
	if got := records[0]["level"]; got != "WARN" {
		t.Errorf("log level = %v, want WARN", got)
	}
	if got := records[0]["limit"]; got != float64(2) {
		t.Errorf("log limit = %v, want 2", got)
	}
}

// stubAccountDB replaces the caller-name lookup for one test. A uid in known
// resolves to an account; every other uid resolves to no entry.
func stubAccountDB(t *testing.T, known map[uint32]string) {
	t.Helper()
	resetUsernameCache(t)
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		if name, ok := known[uid]; ok {
			return accountdb.User{Name: name, UID: uid}, nil
		}
		return accountdb.User{}, accountdb.ErrNotFound
	}
}

// credSequence returns a ReadPeerCred that hands out uids in order and repeats
// the last one. Username is filled from the cache the Broker uses.
func credSequence(t *testing.T, uids ...uint32) func(net.Conn) (Caller, error) {
	t.Helper()
	var mu sync.Mutex
	next := 0
	return func(net.Conn) (Caller, error) {
		mu.Lock()
		uid := uids[len(uids)-1]
		if next < len(uids) {
			uid = uids[next]
		}
		next++
		mu.Unlock()
		return Caller{UID: uid, Username: usernameFor(uid)}, nil
	}
}

// allowEveryCaller lets a test present any uid at the local interface.
func allowEveryCaller(Caller) bool { return true }

// TestGlobalConnectionLimitRefusesExtraConnection pins the broker-wide
// connection cap: with the total limited to two, a third account's connection
// is refused even though no account has reached its own limit.
func TestGlobalConnectionLimitRefusesExtraConnection(t *testing.T) {
	withLocalTotalLimit(t, maxLocalConnections, maxLocalStreams, maxLocalRegistrations, 2)
	stubAccountDB(t, map[uint32]string{1000: "alice", 1001: "bob", 1002: "carol"})
	tb := newTestBroker(t, func(d *Deps) {
		d.AllowCaller = allowEveryCaller
		d.ReadPeerCred = credSequence(t, 1000, 1001, 1002)
	})

	if err := callHello(t, tb.conn); err != nil {
		t.Fatalf("first connection: %v", err)
	}
	second := newCredClient(t, tb.sock)
	if err := callHello(t, second); err != nil {
		t.Fatalf("second connection: %v", err)
	}
	third := newCredClient(t, tb.sock)
	if err := callHello(t, third); status.Code(err) != codes.Unavailable {
		t.Fatalf("third connection: err = %v (code %v), want the handshake refused", err, status.Code(err))
	}

	records := limitLogRecords(t, tb, connectionLimitEventName)
	if len(records) != 1 {
		t.Fatalf("%s records = %d, want 1", connectionLimitEventName, len(records))
	}
	if got := records[0]["scope"]; got != "broker" {
		t.Errorf("log scope = %v, want broker", got)
	}
	if got := records[0]["limit"]; got != float64(2) {
		t.Errorf("log limit = %v, want 2", got)
	}
}

// TestUnknownUIDsShareConnectionQuota checks that uids with no account entry
// share one account's connection slots, while a normal account keeps its own.
func TestUnknownUIDsShareConnectionQuota(t *testing.T) {
	withLocalLimits(t, 2, maxLocalStreams, maxLocalRegistrations)
	stubAccountDB(t, map[uint32]string{1000: "alice"})
	tb := newTestBroker(t, func(d *Deps) {
		d.AllowCaller = allowEveryCaller
		// Two unknown uids fill the shared bucket; the next unknown uid is
		// refused; the last uid is a real account and still connects.
		d.ReadPeerCred = credSequence(t, 300001, 300002, 300003, 1000)
	})

	if err := callHello(t, tb.conn); err != nil {
		t.Fatalf("first unknown-uid connection: %v", err)
	}
	second := newCredClient(t, tb.sock)
	if err := callHello(t, second); err != nil {
		t.Fatalf("second unknown-uid connection: %v", err)
	}
	third := newCredClient(t, tb.sock)
	if err := callHello(t, third); status.Code(err) != codes.Unavailable {
		t.Fatalf("third unknown-uid connection: err = %v (code %v), want the handshake refused", err, status.Code(err))
	}
	known := newCredClient(t, tb.sock)
	if err := callHello(t, known); err != nil {
		t.Fatalf("known account connection: %v, want it accepted while the unknown bucket is full", err)
	}
}

// TestUnknownUIDsShareRegisterQuota checks that Register streams from uids with
// no account entry count against one shared per-account limit.
func TestUnknownUIDsShareRegisterQuota(t *testing.T) {
	withLocalLimits(t, maxLocalConnections, maxLocalStreams, 2)
	stubAccountDB(t, map[uint32]string{})
	tb := newTestBroker(t, func(d *Deps) {
		d.AllowCaller = allowEveryCaller
		d.ReadPeerCred = credSequence(t, 300001, 300002, 300003, 300004)
	})
	if err := callHello(t, tb.conn); err != nil {
		t.Fatalf("warm up: %v", err)
	}

	firstConn := newCredClient(t, tb.sock)
	first := startRegistrationAt(t, localpb.NewLocalClient(firstConn), "", "")
	if got := first.problem(3 * time.Second); got != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING {
		t.Fatalf("first registration reason = %v, want socket_path_missing", got)
	}
	secondConn := newCredClient(t, tb.sock)
	secondClient := localpb.NewLocalClient(secondConn)
	second := startRegistrationAt(t, secondClient, "", "")
	if got := second.problem(3 * time.Second); got != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING {
		t.Fatalf("second registration reason = %v, want socket_path_missing", got)
	}

	thirdConn := newCredClient(t, tb.sock)
	thirdClient := localpb.NewLocalClient(thirdConn)
	stream, err := thirdClient.Register(context.Background())
	if err != nil {
		t.Fatalf("open the third registration: %v", err)
	}
	if err := stream.Send(&localpb.RegisterRequest{}); err != nil {
		t.Fatalf("send the third registration frame: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("third registration from another unknown uid: err = %v (code %v), want ResourceExhausted",
			err, status.Code(err))
	}
}

// TestRegisterSlotReturnsAfterStreamEnds checks that ending a Register stream
// returns its account's registration slot, so the next stream from the same
// account is accepted.
func TestRegisterSlotReturnsAfterStreamEnds(t *testing.T) {
	withLocalLimits(t, maxLocalConnections, maxLocalStreams, 1)
	tb := newTestBroker(t)
	if err := callHello(t, tb.conn); err != nil {
		t.Fatalf("warm up: %v", err)
	}
	uid := uint32(os.Geteuid())

	held := startRegistrationAt(t, tb.client, "", "")
	if got := held.problem(3 * time.Second); got != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING {
		t.Fatalf("held registration reason = %v, want socket_path_missing", got)
	}
	held.cancel()

	waitForReg(t, "the registration slot to return", func() bool {
		key, err := accountKeyFor(uid)
		return err == nil && tb.b.registers.count(key) == 0
	})
	next := startRegistrationAt(t, tb.client, "", "")
	if got := next.problem(3 * time.Second); got != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_PATH_MISSING {
		t.Fatalf("registration after the held one ended = %v, want socket_path_missing", got)
	}
}

// TestCountedConnReleasesOnce checks that closing a connection more than once
// returns its account's slot only once.
func TestCountedConnReleasesOnce(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = server.Close() }()
	releases := 0
	conn := &countedConn{Conn: client, release: func() { releases++ }}
	_ = conn.Close()
	_ = conn.Close()
	if releases != 1 {
		t.Fatalf("a connection closed twice released its slot %d times, want 1", releases)
	}
}
