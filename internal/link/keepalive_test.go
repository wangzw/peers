package link

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/testkit/linktest"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// rawHTTP2Client is a hand-written HTTP/2 client that sends PING frames without
// opening a stream.
type rawHTTP2Client struct {
	conn   *tls.Conn
	framer *http2.Framer
}

// dialRawHTTP2 performs the TLS handshake and the HTTP/2 preface with an
// identity, acknowledges the server's SETTINGS and opens no stream; ALPN is
// restricted to h2.
func dialRawHTTP2(t *testing.T, addr string) *rawHTTP2Client {
	t.Helper()
	cfg := linktest.NewIdentity(t).TLSConfig()
	cfg.NextProtos = []string{"h2"}

	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial keepalive server: %v", err)
	}
	tc := tls.Client(raw, cfg)
	t.Cleanup(func() { _ = tc.Close() })

	if err := tc.SetDeadline(time.Now().Add(eventTimeout)); err != nil {
		t.Fatalf("set connection deadline: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	if err := tc.HandshakeContext(ctx); err != nil {
		t.Fatalf("TLS handshake: %v", err)
	}

	c := &rawHTTP2Client{conn: tc, framer: http2.NewFramer(tc, tc)}
	if _, err := io.WriteString(tc, http2.ClientPreface); err != nil {
		t.Fatalf("write client preface: %v", err)
	}
	if err := c.framer.WriteSettings(); err != nil {
		t.Fatalf("write client SETTINGS: %v", err)
	}
	c.awaitServerSettings(t)
	return c
}

func (c *rawHTTP2Client) awaitServerSettings(t *testing.T) {
	t.Helper()
	for {
		f, err := c.framer.ReadFrame()
		if err != nil {
			t.Fatalf("read server SETTINGS: %v", err)
		}
		sf, ok := f.(*http2.SettingsFrame)
		if !ok || sf.IsAck() {
			continue
		}
		if err := c.framer.WriteSettingsAck(); err != nil {
			t.Fatalf("ack server SETTINGS: %v", err)
		}
		return
	}
}

func (c *rawHTTP2Client) awaitPingAck(t *testing.T, data [8]byte) {
	t.Helper()
	for {
		f, err := c.framer.ReadFrame()
		if err != nil {
			t.Fatalf("read frame while waiting for PING ACK: %v", err)
		}
		switch f := f.(type) {
		case *http2.PingFrame:
			if f.IsAck() && f.Data == data {
				return
			}
		case *http2.GoAwayFrame:
			t.Fatalf("server sent GOAWAY before acknowledging a probe: code %v debug %q", f.ErrCode, f.DebugData())
		}
	}
}

// TestEnforcementAllowsProbesWithoutStream checks that the configured
// enforcement policy accepts probes without an active stream.
func TestEnforcementAllowsProbesWithoutStream(t *testing.T) {
	server := newTransportTestLink(t)
	SetTestKnobs(server.Link, TestKnobs{EnforcementMinTime: time.Microsecond})
	addr := startTransportServer(t, server.Link, func(*conn, *trackedConn) {
		t.Error("a keepalive probe opened a stream")
	})
	client := dialRawHTTP2(t, addr)

	for i := uint64(0); i < 10; i++ {
		var data [8]byte
		binary.BigEndian.PutUint64(data[:], i)
		if err := client.conn.SetDeadline(time.Now().Add(eventTimeout)); err != nil {
			t.Fatalf("set connection deadline: %v", err)
		}
		if err := client.framer.WritePing(false, data); err != nil {
			t.Fatalf("write PING %d: %v", i, err)
		}
		client.awaitPingAck(t, data)
	}
}

// TestEnforcementStillRejectsFlood checks that a probe flood still ends in
// GOAWAY with ENHANCE_YOUR_CALM and too_many_pings.
func TestEnforcementStillRejectsFlood(t *testing.T) {
	server := newTransportTestLink(t)
	SetTestKnobs(server.Link, TestKnobs{EnforcementMinTime: time.Hour})
	addr := startTransportServer(t, server.Link, func(*conn, *trackedConn) {
		t.Error("a keepalive flood opened a stream")
	})
	client := dialRawHTTP2(t, addr)

	if err := client.conn.SetDeadline(time.Now().Add(eventTimeout)); err != nil {
		t.Fatalf("set connection deadline: %v", err)
	}

	var data [8]byte
	for i := uint64(0); i < 20; i++ {
		binary.BigEndian.PutUint64(data[:], i)
		if err := client.framer.WritePing(false, data); err != nil {
			break
		}
	}

	for {
		f, err := client.framer.ReadFrame()
		if err != nil {
			t.Fatalf("read frames after a PING flood without seeing GOAWAY: %v", err)
		}
		ga, ok := f.(*http2.GoAwayFrame)
		if !ok {
			continue
		}
		if ga.ErrCode != http2.ErrCodeEnhanceYourCalm {
			t.Fatalf("GOAWAY code = %v, want %v", ga.ErrCode, http2.ErrCodeEnhanceYourCalm)
		}
		if got := string(ga.DebugData()); got != "too_many_pings" {
			t.Fatalf("GOAWAY debug data = %q, want %q", got, "too_many_pings")
		}
		return
	}
}

// TestProductionPolicyToleratesFifteenSecondProbes checks that the enforcement
// minimum tolerates peer probe intervals of ten and fifteen seconds.
func TestProductionPolicyToleratesFifteenSecondProbes(t *testing.T) {
	l := newTransportTestLink(t).Link

	if got := l.enforcementMinTime; got > 10*time.Second {
		t.Fatalf("enforcement MinTime = %v, want <= 10s (gRPC's minimum client probe interval)", got)
	}
	if got := l.enforcementMinTime; got >= 15*time.Second {
		t.Fatalf("enforcement MinTime = %v, want < 15s (default link.keepalive_interval)", got)
	}

	opts := l.serverOptions()
	if len(opts) == 0 {
		t.Fatal("serverOptions returned no options")
	}
	srv := grpc.NewServer(opts...)
	srv.Stop()
}

// TestClientKeepaliveFromSettings checks that a dialed link probes at the
// configured interval and timeout, and that it does so without an active
// stream.
func TestClientKeepaliveFromSettings(t *testing.T) {
	tl := newTransportTestLink(t)
	tl.settings.SetKeepalive(20*time.Second, 7*time.Second)

	got := tl.clientKeepalive()
	want := keepalive.ClientParameters{
		Time:                20 * time.Second,
		Timeout:             7 * time.Second,
		PermitWithoutStream: true,
	}
	if got != want {
		t.Fatalf("clientKeepalive() = %+v, want %+v", got, want)
	}
}
