package link

import (
	"context"
	"net"
	"time"
)

// TestKnobs overrides Link's fixed constants in tests. Zero fields keep the
// current value; there is deliberately no protocol version field, which is set
// through Options.ProtocolVersion.
type TestKnobs struct {
	SendQueueSize         int
	FirstFrameTimeout     time.Duration
	HelloTimeout          time.Duration
	RejectLinger          time.Duration
	MaxUnauthenticated    int
	MaxStateFrames        int
	MaxStateBytes         int
	EnforcementMinTime    time.Duration
	LocalNets             func() ([]*net.IPNet, error)
	OnDial                func(address string)
	DialContext           func(ctx context.Context, network, address string) (net.Conn, error)
	BeforeDialLoop        func()
	OnRejectLingerExit    func()
	AfterHelloSent        func()
	OnUnauthenticatedRead func(total int64)
}

// SetTestKnobs applies k to l. Zero fields keep the current value. Tests call
// it before the goroutine that reads a knob starts: before Listen or Start for
// the knobs those paths read, and before the PairDial or inbound connection
// whose goroutine reads BeforeDialLoop or OnRejectLingerExit.
func SetTestKnobs(l *Link, k TestKnobs) {
	if k.SendQueueSize != 0 {
		l.sendQueueSize = k.SendQueueSize
	}
	if k.FirstFrameTimeout != 0 {
		l.firstFrameTimeout = k.FirstFrameTimeout
	}
	if k.HelloTimeout != 0 {
		l.helloTimeout = k.HelloTimeout
	}
	if k.RejectLinger != 0 {
		l.rejectLinger = k.RejectLinger
	}
	if k.MaxUnauthenticated != 0 {
		l.maxUnauthenticated = k.MaxUnauthenticated
	}
	if k.MaxStateFrames != 0 {
		l.maxStateFrames = k.MaxStateFrames
	}
	if k.MaxStateBytes != 0 {
		l.maxStateBytes = k.MaxStateBytes
	}
	if k.EnforcementMinTime != 0 {
		l.enforcementMinTime = k.EnforcementMinTime
	}
	if k.LocalNets != nil {
		l.localNets = k.LocalNets
	}
	if k.OnDial != nil {
		l.onDial = k.OnDial
	}
	if k.DialContext != nil {
		l.dialContext = k.DialContext
	}
	if k.BeforeDialLoop != nil {
		l.beforeDialLoop = k.BeforeDialLoop
	}
	if k.OnRejectLingerExit != nil {
		l.onRejectLingerExit = k.OnRejectLingerExit
	}
	if k.AfterHelloSent != nil {
		l.afterHelloSent = k.AfterHelloSent
	}
	if k.OnUnauthenticatedRead != nil {
		l.unauth.onRead = k.OnUnauthenticatedRead
	}
}
