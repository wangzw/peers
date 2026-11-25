package fakelocal

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// AuditRequests records the limit of every Audit call the fake receives; its
// methods are safe for concurrent use.
type AuditRequests struct {
	mu     sync.Mutex
	limits []uint32
}

func (r *AuditRequests) record(limit uint32) {
	r.mu.Lock()
	r.limits = append(r.limits, limit)
	r.mu.Unlock()
}

// Limits returns the limits of the Audit calls received so far, in arrival
// order.
func (r *AuditRequests) Limits() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]uint32, len(r.limits))
	copy(out, r.limits)
	return out
}

// RecordingHandlers returns handlers for the status, audit and config-set RPCs
// that call record with the method name before answering.
func RecordingHandlers(record func(method string)) Handlers {
	return Handlers{
		Status: func(context.Context, *localpb.StatusRequest) (*localpb.StatusResponse, error) {
			record("Status")
			return SampleStatus(), nil
		},
		SetConfig: func(context.Context, *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
			record("SetConfig")
			return &localpb.SetConfigResponse{}, nil
		},
		Audit: func(*localpb.AuditRequest, localpb.Local_AuditServer) error {
			record("Audit")
			return nil
		},
	}
}

// SampleStatus is the status a SampleHandlers server answers with: two
// registered agents and one peer whose link is backing off.
func SampleStatus() *localpb.StatusResponse {
	return &localpb.StatusResponse{
		Agents: []*localpb.AgentStatusEntry{
			{
				AgentId: "agent-alice",
				Account: "wangzw",
				Address: "alice@hostA",
				// Deliberately unsorted: the terminal sorts topics.
				Topics: []string{"topic:beta", "project:alpha"},
				Status: localpb.AgentStatus_AGENT_STATUS_IDLE,
			},
			{
				AgentId:      "agent-bob",
				Account:      "wangzw",
				Address:      "bob@hostA",
				Status:       localpb.AgentStatus_AGENT_STATUS_BUSY,
				PreviousName: "bob-old",
			},
		},
		Peers: []*localpb.BrokerInfo{
			{
				Alias:       "hostB",
				Fingerprint: sampleFingerprint(),
				AdvertisedAddresses: []*localpb.AdvertisedAddress{
					{Kind: "direct", Addr: "hostb.example:17443"},
				},
				IsPeer:    true,
				Status:    localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE,
				Role:      localpb.PeerRole_PEER_ROLE_ACCEPTOR,
				LinkState: localpb.LinkState_LINK_STATE_BACKOFF,
			},
		},
	}
}

func sampleFingerprint() []byte {
	return bytes.Repeat([]byte{0xcd}, 32)
}

// SampleAuditEvents returns five audit events in time order. The kinds and
// fields cover the line format: config values, a peer reference, a quoted
// detail and an assigned alias.
func SampleAuditEvents() []*localpb.AuditEvent {
	return []*localpb.AuditEvent{
		{
			Time:     sampleTime(9, 0),
			Event:    localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_SET,
			Actor:    "claude",
			Account:  "wangzw",
			Key:      "topics.auto_join",
			OldValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: false}},
			NewValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: true}},
		},
		{
			Time:    sampleTime(9, 5),
			Event:   localpb.AuditEventKind_AUDIT_EVENT_KIND_PAIRING_SUCCEEDED,
			Account: "wangzw",
			Peer: &localpb.PeerRef{
				Fingerprint: sampleFingerprint(),
				Alias:       "hostB",
			},
		},
		{
			Time:  sampleTime(9, 10),
			Event: localpb.AuditEventKind_AUDIT_EVENT_KIND_LINK_UP,
			Peer:  &localpb.PeerRef{Alias: "hostB"},
		},
		{
			Time:    sampleTime(9, 15),
			Event:   localpb.AuditEventKind_AUDIT_EVENT_KIND_CONNECTION_REJECTED,
			Account: "wangzw",
			Detail:  "peer refused: version mismatch",
		},
		{
			Time:     sampleTime(9, 20),
			Event:    localpb.AuditEventKind_AUDIT_EVENT_KIND_LOCAL_ALIAS_ASSIGNED,
			NewValue: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "hostA"}},
		},
	}
}

func sampleTime(hour, minute int) *timestamppb.Timestamp {
	return timestamppb.New(time.Date(2026, 11, 18, hour, minute, 0, 0, time.UTC))
}

// sampleStatusText is the exact terminal output for SampleStatus, which the cmd
// subprocess tests compare against.
const sampleStatusText = "" +
	"Agents (2):\n" +
	"  ADDRESS      ACCOUNT  STATUS  TOPICS                    PREVIOUS NAME\n" +
	"  alice@hostA  wangzw   idle    project:alpha,topic:beta  -\n" +
	"  bob@hostA    wangzw   busy    -                         bob-old\n" +
	"Peers (1):\n" +
	"  ALIAS  FINGERPRINT                                                       ROLE      STATUS       LINK     ADDRESSES                   DETAIL\n" +
	"  hostB  cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd  acceptor  connectable  backoff  direct hostb.example:17443  -\n"

// SampleStatusText is the terminal output the status command is expected to
// print for SampleStatus.
func SampleStatusText() string { return sampleStatusText }

// sampleAuditLines has one line per SampleAuditEvents entry, in the same
// order.
var sampleAuditLines = []string{
	"2026-11-18T09:00:00Z config_set actor=claude account=wangzw key=topics.auto_join old=false new=true\n",
	"2026-11-18T09:05:00Z pairing_succeeded account=wangzw peer=hostB(cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd)\n",
	"2026-11-18T09:10:00Z link_up peer=hostB\n",
	"2026-11-18T09:15:00Z connection_rejected account=wangzw detail=\"peer refused: version mismatch\"\n",
	"2026-11-18T09:20:00Z local_alias_assigned new=hostA\n",
}

// SampleAuditText is the terminal output the audit command is expected to
// print: the latest limit events, or every event when limit is zero.
func SampleAuditText(limit uint32) string {
	lines := sampleAuditLines
	if limit > 0 && int(limit) < len(lines) {
		lines = lines[len(lines)-int(limit):]
	}
	return strings.Join(lines, "")
}

// SampleHandlers returns handlers that answer Status with SampleStatus and
// Audit with the events of SampleAuditEvents. A zero limit sends every event;
// a nonzero limit sends only the latest n. The limits are recorded in audit
// when it is not nil. When auditErr is not nil, Audit records the limit and
// returns the error without sending an event.
func SampleHandlers(audit *AuditRequests, auditErr error) Handlers {
	return Handlers{
		Status: func(context.Context, *localpb.StatusRequest) (*localpb.StatusResponse, error) {
			return SampleStatus(), nil
		},
		Audit: func(req *localpb.AuditRequest, stream localpb.Local_AuditServer) error {
			if audit != nil {
				audit.record(req.GetLimit())
			}
			if auditErr != nil {
				return auditErr
			}

			events := SampleAuditEvents()
			if limit := req.GetLimit(); limit > 0 && int(limit) < len(events) {
				events = events[len(events)-int(limit):]
			}
			for _, ev := range events {
				if err := stream.Send(ev); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

// PermissionDenied builds the error a Broker returns when the caller lacks the
// permission for the call, carrying the system error code the terminal shows.
func PermissionDenied(msg string) error {
	return ErrorStatus(codes.PermissionDenied, &localpb.ErrorDetail{
		Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		Message: msg,
	})
}
