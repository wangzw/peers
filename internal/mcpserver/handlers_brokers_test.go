package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver/pairingprompt"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

func newBrokerHandlersForTest(t *testing.T, agentID string, h fakelocal.Handlers) (*handlers, *fakelocal.Server) {
	t.Helper()
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, h)

	cc, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing the fake Local service: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	st := &statusStore{}
	st.Update(func(s *Status) {
		s.Phase = PhaseConnected
		s.Client = localpb.NewLocalClient(cc)
		s.AgentID = agentID
	})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &handlers{st: st, log: log}, srv
}

func statusUnavailable() error {
	return status.Error(codes.Unavailable, "connection lost")
}

// TestListBrokers checks the fields the model reads for each Broker: status,
// is_peer, the local role and link state.
func TestListBrokers(t *testing.T) {
	h, _ := newBrokerHandlersForTest(t, "", fakelocal.Handlers{
		ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
			return &localpb.ListBrokersResponse{Local: &localpb.LocalBrokerState{}, Brokers: []*localpb.BrokerInfo{
				{
					Alias:       "peer1",
					Fingerprint: []byte{0xab, 0xcd, 0x01},
					AdvertisedAddresses: []*localpb.AdvertisedAddress{
						{Kind: "lan", Addr: "10.0.0.1:17443"},
						{Kind: "wan", Addr: "peer.example:17443"},
					},
					IsPeer:       true,
					Status:       localpb.BrokerStatus_BROKER_STATUS_CONNECTED,
					Role:         localpb.PeerRole_PEER_ROLE_DIALER,
					StatusDetail: "link is healthy",
					LinkState:    localpb.LinkState_LINK_STATE_ESTABLISHED,
				},
				{
					Alias:       "far",
					Fingerprint: []byte{0x02},
					IsPeer:      false,
					Role:        localpb.PeerRole_PEER_ROLE_ACCEPTOR,
					Status:      localpb.BrokerStatus_BROKER_STATUS_NOT_DIRECTLY_REACHABLE,
				},
			}}, nil
		},
	})

	out, err := h.ListBrokers(context.Background(), NoArgs{})
	if err != nil {
		t.Fatalf("ListBrokers: %v", err)
	}
	if len(out.Brokers) != 2 {
		t.Fatalf("got %d brokers, want 2", len(out.Brokers))
	}

	peer := out.Brokers[0]
	if peer.Alias != "peer1" {
		t.Errorf("alias = %q, want peer1", peer.Alias)
	}
	if peer.Fingerprint != "abcd01" {
		t.Errorf("fingerprint = %q, want abcd01", peer.Fingerprint)
	}
	if len(peer.Addresses) != 2 || peer.Addresses[0] != (AddressOut{Kind: "lan", Addr: "10.0.0.1:17443"}) {
		t.Errorf("addresses = %+v", peer.Addresses)
	}
	if !peer.IsPeer {
		t.Error("is_peer = false, want true")
	}
	if peer.Status != "connected" {
		t.Errorf("status = %q, want connected", peer.Status)
	}
	if peer.LocalRole != "dialer" {
		t.Errorf("role = %q, want dialer", peer.LocalRole)
	}
	if peer.LinkState != "established" {
		t.Errorf("link_state = %q, want established", peer.LinkState)
	}
	if peer.StatusDetail != "link is healthy" {
		t.Errorf("status_detail = %q", peer.StatusDetail)
	}

	far := out.Brokers[1]
	if far.Status != "not_directly_reachable" {
		t.Errorf("status = %q, want not_directly_reachable", far.Status)
	}
	if far.LocalRole != "" || far.LinkState != "" {
		t.Errorf("non-peer got role %q and link_state %q, want both empty", far.LocalRole, far.LinkState)
	}
	encoded, err := json.Marshal(far)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := fields["role"]; ok {
		t.Errorf("non-peer JSON contains role: %s", encoded)
	}
	if _, ok := fields["link_state"]; ok {
		t.Errorf("non-peer JSON contains link_state: %s", encoded)
	}
}

// TestListBrokersLocalState checks the local field: the answering Broker's own
// offline and degraded state, and an error when a response omits it.
func TestListBrokersLocalState(t *testing.T) {
	const reason = "listen address :17443 is unavailable"
	const fix = "peers config set broker.listen <free address>"
	cases := []struct {
		name    string
		local   *localpb.LocalBrokerState
		want    LocalBrokerOut
		wantErr bool
	}{
		{
			name:  "online",
			local: &localpb.LocalBrokerState{},
			want:  LocalBrokerOut{},
		},
		{
			name:  "offline",
			local: &localpb.LocalBrokerState{Offline: true},
			want:  LocalBrokerOut{Offline: true},
		},
		{
			name: "local only",
			local: &localpb.LocalBrokerState{Degraded: &localpb.DegradedState{
				Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
				Step:   "listen",
				Reason: reason,
				Fix:    fix,
			}},
			want: LocalBrokerOut{Degraded: &DegradedStateOut{
				Level:  "local_only",
				Reason: reason,
				Fix:    fix,
			}},
		},
		{
			name:    "missing local state",
			local:   nil,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newBrokerHandlersForTest(t, "", fakelocal.Handlers{
				ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
					return &localpb.ListBrokersResponse{Local: tc.local}, nil
				},
			})

			out, err := h.ListBrokers(context.Background(), NoArgs{})
			if tc.wantErr {
				const want = "list_brokers failed: the Broker did not report its local state (code: internal)"
				if err == nil {
					t.Fatal("ListBrokers returned no error without local state")
				}
				if err.Error() != want {
					t.Errorf("error = %q, want %q", err, want)
				}
				return
			}
			if err != nil {
				t.Fatalf("ListBrokers: %v", err)
			}
			if out.Local == nil {
				t.Fatal("local is nil, want the answering Broker's state")
			}
			if out.Local.Offline != tc.want.Offline {
				t.Errorf("offline = %v, want %v", out.Local.Offline, tc.want.Offline)
			}
			if tc.want.Degraded == nil {
				if out.Local.Degraded != nil {
					t.Errorf("degraded = %+v, want none", out.Local.Degraded)
				}
			} else if out.Local.Degraded == nil || *out.Local.Degraded != *tc.want.Degraded {
				t.Errorf("degraded = %+v, want %+v", out.Local.Degraded, tc.want.Degraded)
			}
		})
	}
}

// TestListBrokersBeforeRegistration checks that list_brokers reaches the
// Broker before the session has completed registration.
func TestListBrokersBeforeRegistration(t *testing.T) {
	h, srv := newBrokerHandlersForTest(t, "", fakelocal.Handlers{
		ListBrokers: func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
			return &localpb.ListBrokersResponse{Local: &localpb.LocalBrokerState{}}, nil
		},
	})

	if _, err := h.ListBrokers(context.Background(), NoArgs{}); err != nil {
		t.Fatalf("ListBrokers: %v", err)
	}

	calls := srv.Calls()
	if len(calls) != 1 || calls[0].Method != "ListBrokers" {
		t.Fatalf("calls = %+v, want one ListBrokers", calls)
	}
}

// TestConnectBrokerSuccess checks that connect_broker forwards the address, key
// and local alias and reports the alias, fingerprint and warnings it got back.
func TestConnectBrokerSuccess(t *testing.T) {
	var got *localpb.ConnectRequest
	h, _ := newBrokerHandlersForTest(t, "", fakelocal.Handlers{
		Connect: func(_ context.Context, req *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
			got = req
			return &localpb.ConnectResponse{
				Alias:       "peer",
				Fingerprint: []byte{0xde, 0xad},
				ShownHost:   "peer.example:17443",
				Warnings: []*localpb.Warning{
					{Message: "the fingerprint changed since the last pairing"},
				},
			}, nil
		},
	})

	out, err := h.ConnectBroker(context.Background(), ConnectBrokerIn{
		Address:    "peer.example:17443",
		Key:        "one-time-key",
		LocalAlias: "peer-alt",
	})
	if err != nil {
		t.Fatalf("ConnectBroker: %v", err)
	}

	if got == nil {
		t.Fatal("the Broker received no Connect request")
	}
	if got.GetAddress() != "peer.example:17443" || got.GetKey() != "one-time-key" || got.GetLocalAlias() != "peer-alt" {
		t.Errorf("request = %+v", got)
	}
	if got.GetAgentId() != "" {
		t.Errorf("agent_id = %q, want empty while unregistered", got.GetAgentId())
	}

	if out.Alias != "peer" || out.Fingerprint != "dead" || out.Host != "peer.example:17443" {
		t.Errorf("output = %+v", out)
	}
	if len(out.Warnings) != 1 || out.Warnings[0] != "the fingerprint changed since the last pairing" {
		t.Errorf("warnings = %v", out.Warnings)
	}
}

// TestConnectBrokerErrors checks the error text for a taken alias, a denied
// write and a lost connection.
func TestConnectBrokerErrors(t *testing.T) {
	tests := []struct {
		name    string
		rpcErr  error
		wantHas string
	}{
		{
			name: "needs local alias",
			rpcErr: fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_NEEDS_LOCAL_ALIAS,
				Message: "the peer alias is already taken",
			}),
			wantHas: "Not connected:",
		},
		{
			name: "permission denied",
			rpcErr: fakelocal.ErrorStatus(codes.PermissionDenied, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
				Message: "no write permission",
			}),
			wantHas: "write permission",
		},
		{
			name:    "lost contact",
			rpcErr:  statusUnavailable(),
			wantHas: "Not connected:",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newBrokerHandlersForTest(t, "", fakelocal.Handlers{
				Connect: func(context.Context, *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
					return nil, tc.rpcErr
				},
			})
			_, err := h.ConnectBroker(context.Background(), ConnectBrokerIn{Address: "a:1", Key: "k"})
			if err == nil {
				t.Fatal("ConnectBroker returned no error")
			}
			if !strings.Contains(err.Error(), tc.wantHas) {
				t.Errorf("error = %q, want %q", err, tc.wantHas)
			}
			if tc.name == "needs local alias" && !strings.Contains(err.Error(), "needs_local_alias") {
				t.Errorf("error = %q, want needs_local_alias", err)
			}
			if tc.name == "needs local alias" && !strings.Contains(err.Error(), "`local_alias` set to a new name") {
				t.Errorf("error = %q, want the local_alias hint", err)
			}
			if tc.name == "lost contact" && !strings.Contains(err.Error(), "The pairing may still have completed") {
				t.Errorf("error = %q, want the pairing-may-have-completed note", err)
			}
		})
	}
}

// TestConnectBrokerManagementAgentID checks that the management calls carry the
// registered agent id for the audit log.
func TestConnectBrokerManagementAgentID(t *testing.T) {
	var gotConnect *localpb.ConnectRequest
	var gotDisconnect *localpb.DisconnectRequest
	var gotKey *localpb.CreatePairingKeyRequest
	h, _ := newBrokerHandlersForTest(t, "agent-1", fakelocal.Handlers{
		Connect: func(_ context.Context, req *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
			gotConnect = req
			return &localpb.ConnectResponse{}, nil
		},
		Disconnect: func(_ context.Context, req *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error) {
			gotDisconnect = req
			return &localpb.DisconnectResponse{}, nil
		},
		CreatePairingKey: func(_ context.Context, req *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
			gotKey = req
			return &localpb.CreatePairingKeyResponse{Key: "k"}, nil
		},
	})

	if _, err := h.ConnectBroker(context.Background(), ConnectBrokerIn{Address: "a:1", Key: "k"}); err != nil {
		t.Fatalf("ConnectBroker: %v", err)
	}
	if _, err := h.DisconnectBroker(context.Background(), DisconnectBrokerIn{Broker: "peer"}); err != nil {
		t.Fatalf("DisconnectBroker: %v", err)
	}
	if _, err := h.CreatePairingKey(context.Background(), NoArgs{}); err != nil {
		t.Fatalf("CreatePairingKey: %v", err)
	}

	if gotConnect.GetAgentId() != "agent-1" {
		t.Errorf("Connect agent_id = %q, want agent-1", gotConnect.GetAgentId())
	}
	if gotDisconnect.GetAgentId() != "agent-1" {
		t.Errorf("Disconnect agent_id = %q, want agent-1", gotDisconnect.GetAgentId())
	}
	if gotKey.GetAgentId() != "agent-1" {
		t.Errorf("CreatePairingKey agent_id = %q, want agent-1", gotKey.GetAgentId())
	}
}

// TestDisconnectBroker checks that disconnect_broker forwards the alias,
// reports the unpaired result and keeps the not-found code.
func TestDisconnectBroker(t *testing.T) {
	var got *localpb.DisconnectRequest
	h, _ := newBrokerHandlersForTest(t, "", fakelocal.Handlers{
		Disconnect: func(_ context.Context, req *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error) {
			got = req
			return &localpb.DisconnectResponse{}, nil
		},
	})

	out, err := h.DisconnectBroker(context.Background(), DisconnectBrokerIn{Broker: "peer1"})
	if err != nil {
		t.Fatalf("DisconnectBroker: %v", err)
	}
	if out.Result != "Unpaired peer1; the link is closed." {
		t.Errorf("result = %q", out.Result)
	}
	if got.GetBroker() != "peer1" {
		t.Errorf("request broker = %q, want peer1", got.GetBroker())
	}

	h, _ = newBrokerHandlersForTest(t, "", fakelocal.Handlers{
		Disconnect: func(context.Context, *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error) {
			return nil, fakelocal.ErrorStatus(codes.NotFound, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
				Message: "no such peer",
			})
		},
	})
	if _, err := h.DisconnectBroker(context.Background(), DisconnectBrokerIn{Broker: "nobody"}); err == nil {
		t.Fatal("DisconnectBroker returned no error")
	} else if !strings.Contains(err.Error(), "(code: not_found)") {
		t.Errorf("error = %q, want (code: not_found)", err)
	} else if !strings.Contains(err.Error(), "list_brokers shows the paired Brokers") {
		t.Errorf("error = %q, want the list_brokers hint", err)
	}
}

// TestCreatePairingKey checks the key, expiry, shown addresses, note and
// connect prompt of a create_pairing_key result.
func TestCreatePairingKey(t *testing.T) {
	expires := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	h, _ := newBrokerHandlersForTest(t, "", fakelocal.Handlers{
		CreatePairingKey: func(context.Context, *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
			return &localpb.CreatePairingKeyResponse{
				Key:            "key-123",
				ExpiresAt:      timestamppb.New(expires),
				ShownAddresses: []string{"host.example:17443", "10.0.0.5:17443"},
			}, nil
		},
	})

	out, err := h.CreatePairingKey(context.Background(), NoArgs{})
	if err != nil {
		t.Fatalf("CreatePairingKey: %v", err)
	}
	if out.Key != "key-123" {
		t.Errorf("key = %q", out.Key)
	}
	if out.ExpiresAt != "2026-01-02T03:04:05Z" {
		t.Errorf("expires_at = %q, want 2026-01-02T03:04:05Z", out.ExpiresAt)
	}
	if len(out.Addresses) != 2 {
		t.Fatalf("addresses = %v, want two", out.Addresses)
	}
	for _, addr := range out.Addresses {
		if strings.Contains(out.Key, addr) {
			t.Errorf("key %q contains address %q", out.Key, addr)
		}
	}
	const wantNote = "Show the key and the connect prompt to the user only, and never send either through send_message, Claude Code's built-in session messaging or any other agent-to-agent channel."
	if out.Note != wantNote {
		t.Errorf("note = %q, want %q", out.Note, wantNote)
	}
	if want := pairingprompt.ConnectPrompt(out.Addresses, out.Key); out.ConnectPrompt != want {
		t.Errorf("connect_prompt = %q, want the generator output %q", out.ConnectPrompt, want)
	}
	if !strings.Contains(out.ConnectPrompt, out.Key) {
		t.Errorf("connect_prompt = %q, want it to name the key %q", out.ConnectPrompt, out.Key)
	}
	for _, addr := range out.Addresses {
		if !strings.Contains(out.ConnectPrompt, addr) {
			t.Errorf("connect_prompt = %q, want it to name address %q", out.ConnectPrompt, addr)
		}
	}
}

// TestCreatePairingKeyNoAddress checks the note and empty connect prompt when
// the Broker has no address to show.
func TestCreatePairingKeyNoAddress(t *testing.T) {
	h, _ := newBrokerHandlersForTest(t, "", fakelocal.Handlers{
		CreatePairingKey: func(context.Context, *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
			return &localpb.CreatePairingKeyResponse{Key: "key-123"}, nil
		},
	})

	out, err := h.CreatePairingKey(context.Background(), NoArgs{})
	if err != nil {
		t.Fatalf("CreatePairingKey: %v", err)
	}
	const wantNote = "This Broker has no address to show; tell the other side this host's address yourself. Show the key to the user only, and never send it through send_message, Claude Code's built-in session messaging or any other agent-to-agent channel."
	if out.Note != wantNote {
		t.Errorf("note = %q, want %q", out.Note, wantNote)
	}
	if out.ConnectPrompt != "" {
		t.Errorf("connect_prompt = %q, want none without an address", out.ConnectPrompt)
	}
}

// TestCreatePairingKeyNotListening checks the error and hint when the Broker
// does not listen.
func TestCreatePairingKeyNotListening(t *testing.T) {
	h, _ := newBrokerHandlersForTest(t, "", fakelocal.Handlers{
		CreatePairingKey: func(context.Context, *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
			return nil, fakelocal.ErrorStatus(codes.InvalidArgument, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
				Message: "this Broker does not listen",
			})
		},
	})

	_, err := h.CreatePairingKey(context.Background(), NoArgs{})
	if err == nil {
		t.Fatal("CreatePairingKey returned no error")
	}
	const wantNote = "Ask the user to get a pairing key from the other host's user, then connect with connect_broker. Do not ask another agent for a pairing key, and pass it only through the user, never through send_message, Claude Code's built-in session messaging or any other agent-to-agent channel."
	if !strings.Contains(err.Error(), wantNote) {
		t.Errorf("error = %q, want %q", err, wantNote)
	}
	if !strings.Contains(err.Error(), "(code: invalid_argument)") {
		t.Errorf("error = %q, want (code: invalid_argument)", err)
	}
}

// TestBrokerToolCallTimeouts checks that the broker tools return a tool error
// instead of waiting for a Broker that never answers.
func TestBrokerToolCallTimeouts(t *testing.T) {
	t.Parallel()

	blocked := func(ctx context.Context) error {
		<-ctx.Done()
		return status.Error(codes.DeadlineExceeded, "the Broker did not answer")
	}

	t.Run("list_brokers", func(t *testing.T) {
		t.Parallel()
		h, _ := newBrokerHandlersForTest(t, "agent-1", fakelocal.Handlers{
			ListBrokers: func(ctx context.Context, _ *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
				return nil, blocked(ctx)
			},
		})
		h.rpcTimeout = handlerTimeoutSeam

		elapsed, err := waitBoundedCall(t, "ListBrokers", func() error {
			_, err := h.ListBrokers(context.Background(), NoArgs{})
			return err
		})
		checkBoundedError(t, "ListBrokers", err, elapsed, "list_brokers failed", "(code: internal)")
	})

	t.Run("disconnect_broker", func(t *testing.T) {
		t.Parallel()
		h, _ := newBrokerHandlersForTest(t, "agent-1", fakelocal.Handlers{
			Disconnect: func(ctx context.Context, _ *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error) {
				return nil, blocked(ctx)
			},
		})
		h.rpcTimeout = handlerTimeoutSeam

		elapsed, err := waitBoundedCall(t, "DisconnectBroker", func() error {
			_, err := h.DisconnectBroker(context.Background(), DisconnectBrokerIn{Broker: "peer1"})
			return err
		})
		checkBoundedError(t, "DisconnectBroker", err, elapsed, "disconnect_broker failed", "(code: internal)")
	})

	t.Run("create_pairing_key", func(t *testing.T) {
		t.Parallel()
		h, _ := newBrokerHandlersForTest(t, "agent-1", fakelocal.Handlers{
			CreatePairingKey: func(ctx context.Context, _ *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
				return nil, blocked(ctx)
			},
		})
		h.rpcTimeout = handlerTimeoutSeam

		elapsed, err := waitBoundedCall(t, "CreatePairingKey", func() error {
			_, err := h.CreatePairingKey(context.Background(), NoArgs{})
			return err
		})
		checkBoundedError(t, "CreatePairingKey", err, elapsed, "create_pairing_key failed", "(code: internal)")
	})

	t.Run("connect_broker", func(t *testing.T) {
		t.Parallel()
		h, _ := newBrokerHandlersForTest(t, "agent-1", fakelocal.Handlers{
			Connect: func(ctx context.Context, _ *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
				return nil, blocked(ctx)
			},
		})
		h.connectTimeout = handlerTimeoutSeam

		elapsed, err := waitBoundedCall(t, "ConnectBroker", func() error {
			_, err := h.ConnectBroker(context.Background(), ConnectBrokerIn{Address: "host:17443", Key: "k"})
			return err
		})
		checkBoundedError(t, "ConnectBroker", err, elapsed, "Not connected", "(code: internal)")
	})
}
