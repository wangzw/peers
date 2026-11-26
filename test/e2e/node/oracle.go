package node

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// Oracle is a direct client of a Broker's local interface, used to read Broker
// state without being an Agent.
type Oracle struct {
	conn   *grpc.ClientConn
	client localpb.LocalClient
}

// DialOracle connects to the Broker listening on socket. The connection is
// lazy: errors for a Broker that is not up yet surface on the first call.
func DialOracle(socket string) (*Oracle, error) {
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("node: dialing broker at %s: %w", socket, err)
	}
	return &Oracle{conn: conn, client: localpb.NewLocalClient(conn)}, nil
}

// Hello calls the Hello RPC with the current local protocol version.
func (o *Oracle) Hello(ctx context.Context) (*localpb.HelloResponse, error) {
	return o.client.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion})
}

// ListBrokers returns the Brokers known to this Broker.
func (o *Oracle) ListBrokers(ctx context.Context) (*localpb.ListBrokersResponse, error) {
	return o.client.ListBrokers(ctx, &localpb.ListBrokersRequest{})
}

// ListTopics returns the topics the agent with agentID joined and the public
// topics it has not joined.
func (o *Oracle) ListTopics(ctx context.Context, agentID string) (*localpb.ListTopicsResponse, error) {
	return o.client.ListTopics(ctx, &localpb.ListTopicsRequest{AgentId: agentID})
}

// CreatePairingKey creates a one-time pairing key on this Broker.
func (o *Oracle) CreatePairingKey(ctx context.Context) (*localpb.CreatePairingKeyResponse, error) {
	return o.client.CreatePairingKey(ctx, &localpb.CreatePairingKeyRequest{})
}

// Connect pairs this Broker with the Broker at address using key. localAlias
// may be empty.
func (o *Oracle) Connect(ctx context.Context, address, key, localAlias string) (*localpb.ConnectResponse, error) {
	return o.client.Connect(ctx, &localpb.ConnectRequest{
		Address:    address,
		Key:        key,
		LocalAlias: localAlias,
	})
}

// Status returns the Broker's status.
func (o *Oracle) Status(ctx context.Context) (*localpb.StatusResponse, error) {
	return o.client.Status(ctx, &localpb.StatusRequest{})
}

// UpdateStatus returns the version the Broker runs and the release it knows.
func (o *Oracle) UpdateStatus(ctx context.Context) (*localpb.UpdateStatusResponse, error) {
	return o.client.UpdateStatus(ctx, &localpb.UpdateStatusRequest{})
}

// GetConfig returns the configuration entry for key, or every entry when key
// is empty.
func (o *Oracle) GetConfig(ctx context.Context, key string) (*localpb.GetConfigResponse, error) {
	return o.client.GetConfig(ctx, &localpb.GetConfigRequest{Key: key})
}

// Audit reads the audit stream to its end and returns the events in the order
// the Broker sent them. A nil request reads with no filter.
func (o *Oracle) Audit(ctx context.Context, req *localpb.AuditRequest) ([]*localpb.AuditEvent, error) {
	if req == nil {
		req = &localpb.AuditRequest{}
	}
	stream, err := o.client.Audit(ctx, req)
	if err != nil {
		return nil, err
	}
	var events []*localpb.AuditEvent
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return events, nil
		}
		if err != nil {
			return events, err
		}
		events = append(events, event)
	}
}

// Close closes the connection.
func (o *Oracle) Close() error {
	if o.conn == nil {
		return nil
	}
	return o.conn.Close()
}

// ErrorDetailOf returns the ErrorDetail carried in a gRPC status, or nil when
// the error is not a status or carries no ErrorDetail.
func ErrorDetailOf(err error) *localpb.ErrorDetail {
	if err == nil {
		return nil
	}
	st, _ := status.FromError(err)
	for _, detail := range st.Details() {
		if errorDetail, ok := detail.(*localpb.ErrorDetail); ok {
			return errorDetail
		}
	}
	return nil
}
