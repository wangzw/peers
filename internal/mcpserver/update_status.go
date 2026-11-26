package mcpserver

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
)

var errNoBrokerConnection = errors.New("the Broker connection is not ready")

func brokerUpdateStatus(st *statusStore) updateStatusSource {
	return func(ctx context.Context) (updateStatus, error) {
		conn := st.Get().Conn
		if conn == nil {
			return updateStatus{}, errNoBrokerConnection
		}
		return queryUpdateStatus(ctx, conn)
	}
}

func queryUpdateStatus(ctx context.Context, conn *grpc.ClientConn) (updateStatus, error) {
	resp, err := localpb.NewLocalClient(conn).UpdateStatus(ctx, &localpb.UpdateStatusRequest{})
	if err != nil {
		return updateStatus{}, err
	}
	return updateStatus{
		CurrentVersion:   resp.GetCurrentVersion(),
		AvailableVersion: resp.GetAvailableVersion(),
		ReleaseURL:       resp.GetReleaseUrl(),
	}, nil
}
