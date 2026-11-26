package fakelocal

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// UpdateStatusAnswer returns an UpdateStatus handler that reports currentVersion.
func UpdateStatusAnswer(currentVersion string) func(context.Context, *localpb.UpdateStatusRequest) (*localpb.UpdateStatusResponse, error) {
	return func(context.Context, *localpb.UpdateStatusRequest) (*localpb.UpdateStatusResponse, error) {
		return &localpb.UpdateStatusResponse{CurrentVersion: currentVersion}, nil
	}
}

// UpdateStatusFailure returns an UpdateStatus handler that fails with code and message.
func UpdateStatusFailure(code codes.Code, message string) func(context.Context, *localpb.UpdateStatusRequest) (*localpb.UpdateStatusResponse, error) {
	return func(context.Context, *localpb.UpdateStatusRequest) (*localpb.UpdateStatusResponse, error) {
		return nil, status.Error(code, message)
	}
}

// ListedBroker is one Broker entry of a ListBrokers answer.
type ListedBroker struct {
	Alias  string
	IsPeer bool
}

// ListBrokersAnswer returns a ListBrokers handler that reports an empty local Broker state and one entry per broker.
func ListBrokersAnswer(brokers ...ListedBroker) func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
	return func(context.Context, *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
		resp := &localpb.ListBrokersResponse{Local: &localpb.LocalBrokerState{}}
		for _, b := range brokers {
			resp.Brokers = append(resp.Brokers, &localpb.BrokerInfo{Alias: b.Alias, IsPeer: b.IsPeer})
		}
		return resp, nil
	}
}
