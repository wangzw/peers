package broker

import (
	"context"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
)

func versionUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod == localpb.Local_Hello_FullMethodName {
		return handler(ctx, req)
	}
	if ai, ok := authInfoFrom(ctx); ok && ai.rejected.Load() {
		return nil, rejectedErr(ai)
	}
	return handler(ctx, req)
}

func versionStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if info.FullMethod == localpb.Local_Hello_FullMethodName {
		return handler(srv, ss)
	}
	if ai, ok := authInfoFrom(ss.Context()); ok && ai.rejected.Load() {
		return rejectedErr(ai)
	}
	return handler(srv, ss)
}

func rejectedErr(ai *callerAuthInfo) error {
	return toStatus(versionMismatchError(ai.rejectedClientVersion.Load(), ai.rejectedBrokerVersion.Load()))
}
