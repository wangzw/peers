package broker

import (
	"context"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// versionUnaryInterceptor rejects every unary call except Hello on a
// connection that Hello marked rejected after a protocol version mismatch. It
// runs before the access interceptor in Deps.ServerOptions, so a rejected
// connection never reaches a permission check.
func versionUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod == localpb.Local_Hello_FullMethodName {
		return handler(ctx, req)
	}
	if ai, ok := authInfoFrom(ctx); ok && ai.rejected.Load() {
		return nil, rejectedErr(ai)
	}
	return handler(ctx, req)
}

// versionStreamInterceptor applies the same rule to streaming calls.
func versionStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if info.FullMethod == localpb.Local_Hello_FullMethodName {
		return handler(srv, ss)
	}
	if ai, ok := authInfoFrom(ss.Context()); ok && ai.rejected.Load() {
		return rejectedErr(ai)
	}
	return handler(srv, ss)
}

// rejectedErr is the version_mismatch error of a rejected connection. Both
// versions are the ones Hello recorded when it refused the connection.
func rejectedErr(ai *callerAuthInfo) error {
	return toStatus(versionMismatchError(ai.rejectedClientVersion.Load(), ai.rejectedBrokerVersion.Load()))
}
