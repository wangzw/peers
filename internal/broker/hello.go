package broker

import (
	"context"
	"log/slog"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// Hello performs the Local protocol version handshake. The request's version
// must equal the Broker's local protocol version; on a mismatch it returns
// version_mismatch, marks the connection rejected so every later call except
// Hello fails, logs the mismatch and keeps the connection open.
func (b *Broker) Hello(ctx context.Context, req *localpb.HelloRequest) (*localpb.HelloResponse, error) {
	return answerHello(ctx, b.deps.LocalProtocolVersion, req.GetProtocolVersion(), b.log)
}

func answerHello(ctx context.Context, v, got uint32, log *slog.Logger) (*localpb.HelloResponse, error) {
	if got == v {
		return &localpb.HelloResponse{ProtocolVersion: v}, nil
	}

	caller, _ := CallerFrom(ctx)
	log.Warn(EvHelloVersionMismatch,
		"client_version", got,
		"broker_version", v,
		"uid", caller.UID,
	)
	if ai, ok := authInfoFrom(ctx); ok {
		// Store the versions before the flag: a version interceptor that sees
		// rejected set must be able to read both.
		ai.rejectedClientVersion.Store(got)
		ai.rejectedBrokerVersion.Store(v)
		ai.rejected.Store(true)
	}
	return nil, toStatus(versionMismatchError(got, v))
}

func versionMismatchError(got, brokerVersion uint32) *Error {
	return Errorf(localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
		"local protocol version %d is not supported by this Broker (version %d); restart the Broker", got, brokerVersion)
}
