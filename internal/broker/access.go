package broker

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/helper"
	"github.com/wangzw/peers/internal/broker/localpb"
)

// Access is the level of permission an RPC needs on the Broker configuration
// file.
type Access uint8

const (
	// AccessNone marks an RPC that does not touch the configuration file.
	AccessNone Access = iota
	// AccessRead marks an RPC that reads the configuration file.
	AccessRead
	// AccessWrite marks an RPC that modifies the configuration file.
	AccessWrite
)

type accessChecker interface {
	// Check reports whether who may access path with need; an error means the
	// check itself failed, not a denial.
	Check(ctx context.Context, who Caller, path string, need Access) (bool, error)
}

type allowAllAccess struct{}

func (allowAllAccess) Check(context.Context, Caller, string, Access) (bool, error) {
	return true, nil
}

type accessRule struct {
	need         Access
	filterOnDeny bool
}

// methodAccess maps every RPC of the Local service to the permission it needs;
// it must stay consistent with the documented permission table, and a method
// missing here is answered internal.
var methodAccess = map[string]accessRule{
	localpb.Local_Hello_FullMethodName:            {need: AccessNone},
	localpb.Local_Register_FullMethodName:         {need: AccessNone},
	localpb.Local_ListAgents_FullMethodName:       {need: AccessNone},
	localpb.Local_Send_FullMethodName:             {need: AccessNone},
	localpb.Local_JoinTopic_FullMethodName:        {need: AccessNone},
	localpb.Local_LeaveTopic_FullMethodName:       {need: AccessNone},
	localpb.Local_ListTopics_FullMethodName:       {need: AccessNone},
	localpb.Local_ListBrokers_FullMethodName:      {need: AccessNone},
	localpb.Local_GetConfig_FullMethodName:        {need: AccessRead},
	localpb.Local_Audit_FullMethodName:            {need: AccessRead},
	localpb.Local_Status_FullMethodName:           {need: AccessRead, filterOnDeny: true},
	localpb.Local_UpdateStatus_FullMethodName:     {need: AccessNone},
	localpb.Local_ApplyUpdate_FullMethodName:      {need: AccessWrite},
	localpb.Local_SetConfig_FullMethodName:        {need: AccessWrite},
	localpb.Local_CreatePairingKey_FullMethodName: {need: AccessWrite},
	localpb.Local_Connect_FullMethodName:          {need: AccessWrite},
	localpb.Local_Disconnect_FullMethodName:       {need: AccessWrite},
}

type accessDecision struct {
	checker    accessChecker
	configPath func() string
}

func (d accessDecision) decide(ctx context.Context, fullMethod string) (context.Context, error) {
	rule, ok := methodAccess[fullMethod]
	if !ok {
		return ctx, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL, "no access rule for method %s", fullMethod))
	}
	if rule.need == AccessNone {
		return ctx, nil
	}
	who, ok := CallerFrom(ctx)
	if !ok {
		return ctx, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL, "caller identity is missing"))
	}
	path := d.configPath()
	granted, err := d.checker.Check(ctx, who, path, rule.need)
	if err != nil {
		if errors.Is(err, helper.ErrNoCheckSlot) {
			return ctx, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
				"Too many configuration access checks are running; try again shortly."))
		}
		return ctx, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL, "checking configuration file access failed: %v", err))
	}
	if granted {
		return withReadGranted(ctx, true), nil
	}
	if rule.filterOnDeny {
		return withReadGranted(ctx, false), nil
	}
	verb := "read"
	if rule.need == AccessWrite {
		verb = "write"
	}
	return ctx, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		"permission denied: account %s cannot %s the Broker configuration file %s", who.Username, verb, path))
}

func accessUnaryInterceptor(checker accessChecker, configPath func() string) grpc.UnaryServerInterceptor {
	d := accessDecision{checker: checker, configPath: configPath}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := d.decide(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func accessStreamInterceptor(checker accessChecker, configPath func() string) grpc.StreamServerInterceptor {
	d := accessDecision{checker: checker, configPath: configPath}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := d.decide(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &accessServerStream{ServerStream: ss, ctx: ctx})
	}
}

type accessServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *accessServerStream) Context() context.Context { return s.ctx }

type readGrantedContextKey struct{}

func withReadGranted(ctx context.Context, granted bool) context.Context {
	return context.WithValue(ctx, readGrantedContextKey{}, granted)
}

// readAccessGranted reports the recorded read decision; without one the caller
// is treated as allowed.
func readAccessGranted(ctx context.Context) bool {
	granted, ok := ctx.Value(readGrantedContextKey{}).(bool)
	if !ok {
		return true
	}
	return granted
}
