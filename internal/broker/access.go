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

// accessChecker decides whether a local caller may make an RPC that needs the
// given permission on the Broker configuration file. The user Broker
// uses allowAllAccess; the system Broker supplies a checker that inspects
// the ownership and mode of the file.
type accessChecker interface {
	// Check reports whether who may access path with need. An error means the
	// check itself failed; the caller answers internal and does not proceed.
	Check(ctx context.Context, who Caller, path string, need Access) (bool, error)
}

// allowAllAccess is the checker for the user Broker. The configuration file belongs to
// the account, so every local caller is allowed; it never reads the file.
type allowAllAccess struct{}

func (allowAllAccess) Check(context.Context, Caller, string, Access) (bool, error) {
	return true, nil
}

// accessRule is what every call to one RPC needs.
type accessRule struct {
	need Access
	// filterOnDeny marks an RPC that is not rejected without permission but
	// answers a reduced result instead. Status uses it to return only the
	// caller's own agents. The interceptor records the decision on the context
	// so the handler can read it with readAccessGranted.
	filterOnDeny bool
}

// methodAccess maps every RPC of the Local service, by full method name, to the
// permission it needs. design-broker.md:59-66 is the source of this table; a
// method missing here is answered internal.
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

// accessDecision is the part shared by the two interceptors.
type accessDecision struct {
	checker    accessChecker
	configPath func() string
}

// decide applies methodAccess to one call. It returns the context to pass to
// the handler; a denial that must not reach the handler is returned as an
// error instead.
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
			// The Broker is running and its check slots are full, so the call
			// is refused by a quota, not broken. The model sees a sentence it
			// can act on and a code that says retrying will work.
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

// accessUnaryInterceptor returns the interceptor that applies methodAccess to
// every unary call. configPath is called per decision so a path that changes at
// runtime is still current.
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

// accessStreamInterceptor returns the interceptor that applies methodAccess to
// every stream call, including the server side of Register and Audit.
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

// accessServerStream hands the handler the context decide returned.
type accessServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context implements grpc.ServerStream.
func (s *accessServerStream) Context() context.Context { return s.ctx }

// readGrantedContextKey is the context key of the access decision.
type readGrantedContextKey struct{}

// withReadGranted records the read decision on ctx. The interceptor writes it;
// tests that call a handler directly use it to stand in for the interceptor.
func withReadGranted(ctx context.Context, granted bool) context.Context {
	return context.WithValue(ctx, readGrantedContextKey{}, granted)
}

// readAccessGranted reports the read decision recorded by the interceptor.
// Without a recorded decision the caller is treated as allowed, which is what
// an RPC that needs no permission gets.
func readAccessGranted(ctx context.Context) bool {
	granted, ok := ctx.Value(readGrantedContextKey{}).(bool)
	if !ok {
		return true
	}
	return granted
}
