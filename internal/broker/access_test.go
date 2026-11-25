package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/helper"
	"github.com/wangzw/peers/internal/broker/localpb"
)

const accessTestConfigPath = "/fake/config.yaml"

func accessCallerContext(uid uint32, username string) context.Context {
	info := &callerAuthInfo{caller: Caller{UID: uid, GID: uid, Username: username}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: info})
}

type accessCheckCall struct {
	who  Caller
	path string
	need Access
}

type fakeAccessChecker struct {
	granted bool
	err     error
	calls   []accessCheckCall
}

func (c *fakeAccessChecker) Check(_ context.Context, who Caller, path string, need Access) (bool, error) {
	c.calls = append(c.calls, accessCheckCall{who: who, path: path, need: need})
	return c.granted, c.err
}

func accessErrorCode(t *testing.T, err error) localpb.ErrorCode {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error %v is not a gRPC status error", err)
	}
	for _, detail := range st.Details() {
		if d, ok := detail.(*localpb.ErrorDetail); ok {
			return d.Code
		}
	}
	t.Fatalf("error %v carries no *localpb.ErrorDetail", err)
	return localpb.ErrorCode_ERROR_CODE_UNSPECIFIED
}

type accessStreamStub struct {
	ctx context.Context
}

func (s *accessStreamStub) SetHeader(metadata.MD) error  { return nil }
func (s *accessStreamStub) SendHeader(metadata.MD) error { return nil }
func (s *accessStreamStub) SetTrailer(metadata.MD)       {}
func (s *accessStreamStub) Context() context.Context     { return s.ctx }
func (s *accessStreamStub) SendMsg(any) error            { return nil }
func (s *accessStreamStub) RecvMsg(any) error            { return nil }

func runAccessUnary(interceptor grpc.UnaryServerInterceptor, ctx context.Context, method string, handler grpc.UnaryHandler) error {
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
	return err
}

func runAccessStream(interceptor grpc.StreamServerInterceptor, ctx context.Context, method string, handler grpc.StreamHandler) error {
	return interceptor(nil, &accessStreamStub{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: method}, handler)
}

func accessPath() string { return accessTestConfigPath }

// TestMethodAccessCoversServiceDesc checks that every call in the Local service
// has an access rule and that the system checker accepts every rule that needs
// a permission.
func TestMethodAccessCoversServiceDesc(t *testing.T) {
	want := len(localpb.Local_ServiceDesc.Methods) + len(localpb.Local_ServiceDesc.Streams)
	if len(methodAccess) != want {
		t.Fatalf("methodAccess has %d entries, the service desc has %d calls", len(methodAccess), want)
	}
	for _, m := range localpb.Local_ServiceDesc.Methods {
		full := "/" + localpb.Local_ServiceDesc.ServiceName + "/" + m.MethodName
		if _, ok := methodAccess[full]; !ok {
			t.Errorf("methodAccess has no rule for %s", full)
		}
	}
	for _, s := range localpb.Local_ServiceDesc.Streams {
		full := "/" + localpb.Local_ServiceDesc.ServiceName + "/" + s.StreamName
		if _, ok := methodAccess[full]; !ok {
			t.Errorf("methodAccess has no rule for %s", full)
		}
	}

	t.Run("system mode", func(t *testing.T) {
		access := &fakeHelperAccess{}
		checker := systemChecker{c: access}
		wantCalls := 0
		for method, rule := range methodAccess {
			if rule.need == AccessNone {
				continue
			}
			wantCalls++
			if _, err := checker.Check(context.Background(), Caller{UID: 1001}, accessTestConfigPath, rule.need); err != nil {
				t.Errorf("%s: system checker rejected access %v: %v", method, rule.need, err)
			}
		}
		if calls := access.snapshot(); len(calls) != wantCalls {
			t.Errorf("system checker made %d calls, want one for each of the %d ruled methods", len(calls), wantCalls)
		}
	})
}

// TestAccessRulesMatchDesign checks the permission every Local RPC requires
// against the documented table.
func TestAccessRulesMatchDesign(t *testing.T) {
	want := map[string]accessRule{
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
	if len(methodAccess) != len(want) {
		t.Fatalf("methodAccess has %d entries, want %d", len(methodAccess), len(want))
	}
	for method, w := range want {
		got, ok := methodAccess[method]
		if !ok {
			t.Errorf("methodAccess has no rule for %s", method)
			continue
		}
		if got != w {
			t.Errorf("%s: rule = %+v, want %+v", method, got, w)
		}
	}
}

// TestNoneMethodsSkipChecker checks that calls needing no permission reach the
// handler without consulting the checker.
func TestNoneMethodsSkipChecker(t *testing.T) {
	none := []string{
		localpb.Local_Hello_FullMethodName,
		localpb.Local_Register_FullMethodName,
		localpb.Local_ListAgents_FullMethodName,
		localpb.Local_Send_FullMethodName,
		localpb.Local_JoinTopic_FullMethodName,
		localpb.Local_LeaveTopic_FullMethodName,
		localpb.Local_ListTopics_FullMethodName,
		localpb.Local_ListBrokers_FullMethodName,
		localpb.Local_UpdateStatus_FullMethodName,
	}
	checker := &fakeAccessChecker{err: errors.New("checker must not run")}
	handled := 0
	unaryHandler := func(context.Context, any) (any, error) {
		handled++
		return nil, nil
	}
	streamHandler := func(any, grpc.ServerStream) error {
		handled++
		return nil
	}

	for _, method := range none {
		handled = 0
		var err error
		if method == localpb.Local_Register_FullMethodName {
			err = runAccessStream(accessStreamInterceptor(checker, accessPath), context.Background(), method, streamHandler)
		} else {
			err = runAccessUnary(accessUnaryInterceptor(checker, accessPath), context.Background(), method, unaryHandler)
		}
		if err != nil {
			t.Errorf("%s: %v", method, err)
			continue
		}
		if handled != 1 {
			t.Errorf("%s: handler called %d times, want 1", method, handled)
		}
	}
	if len(checker.calls) != 0 {
		t.Errorf("checker called %d times for no-access methods, want 0", len(checker.calls))
	}
}

// TestWriteDeniedDoesNotReachHandler checks that a writing call without write
// permission is rejected and its handler does not run.
func TestWriteDeniedDoesNotReachHandler(t *testing.T) {
	methods := []string{
		localpb.Local_SetConfig_FullMethodName,
		localpb.Local_ApplyUpdate_FullMethodName,
		localpb.Local_CreatePairingKey_FullMethodName,
		localpb.Local_Connect_FullMethodName,
		localpb.Local_Disconnect_FullMethodName,
	}
	ctx := accessCallerContext(1000, "alice")
	checker := &fakeAccessChecker{granted: false}
	handled := 0
	handler := func(context.Context, any) (any, error) {
		handled++
		return nil, nil
	}

	for _, method := range methods {
		handled = 0
		checker.calls = nil
		err := runAccessUnary(accessUnaryInterceptor(checker, accessPath), ctx, method, handler)
		if err == nil {
			t.Fatalf("%s: call was allowed without write permission", method)
		}
		if code := accessErrorCode(t, err); code != localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
			t.Errorf("%s: code = %v, want PERMISSION_DENIED", method, code)
		}
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s: gRPC code = %v, want PermissionDenied", method, status.Code(err))
		}
		if !strings.Contains(err.Error(), "cannot write") {
			t.Errorf("%s: message %q does not name write permission", method, err)
		}
		if !strings.Contains(err.Error(), accessTestConfigPath) {
			t.Errorf("%s: message %q does not name the configuration file", method, err)
		}
		if handled != 0 {
			t.Errorf("%s: handler called %d times, want 0", method, handled)
		}
		if len(checker.calls) != 1 {
			t.Fatalf("%s: checker called %d times, want 1", method, len(checker.calls))
		}
		call := checker.calls[0]
		if call.need != AccessWrite {
			t.Errorf("%s: checker need = %v, want AccessWrite", method, call.need)
		}
		if call.who.UID != 1000 {
			t.Errorf("%s: checker saw uid %d, want 1000", method, call.who.UID)
		}
		if call.path != accessTestConfigPath {
			t.Errorf("%s: checker saw path %q, want %q", method, call.path, accessTestConfigPath)
		}
	}
}

// TestReadDenied checks that GetConfig and the Audit stream are rejected
// without read permission.
func TestReadDenied(t *testing.T) {
	ctx := accessCallerContext(1000, "alice")
	checker := &fakeAccessChecker{granted: false}
	handled := 0
	unaryHandler := func(context.Context, any) (any, error) {
		handled++
		return nil, nil
	}
	streamHandler := func(any, grpc.ServerStream) error {
		handled++
		return nil
	}

	handled = 0
	err := runAccessUnary(accessUnaryInterceptor(checker, accessPath), ctx, localpb.Local_GetConfig_FullMethodName, unaryHandler)
	if err == nil {
		t.Fatal("GetConfig was allowed without read permission")
	}
	if code := accessErrorCode(t, err); code != localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Errorf("GetConfig: code = %v, want PERMISSION_DENIED", code)
	}
	if !strings.Contains(err.Error(), "cannot read") {
		t.Errorf("GetConfig: message %q does not name read permission", err)
	}
	if handled != 0 {
		t.Errorf("GetConfig: handler called %d times, want 0", handled)
	}

	handled = 0
	err = runAccessStream(accessStreamInterceptor(checker, accessPath), ctx, localpb.Local_Audit_FullMethodName, streamHandler)
	if err == nil {
		t.Fatal("Audit was allowed without read permission")
	}
	if code := accessErrorCode(t, err); code != localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Errorf("Audit: code = %v, want PERMISSION_DENIED", code)
	}
	if !strings.Contains(err.Error(), "cannot read") {
		t.Errorf("Audit: message %q does not name read permission", err)
	}
	if handled != 0 {
		t.Errorf("Audit: handler called %d times, want 0", handled)
	}
}

// TestStatusDeniedIsFiltered checks that Status is answered without read
// permission and its handler sees the decision.
func TestStatusDeniedIsFiltered(t *testing.T) {
	for _, granted := range []bool{false, true} {
		t.Run(fmt.Sprintf("granted=%v", granted), func(t *testing.T) {
			checker := &fakeAccessChecker{granted: granted}
			handled := false
			var handlerCtx context.Context
			err := runAccessUnary(accessUnaryInterceptor(checker, accessPath),
				accessCallerContext(1000, "alice"),
				localpb.Local_Status_FullMethodName,
				func(ctx context.Context, _ any) (any, error) {
					handled = true
					handlerCtx = ctx
					return nil, nil
				})
			if err != nil {
				t.Fatalf("Status was rejected: %v", err)
			}
			if !handled {
				t.Fatal("Status handler was not called")
			}
			if got := readAccessGranted(handlerCtx); got != granted {
				t.Errorf("readAccessGranted = %v in the handler, want %v", got, granted)
			}
		})
	}
}

// TestCheckerErrorIsInternal checks that a failing checker is answered internal
// rather than permission_denied.
func TestCheckerErrorIsInternal(t *testing.T) {
	checker := &fakeAccessChecker{err: fmt.Errorf("helper: access check: %w", unix.EPERM)}
	handled := 0
	err := runAccessUnary(accessUnaryInterceptor(checker, accessPath),
		accessCallerContext(1000, "alice"),
		localpb.Local_GetConfig_FullMethodName,
		func(context.Context, any) (any, error) {
			handled++
			return nil, nil
		})
	if err == nil {
		t.Fatal("a failing checker let the call through")
	}
	if code := accessErrorCode(t, err); code != localpb.ErrorCode_ERROR_CODE_INTERNAL {
		t.Errorf("code = %v, want INTERNAL", code)
	}
	if status.Code(err) != codes.Internal {
		t.Errorf("gRPC code = %v, want Internal", status.Code(err))
	}
	if !strings.Contains(err.Error(), "checking configuration file access failed") {
		t.Errorf("message %q does not explain the check failure", err)
	}
	if handled != 0 {
		t.Errorf("handler called %d times, want 0", handled)
	}
}

// TestCheckerNoSlotIsQuotaExceeded checks that a checker out of slots is
// answered quota_exceeded, not internal.
func TestCheckerNoSlotIsQuotaExceeded(t *testing.T) {
	checker := &fakeAccessChecker{err: fmt.Errorf("helper: access check: %w", helper.ErrNoCheckSlot)}
	handled := 0
	err := runAccessUnary(accessUnaryInterceptor(checker, accessPath),
		accessCallerContext(1000, "alice"),
		localpb.Local_GetConfig_FullMethodName,
		func(context.Context, any) (any, error) {
			handled++
			return nil, nil
		})
	if err == nil {
		t.Fatal("a checker without a free slot let the call through")
	}
	if code := accessErrorCode(t, err); code != localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED {
		t.Errorf("code = %v, want QUOTA_EXCEEDED", code)
	}
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("gRPC code = %v, want ResourceExhausted", status.Code(err))
	}
	const want = "Too many configuration access checks are running; try again shortly."
	if !strings.Contains(err.Error(), want) {
		t.Errorf("message %q does not explain the quota", err)
	}
	if handled != 0 {
		t.Errorf("handler called %d times, want 0", handled)
	}
}

// TestCallerMissingIsInternal checks that a call needing a permission without a
// caller identity is answered internal before the checker runs.
func TestCallerMissingIsInternal(t *testing.T) {
	checker := &fakeAccessChecker{granted: true}
	handled := 0
	unaryHandler := func(context.Context, any) (any, error) {
		handled++
		return nil, nil
	}
	streamHandler := func(any, grpc.ServerStream) error {
		handled++
		return nil
	}
	methods := []struct {
		name   string
		stream bool
	}{
		{localpb.Local_GetConfig_FullMethodName, false},
		{localpb.Local_SetConfig_FullMethodName, false},
		{localpb.Local_Audit_FullMethodName, true},
	}

	for _, method := range methods {
		handled = 0
		var err error
		if method.stream {
			err = runAccessStream(accessStreamInterceptor(checker, accessPath), context.Background(), method.name, streamHandler)
		} else {
			err = runAccessUnary(accessUnaryInterceptor(checker, accessPath), context.Background(), method.name, unaryHandler)
		}
		if err == nil {
			t.Fatalf("%s: a call without a caller identity was allowed", method.name)
		}
		if code := accessErrorCode(t, err); code != localpb.ErrorCode_ERROR_CODE_INTERNAL {
			t.Errorf("%s: code = %v, want INTERNAL", method.name, code)
		}
		if status.Code(err) != codes.Internal {
			t.Errorf("%s: gRPC code = %v, want Internal", method.name, status.Code(err))
		}
		if !strings.Contains(err.Error(), "caller identity is missing") {
			t.Errorf("%s: message %q does not explain the missing caller", method.name, err)
		}
		if handled != 0 {
			t.Errorf("%s: handler called %d times, want 0", method.name, handled)
		}
	}
	if len(checker.calls) != 0 {
		t.Errorf("checker called %d times without a caller identity, want 0", len(checker.calls))
	}
}

// TestStreamHandlerGetsDecidedContext checks that the stream interceptor hands
// the handler the context the decision wrote, not the original stream context.
func TestStreamHandlerGetsDecidedContext(t *testing.T) {
	checker := &fakeAccessChecker{granted: true}
	handled := false
	granted := false
	ctx := withReadGranted(accessCallerContext(1000, "alice"), false)
	err := runAccessStream(accessStreamInterceptor(checker, accessPath), ctx,
		localpb.Local_Audit_FullMethodName,
		func(_ any, stream grpc.ServerStream) error {
			handled = true
			granted = readAccessGranted(stream.Context())
			return nil
		})
	if err != nil {
		t.Fatalf("Audit was rejected: %v", err)
	}
	if !handled {
		t.Fatal("Audit handler was not called")
	}
	if !granted {
		t.Error("the handler read a denied grant, want the decision the checker returned")
	}
	if len(checker.calls) != 1 {
		t.Fatalf("checker called %d times, want 1", len(checker.calls))
	}
}

// TestUnknownMethodIsInternal checks that a method outside the table is
// answered internal.
func TestUnknownMethodIsInternal(t *testing.T) {
	handled := 0
	err := runAccessUnary(accessUnaryInterceptor(&fakeAccessChecker{granted: true}, accessPath),
		context.Background(),
		"/internal.broker.localpb.Local/Bogus",
		func(context.Context, any) (any, error) {
			handled++
			return nil, nil
		})
	if err == nil {
		t.Fatal("a method outside the table was allowed")
	}
	if code := accessErrorCode(t, err); code != localpb.ErrorCode_ERROR_CODE_INTERNAL {
		t.Errorf("code = %v, want INTERNAL", code)
	}
	if !strings.Contains(err.Error(), "no access rule for method") {
		t.Errorf("message %q does not name the missing rule", err)
	}
	if handled != 0 {
		t.Errorf("handler called %d times, want 0", handled)
	}
}

// TestAllowAllAccess checks that the user Broker's checker allows both read and
// write for any path.
func TestAllowAllAccess(t *testing.T) {
	checker := allowAllAccess{}
	for _, need := range []Access{AccessRead, AccessWrite} {
		for _, path := range []string{accessTestConfigPath, "/does/not/exist.yaml"} {
			granted, err := checker.Check(context.Background(), Caller{UID: 1000}, path, need)
			if err != nil {
				t.Errorf("Check(need=%v, path=%s) error = %v", need, path, err)
			}
			if !granted {
				t.Errorf("Check(need=%v, path=%s) = false, want true", need, path)
			}
		}
	}
}
