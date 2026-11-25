package mcpserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// TestRestoreOnceReturnsContextError stops with the context's error before
// touching the Broker.
func TestRestoreOnceReturnsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &restorer{mem: &topicMemory{}}
	outcome, err := r.restoreOnce(ctx, nil, "agent-1", "x")
	if outcome != restoreStop || !errors.Is(err, context.Canceled) {
		t.Fatalf("restoreOnce = (%v, %v), want (restoreStop, context.Canceled)", outcome, err)
	}
}

// TestRestoreFallbackOutcomes chooses the next step and remembered topic from
// the private fallback's failure.
func TestRestoreFallbackOutcomes(t *testing.T) {
	const topic = "x"
	tests := []struct {
		name         string
		second       error
		want         restoreOutcome
		wantRemember []rememberedTopic
	}{
		{
			name:         "connection error",
			second:       status.Error(codes.Unavailable, "connection lost"),
			want:         restoreStop,
			wantRemember: []rememberedTopic{{Name: topic, Public: true, Description: "d"}},
		},
		{
			name: "quota",
			second: fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
				Message: "too many topics",
			}),
			want:         restoreQuota,
			wantRemember: []rememberedTopic{{Name: topic, Public: false, Description: "d"}},
		},
		{
			name: "registration gone",
			second: fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED,
				Message: "registration gone",
			}),
			want:         restoreStop,
			wantRemember: []rememberedTopic{{Name: topic, Public: true, Description: "d"}},
		},
		{
			name: "permanent refusal",
			second: fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
				Message: "unknown topic",
			}),
			want:         restoreFinished,
			wantRemember: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sock := fakelocal.SocketPath(t)
			srv := fakelocal.Start(t, sock, fakelocal.Handlers{
				JoinTopic: func(_ context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
					if req.GetIsPublic() {
						return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
							Code:    localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
							Message: "broker is not public",
						})
					}
					return nil, tt.second
				},
			})

			mem := &topicMemory{}
			mem.Remember(rememberedTopic{Name: topic, Public: true, Description: "d"})
			r := &restorer{mem: mem, log: topicsLogger()}

			outcome, err := r.restoreOnce(context.Background(), topicsTestClient(t, sock), "agent-1", topic)
			if outcome != tt.want {
				t.Fatalf("restoreOnce outcome = %v, want %v (err %v)", outcome, tt.want, err)
			}
			got := mem.List()
			if len(got) != len(tt.wantRemember) {
				t.Fatalf("memory = %+v, want %+v", got, tt.wantRemember)
			}
			for i := range got {
				if got[i] != tt.wantRemember[i] {
					t.Fatalf("memory = %+v, want %+v", got, tt.wantRemember)
				}
			}
			calls := srv.Calls()
			if len(calls) != 2 {
				t.Fatalf("server calls = %d, want the public attempt and the private fallback", len(calls))
			}
			publicReq, fallbackReq := calls[0].Request.(*localpb.JoinTopicRequest), calls[1].Request.(*localpb.JoinTopicRequest)
			if !publicReq.GetIsPublic() || fallbackReq.GetIsPublic() {
				t.Fatalf("join requests = %v, want the public form then the private fallback", calls)
			}
		})
	}
}

// TestWaitForUsesARealTimer waits the full duration on the real timer.
func TestWaitForUsesARealTimer(t *testing.T) {
	const d = 20 * time.Millisecond
	r := &restorer{}
	start := time.Now()
	if err := r.waitFor(context.Background(), d); err != nil {
		t.Fatalf("waitFor = %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed < d {
		t.Fatalf("waitFor returned after %v, want at least the %v it was asked to wait", elapsed, d)
	}
}

// TestWaitForReturnsContextError returns the context's error when it ends first.
func TestWaitForReturnsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &restorer{}
	if err := r.waitFor(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitFor = %v, want context.Canceled", err)
	}
}
