package helperpb_test

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/helperpb"
)

// TestDelimitedRoundTrip checks that delimited helper messages survive a round
// trip and that a read past the end reports io.EOF.
func TestDelimitedRoundTrip(t *testing.T) {
	want := []*helperpb.HelperMessage{
		{
			Body: &helperpb.HelperMessage_Ready{
				Ready: &helperpb.Ready{ProtocolVersion: helperpb.ProtocolVersion},
			},
		},
		{
			Body: &helperpb.HelperMessage_Response{
				Response: &helperpb.HelperResponse{
					Id: 7,
					Result: &helperpb.HelperResponse_Session{
						Session: &helperpb.Session{Pid: 9},
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	for _, msg := range want {
		if _, err := protodelim.MarshalTo(&buf, msg); err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}
	}

	r := bufio.NewReader(&buf)
	for i, wantMsg := range want {
		got := &helperpb.HelperMessage{}
		if err := protodelim.UnmarshalFrom(r, got); err != nil {
			t.Fatalf("UnmarshalFrom message %d: %v", i, err)
		}
		if !proto.Equal(got, wantMsg) {
			t.Fatalf("message %d round trip mismatch:\n got: %v\nwant: %v", i, got, wantMsg)
		}
	}

	if err := protodelim.UnmarshalFrom(r, &helperpb.HelperMessage{}); err != io.EOF {
		t.Fatalf("UnmarshalFrom past end: got %v, want io.EOF", err)
	}
}

// TestProtocolVersionIsOne pins the helper pipe protocol version at 1.
func TestProtocolVersionIsOne(t *testing.T) {
	if helperpb.ProtocolVersion != 1 {
		t.Fatalf("ProtocolVersion = %d, want 1", helperpb.ProtocolVersion)
	}
}
