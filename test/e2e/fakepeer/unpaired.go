//go:build e2e

package main

import (
	"context"
	"crypto/rand"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/internal/link/linkpb"
)

// dialUnpaired sends a Hello first so the peer treats the stream as a link
// connection, then pushes the whole record set.
func dialUnpaired(ctx context.Context, open func(context.Context) (linkpb.Link_ConnectClient, error), set *recordSet) string {
	stream, err := open(ctx)
	if err != nil {
		return err.Error()
	}

	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return "nonce: " + err.Error()
	}
	hello := &linkpb.Hello{
		ProtocolVersion: linkpb.ProtocolVersion,
		Alias:           "fakepeer",
		Nonce:           nonce,
	}
	frames := []*linkpb.Frame{
		{Kind: linkpb.KindHello, Payload: marshal(hello)},
		{Kind: directorypb.KindRecords, Payload: marshal(&directorypb.Records{Records: set.all()})},
		{Kind: directorypb.KindRecordList, Payload: marshal(set.list())},
	}
	for _, f := range frames {
		if err := stream.Send(f); err != nil {
			break
		}
	}

	reason := ""
	var lastErr error
	for {
		f, err := stream.Recv()
		if err != nil {
			lastErr = err
			break
		}
		if f.GetKind() != linkpb.KindReject {
			continue
		}
		reject := new(linkpb.Reject)
		if err := proto.Unmarshal(f.GetPayload(), reject); err != nil {
			continue
		}
		reason = linkpb.RejectReason_name[int32(reject.GetReason())]
	}
	if reason != "" {
		return reason
	}
	if lastErr == nil {
		lastErr = errors.New("stream ended without an error")
	}
	return lastErr.Error()
}
