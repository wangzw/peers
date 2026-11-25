package broker

import (
	"crypto/rand"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/wangzw/peers/internal/broker/deliverpb"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/link"
)

type deliverRequest struct {
	MessageID, SenderName, SenderRef, TargetName, TargetRef string
	Remaining                                               time.Duration
	Text                                                    string
}

const (
	uuidLen   = 36
	minRefLen = 6
	maxRefLen = 12
	// maxNameBytes bounds the raw sender and target names a peer may put in a
	// delivery request.
	maxNameBytes = 1024
)

func newMessageID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on supported platforms; a fallback would
		// defeat dedupe.
		panic("broker: crypto/rand failed: " + err.Error())
	}
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// isUUID accepts the 8-4-4-4-12 hex shape in either case; the version and
// variant bits are not checked.
func isUUID(s string) bool {
	if len(s) != uuidLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch i {
		case 8, 13, 18, 23:
			if s[i] != '-' {
				return false
			}
		default:
			if !isHexDigit(s[i]) {
				return false
			}
		}
	}
	return true
}

// isRefSyntax accepts 6 to 12 lowercase hex characters; a peer value must pass
// it before reaching FormatAddress, which panics on a malformed ref.
func isRefSyntax(s string) bool {
	if len(s) < minRefLen || len(s) > maxRefLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func encodeDeliverRequest(r deliverRequest) (link.Frame, error) {
	msg := &deliverpb.DeliverRequest{
		MessageId:       r.MessageID,
		SenderName:      r.SenderName,
		SenderRef:       r.SenderRef,
		TargetName:      r.TargetName,
		TargetRef:       r.TargetRef,
		RemainingBudget: durationpb.New(r.Remaining),
		Body: &deliverpb.Message{
			MessageId: r.MessageID,
			Role:      deliverpb.Role_ROLE_USER,
			Parts:     []*deliverpb.Part{{Content: &deliverpb.Part_Text{Text: r.Text}}},
		},
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return link.Frame{}, fmt.Errorf("cannot encode delivery request: %w", err)
	}
	return link.Frame{Kind: link.FrameKind(deliverpb.KindDeliver), Payload: payload}, nil
}

// decodeDeliverRequest validates and unpacks a kind-400 frame; every failure is
// invalid_argument with the "Not sent" prefix, and the role is not checked.
func decodeDeliverRequest(f link.Frame) (deliverRequest, *Error) {
	if f.Kind != link.FrameKind(deliverpb.KindDeliver) {
		return deliverRequest{}, badDeliverRequest("unexpected frame kind %d, want %d", f.Kind, deliverpb.KindDeliver)
	}
	msg := new(deliverpb.DeliverRequest)
	if err := proto.Unmarshal(f.Payload, msg); err != nil {
		return deliverRequest{}, badDeliverRequest("malformed delivery request: %v", err)
	}
	id := msg.GetMessageId()
	if !isUUID(id) {
		return deliverRequest{}, badDeliverRequest("message id %q is not a UUID", id)
	}
	budget := msg.GetRemainingBudget()
	if budget == nil {
		return deliverRequest{}, badDeliverRequest("message %s has no remaining budget", id)
	}
	if msg.GetSenderName() == "" {
		return deliverRequest{}, badDeliverRequest("message %s has no sender name", id)
	}
	if n := len(msg.GetSenderName()); n > maxNameBytes {
		return deliverRequest{}, badDeliverRequest("message %s has a sender name of %d bytes, over the %d-byte limit", id, n, maxNameBytes)
	}
	if !isRefSyntax(msg.GetSenderRef()) {
		return deliverRequest{}, badDeliverRequest("message %s has an invalid sender ref %q", id, msg.GetSenderRef())
	}
	if msg.GetTargetName() == "" {
		return deliverRequest{}, badDeliverRequest("message %s has no target name", id)
	}
	if n := len(msg.GetTargetName()); n > maxNameBytes {
		return deliverRequest{}, badDeliverRequest("message %s has a target name of %d bytes, over the %d-byte limit", id, n, maxNameBytes)
	}
	if ref := msg.GetTargetRef(); ref != "" && !isRefSyntax(ref) {
		return deliverRequest{}, badDeliverRequest("message %s has an invalid target ref %q", id, ref)
	}
	body := msg.GetBody()
	if body == nil {
		return deliverRequest{}, badDeliverRequest("message %s has no body", id)
	}
	if body.GetMessageId() != id {
		return deliverRequest{}, badDeliverRequest("body message id %q does not match %q", body.GetMessageId(), id)
	}
	parts := body.GetParts()
	if len(parts) != 1 {
		return deliverRequest{}, badDeliverRequest("message %s has %d parts, want 1", id, len(parts))
	}
	if parts[0].GetText() == "" {
		return deliverRequest{}, badDeliverRequest("message %s has empty text", id)
	}
	return deliverRequest{
		MessageID:  id,
		SenderName: msg.GetSenderName(),
		SenderRef:  msg.GetSenderRef(),
		TargetName: msg.GetTargetName(),
		TargetRef:  msg.GetTargetRef(),
		Remaining:  budget.AsDuration(),
		Text:       parts[0].GetText(),
	}, nil
}

func badDeliverRequest(format string, args ...any) *Error {
	return notSent(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, format, args...)
}

// encodeDeliverResult builds a kind-401 frame; a nil e reports success, and the
// error branch carries only the code and message, never the origin fingerprint.
func encodeDeliverResult(e *Error) link.Frame {
	msg := &deliverpb.DeliverResult{
		Result: &deliverpb.DeliverResult_Delivered{Delivered: &deliverpb.Delivered{}},
	}
	if e != nil {
		msg.Result = &deliverpb.DeliverResult_Error{Error: &localpb.ErrorDetail{
			Code:    e.Code,
			Message: e.Message,
		}}
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		// A marshal failure can only come from invalid UTF-8; an empty payload
		// makes the sender see a malformed reply, which maps to delivery_unknown,
		// never success.
		payload = nil
	}
	return link.Frame{Kind: link.FrameKind(deliverpb.KindDeliverResult), Payload: payload}
}

func decodeDeliverResult(f link.Frame) (*localpb.ErrorDetail, error) {
	if f.Kind != link.FrameKind(deliverpb.KindDeliverResult) {
		return nil, fmt.Errorf("cannot decode the delivery result: unexpected frame kind %d, want %d", f.Kind, deliverpb.KindDeliverResult)
	}
	msg := new(deliverpb.DeliverResult)
	if err := proto.Unmarshal(f.Payload, msg); err != nil {
		return nil, fmt.Errorf("cannot decode the delivery result: %w", err)
	}
	switch result := msg.GetResult().(type) {
	case *deliverpb.DeliverResult_Delivered:
		return nil, nil
	case *deliverpb.DeliverResult_Error:
		if result.Error == nil {
			return nil, fmt.Errorf("cannot decode the delivery result: it has no error detail")
		}
		return result.Error, nil
	default:
		return nil, fmt.Errorf("cannot decode the delivery result: it carries no result")
	}
}
