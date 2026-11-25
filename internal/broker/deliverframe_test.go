package broker

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/wangzw/peers/internal/broker/deliverpb"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/link"
)

const testDeliverMessageID = "3f2c1a4b-5d6e-4f70-8a9b-0c1d2e3f4a5b"

func testDeliverRequest() deliverRequest {
	return deliverRequest{
		MessageID:  testDeliverMessageID,
		SenderName: "api-worker",
		SenderRef:  "1a2b3c",
		TargetName: "reviewer",
		TargetRef:  "4d5e6f",
		Remaining:  2 * time.Second,
		Text:       "hello <world> & more",
	}
}

func validDeliverProto() *deliverpb.DeliverRequest {
	return &deliverpb.DeliverRequest{
		MessageId:       testDeliverMessageID,
		SenderName:      "api-worker",
		SenderRef:       "1a2b3c",
		TargetName:      "reviewer",
		TargetRef:       "4d5e6f",
		RemainingBudget: durationpb.New(2 * time.Second),
		Body: &deliverpb.Message{
			MessageId: testDeliverMessageID,
			Role:      deliverpb.Role_ROLE_USER,
			Parts:     []*deliverpb.Part{{Content: &deliverpb.Part_Text{Text: "hello"}}},
		},
	}
}

func deliverFrameOf(t *testing.T, msg *deliverpb.DeliverRequest, kind link.FrameKind) link.Frame {
	t.Helper()
	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	return link.Frame{Kind: kind, Payload: payload}
}

// TestDeliverRequestRoundTrip checks that every field survives encoding and
// decoding and that the A2A body reuses the delivery identifier as one ROLE_USER
// text part.
func TestDeliverRequestRoundTrip(t *testing.T) {
	want := testDeliverRequest()
	f, err := encodeDeliverRequest(want)
	if err != nil {
		t.Fatalf("encodeDeliverRequest: %v", err)
	}
	if f.Kind != link.FrameKind(deliverpb.KindDeliver) {
		t.Fatalf("frame kind = %d, want %d", f.Kind, deliverpb.KindDeliver)
	}

	// Check the wire form the receiver actually sees, not only the round trip:
	// a matching pair of bugs could hide a wrong A2A field.
	var wire deliverpb.DeliverRequest
	if err := proto.Unmarshal(f.Payload, &wire); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if wire.GetMessageId() != want.MessageID {
		t.Fatalf("wire message_id = %q, want %q", wire.GetMessageId(), want.MessageID)
	}
	if got := wire.GetRemainingBudget(); got == nil || got.AsDuration() != want.Remaining {
		t.Fatalf("wire remaining_budget = %v, want %v", got, want.Remaining)
	}
	body := wire.GetBody()
	if body == nil {
		t.Fatal("wire body is nil")
	}
	if body.GetMessageId() != want.MessageID {
		t.Fatalf("body message_id = %q, want %q", body.GetMessageId(), want.MessageID)
	}
	if body.GetRole() != deliverpb.Role_ROLE_USER {
		t.Fatalf("body role = %v, want ROLE_USER", body.GetRole())
	}
	if parts := body.GetParts(); len(parts) != 1 || parts[0].GetText() != want.Text {
		t.Fatalf("body parts = %v, want one part with %q", parts, want.Text)
	}

	got, derr := decodeDeliverRequest(f)
	if derr != nil {
		t.Fatalf("decodeDeliverRequest: %v", derr)
	}
	if got != want {
		t.Fatalf("decoded request = %+v, want %+v", got, want)
	}

	// The role is not part of the request contract.
	noRole := validDeliverProto()
	noRole.Body.Role = deliverpb.Role_ROLE_UNSPECIFIED
	if _, derr := decodeDeliverRequest(deliverFrameOf(t, noRole, link.FrameKind(deliverpb.KindDeliver))); derr != nil {
		t.Fatalf("decode rejected a request by its role: %v", derr)
	}
}

// TestDecodeDeliverRequestRejects checks that each malformed field is refused
// with invalid_argument and the "Not sent" prefix.
func TestDecodeDeliverRequestRejects(t *testing.T) {
	deliverKind := link.FrameKind(deliverpb.KindDeliver)
	cases := []struct {
		name   string
		kind   link.FrameKind
		mutate func(*deliverpb.DeliverRequest)
	}{
		{"not a UUID", deliverKind, func(m *deliverpb.DeliverRequest) { m.MessageId = "not-a-uuid" }},
		{"non-hex character in a UUID-shaped id", deliverKind, func(m *deliverpb.DeliverRequest) {
			m.MessageId = "3f2c1a4b-5d6e-4f70-8a9b-0c1d2e3f4a5z"
		}},
		{"missing budget", deliverKind, func(m *deliverpb.DeliverRequest) { m.RemainingBudget = nil }},
		{"empty sender name", deliverKind, func(m *deliverpb.DeliverRequest) { m.SenderName = "" }},
		{"oversized sender name", deliverKind, func(m *deliverpb.DeliverRequest) {
			m.SenderName = strings.Repeat("x", 1<<20)
		}},
		{"short sender ref", deliverKind, func(m *deliverpb.DeliverRequest) { m.SenderRef = "12345" }},
		{"empty target name", deliverKind, func(m *deliverpb.DeliverRequest) { m.TargetName = "" }},
		{"oversized target name", deliverKind, func(m *deliverpb.DeliverRequest) {
			m.TargetName = strings.Repeat("x", 1<<20)
		}},
		{"bad target ref", deliverKind, func(m *deliverpb.DeliverRequest) { m.TargetRef = "ABCDEF" }},
		{"missing body", deliverKind, func(m *deliverpb.DeliverRequest) { m.Body = nil }},
		{"body message id differs", deliverKind, func(m *deliverpb.DeliverRequest) {
			m.Body.MessageId = "00000000-0000-4000-8000-000000000000"
		}},
		{"two parts", deliverKind, func(m *deliverpb.DeliverRequest) {
			m.Body.Parts = append(m.Body.Parts, &deliverpb.Part{Content: &deliverpb.Part_Text{Text: "second"}})
		}},
		{"empty text", deliverKind, func(m *deliverpb.DeliverRequest) {
			m.Body.Parts[0] = &deliverpb.Part{Content: &deliverpb.Part_Text{Text: ""}}
		}},
		{"wrong kind", link.FrameKind(deliverpb.KindDeliverResult), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := validDeliverProto()
			if tc.mutate != nil {
				tc.mutate(msg)
			}
			_, derr := decodeDeliverRequest(deliverFrameOf(t, msg, tc.kind))
			if derr == nil {
				t.Fatal("decode accepted an invalid request")
			}
			if derr.Code != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
				t.Fatalf("code = %v, want INVALID_ARGUMENT", derr.Code)
			}
			if !strings.HasPrefix(derr.Message, "Not sent") {
				t.Fatalf("message = %q, want the Not sent prefix", derr.Message)
			}
		})
	}

	if _, derr := decodeDeliverRequest(link.Frame{Kind: deliverKind, Payload: []byte{0xff, 0xff, 0xff}}); derr == nil {
		t.Fatal("decode accepted a malformed payload")
	}
}

// TestReceiveOversizedSenderNameNotDeduped checks that an oversized peer name is
// refused before the dedupe table is touched.
func TestReceiveOversizedSenderNameNotDeduped(t *testing.T) {
	f := newReceiveFixture(t, defaultReceiveSettings())
	msg := validDeliverProto()
	msg.SenderName = strings.Repeat("x", 1<<20)
	reply := f.handle(deliverFrameOf(t, msg, link.FrameKind(deliverpb.KindDeliver)))
	wantReceiveError(t, reply, localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "Not sent")
	if n := f.d.dedupe.len(); n != 0 {
		t.Fatalf("dedupe entries = %d, want 0", n)
	}
}

// TestDeliverResultRoundTrip checks both branches of the result oneof and that a
// relayed origin fingerprint is not echoed back.
func TestDeliverResultRoundTrip(t *testing.T) {
	f := encodeDeliverResult(nil)
	if f.Kind != link.FrameKind(deliverpb.KindDeliverResult) {
		t.Fatalf("frame kind = %d, want %d", f.Kind, deliverpb.KindDeliverResult)
	}
	detail, err := decodeDeliverResult(f)
	if err != nil || detail != nil {
		t.Fatalf("decode delivered result = (%v, %v), want (nil, nil)", detail, err)
	}

	want := &Error{
		Code:    localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE,
		Message: "Not sent: target is not visible",
		Origin:  []byte{1, 2, 3},
	}
	f = encodeDeliverResult(want)
	if f.Kind != link.FrameKind(deliverpb.KindDeliverResult) {
		t.Fatalf("frame kind = %d, want %d", f.Kind, deliverpb.KindDeliverResult)
	}
	detail, err = decodeDeliverResult(f)
	if err != nil {
		t.Fatalf("decode error result: %v", err)
	}
	if detail == nil {
		t.Fatal("decode error result returned nil detail")
	}
	if detail.GetCode() != want.Code {
		t.Fatalf("code = %v, want %v", detail.GetCode(), want.Code)
	}
	if detail.GetMessage() != want.Message {
		t.Fatalf("message = %q, want %q", detail.GetMessage(), want.Message)
	}
	if origin := detail.GetOriginatingBrokerFingerprint(); len(origin) != 0 {
		t.Fatalf("origin fingerprint = %x, want empty", origin)
	}
}

// TestDecodeDeliverResultMalformed checks that a reply the sender cannot read is
// an error, never a successful delivery.
func TestDecodeDeliverResultMalformed(t *testing.T) {
	requestFrame, err := encodeDeliverRequest(testDeliverRequest())
	if err != nil {
		t.Fatalf("encodeDeliverRequest: %v", err)
	}
	resultKind := link.FrameKind(deliverpb.KindDeliverResult)
	cases := []struct {
		name  string
		frame link.Frame
	}{
		{"wrong kind", link.Frame{Kind: link.FrameKind(deliverpb.KindDeliver), Payload: requestFrame.Payload}},
		{"garbage payload", link.Frame{Kind: resultKind, Payload: []byte{0xff, 0xff, 0xff}}},
		{"empty payload", link.Frame{Kind: resultKind}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if detail, err := decodeDeliverResult(tc.frame); err == nil {
				t.Fatalf("decode returned (%v, nil), want an error", detail)
			}
		})
	}
}

// TestNewMessageIDIsUUIDv4 checks uniqueness and the version and variant bits.
func TestNewMessageIDIsUUIDv4(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := newMessageID()
		if !isUUID(id) {
			t.Fatalf("newMessageID returned %q, which is not a UUID", id)
		}
		if id[14] != '4' {
			t.Fatalf("version character of %q is %q, want '4'", id, id[14])
		}
		if !strings.ContainsRune("89ab", rune(id[19])) {
			t.Fatalf("variant character of %q is %q, want one of 89ab", id, id[19])
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("newMessageID repeated %q", id)
		}
		seen[id] = struct{}{}
		if upper := strings.ToUpper(id); !isUUID(upper) {
			t.Fatalf("isUUID rejected the uppercase form %q", upper)
		}
	}
}

// TestIsRefSyntax pins the ref grammar so a malformed peer ref cannot reach
// FormatAddress, which panics on it.
func TestIsRefSyntax(t *testing.T) {
	for _, s := range []string{"1a2b3c", "1a2b3c4d5e6f"} {
		if !isRefSyntax(s) {
			t.Fatalf("isRefSyntax(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"", "12345", "1a2b3c4d5e6f7", "ABCDEF", "1a2b3g"} {
		if isRefSyntax(s) {
			t.Fatalf("isRefSyntax(%q) = true, want false", s)
		}
	}
}
