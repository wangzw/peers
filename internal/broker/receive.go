package broker

import (
	"context"
	"strings"
	"time"

	"github.com/wangzw/peers/internal/broker/deliverpb"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/link"
)

// EvDeliver is the runtime log event recorded for every delivery request this
// Broker processes.
const EvDeliver = "deliver"

// EvDeliveryRefusedOffline is the runtime log event recorded instead of
// EvDeliver when broker.offline refused the request.
const EvDeliveryRefusedOffline = "delivery_refused_offline"

// register installs the kind-400 handler; it must run before the link starts so no request arrives unhandled.
func (d *delivery) register() {
	d.deps.Link.HandleRequest(link.FrameKind(deliverpb.KindDeliver), d.handleDeliver)
}

// handleDeliver processes one kind-400 delivery request and returns the kind-401
// result. The dedupe entry is completed in a deferred function so a panic
// cannot leave a duplicate waiting.
func (d *delivery) handleDeliver(ctx context.Context, peer peerID, f link.Frame) (reply link.Frame) {
	start := time.Now()

	req, decodeErr := decodeDeliverRequest(f)
	if decodeErr != nil {
		d.logDeliver(peer, deliverRequest{}, resultLabel(errorRecord(decodeErr)), start)
		return encodeDeliverResult(decodeErr)
	}

	// Offline is checked after decoding and before the dedupe table, so a
	// refused message id can still be delivered after going online.
	if d.deps.Settings.Offline() {
		d.logDeliveryRefusedOffline(peer, req)
		return encodeDeliverResult(Errorf(localpb.ErrorCode_ERROR_CODE_BROKER_OFFLINE,
			"Broker %s is offline and refuses messages from other hosts", d.deps.Settings.LocalAlias()))
	}

	key := dedupeKey{Source: peer, MessageID: req.MessageID}

	// A recorded result answers a resend whatever the sender's visibility is
	// now, so the lookup runs before the roster check.
	if entry, ok := d.dedupe.lookup(key); ok {
		return d.answerHeld(ctx, peer, req, entry, start)
	}

	members := d.deps.Topics.RemoteMembers(peer)
	refsByName, senderFound, senderRefRequired := remoteSenderRoster(members, req.SenderName, req.SenderRef)
	if !senderFound {
		// A concurrent copy may have claimed the id; its result wins over this
		// rejection.
		if entry, ok := d.dedupe.lookup(key); ok {
			return d.answerHeld(ctx, peer, req, entry, start)
		}
		res := errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE,
			"sender %s [%s] is not a member this link announced", req.SenderName, req.SenderRef))
		d.logDeliver(peer, req, resultLabel(res), start)
		return replyForResult(res)
	}

	// Only a sender this link announced may hold a table entry: a peer that is
	// not a member cannot fill the table with ids that can never be delivered.
	entry, owner := d.dedupe.acquire(key)
	if !owner {
		return d.answerHeld(ctx, peer, req, entry, start)
	}

	// keepUntil must outlive the sender's budget even when dedupe_window is
	// shorter, and is measured on the dedupe table's clock, not the wall clock.
	settings := d.deps.Settings.Send()
	keepUntil := d.dedupe.now().Add(min(req.Remaining, settings.RequestTimeout))

	res := errorRecord(unknownResult("processing message %s did not finish", req.MessageID))
	done := false
	defer func() {
		if !done {
			if p := recover(); p != nil {
				d.logDeliver(peer, req, "panic", start)
			} else {
				d.logDeliver(peer, req, resultLabel(res), start)
			}
			reply = replyForResult(res)
		}
		d.dedupe.complete(entry, res, keepUntil)
	}()

	processCtx, cancelProcess := d.processContext(ctx)
	defer cancelProcess()
	res = d.processDeliver(processCtx, peer, req, refsByName, senderRefRequired, start)
	done = true
	d.logDeliver(peer, req, resultLabel(res), start)
	return replyForResult(res)
}

func (d *delivery) answerHeld(ctx context.Context, peer peerID, req deliverRequest, entry *dedupeEntry, start time.Time) link.Frame {
	res, ok := entry.wait(ctx)
	if !ok {
		res = errorRecord(unknownResult("the first copy of message %s is still being processed", req.MessageID))
	}
	d.logDeliver(peer, req, resultLabel(res), start)
	return replyForResult(res)
}

func remoteSenderRoster(members []remoteMember, name, ref string) (map[string]map[string]struct{}, bool, bool) {
	refsByName := make(map[string]map[string]struct{}, len(members))
	found := false
	senderRefRequired := false
	for _, m := range members {
		// Only members whose publisher does not require a ref take part in the
		// uniqueness count.
		if !m.RefRequired {
			if refsByName[m.Name] == nil {
				refsByName[m.Name] = make(map[string]struct{})
			}
			refsByName[m.Name][m.Ref] = struct{}{}
		}
		if m.Name == name && m.Ref == ref {
			found = true
			senderRefRequired = m.RefRequired
		}
	}
	return refsByName, found, senderRefRequired
}

// processDeliver runs the checks and the inbox write for one owned delivery request, resolving the target among local Agents only: a received message is never forwarded to another link.
func (d *delivery) processDeliver(ctx context.Context, peer peerID, req deliverRequest, refsByName map[string]map[string]struct{}, senderRefRequired bool, start time.Time) *deliverpb.DeliverResult {
	alias := d.deps.Peers.EffectiveAlias(peer)
	addr := func(name, ref string, unique bool) string {
		sig, ok := signatureFor(name, ref, unique, alias)
		if !ok {
			// An unpaired peer has an empty alias, so no address can be spelled;
			// the name part is kept for the error text.
			return inboxNamePart(name, ref, unique)
		}
		return trimAlias(sig)
	}
	cands := localCandidates(d.deps.Agents.All(), func(a agentView) bool {
		return d.deps.Topics.VisibleRemote(a.ID, peer, req.SenderName, req.SenderRef)
	}, addr)

	var refusal nameOwnerRefusal
	idx, perr := pickTargetRecording(parsedAddress{Name: req.TargetName, Ref: req.TargetRef}, cands, localRefMatch, "on this Broker", &refusal)
	if perr != nil {
		if refusal.OwnerKnown {
			d.logDeliveryRefusedOwner(refusal.Name, refusal.OwnerUID, refusal.MatchedUIDs, peer.String())
		}
		return errorRecord(perr)
	}

	sig, ok := signatureFor(req.SenderName, req.SenderRef,
		len(refsByName[req.SenderName]) == 1 && !senderRefRequired, alias)
	if !ok {
		return errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "sender name has no usable signature"))
	}

	// The sender checks the longest possible signature, but the receiver is the
	// last size check before the write; it must not be dropped as redundant.
	if size := inbox.FrameSize(sig, req.Text); size > maxInboxFrameBytes {
		return errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_TOO_LARGE,
			"message encodes to %d bytes with the receiver's signature; the limit is %d",
			size, maxInboxFrameBytes))
	}

	settings := d.deps.Settings.Send()
	budget := min(req.Remaining, settings.RequestTimeout)
	if budget <= 0 {
		return errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE, "no time left in the sender's budget"))
	}
	deadline := start.Add(budget)

	target, err := d.deps.Agents.Reread(cands[idx].ID)
	if err != nil {
		return errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE, "%s has ended", cands[idx].Name))
	}

	// The quota is taken once per message, after every check has passed and
	// before the write; the inbox retries below share this one token.
	if ok, retryAfter := d.inbound.take(peer, target.ID); !ok {
		secs := inboundRetrySeconds(retryAfter)
		unit := "seconds"
		if secs == 1 {
			unit = "second"
		}
		return errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED,
			"%s is receiving too many messages from your Broker; try again in %d %s",
			target.Name, secs, unit))
	}

	switch d.writeInbox(ctx, deadline, target, sig, req.Text, req.MessageID) {
	case written:
		return deliveredRecord()
	case uncertain:
		return errorRecord(unknownResult("the message may have reached %s", target.Name))
	default:
		return errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE, "%s did not accept the message", target.Name))
	}
}

func inboxNamePart(name, ref string, unique bool) string {
	s := inbox.CleanSignature(name)
	if !unique && ref != "" {
		return s + " [" + ref + "]"
	}
	return s
}

func trimAlias(sig string) string {
	if i := strings.LastIndex(sig, "@"); i >= 0 {
		return sig[:i]
	}
	return sig
}

func deliveredRecord() *deliverpb.DeliverResult {
	return &deliverpb.DeliverResult{Result: &deliverpb.DeliverResult_Delivered{Delivered: &deliverpb.Delivered{}}}
}

// errorRecord builds a failed result; the origin fingerprint is never put on the wire.
func errorRecord(e *Error) *deliverpb.DeliverResult {
	return &deliverpb.DeliverResult{Result: &deliverpb.DeliverResult_Error{Error: &localpb.ErrorDetail{
		Code:    e.Code,
		Message: e.Message,
	}}}
}

// replyForResult turns a recorded result into a kind-401 frame; an unreadable result becomes delivery_unknown, never success.
func replyForResult(res *deliverpb.DeliverResult) link.Frame {
	switch result := res.GetResult().(type) {
	case *deliverpb.DeliverResult_Delivered:
		return encodeDeliverResult(nil)
	case *deliverpb.DeliverResult_Error:
		if result.Error != nil {
			return encodeDeliverResult(&Error{Code: result.Error.GetCode(), Message: result.Error.GetMessage()})
		}
	}
	return encodeDeliverResult(unknownResult("the recorded result is unreadable"))
}

func resultLabel(res *deliverpb.DeliverResult) string {
	switch result := res.GetResult().(type) {
	case *deliverpb.DeliverResult_Delivered:
		return "delivered"
	case *deliverpb.DeliverResult_Error:
		return errorCodeName(result.Error.GetCode())
	default:
		return "unknown"
	}
}

func (d *delivery) logDeliveryRefusedOffline(peer peerID, req deliverRequest) {
	logger := d.logger()
	logger.Debug(EvDeliveryRefusedOffline,
		"message_id", req.MessageID,
		"sender", req.SenderName,
		"target", req.TargetName,
		"peer", peer.String(),
	)
}

func (d *delivery) logDeliver(peer peerID, req deliverRequest, result string, start time.Time) {
	logger := d.logger()
	attrs := []any{
		"message_id", req.MessageID,
		"sender", req.SenderName,
		"target", req.TargetName,
		"result", result,
		"elapsed_ms", time.Since(start).Milliseconds(),
		"peer", peer.String(),
	}
	if result == "panic" {
		logger.Error(EvDeliver, attrs...)
		return
	}
	logger.Info(EvDeliver, attrs...)
}
