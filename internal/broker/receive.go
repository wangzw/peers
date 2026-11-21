package broker

import (
	"context"
	"log/slog"
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

// register installs the kind-400 handler. It is called once, before the link
// starts, so no request can arrive unhandled.
func (d *delivery) register() {
	d.deps.Link.HandleRequest(link.FrameKind(deliverpb.KindDeliver), d.handleDeliver)
}

// handleDeliver processes one kind-400 delivery request and returns the kind-401
// result frame. Validation failures never enter the dedupe table; a request that
// does enters it once, and a duplicate waits for the first copy's result.
//
// Processing runs on a context detached from the link: a link drop must not
// interrupt a write that the sender may retry (design-broker.md:259). The
// dedupe entry is completed in a deferred function so a panic during processing
// cannot leave a duplicate waiting forever.
func (d *delivery) handleDeliver(ctx context.Context, peer peerID, f link.Frame) (reply link.Frame) {
	start := time.Now()

	req, decodeErr := decodeDeliverRequest(f)
	if decodeErr != nil {
		// Even a request rejected before decoding logs one line: the event is
		// the only record that the peer tried to deliver something.
		d.logDeliver(peer, deliverRequest{}, resultLabel(errorRecord(decodeErr)), start)
		return encodeDeliverResult(decodeErr)
	}

	// Offline is checked after decoding and before the dedupe table, so a
	// refused message id can still be delivered after going online. The
	// configuration is read for every request; a change takes effect on the
	// next one without subscribing.
	if d.deps.Settings.Offline() {
		d.logDeliveryRefusedOffline(peer, req)
		// LocalAlias reads broker.alias afresh and would return the empty
		// string on a failed read, unlike Broker.localAlias, which keeps the
		// last valid value for registration streams that outlive the link.
		// The failure cannot happen on this path: Run closes the configuration
		// only after the link has stopped and its handlers have returned.
		return encodeDeliverResult(Errorf(localpb.ErrorCode_ERROR_CODE_BROKER_OFFLINE,
			"Broker %s is offline and refuses messages from other hosts", d.deps.Settings.LocalAlias()))
	}

	key := dedupeKey{Source: peer, MessageID: req.MessageID}

	// A recorded result answers a resend whatever the sender's visibility is
	// now: the first copy already passed the roster check, and the answer must
	// stay the same (REQ-agent-messaging-020). Looking up before that check is
	// what keeps an idempotent resend from turning into not_visible once the
	// sender leaves the intersection.
	if entry, ok := d.dedupe.lookup(key); ok {
		return d.answerHeld(ctx, peer, req, entry, start)
	}

	members := d.deps.Topics.RemoteMembers(peer)
	refsByName, senderFound := remoteSenderRoster(members, req.SenderName, req.SenderRef)
	if !senderFound {
		// A concurrent copy may have claimed the id between the lookup and
		// here; its result still wins over this rejection.
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

	// The entry must outlive the sender's budget even when dedupe_window is
	// shorter, or a resend inside the budget would write the message again.
	// The expiry is compared on the dedupe table's clock, so keepUntil is
	// measured on that clock too: the wall clock would keep the entry for the
	// difference between the two readings.
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
	res = d.processDeliver(processCtx, peer, req, refsByName, start)
	done = true
	d.logDeliver(peer, req, resultLabel(res), start)
	return replyForResult(res)
}

// answerHeld returns the recorded result of an entry this request did not own.
func (d *delivery) answerHeld(ctx context.Context, peer peerID, req deliverRequest, entry *dedupeEntry, start time.Time) link.Frame {
	res, ok := entry.wait(ctx)
	if !ok {
		res = errorRecord(unknownResult("the first copy of message %s is still being processed", req.MessageID))
	}
	d.logDeliver(peer, req, resultLabel(res), start)
	return replyForResult(res)
}

// remoteSenderRoster indexes the peer's announced members by name and reports
// whether the sender's name and ref are among them.
func remoteSenderRoster(members []remoteMember, name, ref string) (map[string]map[string]struct{}, bool) {
	refsByName := make(map[string]map[string]struct{}, len(members))
	found := false
	for _, m := range members {
		if refsByName[m.Name] == nil {
			refsByName[m.Name] = make(map[string]struct{})
		}
		refsByName[m.Name][m.Ref] = struct{}{}
		if m.Name == name && m.Ref == ref {
			found = true
		}
	}
	return refsByName, found
}

// processDeliver runs the checks and the write for one owned delivery request.
// The sender is already known to be a member; refsByName carries that roster
// for the uniqueness rule of the signature. It resolves the target only among
// local Agents: a received message is never forwarded to another link
// (architecture.md:281).
func (d *delivery) processDeliver(ctx context.Context, peer peerID, req deliverRequest, refsByName map[string]map[string]struct{}, start time.Time) *deliverpb.DeliverResult {
	alias := d.deps.Peers.EffectiveAlias(peer)
	addr := func(name, ref string, unique bool) string {
		sig, ok := signatureFor(name, ref, unique, alias)
		if !ok {
			// The alias of a peer that has just been unpaired is empty, so no
			// address can be spelled; the request is rejected below for the
			// same reason. Keep the name for the error text.
			return inboxNamePart(name, ref, unique)
		}
		return trimAlias(sig)
	}
	cands := localCandidates(d.deps.Agents.All(), func(a agentView) bool {
		return d.deps.Topics.VisibleRemote(a.ID, peer, req.SenderName, req.SenderRef)
	}, addr)

	idx, perr := pickTarget(parsedAddress{Name: req.TargetName, Ref: req.TargetRef}, cands, localRefMatch, "on this Broker")
	if perr != nil {
		return errorRecord(perr)
	}

	sig, ok := signatureFor(req.SenderName, req.SenderRef, len(refsByName[req.SenderName]) == 1, alias)
	if !ok {
		return errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "sender name has no usable signature"))
	}

	// The sender checks the longest possible signature, but the receiver is the
	// last check before the write: a request over the limit must not reach an
	// inbox (REQ-agent-messaging-026).
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

	switch d.writeInbox(ctx, deadline, target, sig, req.Text, req.MessageID) {
	case written:
		return deliveredRecord()
	case uncertain:
		return errorRecord(unknownResult("the message may have reached %s", target.Name))
	default:
		return errorRecord(notSent(localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE, "%s did not accept the message", target.Name))
	}
}

// inboxNamePart spells the name part of an address without an alias: the
// cleaned name, with the ref when the name is not unique.
func inboxNamePart(name, ref string, unique bool) string {
	s := inbox.CleanSignature(name)
	if !unique && ref != "" {
		return s + " [" + ref + "]"
	}
	return s
}

// trimAlias drops the "@alias" suffix from an address, leaving the name part.
func trimAlias(sig string) string {
	if i := strings.LastIndex(sig, "@"); i >= 0 {
		return sig[:i]
	}
	return sig
}

// deliveredRecord is a successful DedupeRecord result.
func deliveredRecord() *deliverpb.DeliverResult {
	return &deliverpb.DeliverResult{Result: &deliverpb.DeliverResult_Delivered{Delivered: &deliverpb.Delivered{}}}
}

// errorRecord is a failed DedupeRecord result. The origin fingerprint is never
// put on the wire: the receiver speaks for itself only.
func errorRecord(e *Error) *deliverpb.DeliverResult {
	return &deliverpb.DeliverResult{Result: &deliverpb.DeliverResult_Error{Error: &localpb.ErrorDetail{
		Code:    e.Code,
		Message: e.Message,
	}}}
}

// replyForResult turns a recorded result back into the kind-401 frame. A result
// this version cannot read becomes delivery_unknown, never a success.
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

// resultLabel names a result for the runtime log: "delivered" or the lowercase
// wire code name.
func resultLabel(res *deliverpb.DeliverResult) string {
	switch result := res.GetResult().(type) {
	case *deliverpb.DeliverResult_Delivered:
		return "delivered"
	case *deliverpb.DeliverResult_Error:
		return strings.ToLower(strings.TrimPrefix(result.Error.GetCode().String(), "ERROR_CODE_"))
	default:
		return "unknown"
	}
}

// logDeliveryRefusedOffline records a request refused because broker.offline
// is set. It is the only line for that request, and it records no message text
// (architecture.md:279).
func (d *delivery) logDeliveryRefusedOffline(peer peerID, req deliverRequest) {
	logger := d.deps.Log
	if logger == nil {
		logger = slog.Default()
	}
	logger.Debug(EvDeliveryRefusedOffline,
		"message_id", req.MessageID,
		"sender", req.SenderName,
		"target", req.TargetName,
		"peer", peer.String(),
	)
}

// logDeliver records one delivery event. It records no message text
// (architecture.md:279).
func (d *delivery) logDeliver(peer peerID, req deliverRequest, result string, start time.Time) {
	logger := d.deps.Log
	if logger == nil {
		logger = slog.Default()
	}
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
