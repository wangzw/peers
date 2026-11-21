package pairing

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"google.golang.org/protobuf/proto"
)

const (
	// handshakeTimeout bounds one pairing handshake on each side. It is a
	// constant, not a configuration value.
	handshakeTimeout = 10 * time.Second

	nonceLength     = 32
	lookupTagLength = 8
	proofLength     = sha256.Size
)

// Connect pairs with the Broker at address using key. It dials through the
// injected transport, runs the five-frame handshake, commits the peer and
// leaves the connection to link. shownHost is the display-only host name from
// the "host(addr:port)" address form and never enters the peer table.
func (p *Pairing) Connect(ctx context.Context, address, key, localAlias string) (Peer, string, error) {
	if !p.handshakeReady() {
		return Peer{}, "", ErrNotOpen
	}
	dialAddr, shownHost, err := parseConnectAddress(address)
	if err != nil {
		return Peer{}, "", err
	}
	if localAlias != "" && !validAlias(localAlias) {
		return Peer{}, "", ErrInvalidAlias
	}
	ownAlias, err := p.alias()
	if err != nil {
		return Peer{}, "", err
	}
	ni, err := p.nonce()
	if err != nil {
		return Peer{}, "", err
	}
	hint := computeHint([]byte(key), ni)

	var result Peer
	err = p.transport.PairDial(ctx, dialAddr, func(conn PairConn) error {
		// The 10-second limit starts here: the connection exists and link's
		// dial timeout has already done its job.
		runCtx, cancel := withClockTimeout(ctx, p.clock, handshakeTimeout)
		defer cancel()

		peer, err := p.runInitiator(runCtx, ctx, conn, key, ni, hint, ownAlias, localAlias, dialAddr)
		if err != nil {
			return err
		}
		result = peer
		return nil
	})
	if err != nil {
		switch {
		case errors.Is(err, ErrDialFailed):
			// A canceled dial is the caller's cancellation, not an
			// unreachable peer: link wraps both sentinels in that case.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return Peer{}, "", ctxErr
			}
			return Peer{}, "", ErrPeerUnreachable
		case errors.Is(err, context.DeadlineExceeded):
			return Peer{}, "", &PairingError{Reason: ReasonTimeout, Detail: "handshake timed out"}
		default:
			return Peer{}, "", err
		}
	}
	return result, shownHost, nil
}

// runInitiator sends PairRequest, verifies the responder proof, reserves the
// effective alias, sends PairConfirm and commits the peer after PairDone.
func (p *Pairing) runInitiator(runCtx, parent context.Context, conn PairConn, key string, ni, hint []byte, ownAlias, localAlias, dialAddr string) (Peer, error) {
	acceptorFP := conn.PeerFingerprint()
	acceptorPub := conn.PeerPublicKey()
	if len(acceptorPub) != 32 || FingerprintOf(acceptorPub) != acceptorFP {
		_ = conn.Close()
		return Peer{}, &PairingError{Reason: ReasonVerificationFailed, Detail: "peer certificate does not match its fingerprint"}
	}

	request := &pairingpb.PairRequest{
		InitiatorNonce: ni,
		InitiatorAlias: ownAlias,
		LookupTag:      hint,
	}
	if err := sendMessage(runCtx, conn, kindPairRequest, request); err != nil {
		return Peer{}, p.initiatorFrameError(conn, parent, runCtx, err)
	}

	kind, payload, err := conn.Receive(runCtx)
	if err != nil {
		return Peer{}, p.initiatorFrameError(conn, parent, runCtx, err)
	}
	if kind == kindPairReject {
		_ = conn.Close()
		return Peer{}, rejectError(payload)
	}
	if kind != kindPairChallenge {
		_ = conn.Close()
		return Peer{}, fmt.Errorf("%w: expected PairChallenge, got frame %d", ErrProtocol, kind)
	}
	var challenge pairingpb.PairChallenge
	if err := proto.Unmarshal(payload, &challenge); err != nil {
		_ = conn.Close()
		return Peer{}, fmt.Errorf("%w: cannot decode PairChallenge: %v", ErrProtocol, err)
	}
	nr := challenge.GetResponderNonce()
	if len(nr) != nonceLength || len(challenge.GetProof()) != proofLength {
		_ = conn.Close()
		return Peer{}, fmt.Errorf("%w: PairChallenge has a malformed nonce or proof", ErrProtocol)
	}

	want := responderProof([]byte(key), ni, nr, acceptorFP, p.identity.fingerprint())
	if !hmac.Equal(want, challenge.GetProof()) {
		_ = conn.Close()
		return Peer{}, &PairingError{Reason: ReasonVerificationFailed, Detail: "responder proof did not verify"}
	}

	finalAlias, local := p.chooseInitiatorAlias(acceptorFP, challenge.GetResponderAlias(), ownAlias, localAlias)
	token, reserved := uint64(0), false
	if finalAlias != "" {
		token, reserved = p.reserveAlias(acceptorFP, finalAlias, ownAlias)
	}
	if !reserved {
		_ = conn.Close()
		return Peer{}, ErrNeedsLocalAlias
	}
	committed := false
	defer func() {
		if !committed {
			p.releaseAlias(token)
		}
	}()

	proof := initiatorProof([]byte(key), ni, nr, p.identity.fingerprint(), acceptorFP)
	if err := sendMessage(runCtx, conn, kindPairConfirm, &pairingpb.PairConfirm{Proof: proof}); err != nil {
		return Peer{}, p.initiatorFrameError(conn, parent, runCtx, err)
	}

	kind, payload, err = conn.Receive(runCtx)
	if err != nil {
		return Peer{}, p.initiatorFrameError(conn, parent, runCtx, err)
	}
	switch kind {
	case kindPairReject:
		_ = conn.Close()
		return Peer{}, rejectError(payload)
	case kindPairDone:
	default:
		_ = conn.Close()
		return Peer{}, fmt.Errorf("%w: expected PairDone, got frame %d", ErrProtocol, kind)
	}

	// The reservation and the own alias are re-checked inside the peer-table
	// lock in the same critical section that commits, so neither a concurrent
	// rename nor another handshake can slip in after this point.
	entry := p.dialerEntry(acceptorFP, acceptorPub, challenge.GetResponderAlias(), local, dialAddr, challenge.GetAdvertisedAddresses())
	replaced, committedAlias, frozen, err := p.peers.commitHandshake(entry, token)
	if err != nil {
		_ = conn.Close()
		return Peer{}, err
	}
	committed = true
	if replaced {
		p.transport.Drop(acceptorFP)
	}
	// Audit the alias the table actually committed when re-pairing froze the
	// current effective alias instead of adopting the reserved one once the
	// tombstone cap is reached.
	if frozen {
		auditLocalAliasAssigned(p.settings, p.clock.Now(), acceptorFP, committedAlias)
	}
	return p.peerView(acceptorFP), nil
}

// Accept runs the responder side of a pairing handshake over conn. The caller
// (link) hands over the connection when the first frame is a PairRequest. On
// success the connection is left open for link; on any failure Accept closes
// it.
func (p *Pairing) Accept(conn PairConn) error {
	if !p.handshakeReady() {
		return ErrNotOpen
	}
	if conn == nil {
		return fmt.Errorf("%w: nil connection", ErrProtocol)
	}
	ctx, cancel := withClockTimeout(context.Background(), p.clock, handshakeTimeout)
	defer cancel()

	initiatorFP := conn.PeerFingerprint()
	initiatorPub := conn.PeerPublicKey()
	if len(initiatorPub) != 32 || FingerprintOf(initiatorPub) != initiatorFP {
		_ = conn.Close()
		return fmt.Errorf("%w: peer certificate does not match its fingerprint", ErrProtocol)
	}

	kind, payload, err := conn.Receive(ctx)
	if err != nil {
		return p.acceptFrameError(conn, ctx, initiatorFP, "", err)
	}
	if kind != kindPairRequest {
		_ = conn.Close()
		return fmt.Errorf("%w: expected PairRequest, got frame %d", ErrProtocol, kind)
	}
	var request pairingpb.PairRequest
	if err := proto.Unmarshal(payload, &request); err != nil {
		_ = conn.Close()
		return fmt.Errorf("%w: cannot decode PairRequest: %v", ErrProtocol, err)
	}
	ni := request.GetInitiatorNonce()
	if len(ni) != nonceLength || len(request.GetLookupTag()) != lookupTagLength {
		_ = conn.Close()
		return fmt.Errorf("%w: PairRequest has a malformed nonce or lookup tag", ErrProtocol)
	}
	initiatorAlias := request.GetInitiatorAlias()

	now := p.clock.Now()
	maxFailures, err := p.settings.MaxFailures()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("%w: %v", ErrStore, err)
	}
	maxAttempts, err := p.settings.MaxAttemptsPerMinute()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("%w: %v", ErrStore, err)
	}
	match, found, lookupErr := p.keys.lookupAtRequest(ni, request.GetLookupTag(), now, sourcePrefix(conn.RemoteAddr()), maxAttempts, p.settings.StaticKey)
	if !found {
		reason := ReasonKeyInvalid
		if lookupErr != nil {
			var pairErr *PairingError
			if errors.As(lookupErr, &pairErr) {
				reason = pairErr.Reason
			}
		}
		return p.rejectAcceptor(conn, ctx, initiatorFP, initiatorAlias, reason, now)
	}

	nr, err := p.nonce()
	if err != nil {
		_ = conn.Close()
		return err
	}
	ownAlias, err := p.alias()
	if err != nil {
		_ = conn.Close()
		return err
	}
	advertised, err := p.settings.Advertised()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("%w: %v", ErrStore, err)
	}

	challenge := &pairingpb.PairChallenge{
		ResponderNonce:      nr,
		ResponderAlias:      ownAlias,
		AdvertisedAddresses: advertisedToPB(advertised),
		Proof:               responderProof([]byte(match.key), ni, nr, p.identity.fingerprint(), initiatorFP),
	}
	if err := sendMessage(ctx, conn, kindPairChallenge, challenge); err != nil {
		return p.acceptFrameError(conn, ctx, initiatorFP, initiatorAlias, err)
	}

	kind, payload, err = conn.Receive(ctx)
	if err != nil {
		return p.acceptFrameError(conn, ctx, initiatorFP, initiatorAlias, err)
	}
	if kind != kindPairConfirm {
		_ = conn.Close()
		return fmt.Errorf("%w: expected PairConfirm, got frame %d", ErrProtocol, kind)
	}
	var confirm pairingpb.PairConfirm
	if err := proto.Unmarshal(payload, &confirm); err != nil {
		_ = conn.Close()
		return fmt.Errorf("%w: cannot decode PairConfirm: %v", ErrProtocol, err)
	}
	if len(confirm.GetProof()) != proofLength {
		_ = conn.Close()
		return fmt.Errorf("%w: PairConfirm has a malformed proof", ErrProtocol)
	}

	verifiedAt := p.clock.Now()
	ok, gone := func() (bool, RejectReason) {
		match.proof = confirm.GetProof()
		return p.keys.verifyAtConfirm(match, ni, nr, p.identity.fingerprint(), initiatorFP, p.settings.StaticKey(), verifiedAt)
	}()
	if gone != 0 {
		return p.rejectAcceptor(conn, ctx, initiatorFP, initiatorAlias, gone, verifiedAt)
	}
	if !ok {
		p.keys.recordVerificationFailure(match, maxFailures, verifiedAt)
		return p.rejectAcceptor(conn, ctx, initiatorFP, initiatorAlias, ReasonVerificationFailed, verifiedAt)
	}

	ownAliasNow, err := p.alias()
	if err != nil {
		_ = conn.Close()
		return err
	}
	_, localAlias, token, ok := p.reserveAcceptorAlias(initiatorFP, initiatorAlias, ownAliasNow)
	if !ok {
		_ = conn.Close()
		return ErrNeedsLocalAlias
	}
	reserved := true
	defer func() {
		if reserved {
			p.releaseAlias(token)
		}
	}()

	pending := peerEntry{
		fingerprint: initiatorFP,
		publicKey:   append([]byte(nil), initiatorPub...),
		alias:       initiatorAlias,
		localAlias:  localAlias,
		role:        RoleAcceptor,
		pairedAt:    p.clock.Now(),
	}

	if err := sendMessage(ctx, conn, kindPairDone, &pairingpb.PairDone{}); err != nil {
		return p.acceptFrameError(conn, ctx, initiatorFP, initiatorAlias, err)
	}

	// The peer becomes a committed peer only after the initiator's first link
	// Hello arrives. Put that frame back for link to read.
	kind, payload, err = conn.Receive(ctx)
	if err != nil {
		return p.acceptFrameError(conn, ctx, initiatorFP, initiatorAlias, err)
	}
	if kind != kindHello {
		_ = conn.Close()
		return fmt.Errorf("%w: expected link Hello after PairDone, got frame %d", ErrProtocol, kind)
	}
	conn.Unread(kindHello, payload)

	// Re-check the reservation and the own alias in the same lock that commits.
	// A competing handshake or a concurrent rename between the reservation
	// before PairDone and this point turns into ErrNeedsLocalAlias instead of an
	// inconsistent commit.
	replaced, committedAlias, frozen, err := p.peers.commitHandshake(pending, token)
	if err != nil {
		_ = conn.Close()
		return err
	}
	reserved = false
	if replaced {
		p.transport.Drop(initiatorFP)
	}

	// Audit the alias the table actually committed, not the one reserved before
	// PairDone. Re-pairing can freeze the current effective alias instead of
	// adopting the reserved one once the tombstone cap is reached.
	committedAt := p.clock.Now()
	auditPairingSucceeded(p.settings, committedAt, initiatorFP, committedAlias)
	if localAlias != "" || frozen {
		auditLocalAliasAssigned(p.settings, committedAt, initiatorFP, committedAlias)
	}
	return nil
}

// responderProof computes the acceptor's authentication code.
func responderProof(key, ni, nr []byte, acceptor, initiatorSeen Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(pairResponderLabel))
	mac.Write(ni)
	mac.Write(nr)
	mac.Write(acceptor[:])
	mac.Write(initiatorSeen[:])
	return mac.Sum(nil)
}

// initiatorProof computes the initiator's authentication code.
func initiatorProof(key, ni, nr []byte, initiator, acceptorSeen Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(pairInitiatorLabel))
	mac.Write(ni)
	mac.Write(nr)
	mac.Write(initiator[:])
	mac.Write(acceptorSeen[:])
	return mac.Sum(nil)
}

// rejectAcceptor sends PairReject with reason, records the acceptor-owned audit
// event, closes the connection and returns the matching error. The self-reported
// alias has not been authenticated on these paths, so an alias outside the alias
// character set is recorded as empty and never reaches the audit log.
func (p *Pairing) rejectAcceptor(conn PairConn, ctx context.Context, fp Fingerprint, alias string, reason RejectReason, now time.Time) error {
	if !validAlias(alias) {
		alias = ""
	}
	mergeKey := ""
	switch reason {
	case ReasonTryLater:
		mergeKey = auditSourceMergeKey(conn.RemoteAddr())
		auditPairingFailed(p.settings, now, fp, alias, auditReasonTryLater, mergeKey)
	case ReasonVerificationFailed:
		auditPairingFailed(p.settings, now, fp, alias, auditReasonVerificationFailed, "")
	case ReasonKeyExpired:
		auditPairingFailed(p.settings, now, fp, alias, auditReasonKeyExpired, "")
	default:
		auditPairingFailed(p.settings, now, fp, alias, auditReasonKeyInvalid, "")
	}
	_ = sendRejectAndLinger(ctx, conn, reason)
	return &PairingError{Reason: reason, Detail: rejectDetail(reason)}
}

// acceptFrameError maps a transport failure on the acceptor side to the
// handshake error the design assigns it, closing the connection and recording
// the pairing-owned audit event.
func (p *Pairing) acceptFrameError(conn PairConn, ctx context.Context, fp Fingerprint, alias string, err error) error {
	_ = conn.Close()
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		auditPairingFailed(p.settings, p.clock.Now(), fp, alias, auditReasonTimeout, "")
		return &PairingError{Reason: ReasonTimeout, Detail: "handshake timed out"}
	}
	auditPairingFailed(p.settings, p.clock.Now(), fp, alias, auditReasonNotConnected, "")
	return &PairingError{Reason: ReasonNotConnected, Detail: "connection closed during pairing"}
}

// initiatorFrameError maps a transport failure on the initiator side. The
// broker owns the initiator's audit event, so this only classifies the error.
func (p *Pairing) initiatorFrameError(conn PairConn, parent, runCtx context.Context, err error) error {
	_ = conn.Close()
	if parent.Err() != nil {
		return parent.Err()
	}
	if runCtx.Err() != nil {
		return &PairingError{Reason: ReasonTimeout, Detail: "handshake timed out"}
	}
	return &PairingError{Reason: ReasonNotConnected, Detail: "connection closed during pairing"}
}

// chooseInitiatorAlias picks the alias the initiator will hold. A caller
// supplied alias wins; an existing local alias is kept so re-pairing does not
// rename a peer; otherwise a valid, non-conflicting self-reported alias is
// used. An empty result means the caller must supply a local alias.
func (p *Pairing) chooseInitiatorAlias(fp Fingerprint, reported, ownAlias, provided string) (alias, localAlias string) {
	if provided != "" {
		return provided, provided
	}
	if entry, ok := p.peers.get(fp); ok && entry.localAlias != "" {
		return entry.localAlias, entry.localAlias
	}
	if validAlias(reported) && reported != ownAlias {
		return reported, ""
	}
	return "", ""
}

// reserveAcceptorAlias reserves the alias the acceptor will use and reports the
// reservation token and whether a local alias had to be generated.
func (p *Pairing) reserveAcceptorAlias(fp Fingerprint, reported, ownAlias string) (alias, localAlias string, token uint64, ok bool) {
	p.peers.mu.Lock()
	defer p.peers.mu.Unlock()

	if validAlias(reported) && reported != ownAlias {
		if token, ok := p.peers.reserveAliasLocked(reported, fp); ok {
			return reported, "", token, true
		}
	}
	generated, token, generatedOK := p.peers.assignLocalAliasLocked(fp, reported, ownAlias)
	if !generatedOK {
		return "", "", 0, false
	}
	return generated, generated, token, true
}

// reserveAlias takes the final effective alias for an in-flight handshake and
// returns its reservation token.
func (p *Pairing) reserveAlias(fp Fingerprint, alias, ownAlias string) (uint64, bool) {
	if alias == "" || alias == ownAlias {
		return 0, false
	}
	p.peers.mu.Lock()
	defer p.peers.mu.Unlock()
	return p.peers.reserveAliasLocked(alias, fp)
}

// releaseAlias frees the reservation held by one in-flight handshake.
func (p *Pairing) releaseAlias(token uint64) {
	if token == 0 {
		return
	}
	p.peers.mu.Lock()
	p.peers.releaseTokenLocked(token)
	p.peers.mu.Unlock()
}

// peerView returns the committed entry as the exported view. The entry is read
// back so candidate sources and filtering come from the peer table itself.
func (p *Pairing) peerView(fp Fingerprint) Peer {
	for _, peer := range p.peers.snapshot() {
		if peer.Fingerprint == fp {
			return peer
		}
	}
	entry, _ := p.peers.get(fp)
	return entry.toPeer()
}

// dialerEntry builds the peer-table entry the initiator commits.
func (p *Pairing) dialerEntry(fp Fingerprint, pub []byte, alias, localAlias, dialAddr string, advertised []*pairingpb.AdvertisedAddress) peerEntry {
	entry := peerEntry{
		fingerprint:   fp,
		publicKey:     append([]byte(nil), pub...),
		alias:         alias,
		localAlias:    localAlias,
		role:          RoleDialer,
		pairedAt:      p.clock.Now(),
		lastConnected: dialAddr,
	}
	// Re-pairing keeps the card-sourced candidates; the acceptor and pairing
	// sources describe this handshake and replace the old values.
	if prior, ok := p.peers.get(fp); ok {
		entry.card = append([]config.AdvertisedAddr(nil), prior.card...)
	}
	entry.acceptor = capKnown(advertisedFromPB(advertised))
	entry.pairingAddr = dialAddr
	return entry
}

// sendRejectAndLinger writes a PairReject frame and lets the connection stay
// open briefly, so the frame can leave the transport's write buffer before the
// close. Local-only reasons never travel on the wire; they close the
// connection at once.
func sendRejectAndLinger(ctx context.Context, conn PairConn, reason RejectReason) error {
	pbReason, ok := rejectReasonToPB(reason)
	if !ok {
		return conn.Close()
	}
	payload, err := proto.Marshal(&pairingpb.PairReject{Reason: pbReason})
	if err != nil {
		_ = conn.Close()
		return err
	}
	if sender, ok := conn.(finalSender); ok {
		return sender.SendAndLinger(ctx, kindPairReject, payload)
	}
	if err := conn.Send(ctx, kindPairReject, payload); err != nil {
		_ = conn.Close()
		return err
	}
	return conn.Close()
}

// rejectError turns a PairReject payload into a PairingError. An unrecognised
// reason is a protocol error.
func rejectError(payload []byte) error {
	var reject pairingpb.PairReject
	if err := proto.Unmarshal(payload, &reject); err != nil {
		return fmt.Errorf("%w: cannot decode PairReject: %v", ErrProtocol, err)
	}
	reason, ok := rejectReasonFromPB(reject.GetReason())
	if !ok {
		return fmt.Errorf("%w: unknown PairReject reason %d", ErrProtocol, int32(reject.GetReason()))
	}
	return &PairingError{Reason: reason, Detail: rejectDetail(reason)}
}

// rejectReasonToPB maps a local reject reason to the wire enum. Timeout and
// not-connected are local only.
func rejectReasonToPB(reason RejectReason) (pairingpb.PairingRejectReason, bool) {
	switch reason {
	case ReasonKeyInvalid:
		return pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_KEY_INVALID, true
	case ReasonVerificationFailed:
		return pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_VERIFICATION_FAILED, true
	case ReasonTryLater:
		return pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_TRY_LATER, true
	case ReasonKeyExpired:
		return pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_KEY_EXPIRED, true
	default:
		return pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_UNSPECIFIED, false
	}
}

// rejectReasonFromPB maps a wire reject reason to the local reason.
func rejectReasonFromPB(reason pairingpb.PairingRejectReason) (RejectReason, bool) {
	switch reason {
	case pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_KEY_INVALID:
		return ReasonKeyInvalid, true
	case pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_VERIFICATION_FAILED:
		return ReasonVerificationFailed, true
	case pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_TRY_LATER:
		return ReasonTryLater, true
	case pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_KEY_EXPIRED:
		return ReasonKeyExpired, true
	default:
		return 0, false
	}
}

// rejectDetail is the human-readable detail carried by PairingError.
func rejectDetail(reason RejectReason) string {
	switch reason {
	case ReasonKeyInvalid:
		return "pairing key is invalid"
	case ReasonVerificationFailed:
		return "verification failed"
	case ReasonTryLater:
		return "try again later"
	case ReasonTimeout:
		return "handshake timed out"
	case ReasonNotConnected:
		return "not connected"
	case ReasonKeyExpired:
		return "key expired; ask the other side to generate a new pairing key"
	default:
		return "pairing rejected"
	}
}

// sendMessage marshals one protobuf message and sends it as a frame.
func sendMessage(ctx context.Context, conn PairConn, kind uint32, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.Send(ctx, kind, payload)
}

// advertisedToPB converts the config address list to its wire form.
func advertisedToPB(addrs []config.AdvertisedAddr) []*pairingpb.AdvertisedAddress {
	out := make([]*pairingpb.AdvertisedAddress, 0, len(addrs))
	for _, addr := range addrs {
		out = append(out, &pairingpb.AdvertisedAddress{Kind: addr.Kind, Addr: addr.Addr})
	}
	return out
}

// advertisedFromPB converts the wire address list back to config's form.
func advertisedFromPB(addrs []*pairingpb.AdvertisedAddress) []config.AdvertisedAddr {
	out := make([]config.AdvertisedAddr, 0, len(addrs))
	for _, addr := range addrs {
		if addr == nil {
			continue
		}
		out = append(out, config.AdvertisedAddr{Kind: addr.GetKind(), Addr: addr.GetAddr()})
	}
	return out
}

// parseConnectAddress accepts "host:port" and "shownName(host:port)". The
// shown name never participates in dialing or identity checks.
func parseConnectAddress(address string) (dialAddr, shownHost string, err error) {
	if address == "" {
		return "", "", ErrInvalidAddress
	}
	dialAddr = address
	if i := strings.LastIndexByte(address, '('); i >= 0 {
		if i == 0 || !strings.HasSuffix(address, ")") {
			return "", "", ErrInvalidAddress
		}
		shownHost = address[:i]
		dialAddr = address[i+1 : len(address)-1]
		if strings.Contains(shownHost, "(") || strings.Contains(shownHost, ")") {
			return "", "", ErrInvalidAddress
		}
	}
	host, port, splitErr := net.SplitHostPort(dialAddr)
	if splitErr != nil || host == "" || port == "" || !config.ValidDialHost(host) {
		return "", "", ErrInvalidAddress
	}
	for i := 0; i < len(port); i++ {
		if port[i] < '0' || port[i] > '9' {
			return "", "", ErrInvalidAddress
		}
	}
	number, convErr := strconv.Atoi(port)
	if convErr != nil || number < 1 || number > 65535 {
		return "", "", ErrInvalidAddress
	}
	return dialAddr, shownHost, nil
}

// alias reads this Broker's self-reported alias, wrapping a configuration
// failure as a store error.
func (p *Pairing) alias() (string, error) {
	alias, err := p.settings.Alias()
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrStore, err)
	}
	return alias, nil
}

// nonce returns a fresh 32-byte random value. randRead is the test seam
// installed by newPairing; crypto/rand is the production fallback.
func (p *Pairing) nonce() ([]byte, error) {
	buf := make([]byte, nonceLength)
	if p.randRead != nil {
		if _, err := p.randRead(buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// handshakeReady reports whether the instance has the collaborators a
// handshake needs.
func (p *Pairing) handshakeReady() bool {
	return p != nil && p.identity != nil && p.transport != nil &&
		p.settings != nil && p.clock != nil && p.peers != nil && p.keys != nil
}
