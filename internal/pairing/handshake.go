package pairing

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"google.golang.org/protobuf/proto"
)

const (
	handshakeTimeout = 10 * time.Second

	nonceLength     = 32
	lookupTagLength = 8
	proofLength     = sha256.Size
)

// Connect pairs with the Broker at address using key and commits the peer.
// localAlias is the local alias to reserve, required when the peer's alias
// conflicts or the call returns ErrNeedsLocalAlias. The handshake has a
// ten-second deadline that starts after the dial and ends in a *PairingError
// with ReasonTimeout. shownHost is the display-only host name from the
// "host(addr:port)" form. It returns ErrNotOpen, ErrInvalidAddress,
// ErrInvalidAlias, ErrPeerUnreachable, ErrProtocol, ErrStore, link's ErrClosed,
// the ctx error or a *PairingError, and leaves the connection to link on success.
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
	hint := computeHint([]byte(key), ni, p.identity.fingerprint())

	var result Peer
	err = p.transport.PairDial(ctx, dialAddr, func(conn PairConn) error {
		// The handshake deadline starts after the dial; link's dial timeout
		// has already bounded the connect.
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
			// A canceled dial is the caller's cancellation: link reports every
			// dial failure as ErrDialFailed, so cancellation is read from ctx.Err().
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

// runInitiator withholds the initiator's alias until the responder proof
// verifies: the alias travels only in PairConfirm, sent after that check and
// after the alias is reserved.
func (p *Pairing) runInitiator(runCtx, parent context.Context, conn PairConn, key string, ni, hint []byte, ownAlias, localAlias, dialAddr string) (Peer, error) {
	acceptorFP := conn.PeerFingerprint()
	acceptorPub := conn.PeerPublicKey()
	if len(acceptorPub) != 32 || FingerprintOf(acceptorPub) != acceptorFP {
		return closeAndFail(conn, &PairingError{Reason: ReasonVerificationFailed, Detail: "peer certificate does not match its fingerprint"})
	}

	request := &pairingpb.PairRequest{
		InitiatorNonce:  ni,
		LookupTag:       hint,
		ProtocolVersion: conn.ProtocolVersion(),
	}
	if err := sendMessage(runCtx, conn, kindPairRequest, request); err != nil {
		return Peer{}, p.initiatorFrameError(conn, parent, runCtx, err)
	}

	kind, payload, err := conn.Receive(runCtx)
	if err != nil {
		return Peer{}, p.initiatorFrameError(conn, parent, runCtx, err)
	}
	if kind == kindPairReject {
		return closeAndFail(conn, rejectError(payload))
	}
	if kind != kindPairChallenge {
		return closeAndFail(conn, fmt.Errorf("%w: expected PairChallenge, got frame %d", ErrProtocol, kind))
	}
	var challenge pairingpb.PairChallenge
	if err := proto.Unmarshal(payload, &challenge); err != nil {
		return closeAndFail(conn, fmt.Errorf("%w: cannot decode PairChallenge: %v", ErrProtocol, err))
	}
	nr := challenge.GetResponderNonce()
	if len(nr) != nonceLength || len(challenge.GetProof()) != proofLength {
		return closeAndFail(conn, fmt.Errorf("%w: PairChallenge has a malformed nonce or proof", ErrProtocol))
	}

	want := responderProof([]byte(key), ni, nr, acceptorFP, p.identity.fingerprint())
	if !hmac.Equal(want, challenge.GetProof()) {
		return closeAndFail(conn, &PairingError{Reason: ReasonVerificationFailed, Detail: "responder proof did not verify"})
	}

	// Reserve the effective alias before sending PairConfirm; commitHandshake
	// re-checks it under the peer-table lock.
	finalAlias, local := p.chooseInitiatorAlias(acceptorFP, challenge.GetResponderAlias(), ownAlias, localAlias)
	token, reserved := uint64(0), false
	if finalAlias != "" {
		token, reserved = p.reserveAlias(acceptorFP, finalAlias, ownAlias)
	}
	if !reserved {
		return closeAndFail(conn, ErrNeedsLocalAlias)
	}
	committed := false
	defer func() {
		if !committed {
			p.releaseAlias(token)
		}
	}()

	proof := initiatorProof([]byte(key), ni, nr, p.identity.fingerprint(), acceptorFP)
	confirm := &pairingpb.PairConfirm{Proof: proof, InitiatorAlias: ownAlias}
	if err := sendMessage(runCtx, conn, kindPairConfirm, confirm); err != nil {
		return Peer{}, p.initiatorFrameError(conn, parent, runCtx, err)
	}

	kind, payload, err = conn.Receive(runCtx)
	if err != nil {
		return Peer{}, p.initiatorFrameError(conn, parent, runCtx, err)
	}
	switch kind {
	case kindPairReject:
		return closeAndFail(conn, rejectError(payload))
	case kindPairDone:
	default:
		return closeAndFail(conn, fmt.Errorf("%w: expected PairDone, got frame %d", ErrProtocol, kind))
	}

	// The reservation and own alias are re-checked under the peer-table lock in
	// the same critical section that commits.
	entry := p.dialerEntry(acceptorFP, acceptorPub, challenge.GetResponderAlias(), local, dialAddr, challenge.GetAdvertisedAddresses())
	replaced, committedAlias, frozen, err := p.peers.commitHandshake(entry, token)
	if err != nil {
		return closeAndFail(conn, err)
	}
	committed = true
	if replaced {
		p.transport.Drop(acceptorFP)
	}
	// Audit the alias the table committed: re-pairing can freeze the current
	// effective alias instead of adopting the reserved one.
	if frozen {
		auditLocalAliasAssigned(p.settings, p.clock.Now(), acceptorFP, committedAlias)
	}
	return p.peerView(acceptorFP), nil
}

// Accept runs the acceptor side of a pairing handshake over conn, which link
// hands over when the first frame is a PairRequest. It has a ten-second
// deadline that ends in a *PairingError with ReasonTimeout. On success the
// connection is left open for link; a failure after the handshake begins closes
// it, while ErrNotOpen returns without closing conn and leaves it to the caller.
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
		return closeWithError(conn, fmt.Errorf("%w: peer certificate does not match its fingerprint", ErrProtocol))
	}

	kind, payload, err := conn.Receive(ctx)
	if err != nil {
		return p.acceptFrameError(conn, ctx, initiatorFP, "", err)
	}
	if kind != kindPairRequest {
		return closeWithError(conn, fmt.Errorf("%w: expected PairRequest, got frame %d", ErrProtocol, kind))
	}
	var request pairingpb.PairRequest
	if err := proto.Unmarshal(payload, &request); err != nil {
		return closeWithError(conn, fmt.Errorf("%w: cannot decode PairRequest: %v", ErrProtocol, err))
	}
	// The version check comes before any key lookup, so a version mismatch is
	// never reported as a wrong key.
	if request.GetProtocolVersion() != conn.ProtocolVersion() {
		return p.rejectAcceptor(conn, ctx, initiatorFP, ReasonVersionIncompatible, p.clock.Now())
	}
	ni := request.GetInitiatorNonce()
	if len(ni) != nonceLength || len(request.GetLookupTag()) != lookupTagLength {
		return closeWithError(conn, fmt.Errorf("%w: PairRequest has a malformed nonce or lookup tag", ErrProtocol))
	}

	// The alias arrives in PairConfirm and is read only after the initiator
	// proof verifies; every earlier failure records the fingerprint alone.
	initiatorAlias := ""

	now := p.clock.Now()
	maxFailures, err := p.settings.MaxFailures()
	if err != nil {
		return closeWithError(conn, fmt.Errorf("%w: %v", ErrStore, err))
	}
	maxAttempts, err := p.settings.MaxAttemptsPerMinute()
	if err != nil {
		return closeWithError(conn, fmt.Errorf("%w: %v", ErrStore, err))
	}
	match, found, lookupErr := p.keys.lookupAtRequest(ni, request.GetLookupTag(), initiatorFP, now, sourcePrefix(conn.RemoteAddr()), maxAttempts, p.settings.StaticKey)
	if !found {
		reason := ReasonKeyInvalid
		if lookupErr != nil {
			var pairErr *PairingError
			if errors.As(lookupErr, &pairErr) {
				reason = pairErr.Reason
			}
		}
		return p.rejectAcceptor(conn, ctx, initiatorFP, reason, now)
	}

	nr, err := p.nonce()
	if err != nil {
		return closeWithError(conn, err)
	}
	ownAlias, err := p.alias()
	if err != nil {
		return closeWithError(conn, err)
	}
	advertised, err := p.settings.Advertised()
	if err != nil {
		return closeWithError(conn, fmt.Errorf("%w: %v", ErrStore, err))
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
		return closeWithError(conn, fmt.Errorf("%w: expected PairConfirm, got frame %d", ErrProtocol, kind))
	}
	var confirm pairingpb.PairConfirm
	if err := proto.Unmarshal(payload, &confirm); err != nil {
		return closeWithError(conn, fmt.Errorf("%w: cannot decode PairConfirm: %v", ErrProtocol, err))
	}
	if len(confirm.GetProof()) != proofLength {
		return closeWithError(conn, fmt.Errorf("%w: PairConfirm has a malformed proof", ErrProtocol))
	}

	verifiedAt := p.clock.Now()
	ok, gone := func() (bool, RejectReason) {
		match.proof = confirm.GetProof()
		return p.keys.verifyAtConfirm(match, ni, nr, p.identity.fingerprint(), initiatorFP, p.settings.StaticKey(), verifiedAt)
	}()
	if gone != 0 {
		return p.rejectAcceptor(conn, ctx, initiatorFP, gone, verifiedAt)
	}
	if !ok {
		p.keys.recordVerificationFailure(match, maxFailures, verifiedAt)
		return p.rejectAcceptor(conn, ctx, initiatorFP, ReasonVerificationFailed, verifiedAt)
	}

	initiatorAlias = confirm.GetInitiatorAlias()

	ownAliasNow, err := p.alias()
	if err != nil {
		return closeWithError(conn, err)
	}
	finalAlias, localAlias, token, ok := p.reserveAcceptorAlias(initiatorFP, initiatorAlias, ownAliasNow)
	if !ok {
		return closeWithError(conn, ErrNeedsLocalAlias)
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
		return p.acceptFrameError(conn, ctx, initiatorFP, finalAlias, err)
	}

	// The peer commits only after the initiator's first link Hello arrives; the
	// frame is put back for link to read.
	kind, payload, err = conn.Receive(ctx)
	if err != nil {
		return p.acceptFrameError(conn, ctx, initiatorFP, finalAlias, err)
	}
	if kind != kindHello {
		return closeWithError(conn, fmt.Errorf("%w: expected link Hello after PairDone, got frame %d", ErrProtocol, kind))
	}
	conn.Unread(kindHello, payload)

	// Re-check the reservation and the own alias in the same lock that commits,
	// so a competing handshake or rename cannot produce an inconsistent commit.
	replaced, committedAlias, frozen, err := p.peers.commitHandshake(pending, token)
	if err != nil {
		return closeWithError(conn, err)
	}
	reserved = false
	if replaced {
		p.transport.Drop(initiatorFP)
	}

	// Audit the alias the table committed, not the one reserved before PairDone:
	// re-pairing can freeze the current effective alias.
	committedAt := p.clock.Now()
	auditPairingSucceeded(p.settings, committedAt, initiatorFP, committedAlias)
	if localAlias != "" || frozen {
		auditLocalAliasAssigned(p.settings, committedAt, initiatorFP, committedAlias)
	}
	return nil
}

func pairProof(label string, key, ni, nr []byte, self, seen Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(label))
	mac.Write(ni)
	mac.Write(nr)
	mac.Write(self[:])
	mac.Write(seen[:])
	return mac.Sum(nil)
}

func responderProof(key, ni, nr []byte, acceptor, initiatorSeen Fingerprint) []byte {
	return pairProof(pairResponderLabel, key, ni, nr, acceptor, initiatorSeen)
}

func initiatorProof(key, ni, nr []byte, initiator, acceptorSeen Fingerprint) []byte {
	return pairProof(pairInitiatorLabel, key, ni, nr, initiator, acceptorSeen)
}

func (p *Pairing) rejectAcceptor(conn PairConn, ctx context.Context, fp Fingerprint, reason RejectReason, now time.Time) error {
	switch reason {
	case ReasonTryLater:
		auditPairingFailed(p.settings, now, fp, "", auditReasonTryLater, auditSourceMergeKey(conn.RemoteAddr()))
	case ReasonVerificationFailed:
		auditPairingFailed(p.settings, now, fp, "", auditReasonVerificationFailed, "")
	case ReasonKeyExpired:
		auditPairingFailed(p.settings, now, fp, "", auditReasonKeyExpired, "")
	case ReasonVersionIncompatible:
		// A version mismatch is not counted as an attempt, so the audit merges
		// these by source to bound the log.
		auditPairingFailed(p.settings, now, fp, "", auditReasonVersionIncompatible, auditSourceMergeKey(conn.RemoteAddr()))
	default:
		auditPairingFailed(p.settings, now, fp, "", auditReasonKeyInvalid, "")
	}
	_ = sendRejectAndLinger(ctx, conn, reason)
	return &PairingError{Reason: reason, Detail: rejectDetail(reason)}
}

func (p *Pairing) acceptFrameError(conn PairConn, ctx context.Context, fp Fingerprint, alias string, err error) error {
	_ = conn.Close()
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		auditPairingFailed(p.settings, p.clock.Now(), fp, alias, auditReasonTimeout, "")
		return &PairingError{Reason: ReasonTimeout, Detail: "handshake timed out"}
	}
	auditPairingFailed(p.settings, p.clock.Now(), fp, alias, auditReasonNotConnected, "")
	return &PairingError{Reason: ReasonNotConnected, Detail: "connection closed during pairing"}
}

// initiatorFrameError only classifies the error: the broker owns the
// initiator's audit event.
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

func closeAndFail(conn PairConn, err error) (Peer, error) {
	_ = conn.Close()
	return Peer{}, err
}

func closeWithError(conn PairConn, err error) error {
	_ = conn.Close()
	return err
}

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

func (p *Pairing) reserveAlias(fp Fingerprint, alias, ownAlias string) (uint64, bool) {
	if alias == "" || alias == ownAlias {
		return 0, false
	}
	p.peers.mu.Lock()
	defer p.peers.mu.Unlock()
	return p.peers.reserveAliasLocked(alias, fp)
}

func (p *Pairing) releaseAlias(token uint64) {
	if token == 0 {
		return
	}
	p.peers.mu.Lock()
	p.peers.releaseTokenLocked(token)
	p.peers.mu.Unlock()
}

func (p *Pairing) peerView(fp Fingerprint) Peer {
	entry, _ := p.peers.get(fp)
	return entry.toPeer()
}

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
	if prior, ok := p.peers.get(fp); ok {
		entry.card = append([]config.AdvertisedAddr(nil), prior.card...)
	}
	entry.acceptor = capKnown(advertisedFromPB(advertised))
	entry.pairingAddr = dialAddr
	return entry
}

// sendRejectAndLinger keeps the connection open briefly so the PairReject can
// leave the transport's write buffer before the close.
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
	case ReasonVersionIncompatible:
		return pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_VERSION_INCOMPATIBLE, true
	default:
		return pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_UNSPECIFIED, false
	}
}

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
	case pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_VERSION_INCOMPATIBLE:
		return ReasonVersionIncompatible, true
	default:
		return 0, false
	}
}

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
	case ReasonVersionIncompatible:
		return "link protocol version incompatible with the other Broker; upgrade the older Broker"
	default:
		return "pairing rejected"
	}
}

func sendMessage(ctx context.Context, conn PairConn, kind uint32, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.Send(ctx, kind, payload)
}

func advertisedToPB(addrs []config.AdvertisedAddr) []*pairingpb.AdvertisedAddress {
	out := make([]*pairingpb.AdvertisedAddress, 0, len(addrs))
	for _, addr := range addrs {
		out = append(out, &pairingpb.AdvertisedAddress{Kind: addr.Kind, Addr: addr.Addr})
	}
	return out
}

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
	if splitErr != nil || host == "" || !validDialPort(port) || !config.ValidDialHost(host) {
		return "", "", ErrInvalidAddress
	}
	return dialAddr, shownHost, nil
}

func (p *Pairing) alias() (string, error) {
	alias, err := p.settings.Alias()
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrStore, err)
	}
	return alias, nil
}

func (p *Pairing) nonce() ([]byte, error) {
	buf := make([]byte, nonceLength)
	if err := p.randomBytes(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (p *Pairing) handshakeReady() bool {
	return p != nil && p.identity != nil && p.transport != nil &&
		p.settings != nil && p.clock != nil && p.peers != nil && p.keys != nil
}
