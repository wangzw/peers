package broker

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// peerDeliveryErrorCodes is the set of error codes a receiving Broker returns
// from its delivery handler. A code outside this set is not trusted: the peer
// may be a different version, or a faulty one, and the send reports an unknown
// result under delivery_unknown instead. Every code the receiver can return
// belongs here; adding one there requires adding it here.
var peerDeliveryErrorCodes = map[localpb.ErrorCode]bool{
	localpb.ErrorCode_ERROR_CODE_NOT_FOUND:        true,
	localpb.ErrorCode_ERROR_CODE_AMBIGUOUS:        true,
	localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE:      true,
	localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT: true,
	localpb.ErrorCode_ERROR_CODE_TOO_LARGE:        true,
	localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE: true,
	localpb.ErrorCode_ERROR_CODE_BROKER_OFFLINE:   true,
	localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN: true,
}

// peerMessageMaxBytes bounds the peer's message that this Broker quotes back.
// The receiving Broker already bounds its own text, but a different or faulty
// peer must not be able to make this Broker's error unbounded.
const peerMessageMaxBytes = 512

// peerError turns one error detail a peer returned into this Broker's error.
// An allowed code keeps its code, and its message is quoted under a local
// lead-in so the peer's text is presented as the peer's words. Any other code
// becomes delivery_unknown with fixed local text, and the peer's message is not
// repeated; only the peer fingerprint and the integer code are logged.
func (d *delivery) peerError(peer peerID, alias string, detail *localpb.ErrorDetail) *Error {
	origin := peerFingerprintBytes(peer)
	code := detail.GetCode()

	if !peerDeliveryErrorCodes[code] {
		d.logPeerDeliveryErrorUnrecognized(peer, code)
		return &Error{
			Code:    localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN,
			Message: "Delivery unknown: Broker " + alias + " returned an error this Broker does not recognize",
			Origin:  origin,
		}
	}

	phrase := "Not sent"
	if code == localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		phrase = "Delivery unknown"
	}
	message := trimPeerResultPrefix(detail.GetMessage())
	message = truncatePeerMessage(message)
	return &Error{
		Code:    code,
		Message: phrase + ": Broker " + alias + " said: " + strconv.Quote(message),
		Origin:  origin,
	}
}

// trimPeerResultPrefix removes the result lead-in the peer may have put on its
// message, because this Broker adds its own.
func trimPeerResultPrefix(message string) string {
	for _, prefix := range []string{"Not sent: ", "Delivery unknown: "} {
		if strings.HasPrefix(message, prefix) {
			return strings.TrimPrefix(message, prefix)
		}
	}
	return message
}

// truncatePeerMessage cuts message to peerMessageMaxBytes on a rune boundary,
// marking the cut with "...".
func truncatePeerMessage(message string) string {
	if len(message) <= peerMessageMaxBytes {
		return message
	}
	cut := peerMessageMaxBytes
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}
	return message[:cut] + "..."
}

// logPeerDeliveryErrorUnrecognized records a code outside peerDeliveryErrorCodes
// at warn level. The peer's message is deliberately not recorded.
func (d *delivery) logPeerDeliveryErrorUnrecognized(peer peerID, code localpb.ErrorCode) {
	if d.deps.Log == nil {
		return
	}
	d.deps.Log.Warn(EvPeerDeliveryErrorUnrecognized,
		"peer", hex.EncodeToString(peer[:]),
		"code", int32(code))
}
