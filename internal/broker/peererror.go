package broker

import (
	"encoding/hex"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// peerDeliveryErrorCodes is the set of error codes a receiving Broker returns
// from its delivery handler; a code outside it becomes delivery_unknown, so a
// code the receiver adds must be added here.
var peerDeliveryErrorCodes = map[localpb.ErrorCode]bool{
	localpb.ErrorCode_ERROR_CODE_NOT_FOUND:        true,
	localpb.ErrorCode_ERROR_CODE_AMBIGUOUS:        true,
	localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE:      true,
	localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT: true,
	localpb.ErrorCode_ERROR_CODE_TOO_LARGE:        true,
	localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE: true,
	localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED:   true,
	localpb.ErrorCode_ERROR_CODE_BROKER_OFFLINE:   true,
	localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN: true,
}

// peerMessageMaxBytes bounds the peer message this Broker quotes so a faulty peer cannot make the error unbounded.
const peerMessageMaxBytes = 512

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

func trimPeerResultPrefix(message string) string {
	for _, prefix := range []string{"Not sent: ", "Delivery unknown: "} {
		if strings.HasPrefix(message, prefix) {
			return strings.TrimPrefix(message, prefix)
		}
	}
	return message
}

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

func (d *delivery) logPeerDeliveryErrorUnrecognized(peer peerID, code localpb.ErrorCode) {
	if d.deps.Log == nil {
		return
	}
	d.deps.Log.Warn(EvPeerDeliveryErrorUnrecognized,
		"peer", hex.EncodeToString(peer[:]),
		"code", int32(code))
}
