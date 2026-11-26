// Package pairingpb defines the pairing handshake frames and their frame kinds.
package pairingpb

// Frame kinds for the pairing handshake payloads.
const (
	KindPairRequest   uint32 = 100
	KindPairChallenge uint32 = 101
	KindPairConfirm   uint32 = 102
	KindPairDone      uint32 = 103
	KindPairReject    uint32 = 104
)
