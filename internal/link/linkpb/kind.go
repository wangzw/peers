package linkpb

// ProtocolVersion is the link protocol version carried in Hello.protocol_version.
// Version 2 changed the meaning of the topic digest fields, so a version 1
// Broker cannot share topics with a version 2 Broker and the link is refused.
const ProtocolVersion uint32 = 2

// Frame kinds for link payloads.
const (
	KindHello       uint32 = 1
	KindPeerInfo    uint32 = 2
	KindReject      uint32 = 3
	KindUnsupported uint32 = 4
)
