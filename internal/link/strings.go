package link

// String returns the lowercase phase name used in logs and status details.
func (p Phase) String() string {
	switch p {
	case PhaseNone:
		return "none"
	case PhaseDialing:
		return "dialing"
	case PhaseWaiting:
		return "waiting"
	case PhaseHandshaking:
		return "handshaking"
	case PhaseEstablished:
		return "established"
	case PhaseBackoff:
		return "backoff"
	case PhaseRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// String returns the lowercase failure-reason name used in logs and audit
// details.
func (r FailReason) String() string {
	switch r {
	case FailNone:
		return "none"
	case FailUnreachable:
		return "unreachable"
	case FailRoleMismatch:
		return "role_mismatch"
	case FailVersionIncompatible:
		return "version_incompatible"
	case FailNotAPeer:
		return "not_a_peer"
	case FailQueueFull:
		return "queue_full"
	case FailConnectionLost:
		return "connection_lost"
	case FailProtocol:
		return "protocol"
	case FailInboundLimit:
		return "inbound_limit"
	default:
		return "unknown"
	}
}
