// Package deliverpb defines the cross-Broker delivery messages and the frame
// kinds they travel under.
package deliverpb

// Frame kinds for cross-Broker delivery payloads.
const (
	KindDeliver       uint32 = 400
	KindDeliverResult uint32 = 401
)
