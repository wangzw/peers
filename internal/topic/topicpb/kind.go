// Package topicpb defines the messages Brokers exchange to compare and share
// topic rosters.
package topicpb

// Frame kinds for topic roster payloads.
const (
	KindTopicDigests uint32 = 200
	KindRoster       uint32 = 201
	KindRosterDelta  uint32 = 202
)
