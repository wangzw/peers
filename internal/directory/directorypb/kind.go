// Package directorypb defines the messages Brokers exchange to reconcile
// directory records.
package directorypb

// Frame kinds for directory reconciliation payloads.
const (
	KindRecordList    uint32 = 300
	KindRecordRequest uint32 = 301
	KindRecords       uint32 = 302
)
