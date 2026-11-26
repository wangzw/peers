// Package fakesession provides a fake Claude Code session for tests: it writes
// a session registry record, binds an inbox socket, and records the frames it
// receives. It depends only on the standard library and is meant for _test.go
// files, including end-to-end tests.
package fakesession
