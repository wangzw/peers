package mcpserver

import "time"

// eventTimeout is the package-wide fallback for a wait on an event that is
// expected to arrive but that the test cannot force, whether the event is a
// child MCP process answering on stdio, a Register stream opened on a real
// Unix socket, or a registration returning. It only decides how long a wait
// that will never complete takes to fail: the success path selects on the
// event itself and never spends this time, so a larger value costs nothing.
// Thirty seconds is far above the worst normal latency under load; it is not a
// claim that an event should take that long.
const eventTimeout = 30 * time.Second
