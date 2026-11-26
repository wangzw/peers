package node

import "time"

// eventTimeout is the package-wide fallback for a wait on a state the test
// itself caused. It matches harness.EventTimeout, which this package cannot
// import because the harness imports node.
const eventTimeout = 30 * time.Second
