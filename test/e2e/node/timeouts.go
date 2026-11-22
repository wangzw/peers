package node

import "time"

// eventTimeout is the package-wide fallback for a wait on a state the test
// itself caused, with no product timer behind it. It only decides how long a
// wait that will never complete takes to fail: the success path observes the
// state itself and never spends this time, so a larger value costs nothing.
// Thirty seconds is far above the worst normal latency under load. It matches
// harness.EventTimeout, which this package cannot import because the harness
// imports node.
const eventTimeout = 30 * time.Second
