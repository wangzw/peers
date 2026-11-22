package main

import "time"

// eventTimeout is the package-wide fallback for an event this package cannot
// force. The events are a simulated Agent stopping after its context is
// cancelled, a line arriving on a unix socket, a SIGKILLed helper process
// exiting, and a filesystem watcher waking its waiters. It only decides how
// long a wait that will never complete takes to fail: the success path selects
// on the event itself and never spends this time, so a larger value costs
// nothing. Thirty seconds is far above the worst normal latency under load
// (including a cold subprocess start); it is not a claim that an event should
// take that long.
const eventTimeout = 30 * time.Second
