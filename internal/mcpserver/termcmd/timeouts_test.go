package termcmd

import "time"

// eventTimeout is the package-wide fallback for a wait on an event that a test
// expects to arrive: a call returning after the fake Broker served it, an
// Audit handler sending its first event, or RunAudit returning once its
// context ends. It only decides how long a wait that will never complete takes
// to fail: the success path selects on the event itself and never spends this
// time, so a larger value costs nothing. Thirty seconds is far above the worst
// normal latency under load, including scheduler and -race overhead; it is not
// a claim that an event should take that long.
const eventTimeout = 30 * time.Second
