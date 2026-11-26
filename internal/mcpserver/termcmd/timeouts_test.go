package termcmd

import "time"

// eventTimeout bounds a test's wait for an event that should arrive, so a wait
// that never completes fails instead of hanging.
const eventTimeout = 30 * time.Second
