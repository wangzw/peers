package broker

import "time"

// eventTimeout bounds a wait that will never fail on its own: the success path
// selects on the event and never spends this time.
const eventTimeout = 30 * time.Second
