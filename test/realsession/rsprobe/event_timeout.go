package main

import "time"

// eventTimeout bounds only a wait that can never complete: the success path
// selects on the event itself, so a larger value costs nothing.
const eventTimeout = 30 * time.Second
