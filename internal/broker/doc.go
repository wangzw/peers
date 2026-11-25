// Package broker is the body and assembly entry point of the Broker process. It
// decides the deployment form, holds the agent table, serves the local
// interface with per-account permission checks, executes a send from address
// resolution to result, and wires together the packages that do not import one
// another.
package broker
