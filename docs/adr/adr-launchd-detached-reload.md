# Reload on macOS in two stages through a detached helper process

## Context

The update runs inside the Broker process, and after the service definition is swapped the service manager has to reload it. Linux's `daemon-reload` does not stop a running process and can complete synchronously before the reply. macOS's `bootout` stops the Broker that is handling this update request, and the new binary, as a child process of the Broker, is in the same launchd job and is stopped with it; a synchronous reload would therefore interrupt the reply, and `bootstrap` would have no chance to run.

The reload must still happen: without it, launchd keeps starting the Broker with the old `ProgramArguments`, and the new version's startup method does not take effect.

## Options Considered

### Run `bootout` and `bootstrap` synchronously

Reload directly after rewriting the plist and before sending the reply.

**Rejected**: `bootout` stops the Broker handling the request and its child processes, so the caller gets no reply and `bootstrap` does not run either.

### Run only `bootstrap` without stopping the loaded job

**Rejected**: when the same label is already loaded, `bootstrap` does not replace it with the new plist, and the new `ProgramArguments` does not take effect.

### Have the user or a terminal command reload manually after the update

**Rejected**: an upgrade started from a session has no terminal; no one confirms the reload's result, and the update may report success while the service still runs the old definition.

### Have a helper process detached from the current job reload after the Broker exits

The new binary writes the plist and starts a helper process that is not part of the launchd job before sending the reply. After the Broker exits, the helper process runs `bootout` and then `bootstrap`, so the new definition takes effect.

**Adopted**; see Decision.

## Decision

The macOS reload is done in two stages. The first stage runs synchronously before the reply: the new binary rewrites the plist and starts the helper process. The second stage runs after the reply: the helper process waits for the Broker that started the update to exit, then reloads the service definition.

The helper process gives up the reload in two cases and only records the reason: the Broker has not exited within the time limit, so the update may already have rolled back; or the executable the plist points at is no longer the new binary that started the helper, meaning the binary has already been rolled back. Either case means `bootout` would stop a Broker that is serving the old version.

## Consequences

- On macOS, `bootout` followed by `bootstrap` briefly unloads the service; when `bootstrap` fails the service stays unloaded and has to be handled by hand according to the log and audit records.
- A second-stage failure happens after the successful reply and can no longer be returned to the caller, only written to the log and audit; `peers update` times out while waiting for the new version and gives the log location.
- Each update adds a detached process whose lifetime the service manager does not manage.
- Linux needs no helper process; the first stage completes the reload synchronously.

## Invalidation Conditions

When launchd provides a reload method that does not stop the job, or the deployment no longer uses launchd, this decision has to be re-evaluated.
