# Read accounts and groups from the system account database

## Context

A system Broker needs account and group information: resolving `broker.local_socket_group`, spawning a helper process that switches to an account, and spawning a process running with that account's effective identity for permission checks. This information all comes from the account database. The account database on Linux is determined by the NSS configuration, and `/etc/passwd` and `/etc/group` are only one source of it; sources such as LDAP, SSSD and extrausers can answer queries just as well.

Release builds always use `CGO_ENABLED=0`, so the binary is static and deployment does not depend on the host's C library version. Under this build, `os/user` reads local files only and does not go through NSS. Accounts provided through NSS therefore cannot be resolved: the system Broker cannot spawn helper processes for these accounts, permission checks always deny, and a group named in the configuration that comes from such a source is also judged not to exist.

## Options Considered

### Turn on cgo in the Linux build

Let `os/user` use glibc's `getpwnam` and similar interfaces, with the C library performing the NSS query.

**Rejected**: with cgo on, the binary is no longer static, and both linking and running depend on the C library of the build host and the target host; the way release artifacts and container images are built, and the assumption of cross-distribution deployment, would all have to change. Abandoning a static build for one query feature is a cost out of proportion to the benefit.

### Keep reading only `/etc/passwd` and `/etc/group`

Stay as is and put accounts from NSS sources outside the service scope of the system Broker.

**Rejected**: this is exactly the limitation that needs fixing. A system Broker serves every account on a host, and the account source is configured by the system administrator; drawing a service boundary by file source has no basis.

### Require the deployer to copy accounts into `/etc/passwd`

Have the deployer maintain a local copy and keep the Broker reading files.

**Rejected**: the same account data would have two sources, and the copy would drift from the directory service; changes to group membership would also not take effect on the next call, conflicting with `REQ-broker-config-027`.

### Call getent on Linux

Find getent by absolute path and let it query the account database according to the NSS configuration.

**Adopted**.

## Decision

On Linux, account and group queries go through `getent`. The lookup order is fixed to `/usr/bin/getent`, then `/bin/getent`, without using `PATH`; the child process environment gives only `LANG=C`; an argument is prefixed with `--` before the account name or uid, and no shell is involved. Each call is limited to 5 seconds.

Account names are validated first; an empty string, or a name containing a colon, newline, NUL or whitespace, is always refused. The output must be exactly one line: a passwd entry has exactly 7 fields and a group entry exactly 4, the returned name or uid matches the request, uid and gid are parsed as decimal 32-bit values, and the home directory must be an absolute path. Supplementary groups come from `getent initgroups`, and the result is merged with the primary group, deduplicated and sorted.

Exit code 2 means the entry does not exist in the account database and maps to `ErrNotFound`. When getent is missing, times out or returns another exit code, or when the output fails the validation above, the query falls back to `os/user` and logs one Warn. When the supplementary group query fails, it also falls back to `os/user`, but first checks that the local entry's uid matches the request; if it does not match, the result is not used, and the original query failure is reported as a temporary error rather than mapped to `ErrNotFound`. Other platforms use `os/user` directly, with no change in behavior.

## Consequences

- Each query adds one process spawn. Permission checks and helper process derivation happen far less often than message delivery, so this cost does not change the latency design of the call path.
- The account source follows the system's NSS configuration. When `nsswitch.conf` is misconfigured, the Broker sees exactly the data that configuration points to; after an administrator changes group membership, the next call takes effect.
- The build still uses `CGO_ENABLED=0`, so release binaries stay static and introduce no C library version constraint.
- Falling back to `os/user` sees only local files. When getent is missing or fails, accounts from NSS sources are still invisible, and the Warn in the log is the only clue; permission checks then deny rather than allow. When supplementary groups fall back to the local source, the uid is checked, so an account with the same name but a different uid is not mixed in; when the check fails or the account is also absent locally, the original query failure is reported as a temporary error, and permission checks fail as `internal` rather than reporting the account as absent.
- Distinguishing not found from a failed query depends on getent's exit code convention. If a getent version changes this convention, the boundary between `ErrNotFound` and the fallback changes with it.
- The absolute path, the empty environment and `--` keep a Broker running as root from executing a same-named program on `PATH` and from treating an account name as a command-line option; the one-line and field validation keep a multi-line or malformed reply from replacing a real entry.

## Invalidation Conditions

If Linux gains a stable account database interface usable by a static binary, or the Go standard library can query NSS without cgo, the getent layer can be removed and this record rewritten accordingly.
