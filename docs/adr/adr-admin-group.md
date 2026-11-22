# Delegate system Broker administration to a group

## Context

The system Broker runs as root and serves several accounts on one host. Operations that need write permission on the configuration file, including generating pairing keys, changing configuration, initiating pairing and updating the Broker, could originally be performed only by root. Production environments do not use root for day-to-day management; administrators need an ordinary account to complete these operations, with usage similar to the docker group.

The admin group is not root: what accounts in the group can do must be what an administrator could already do by editing the configuration file by hand. The Broker runs as root, so any configuration item that lets an account in the group use it to execute arbitrary code, write arbitrary files or gain root capability would turn the admin group into de facto root. The privilege-escalation review is therefore a precondition for adopting this decision.

## Options Considered

### Write documentation only and let administrators chgrp and chmod the configuration file themselves

Installation creates no group, and the documentation explains how to hand the configuration file to a group.

**Not adopted**: it is compatible with this decision, and an existing installation still needs this step. The default state of a new installation stays writable only by root, and "provide an admin group at install time" is not implemented.

### Create and authorize a group at install time

The system service installation creates an admin group, the newly generated configuration file takes the group as its owning group and is granted group read and write permission, and the install script asks whether to add the account running the install to the group.

**Adopted**; see Decision.

### A sudoers rule

Write a sudo rule for the admin group that allows it to run specific subcommands of `peers`.

**Rejected**: a sudoers rule hands command execution to root, and members of the admin group in effect gain the ability to run `peers` as root. The configuration file permission model no longer applies, and the authorization cannot be limited to the configuration file the Broker currently uses.

### A separate privileged token on the local interface

Management operations are authenticated through a token over the local interface, independent of file permissions.

**Rejected**: the token has to be issued, kept and revoked, and the configuration file's file permissions are no longer the only basis. It conflicts with [ADR-management-by-config-file-permission](adr-management-by-config-file-permission.md).

## Decision

Adopt creating and authorizing a group at install time. The admin group is named `peers` by default and can be specified with `service install --system --admin-group <name>`; if the group does not exist it is created, and if it exists it is reused without changing its members, with the members checked against the rules below before reuse. A newly generated system configuration file takes root as its owner and the admin group as its owning group, with mode `0660`; an existing configuration file's contents, owning group and mode are left untouched, and the install output gives the commands that hand it to the admin group. The release package's install script, under `--system`, asks whether to add the account running the install to the admin group, and does not add it by default; `--join-admin-group` adds it directly, and `--no-admin-group` skips it.

When an existing group is reused, a group with gid 0 is always refused, because that group is root's group and accounts in it would also gain permission on the configuration file. Installation is also refused when all of the following hold: the group already exists and was not created this time; `/var/lib/peers/admin-group` does not record this group name, meaning it was not created by a previous peers installation; a new configuration file is about to be generated; the administrator did not explicitly specify the group name with `--admin-group`; and the group lists members. Those members would gain management authority over the system Broker without the administrator knowing, and explicitly specifying the group name is treated as the administrator confirming them, so no further question is asked. The refusal happens before any file is written and, like a failure to create the group, leaves no configuration file.

Membership includes the supplementary members the account database lists and accounts whose primary group is this group; on macOS it also includes nested groups listed in `NestedGroups`, and accounts in a nested group likewise gain permission on the configuration file. A failure to read membership is treated as a refusal, and a group that cannot be read is not treated as having no members. When the administrator explicitly specifies the group name with `--admin-group`, or when an existing configuration file is reused this time, installation skips this check and does not read membership, so a reinstall is not blocked by a failure of the account database.

Whether the group is new or reused, the install summary lists an `admin group:` line stating the group name and its source: `peers (created)`, `peers (existing)`, `peers (existing, members: alice, bob)` or `peers (existing, no members listed)`. `existing` alone means membership reading was skipped this time.

The admin group is equivalent to "can edit the configuration file by hand".

### Conclusions of the privilege-escalation review

After reviewing each configuration item, only the low-port path gives an admin group member a capability beyond editing the configuration file, and it is eliminated by the rules below:

- When `broker.listen`'s port is below 1024, it takes effect only when the configuration file is writable only by root, that is, neither the owning group nor others has write permission and the access control list grants no extra write authorization. When `broker.listen` is set to a low port through the local interface and the caller is not root, the call returns `permission_denied`, explaining that a low port requires root to change and the configuration file must be writable only by root. When the configuration file is writable by a group or others, a hot reload does not adopt the new low port, keeps the old value, writes a Warn-level run log and writes an audit record; at startup the same condition refuses to start, with the exit code that an invalid configuration uses. A user Broker is unaffected, since an ordinary account cannot bind a low port anyway. After the administrator tightens the configuration file's permissions, a low-port value already in the file is adopted, and it may have been written by another account that could write the file; the Broker therefore does not rewrite this value, but on refusal writes the current value of `broker.listen` in the file, for root to check before tightening the permissions.
- The path that writes the configuration file is opened with `O_NOFOLLOW`; when the configuration file is a symbolic link, the write fails and the original file is unchanged. This rule rests on a hard premise: the configuration directory `/etc/peers` is owned by root and not writable by the group.
- A reload triggered by editing the configuration file directly has no account attribution: the audit record states only that the source is a file, with `account` and `actor` empty; a change through the local interface records the caller's account. This is a known boundary and does not change the implementation.

The other configuration items have no privilege-escalation path. The admin group therefore cannot gain the ability to execute code as root or write arbitrary files. The `agent-peer update` command for the system installation was later changed so that the Broker downloads, verifies and replaces the binary itself ([adr-self-update](adr-self-update.md)); the admin group's write permission therefore also allows updating the system installation's binary, and the trust scope widens to that action accordingly. Members still cannot specify the source or version, or request a downgrade, and cannot write files other than the configuration file.

Difference from the docker group: members of the docker group can make docker run arbitrary code as root, so the group permission equals root; this group can only read and write one configuration file, and once the low-port path is eliminated by the rules above, members cannot use the Broker to execute arbitrary code or write files other than the configuration file.

## Consequences

An admin group member can perform operations that need read and write permission on the configuration file without root or sudo, and the audit records that account. The default security boundary of a system installation changes from "only root can manage" to "root and the configuration file's owning group can manage".

**Costs accepted:**

- The configuration file is writable by the admin group, so any account in the group can change every configuration item, including the advertised address, the listen address and the public switch. These changes have the same effect as the administrator editing the file by hand.
- The member check on reusing an existing group depends on the platform's account and group lookup: accounts the lookup does not enumerate are not seen. When the lookup fails, installation refuses, and a group that cannot be read is not treated as having no members.
- Before the administrator tightens the configuration file's permissions, a low-port value written by an account in the group is adopted after the tightening. The Broker writes that value out on refusal but does not record which account wrote it, and root has to check against it accordingly.
- The privilege-escalation review depends on the constraint that "a port below 1024 requires root"; when a configuration item is added or a key's meaning changes, it has to be reviewed again to see whether it gives an account in the group root capability or the ability to write arbitrary files.
- A change triggered by editing the configuration file directly has no account attribution in the audit and can be traced only to the file.
- An existing installation is not migrated automatically: the configuration file's owning group and permissions stay as they are, and the administrator has to authorize by hand with the commands the install output gives.

## Invalidation Conditions

When the admin group is no longer equivalent to "can edit the configuration file by hand", for example when some configuration item lets an account in the group execute arbitrary code or write any file other than the configuration file, this decision no longer holds and the authorization method has to be redesigned. The same applies when the system Broker changes to not run as root and the permission premise of management operations is no longer the configuration file's file permissions.
