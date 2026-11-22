# Gate management operations on the caller's permission on the configuration file

## Context

The system [Broker](../glossary.md) serves agents under several accounts on one host. Changing configuration, generating pairing keys, initiating pairing and unpairing change the whole Broker's state and affect every account under it; listing and sending involve only the caller's own agents. A Broker's configuration is a file, and who may read it and who may change it is decided by the operating system's file permissions. Operating a Broker through tools is for convenience and should not let an account do what it could not do by changing the file directly.

## Options Considered

### Every served account can perform management operations

Every account that can use the Broker can change configuration, pair and unpair.

**Rejected**: under a system Broker, any account could pair the whole host with an unknown host or change configuration that other accounts depend on.

### Set up a separate administrator list in the configuration

The configuration lists the accounts that can perform management operations, and the Broker judges by it.

**Rejected**: who can change the configuration is already decided by file permissions; a second list is a second expression of the same thing, and the two can disagree. The list itself lives in the configuration file, so an account that can change the file can still add itself to it.

### Judge by the caller's read and write permission on the configuration file

The Broker determines the caller's account from the process credentials that the kernel provides for the process at the other end of the local socket (process credentials), and does not trust the caller's statement. Only when the caller's account has write permission on the Broker's configuration file can it change configuration, generate pairing keys, initiate pairing and unpair; only with read permission can it read configuration, view every agent's status and view the audit log. Registering, listing, joining and leaving topics, and sending messages require no file permission. Whether the permission is held is determined by an actual access check of the configuration file by a process running under the caller's account.

**Adopted**; see Decision.

## Decision

A management operation on a Broker is gated by the caller account's access permission on the Broker's configuration file: write permission is needed to change configuration, generate pairing keys, initiate pairing and unpair, and read permission is needed to read configuration. The caller's account comes from the process credentials the kernel provides. Operations that do not change the Broker's state and involve only the caller's own agents require no file permission.

## Consequences

What can be done through the tools does not exceed what the account can do by operating the file directly. An administrator delegates management with the means the operating system already provides: grant write permission on the configuration file to a group, and accounts in that group can manage the Broker through the tools. A user Broker's configuration file belongs to the account, so this rule is satisfied naturally for it, and a user Broker makes no judgment.

**Costs accepted:**

- There are only the two levels of read and write: it is not possible to allow an account to pair but not to change configuration.
- Generating a pairing key, initiating pairing and unpairing do not write the configuration file, yet they are judged by write permission on the configuration file; file permission here stands for management authority.
- What is identified is the process's account, not a person and not a model: any process under the same account can operate with that account's permission, and a call started by a model under prompt injection cannot be told apart from the user's own call.
- The judgment depends on the operating system providing process credentials, and the caller's supplementary groups come from the account database and may differ from the groups the calling process actually carries.
- A system Broker cannot work it out from the permission bits by itself; every judgment has to fork a process that checks under the caller account's effective identity, at the cost of one more process fork.
