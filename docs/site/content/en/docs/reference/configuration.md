---
title: Configuration keys
weight: 3
---

Both deployment forms read one YAML file: `~/.peers/config.yaml` for a user Broker and `/etc/peers/config.yaml` for a system Broker. A Broker runs normally without a file, and every key is optional: an absent key takes its default. See [The generated file](#the-generated-file) below for how `peers service install` creates it. The Broker watches the file, so a valid change takes effect while it runs, whether the change is written by hand, by `set_config` or by `peers config set`. The two keepalive keys are the exception: they are read when a listener or a link is created, so a change applies only to listeners or links established afterwards.

## The generated file

`peers service install` writes the template only when the file for its level does not exist; an existing file is left unchanged. The template lists every key, with one line of explanation above each. A key with a fixed default is written out with that value. The five keys whose default is empty or computed at runtime are written as comments with an example instead: `broker.alias`, whose default is computed at runtime, and `broker.advertise`, `broker.advertise_extra`, `broker.local_socket_group` and `pairing.static_key`, which default to empty. Uncomment a line and fill in a valid value, and the change takes effect while the Broker runs, like any other valid change. A Broker started without a configuration file does not get a template.

The two templates differ in three places. The first line names the level: `# peers user Broker configuration.` or `# peers system Broker configuration.` The opening comment differs: the user template says the file belongs to the account alone, while the system template explains that granting a group read access to the file lets that group's accounts read the configuration, the full status and the audit log, and granting write access lets them change the configuration and manage pairing. The user template also notes after `broker.local_socket_group` and `broker.local_socket_mode` that those keys apply to the system Broker only.

The template begins with a comment block:

```yaml
# peers user Broker configuration.
#
# Every key is optional; a key that is absent takes its default value.
# This file belongs to this account only: only this account runs the Broker
# and reads or writes the file.
```

The file groups its keys into sections. The `broker` section comes between this opening comment and `pairing`; the `topics`, `directory`, `send`, `link` and `log` sections follow `pairing`. The excerpt below omits them and shows the `pairing` section:

```yaml
pairing:
  # Pairing requests per minute that match no pairing key; successful matches of the static key are not counted.
  max_attempts_per_minute: 10
  # Failed verifications allowed before a one-time pairing key is invalidated.
  max_failures: 5
  # Static pairing key of 16 to 128 characters; a non-empty value takes effect.
  # static_key: ""
  # Fill in 16 to 128 random characters, for example the output of `openssl rand -hex 32`. An empty value leaves the static key unset.
```

## Keys

| Key | Type | Default | Meaning | `set_config`, `peers config set` |
|---|---|---|---|---|
| `broker.alias` | string | derived | The alias this Broker reports. Derived from the hostname and, for a user Broker, the account when not set. 1 to 24 characters from letters, digits, `.`, `-` and `_`. The default is described below the table. | Yes |
| `broker.public` | boolean | `false` | Whether this Broker publishes a broker card and is visible to the whole connected network. | Yes |
| `broker.listen` | string | `:17443` | The address and port to bind, written `host:port`, `:port` or `[v6]:port`. An empty host means every local address; an empty string means do not listen. | Yes, except under `--listen` |
| `broker.advertise` | string | empty | Replaces the automatically detected advertised address. Accepted forms are empty, `host`, `host:port` and `:port`. `host` takes the listen port; `:port` takes the addresses of the default-route interfaces and requires listening on every address. A non-empty value requires a listen address. | Yes |
| `broker.advertise_extra` | string list | empty | Extra advertised addresses to add. Each item is `host` or `host:port`; an item without a port uses the port in `broker.advertise` when it has one, otherwise the listen port. Requires a listen address. | Yes |
| `broker.address_check_interval` | duration | `1m` | How often the Broker re-reads the host's local addresses. | Yes |
| `broker.local_socket_group` | string | empty | The group that owns the system Broker's local socket. Empty keeps the runtime directory's group. The group must exist in the account database; the value must not contain whitespace, `:` or `/`. Ignored by a user Broker. | Yes |
| `broker.local_socket_mode` | string | `0666` | The file mode of the system Broker's local socket, written as 3 or 4 octal digits. A user Broker always uses `0600`. | Yes |
| `broker.offline` | boolean | `false` | Whether this Broker refuses messages from other hosts. Links and the directory keep working, and a change applies while the Broker runs. | Yes |
| `pairing.static_key` | secret string | empty | A pre-provisioned static pairing key, 16 to 128 characters. The key is active as soon as it is set. Generate one with `openssl rand -hex 32`, which produces 64 hexadecimal characters. | No, file only |
| `pairing.max_failures` | integer | `5` | Failed verifications allowed before a one-time pairing key is discarded. | Yes |
| `pairing.max_attempts_per_minute` | integer | `10` | Pairing requests per minute that match no pairing key. Requests matched by the static key do not count. | Yes |
| `topics.auto_join` | boolean | `true` | Whether sessions join their default topics automatically. | Yes |
| `directory.reconcile_interval` | duration | `10m` | How often paired Brokers reconcile their directories. | Yes |
| `directory.max_records` | integer | `1000` | Maximum number of records in the directory. | Yes |
| `directory.max_records_per_link` | integer | `250` | Maximum number of records accepted over one link. | Yes |
| `directory.max_topics_per_publisher` | integer | `16` | Maximum number of public-topic declarations and records of unknown kind from one publisher. Broker cards and revocations do not count. | Yes |
| `send.link_retries` | integer | `5` | Retries when a link is not established. | Yes |
| `send.link_backoff` | duration | `500ms` | Wait before the first retry; it doubles each time, up to 8 seconds per wait. | Yes |
| `send.request_timeout` | duration | `20s` | Total time budget for one send, including retries and waits. | Yes |
| `send.inbox_retries` | integer | `2` | Retries when the inbox write definitely did not happen. | Yes |
| `send.inbox_backoff` | duration | `200ms` | Wait between inbox write retries. | Yes |
| `send.inbox_timeout` | duration | `5s` | Deadline for one inbox write. | Yes |
| `send.dedupe_window` | duration | `5m` | How long entries stay in the duplicate-message table. | Yes |
| `link.dial_timeout` | duration | `5s` | Connection deadline for each address while dialing. | Yes |
| `link.reconnect_min` | duration | `1s` | Initial backoff when re-establishing a link. | Yes |
| `link.reconnect_max` | duration | `1m` | Maximum backoff when re-establishing a link. | Yes |
| `link.keepalive_interval` | duration | `15s` | Keepalive probe interval. gRPC raises an interval below 10 seconds to 10 seconds. | Yes |
| `link.keepalive_timeout` | duration | `10s` | Deadline for a keepalive reply. | Yes |
| `log.level` | string | `info` | Run log level: `debug`, `info`, `warn` or `error`. | Yes |
| `update.check` | boolean | `true` | Whether the Broker periodically checks GitHub for a newer formal release. When false, the Broker sends no check requests. | Yes |
| `update.check_interval` | duration | `24h` | How often the Broker checks for a newer release. The minimum is `1h`. | Yes |

The update check contacts GitHub. [Security model]({{< relref "docs/security" >}}) describes what it sends and what it receives. `peers update` also contacts GitHub, but only when you run it.

When `broker.alias` is not set, a system Broker uses the first segment of the system hostname, the part before the first `.`. A user Broker uses that segment, a `.` and the name of the account that runs it, for example `workstation` and `workstation.alice`. The alias does not depend on the listen address, the listen port or the advertised addresses. A character outside the alias character set is replaced with `-`. When the account name cannot be found or is empty, the decimal uid takes its place, for example `workstation.1000`. When the hostname cannot be read or is empty, the hostname part is `broker`. An alias longer than 24 characters is shortened first in its hostname part, which keeps at least 1 character, and, if it is still too long, then in its account part. After shortening, trailing `.` and `-` are removed; when the account part is the uid, it is the uid that is shortened. Set `broker.alias` to choose the alias yourself.

## Types and constraints

- A duration is a string in Go duration syntax, for example `10m`, `500ms`, `1s` or `1h30m`. Durations must be greater than zero.
- Integers must be greater than zero, except `send.link_retries` and `send.inbox_retries`, which accept `0`.
- `link.reconnect_max` must be greater than or equal to `link.reconnect_min`, and `link.keepalive_timeout` must be less than `link.keepalive_interval`. Changing one key of either pair is checked against the other.
- `broker.listen` is written `host:port`, `:port` or `[v6]:port`; the port is 1 to 65535. Setting a different value probes the address, and an address that cannot be bound is rejected. On a system Broker, a port below 1024 takes effect only while the configuration file can be written by root alone. When a group or others can write the file, a Broker started in the foreground fails to start, and one running as a service enters a local-only degraded run; `chmod g-w <path>` or a port of 1024 or higher fixes it. [Manage a Broker by file permission]({{< relref "management-permissions.md" >}}#the-admin-group) describes the rule.
- `broker.advertise` and `broker.advertise_extra` require a listen address, and a port-only `broker.advertise` also requires listening on every address.
- The configured advertised addresses, including the implicit one for a specific listen address, may hold at most 8 entries. Addresses taken from the default-route interfaces are dropped from the end when the list would exceed 8; configured entries are all kept.
- `broker.local_socket_group` must name a group that exists; startup fails when it does not, even with `--allow-degraded`.
- `broker.local_socket_mode` accepts 3 or 4 octal digits, up to `0777`.
- `pairing.static_key` accepts 16 to 128 characters; an empty value means no static key.
- `update.check_interval` must be at least `1h`.

A value that fails a check is rejected, and the configuration remains as it was. At startup, a file that cannot be read or parsed, or a value that fails a check, stops the Broker, in the foreground and as a service alike. Only an unavailable data directory or audit log leads to the diagnostics-only degraded run.

## Changing the configuration

`set_config` takes the new value as text and returns the old and new values. `peers config set` does the same from the terminal and follows the same rules. When either changes a key that the template left commented out, it uncomments that line at the same position and writes the new value, leaving the rest of the file unchanged. When the file does not exist, the file it creates holds only that key. Neither can change `pairing.static_key`, and neither can change `broker.listen` while the Broker runs with `--listen`. A change to `update.check_interval` does not interrupt the current check cycle and applies from the next one. Setting `update.check` to false stops the checks and clears the reported version. When the file is edited by hand, the Broker applies the valid changes; a key whose value is invalid keeps its old value and the Broker writes an error to its log.

The file locations and their permissions are listed under [Files and paths]({{< relref "docs/reference/files" >}}). Which topics sessions join by default is described under [Project configuration file]({{< relref "docs/reference/project-config" >}}).
