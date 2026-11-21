---
title: Broker
weight: 1
---

A Broker is the daemon on a host that registers the agent sessions it serves, keeps its links to other Brokers and writes incoming messages into the target session's inbox socket. A session that enables the `peers` tools registers with one Broker on its own host when its name is usable.

## Two forms

A Broker runs in one of two forms.

A **system Broker** runs as a privileged system service. There is at most one per host, and it serves the sessions of every account on the host that is allowed to use it. An administrator installs it. Accounts and groups come from the system's account database: on Linux the Broker queries it through `getent`, which follows the NSS configuration, and falls back to the local files with a warning when `getent` is missing, fails or returns output it cannot validate.

A **user Broker** runs under an ordinary account and serves only that account's sessions. An account runs at most one instance. It is started by the user's service manager, or by the user in a terminal; the MCP server process only connects. An account that can use the host's system Broker needs no user Broker; a running user Broker takes precedence over the system Broker.

A Broker installed as a service does not exit when it cannot bind its listen address; it keeps serving this host's sessions in a local-only degraded run, which [the port question in the FAQ]({{< relref "/docs/faq" >}}#what-happens-if-the-port-is-already-in-use) describes.

Each instance has its own fingerprint, peers and configuration. Two Brokers on one host are independent Brokers on the network, not parts of one process. Their default aliases differ; the rule is under [Address and identity](#address-and-identity) below. On a host whose hostname is `workstation.example.com`, the system Broker takes `workstation`, and the user Brokers of accounts alice and bob take `workstation.alice` and `workstation.bob`. These are three Brokers with three different aliases, and a list sorted by alias keeps the Brokers of one host together.

## Address and identity

By default a Broker listens on port 17443 on all local addresses, IPv6 included when enabled. Other Brokers reach it at its advertised addresses, which by default are the addresses of the network interface that carries the default route. A Broker whose listen address is empty does not listen; it can only dial other Brokers.

The fingerprint is the identity: a digest of the Broker's public key that does not change when the Broker restarts, renames itself or changes its listen address. The alias is the display name that appears after `@` in an agent address. When `broker.alias` is not set, a system Broker takes the first segment of the host's hostname, the part before the first `.`, and a user Broker takes that segment, a `.` and the name of the account that runs it. The alias does not depend on the listen address, the port or the advertised addresses, so changing the port or stopping the listener leaves it unchanged. On a host named `workstation.example.com`, the system Broker is `workstation` and the user Broker of account alice is `workstation.alice`. [Configuration keys]({{< relref "/docs/reference/configuration" >}}) gives the full rule, including how characters outside the alias character set are replaced, what happens when the account name cannot be looked up and how a long alias is shortened.

A Broker is private by default. A public Broker publishes a broker card with its fingerprint, alias and advertised addresses, and every Broker in the connected network can learn it. Pairing with a public Broker is still a separate decision.

## Configuration

The configuration is a YAML file. A user Broker reads `~/.peers/config.yaml`; a system Broker reads `/etc/peers/config.yaml`. The tools and terminal commands that read or change configuration follow the operating system's permissions on that file: read access lets an account read the configuration, the full status and the audit log; write access lets it change settings, create one-time pairing keys, pair or unpair. Listing agents and topics, joining a topic and sending messages need neither.

A Broker can also be set offline, which refuses messages from other hosts while links and pairings stay in place; [Command line]({{< relref "/docs/reference/cli" >}}#peers-service-online-and-offline) gives the two commands.

See [Topics]({{< relref "/docs/concepts/topics" >}}) for how a Broker decides visibility, [Pairing]({{< relref "/docs/concepts/pairing" >}}) for how it trusts another Broker and [Links]({{< relref "/docs/concepts/links" >}}) for the connection between peers. [Run the Broker as a service]({{< relref "/docs/guides/run-as-service" >}}) and [Run a system Broker]({{< relref "/docs/guides/system-broker" >}}) cover the two deployment forms; [Configuration keys]({{< relref "/docs/reference/configuration" >}}) and [Files and paths]({{< relref "/docs/reference/files" >}}) list the keys and paths.
