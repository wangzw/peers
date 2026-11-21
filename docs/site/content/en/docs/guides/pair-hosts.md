---
title: Pair two hosts
linkTitle: Pair two hosts
weight: 2
---

Pairing makes two Brokers trust each other by their fingerprints. It is the only way to connect two hosts, and it starts with a key that one side creates and both sides share.

## Before you start

- Both Brokers are running and are not paired yet.
- On the host that will accept the pairing, the Broker listens on its default address: port 17443 on every local address. The other host can reach one of the accepting Broker's advertised addresses; that other host may be behind NAT.
- The two users have a trusted channel outside `peers` for the connect prompt and the key.
- The account running each session or command has write permission on its Broker's configuration file. A user Broker belongs to that account, so it does. On a system Broker, the admin group holds write permission: a member of the group runs the commands on that side directly, and an account that is not in the group asks an administrator to add it to the group or to run the commands on its behalf. An account whose own user Broker is running uses `sudo` to reach the system Broker. [The admin group]({{< relref "management-permissions.md" >}}#the-admin-group) covers the group and the permission rules.

## Create the pairing key

On the accepting host, say “Create a pairing key.” and approve the call (the model calls `create_pairing_key`), or run:

```sh
peers key create
```

Either way the result gives:

- a one-time key that expires after 10 minutes and stops working after one successful pairing;
- the Broker's advertised addresses, each shown with its host name;
- a paste-ready connect prompt that names the key and the addresses.

Pass the connect prompt to the other host's user through a channel outside `peers`, and do not let an agent forward the key or the prompt; [the security model]({{< relref "/docs/security" >}}#pairing-and-fingerprints) explains why. The address is not secret and may travel separately from the key.

## Connect from the other host

On the other host, paste the connect prompt into a session and approve the call (the model calls `connect_broker`), or run:

```sh
peers broker connect <address> --key <key>
```

The prompt includes this command with the address and key filled in: you can run it on the other host directly, or paste the whole prompt into a session there. The address is written `host:port`, or `hostname(host:port)` exactly as the other side showed it; the host name in that form is for the user and is not used for the connection. An address written `hostname(host:port)` needs shell quoting when you type it into a command by hand; the command in the prompt is already quoted.

Either way the result gives the accepting Broker's alias and fingerprint.

## Check the link

Say “List the Brokers this Broker knows.” on both hosts (the model calls `list_brokers`), or run `peers broker list`. Each side lists the other as a peer with status `connected`. For a peer, the result also gives this Broker's role: the side that called `connect_broker` or ran `peers broker connect` is the `dialer`, and the side that created the key is the `acceptor`.

The dialer re-establishes the link after a network interruption without any user action. The acceptor never dials, so the two-way link only needs one reachable direction.

## Special cases

- The key expired or was used. A one-time key is valid for 10 minutes and for one pairing. Using it later returns an error that the key is invalid or expired, and neither side stores the other's identity. Create a new key and start again.
- Pairing is refused as too frequent. A pairing request that matches no key is limited per source, and all sources together have a further cap; over either limit, the other side refuses with `try again later`. The limit is checked before the static key, so a request that holds the static key is refused the same way once a source or all sources reach the limit, while a request that matches the static key does not add to the count. The model sees `Not connected: try again later (code: pairing_rejected)`, and `peers broker connect` prints `peers broker connect: pairing_rejected: try again later`. Wait a minute and try again.
- The two Brokers speak different link protocol versions. At the pairing stage the message names `the other Broker`, unlike the link-stage message below, which names `this peer`. The model sees `Not connected: link protocol version incompatible with the other Broker; upgrade the older Broker (code: pairing_rejected)`, and `peers broker connect` prints `peers broker connect: pairing_rejected: link protocol version incompatible with the other Broker; upgrade the older Broker`. Upgrade the older Broker so both sides use the same link protocol version. A Broker at v0.5.0 or older that initiates against this Broker reports a protocol error, and this Broker records a `pairing_failed` event in the audit log whose `detail` is `version incompatible`. When this Broker connects to a Broker at v0.5.0 or older instead, the older Broker reports `pairing key is invalid` even when the key is correct.
- The alias is already taken. A common cause is the default alias, which contains only the first segment of the hostname: two hosts whose hostnames share that segment, for example `workstation.example.com` and `workstation.example.net`, give their system Brokers the same alias, and so do the user Brokers of the same-named account on each host. If the accepting Broker's alias matches this Broker's own alias, another peer's alias or a former alias that another peer still keeps reserved, `connect_broker` asks for a local alias. Say “Connect to the Broker at `<address>` with the key `<key>` and the local alias `<alias>`.” with an alias of up to 24 characters (the model calls `connect_broker`), or run `peers broker connect <address> --key <key> --alias <alias>` with the same limit; the alias applies only on this Broker.
- The shown address does not work. The addresses returned with the key may not be reachable from the other side, for example when the accepting host is behind port forwarding. When the prompt lists more than one address, its last line names the rest as replacements. Say “Connect to the Broker at `<address>` with the key `<key>`.” with an address that the other side can reach (the model calls `connect_broker`), or use it in `peers broker connect`.
- The network drops. Once the Brokers are paired, the dialer reconnects without a key. Wait a moment and say “List the Brokers this Broker knows.” again (the model calls `list_brokers`), or run `peers broker list`; the peer returns to `connected`.
- The link never comes up and the detail says the versions are incompatible. Two Brokers link only when they speak the same link protocol version. The `DETAIL` column of `peers service status` and `status_detail` from `list_brokers` show `link protocol version incompatible with this peer; upgrade the older Broker`; on the accepting side it continues `: peer speaks link protocol version <N>`. The dialer keeps retrying in `backoff`, and the acceptor stays in `waiting`. `peers broker list` does not show the reason, and the Broker's run log contains `link: protocol version mismatch`. Bring the older Broker to the same version as the newer one; the dialer re-establishes the link, and you do not pair again.
- You deploy many hosts the same way. Instead of a one-time key, the accepting Broker can have a pre-provisioned static key in its configuration file, under `pairing.static_key`. The value is 16 to 128 characters, does not expire and works for any number of pairings. The tools never generate it, set it or return it; before the deployment, uncomment the `static_key` line in the configuration file and fill in a key such as the output of `openssl rand -hex 32`. A file written by running the Broker or `peers config set` directly has no such line; in that case add `static_key: "<key>"` under `pairing:`. Then say “Connect to the Broker at `<address>` with the key `<key>`.” with that key (the model calls `connect_broker`), or run `peers broker connect <address> --key <key>`.
