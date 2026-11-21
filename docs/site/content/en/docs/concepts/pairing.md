---
title: Pairing
weight: 4
---

Pairing is how two Brokers establish trust. The accepting side creates a one-time pairing key, and the initiating side uses it to connect; after a successful handshake each Broker saves the other's identity and recognizes it by its public-key fingerprint. No certificate authority is involved, and the trust is built one pair at a time.

## Pairing keys

The accepting side creates a one-time pairing key by saying “Create a pairing key.” (the model calls `create_pairing_key`), or with `peers key create`. It is valid for 10 minutes and stops working after one successful pairing. The user passes the key, together with the Broker's address, to the other user through a channel outside `peers`; the address is not secret and can travel separately.

A Broker can also use a pre-provisioned static key written into its configuration file by the user or administrator. The key can be used for any number of pairings and does not expire. `peers` never generates it; the configuration file that `peers service install` generates includes a comment showing how to generate one, for example with `openssl rand -hex 32`.

The initiating side says “Connect to the Broker at `<address>` with the key `<key>`.” (the model calls `connect_broker`), or runs `peers broker connect <address> --key <key>`, with the other Broker's address and the key. The address is written `host:port`, or `hostname(host:port)` in the form shown by `create_pairing_key` or by the output of `peers key create`. A wrong, expired or already used key makes the pairing fail, and neither side saves the other's identity.

## After pairing

The two Brokers are now peers. If the new peer's alias matches this Broker's own alias or is already used by another peer, the initiating side must choose a local alias before pairing completes: say “Connect to the Broker at `<address>` with the key `<key>` and the local alias `<alias>`.” (the model calls `connect_broker` with `local_alias`), or pass `--alias` on the command line.

Alias collisions arise in two ways. A default alias contains only the first segment of the hostname, so two hosts whose hostnames share that segment, for example `workstation.example.com` and `workstation.example.net`, give their system Brokers the same alias, and the user Brokers of the same-named account on each host collide in the same way; pairing such Brokers needs a local alias. A collision can also arise after pairing: when a peer changes its alias, and the new alias matches this Broker's own alias or another peer's, this Broker gives the peer a new local alias that no other peer uses and that is not this Broker's own alias. Existing agent addresses keep pointing at the Broker they named before and do not start addressing another one.

Pairing survives a restart of either Broker. Re-establishing a link after a network failure needs no pairing key.

See [Broker]({{< relref "/docs/concepts/broker" >}}) for the alias and fingerprint, [Links]({{< relref "/docs/concepts/links" >}}) for the connection that pairing allows, [Pair two hosts]({{< relref "/docs/guides/pair-hosts" >}}) for the pairing steps, and [MCP tools]({{< relref "/docs/reference/mcp-tools" >}}) and [Configuration keys]({{< relref "/docs/reference/configuration" >}}) for `connect_broker` and the static key.
