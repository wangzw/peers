---
title: Pairing
weight: 4
---

Pairing is how two Brokers establish trust. The accepting side creates a one-time pairing key, and the initiating side uses it to connect; after a successful handshake each Broker saves the other's identity and recognizes it by its public-key fingerprint. No certificate authority is involved, and the trust is built one pair at a time.

## Pairing keys

The accepting side creates a one-time pairing key by saying “Create a pairing key.” (the model calls `create_pairing_key`), or with `peers key create`. It is valid for 10 minutes and stops working after one successful pairing. The result also gives a paste-ready connect prompt that names the key and this Broker's addresses. The user passes the prompt, or the key and an address, to the other host's user through a channel outside `peers`; the address is not secret and can travel separately. Do not let an agent forward the key or the prompt; [the security model]({{< relref "/docs/security" >}}#pairing-and-fingerprints) explains why.

A Broker can also use a pre-provisioned static key written into its configuration file by the user or administrator. The key can be used for any number of pairings and does not expire. `peers` never generates it; the configuration file that `peers service install` generates includes a comment showing how to generate one, for example with `openssl rand -hex 32`.

The initiating side says “Connect to the Broker at `<address>` with the key `<key>`.” (the model calls `connect_broker`), or runs `peers broker connect <address> --key <key>`, with the other Broker's address and the key. The address is written `host:port`, or `hostname(host:port)` in the form shown by `create_pairing_key` or by the output of `peers key create`. A wrong, expired or already used key makes the pairing fail, and neither side saves the other's identity.

## After pairing

The two Brokers are now peers. If the new peer's alias matches this Broker's own alias, is already used by another peer or is a former alias that another peer still keeps reserved, the initiating side must choose a local alias before pairing completes: say “Connect to the Broker at `<address>` with the key `<key>` and the local alias `<alias>`.” (the model calls `connect_broker` with `local_alias`), or pass `--alias` on the command line.

Alias collisions arise in two ways. A default alias contains only the first segment of the hostname, so two hosts whose hostnames share that segment, for example `workstation.example.com` and `workstation.example.net`, give their system Brokers the same alias, and the user Brokers of the same-named account on each host collide in the same way; pairing such Brokers needs a local alias. A collision can also arise after pairing: when a peer changes its alias, and the new alias matches this Broker's own alias, another peer's alias or a former alias that another peer keeps reserved, this Broker gives the peer a new local alias that no other peer uses and that is not this Broker's own alias. While the pairing lasts, an alias a peer gives up stays reserved for that peer: no other peer can take it, and this Broker's own `broker.alias` cannot change to it. The former alias no longer resolves to any Broker, so a message sent to `<agent>@<alias>`, where `<alias>` is the former alias, fails with `Not sent: no Broker is known as "<alias>" (code: not_found)`; an address handed out earlier therefore never starts pointing at another Broker. A peer keeps at most 8 former aliases; when the alias would change again, the current alias is frozen as its local alias, and later changes to the peer's alias no longer affect the alias this Broker uses. Unpairing releases these former aliases.

Pairing survives a restart of either Broker. Re-establishing a link after a network failure needs no pairing key.

See [Broker]({{< relref "/docs/concepts/broker" >}}) for the alias and fingerprint, [Links]({{< relref "/docs/concepts/links" >}}) for the connection that pairing allows, [Pair two hosts]({{< relref "/docs/guides/pair-hosts" >}}) for the pairing steps, and [MCP tools]({{< relref "/docs/reference/mcp-tools" >}}) and [Configuration keys]({{< relref "/docs/reference/configuration" >}}) for `connect_broker` and the static key.
