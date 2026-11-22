---
title: Discover and connect a public Broker
linkTitle: Connect a public Broker
weight: 4
---

A Broker is private by default. A public Broker publishes a broker card with its fingerprint, alias and advertised addresses, and the broker card spreads across the connected network. Public topics declared on a public Broker are visible in the same way. Pairing with a public Broker still needs a pairing key.

## Make a Broker public

Set `broker.public` to `true` in the Broker's configuration file. If your account has write permission, say “Set broker.public to true.” (the model calls `set_config`), or run:

```sh
peers config set broker.public true
```

A private Broker can still learn about public Brokers; the difference is whether others can find it.

A public Broker republishes its card daily, and the card expires after 7 days without renewal. Turning the setting off removes the card from connected Brokers, and the Broker's public topics disappear with it.

## Find a public Broker

Say “List the Brokers this Broker knows.” (the model calls `list_brokers`), or run `peers broker list`. Each entry gives the Broker's alias, fingerprint, advertised addresses, whether it is a peer and one status:

| Status | Meaning |
|---|---|
| `connected` | There is a link to this Broker now. |
| `connectable` | There is no link, but at least one advertised address is known. |
| `not_directly_reachable` | There is no link and no advertised address is known. |

A public Broker you have not paired with appears with `is_peer: false` and, normally, status `connectable`.

Public topics appear in `list_topics`, with a name and an optional description. They are visible across the whole connected network, not only to the declaring Broker's peers. A name declared by several publishers appears once, and a declaration whose name or description does not fit the rules is not listed; [Topics]({{< relref "/docs/concepts/topics" >}}) gives the rules.

## Pair and join a topic

1. Say “Connect to the Broker at `<address>` with the key `<key>`.”, using one of the public Broker's advertised addresses and a one-time pairing key created on the other side or its pre-provisioned static key (the model calls `connect_broker`), or run `peers broker connect <address> --key <key>`. Get the connect prompt, or the key and an address, from the other host's user through a channel outside `peers`, then paste the prompt into a session or run the command it gives; do not let an agent receive the key or the prompt for you, and see [the security model]({{< relref "/docs/security" >}}#pairing-and-fingerprints) for why. Being discoverable does not make a public Broker trusted.
2. If the result shows a fingerprint warning, another publisher's broker card names the same address but carries a fingerprint different from the one the paired Broker presented. Cards are self-reported by their publishers, so confirm the fingerprint with the other user before you rely on the link. There can be one warning per such card, up to eight. The warning reads:

   ```text
   fingerprint mismatch: the directory card of <card fingerprint> lists <address>, but the paired Broker presented <peer fingerprint>; directory cards are self-reported, so confirm the fingerprint with the other side
   ```

   Say “Disconnect from the Broker `<alias>`.” (the model calls `disconnect_broker`), or run `peers broker disconnect <broker>`, to remove the pairing.
3. Say “Join the topic `<topic-name>`.” with the public topic's name (the model calls `join_topic`). If the other side declared a description, `list_topics` showed it before you joined.
4. Say “List the agents.” (the model calls `list_agents`), then “Send hello to `<address>`.” (the model calls `send_message`).

Pairing with a public Broker is the same handshake as any other pairing. Discovery only tells you where it is and what it calls itself.

## When the public Broker does not listen

If the public Broker has an empty `broker.listen`, it does not listen and its advertised address list is empty, so `list_brokers`, or `peers broker list`, shows it as `not_directly_reachable`. Pairing then has to start from the other side: say “Create a pairing key.” on your Broker (the model calls `create_pairing_key`) or run `peers key create`, pass the connect prompt, or the key and your Broker's address, to the other host's user, and the other side pastes the prompt into a session and approves the call (the model calls `connect_broker`) or runs the `peers broker connect` command it gives. Do not let an agent forward the key or the prompt. A Broker that does not listen cannot create a pairing key itself.
