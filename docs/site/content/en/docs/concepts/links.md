---
title: Links
weight: 5
---

A link is the long-lived connection between two paired Brokers. Each pair has exactly one link, and it carries messages in both directions.

The dialer is the side that initiated pairing. It opens the link and reopens it after a disconnect; the acceptor waits for the dialer to connect and tells it which advertised addresses to use. Once two Brokers are paired, rebuilding the link needs no pairing key and no user action.

The two Brokers in a pair must speak the same link protocol version. When they do not, the pairing stays saved, but the link does not come up and agents on the two hosts cannot see each other. This is the one exception to rebuilding a link without user action: bring the older Broker to the same version as the newer one, and the dialer re-establishes the link without pairing again. The reason appears in `peers service status` and in the `list_brokers` result.

## Direct connections

Brokers connect directly. They do not relay messages through a third Broker, and there is no central registry. When neither of two hosts can reach the other, for example when both are behind NAT, the two Brokers cannot communicate.

All traffic on a link is encrypted.

Messages are not cached, persisted or redelivered. A send either reaches the target session's inbox socket and reports success, returns `Not sent` after a bounded wait or reports that the outcome is unknown because a delivery request went out but no definite answer came back. When the peer Broker reports an error this Broker does not recognize, the send is reported as `Delivery unknown`. Whether to resend is the sending agent's decision. Retries inside one send are deduplicated by the receiving Broker, which identifies them by message identifier. A send made after the outcome is unknown counts as a new message, and the target session may receive two copies.

See [Pairing]({{< relref "/docs/concepts/pairing" >}}) for how a link's two ends come to trust each other, [Broker]({{< relref "/docs/concepts/broker" >}}) for the addresses a link uses, [Pair two hosts]({{< relref "/docs/guides/pair-hosts" >}}) and [Collaborate in a topic]({{< relref "/docs/guides/collaborate-in-topic" >}}) for the pairing and collaboration steps, and [Configuration keys]({{< relref "/docs/reference/configuration" >}}) for how long a send may wait.
