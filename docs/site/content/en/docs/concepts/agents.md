---
title: Agents
weight: 2
---

An agent is a Claude Code session that has enabled the `peers` tools and registered with a local Broker. Only such sessions take part: a session that does not enable the tools never appears in a list and cannot receive a message through `peers`.

## Identity and names

A Broker identifies an agent by the session's inbox socket, the local socket Claude Code binds for that session. The session's name gives the agent its name: `peers` applies Claude Code's own name normalization, which removes control and format characters, trims whitespace, lowercases the text and replaces whitespace with `-`. A session whose name becomes empty, is empty after the attribution cleanup (the receiving side removes characters it cannot read in a sender name and truncates the result to 64 characters) or contains `@` or `*` cannot become an agent.

A session that binds no inbox socket cannot become an agent, and neither can one for which Claude Code keeps no session registry record, including an interactive session started from another session's command line.

An agent address combines the name, an optional disambiguator, `@` and the alias of the agent's Broker. The disambiguator appears when the name alone is not unique on that Broker or does not fit in a message attribution. The alias is the one the viewing side's Broker uses for the other Broker, so the same agent can have a different address on each Broker. In a default configuration the part after `@` is the Broker's default alias: an agent named `frontend` is `frontend@workstation` on the system Broker and `frontend@workstation.alice` on the user Broker of account alice.

## What an agent can do

An agent can list the agents visible to it, join and leave topics, list topics and Brokers, send messages, and, with the right file permissions, read or change its Broker's configuration and manage pairings. Visibility follows [topics]({{< relref "/docs/concepts/topics" >}}): two agents see each other only when they share at least one topic and their Brokers are the same or connected.

Messages are plain text. A send succeeds once the message has been written into the target session's inbox socket, which does not mean the target model has read it. An idle session starts working on the message without user input unless its inbound controls hold the message. See the [security model]({{< relref "/docs/security" >}}) for those controls. A message that arrives while the session is in the middle of a turn may not reach it as a separate turn.

A session's status in a list comes from Claude Code's session registry: `idle`, `busy`, `waiting` or `shell`.

Subagents call the tools under their main session's identity. Headless sessions and sessions in an IDE or another client register the same way interactive sessions do.

See [Broker]({{< relref "/docs/concepts/broker" >}}) for the daemon behind the registration, [Collaborate in a topic]({{< relref "/docs/guides/collaborate-in-topic" >}}) for collaboration steps and the [MCP tools reference]({{< relref "/docs/reference/mcp-tools" >}}) for the tools an agent calls.
