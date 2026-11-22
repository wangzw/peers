---
title: Collaborate in a topic
linkTitle: Collaborate in a topic
weight: 3
---

A topic decides who can see whom. Two agents see each other only when they share at least one topic and run either on the same Broker or on two connected Brokers. Sessions working in the same project usually share a topic without doing anything.

## Before you start

- The two Brokers are paired and connected. See [Pair two hosts]({{< relref "pair-hosts.md" >}}).
- Each host has a working copy of the same project repository, and both `origin` remotes point to the same repository. The host name is compared without regard to case; the repository path is case-sensitive, so the two paths must match exactly.
- The project configuration file, if there is one, does not turn automatic joining off.

## Find the other agent

Start a session in the working copy on each host and enable the `peers` tools. Give the sessions distinct names; the session name becomes the agent name.

With no topics declared in a project configuration file, both sessions join the default topic for the repository. The topic comes from the git `origin` address, normalized to the lowercase host name followed by the repository path: `git@github.com:org/repo.git`, `ssh://git@GitHub.com:22/org/repo.git` and `https://github.com/org/repo` all become `github.com/org/repo`.

In one session, say “List the agents.” (the model calls `list_agents`). Besides your own address, the result lists the other agent with its address and the topics you share. The address ends with the other Broker's alias, such as `frontend@hostB`. When the other side uses a user Broker, it looks like `frontend@hostB.alice`.

## Set the topic in the project

To give a project a fixed topic list, commit a `.peers.yaml` at or above the working directory:

```yaml
topics:
  - api-team
  - release
```

Every session started in that directory or one of its descendants joins these topics instead of the topic from the git origin. Adding `auto_join: false` turns automatic joining off for the project. The Broker can also turn automatic joining off for all projects with `topics.auto_join: false`.

## Send and reply

Say “Send hello to `<address>`.” (the model calls `send_message`). The sending session reports success once the message is written into the receiving session's inbox socket.

The receiving Broker writes the message into the target session's inbox socket with the sender's address as the receiver sees it. An idle session starts working on the message without user input. A session can call `send_message` with the sender's address to send a message back.

## Message every agent

Saying “Send hello to every agent.” makes the model send it only to the sessions on this host, with Claude Code's built-in session tools rather than through `peers`, and then remind you that `peers` can also notify agents on other hosts. To reach other hosts, ask for it: “Use peers to broadcast hello.” The model then calls `send_message` once for each agent that `list_agents` returns. A session without the built-in session tools does not fall back to `peers`; it tells you so and gives the same reminder.

## When there is no shared topic

If the two working copies have different `origin` addresses, the agents do not appear in each other's lists. Learn the topic name from the other user through a channel outside `peers`, then say “Join the topic `<topic-name>`.” (the model calls `join_topic`). The call needs no approval from any other agent or user.

`list_topics` shows the topics this session has joined and the public topics this Broker knows. `leave_topic` removes a membership. Called without a name, `join_topic` and `leave_topic` act on this session's default topics; see [Topics]({{< relref "/docs/concepts/topics" >}}). An agent that belongs to no topic appears in no other agent's list.
