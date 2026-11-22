---
title: Project configuration file
weight: 5
description: "A .peers.yaml file in a project names the topics that sessions started there join automatically, so every checkout of the repository gets them."
---

A `.peers.yaml` file in a project declares the topics that sessions started in that project join automatically. Commit it to the repository so every checkout gets the same default topics.

## Where the file is read from

The MCP server process starts in the session's project directory. It looks for `.peers.yaml` there and then in each parent directory up to the filesystem root, and uses the first one it finds.

- The entry must be a regular file. A symbolic link is followed, and it counts when its target is a regular file.
- The file and the directory that holds it must be owned by the calling account or by root. A file that fails this check is ignored and the search continues upward.
- If the first file found cannot be read or parsed, no default topic is joined. The search does not fall back to a file in an ancestor directory.

## Keys

| Key | Type | Default | Meaning |
|---|---|---|---|
| `topics` | string list | empty | Topics to join automatically. Empty strings and duplicates are removed; the first occurrence is kept. |
| `auto_join` | boolean | `true` | Whether to join the default topics at all. `false` joins none. |

Unknown keys are ignored.

```yaml
topics:
  - team-a
  - release-notes
auto_join: true
```

## Default topics a session joins

After the session registers, it joins default topics in this order, and the first rule that applies decides:

1. If the Broker's `topics.auto_join` is `false`, no topic is joined.
2. If a project configuration file is found and its `auto_join` is `false`, no topic is joined.
3. If the file declares a non-empty `topics` list, every listed topic is joined, in order.
4. Otherwise, if the project's git repository has an `origin` address that can be normalized, one topic named after the address is joined.
5. Otherwise no topic is joined.

`topics: []` counts as no declaration and reaches rule 4. Automatic joining never declares a topic public. A session can join more topics later with `join_topic`. A nameless `join_topic` or `leave_topic` acts on these default topics even when automatic joining is off, using rules 3 to 5 alone; see [Topics]({{< relref "/docs/concepts/topics" >}}).

The Broker's own switch is `topics.auto_join` in [Configuration keys]({{< relref "docs/reference/configuration" >}}). Topics and visibility are explained in [Concepts]({{< relref "docs/concepts" >}}).

## Repository addresses and topic names

A session that reaches rule 4 takes a default topic from the repository's `origin` address. The address is read from the repository's local configuration, so a `remote.origin.url` set in the user's global git configuration is not used.

The address is normalized to `host/path`:

- In a URL with `https`, `http`, `ssh` or `git`, the host is the URL host without user information or port, and the path is the URL path.
- In the scp-like form `user@host:path`, the host is the text between the last `@` and the first colon, and everything after the colon is the path.
- A local path or a `file:` URL produces no topic.

The path loses its leading and trailing slashes and a trailing `.git`; the host is lowercased; the path keeps its case. For example, all three of these addresses produce the topic `github.com/org/repo`:

- `git@github.com:org/repo.git`
- `ssh://git@github.com/org/repo.git`
- `https://github.com/org/repo`
