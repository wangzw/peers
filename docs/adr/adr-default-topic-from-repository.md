# Derive the default topic from the project declaration or the repository address

## Context

An agent in no [topic](../glossary.md) is invisible to other agents. Agents working in the same project are the most common parties to communicate, they are spread across hosts, and each workspace is a copy of the same git repository. The same repository has several spellings of its remote address.

## Options Considered

### No default topic

After registering, each agent is in no topic, and the user asks it to join one.

**Rejected**: An agent in the same project would have to join by hand on every start, and both sides would have to agree on the name in advance, or the list stays empty.

### Use the repository remote address as the default topic

On registration an agent joins automatically a topic named after the normalized result of the workspace git repository's `origin` address. A project can declare a topic list in the project configuration file to replace it, and can turn automatic joining off; it can also be turned off entirely in the Broker configuration.

**Adopted**; see Decision.

## Decision

On registration an agent joins its default topic automatically: the topic list the project configuration file declares when there is one, otherwise the normalized result of the workspace's `origin` address, and nothing when neither exists. Automatic joining can be turned off in the Broker configuration or the project configuration file, and an automatically joined topic is private.

## Consequences

On two paired hosts, two agents of the same repository are visible to each other with no action. A project can use the project configuration file to split one large repository into several topics, or to have several repositories share one topic.

**Costs accepted:**

- Enabling the tools means being visible to remote agents of the same repository, and a user who does not want that must turn automatic joining off.
- The default topic's name is determined by the repository address, and anyone who can reach the repository can derive it.
- A fork's `origin` differs from the upstream, so the agents on the two sides are not in the same topic by default.
- Once the address normalization rule is in use, changing it puts agents of the old and new versions into different topics.
- A repository whose `origin` is a local path cannot derive a default topic this way: the same path on different hosts is not the same repository.
