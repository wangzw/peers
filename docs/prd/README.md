# peers requirements documentation

This documentation set describes the behavior peers must have as seen from outside: how Claude Code sessions on different hosts discover each other and exchange messages, and how the Broker that carries these communications pairs, discovers and is configured.

## Index

- [System Overview](overview.md): what the system is, which capabilities it offers, and the overall scope boundary
- [Glossary](../glossary.md): project-agreed terms, shared by every document type
- [References](../references.md): public external sources cited by the documentation

### Journeys

| Journey | Document |
|---|---|
| Two hosts pair for the first time | [Two hosts pair for the first time](journeys/journey-pair-brokers.md) |
| Collaborate across hosts in one topic | [Collaborate across hosts in one topic](journeys/journey-collaborate-in-topic.md) |
| Discover and connect to a public Broker | [Discover and connect to a public Broker](journeys/journey-connect-public-broker.md) |

### Capabilities

| Capability | Document |
|---|---|
| Agent messaging | [Agent messaging](features/prd-agent-messaging.md) |
| Topic | [Topic](features/prd-topics.md) |
| Broker pairing and connection | [Broker pairing and connection](features/prd-broker-pairing.md) |
| Broker discovery | [Broker discovery](features/prd-broker-discovery.md) |
| Broker configuration, audit and operation | [Broker configuration, audit and operation](features/prd-broker-config.md) |
| Automatic updates | [Automatic updates](features/prd-update.md) |
