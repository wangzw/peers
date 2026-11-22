# peers design documentation

peers lets Claude Code sessions on different hosts discover each other and send text messages.

## Index

- [Architecture](architecture.md): the overall system structure, cross-cutting design and architecture invariants
- [Architecture decision records](../adr/README.md): the architecture decisions in force, with the options and costs weighed for each
- [Glossary](../glossary.md): project-agreed terms, shared by every document type
- [References](../references.md): public external sources cited by the documentation

### Modules

| Module | Document |
|---|---|
| `broker` | [broker](modules/design-broker.md) |
| `mcpserver` | [mcpserver](modules/design-mcpserver.md) |
| `inbox` | [inbox](modules/design-inbox.md) |
| `topic` | [topic](modules/design-topic.md) |
| `link` | [link](modules/design-link.md) |
| `pairing` | [pairing](modules/design-pairing.md) |
| `directory` | [directory](modules/design-directory.md) |
| `config` | [config](modules/design-config.md) |
