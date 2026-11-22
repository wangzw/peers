# Build the command line on cobra and group commands by noun and verb

## Context

The command line must meet these constraints:

- Command names and grouping follow one rule and match the MCP tools and the glossary.
- Help, usage, shell completion and man pages are generated from one command definition, and a new command is registered once.

Hand-written command dispatch, usage constants and option parsing cannot meet the constraints on naming and on generation from one definition: the local Broker's operations are scattered across the top-level `broker`, `status`, `online`, `offline`, `audit` commands and the service uninstall subcommand, a peer is called `peer`, a pairing key is called `pairing-key`, and reading the whole configuration reuses one option. Help, usage and shell completion are therefore maintained separately, and a new command easily misses one of them.

## Options Considered

### Keep the hand-written command dispatch and only rename the commands

Continue using the existing dispatch and usage constants, changing only command names and help text.

**Rejected**: help, usage, grouping and shell completion still have to be hand-written, and a third-party command-line library is introduced instead; adding completion support would keep growing the hand-written usage constants.

### Introduce a third-party framework and keep the old commands as aliases

Reorder the commands with a third-party framework, keeping old spellings such as `status`, `peer` and `pairing-key` as aliases.

**Rejected**: keeping aliases would put two sets of command names in help and completion at once, leave the grouping just as scattered, and keep the old names in scripts, tests and documentation for a long time.

### Introduce cobra, reorder the commands as noun + verb, and keep no old command

cobra generates the command tree, help groups, usage and shell completion, as well as man pages ([REF-cobra](../references.md)). The nouns match the MCP tools and the glossary: a peer is `broker`, a pairing key is `key`, all operations on the local Broker fall under `service`, and configuration falls under `config`; actions on the whole installation stay at the top level as `update`, `uninstall` and `completion`.

**Adopted**; see Decision.

## Decision

Build the command line with cobra, which generates help, usage, grouping, shell completion and man pages. The command tree is organized as noun + verb, with `mcp` and `internal` hidden.

Compatibility with old commands is not considered: keep no old command and offer no aliases, so old spellings may stop working at once. The old top-level commands and nouns such as `peer` and `pairing-key` no longer exist.

## Consequences

The command line is defined by one command tree, so help, usage, completion and man pages share their source with the code, and a new command is registered once; command names match the MCP tools and the glossary, and the user finds peers, keys, configuration and local Broker operations under one set of nouns.

**Costs accepted:**

- Breaking change: old command spellings stop working at once, with no alias and no migration hint.
- cobra becomes an external dependency, and release artifacts must carry its license; some command-line behavior depends on cobra's default conventions, such as how usage errors are handled and the format of completion output.
- Help is grouped by pairing, local Broker and installation, and a new command must belong to one of these groups; hidden commands do not appear in help. The shape of the command tree becomes part of the external contract.
- Man pages become generated artifacts; a generation check rejects a merge when they disagree with the code.

## Invalidation Conditions

When the command line must also support old commands to stay compatible with existing automation, the premise of keeping no old command no longer holds. When cobra cannot express the required help groups, completion or man page generation, the decision to adopt it needs to be re-evaluated.
