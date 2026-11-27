# peers

A cross-host communication system for agents. A host runs a Broker either as a system service that serves every account or as a user service that serves one account. Claude Code sessions join their Broker through MCP tools, and once two Brokers are paired, agents on different hosts can discover each other and exchange messages.

## Language

- **Documents under `docs/` are written in English.** This covers the requirements, design documents, architecture decision records, glossary, references and roadmap.
- **The website under `docs/site/` is bilingual.** Every page exists in English under `docs/site/content/en/` and in Chinese under `docs/site/content/zh/`, and the rule above for documents under `docs/` does not apply to it. The two versions of a page say the same thing: the English page is written first and the Chinese page is its translation, with the same headings, code blocks and links. A change to one version changes the other in the same commit. Its configuration and layouts are written in English.
- **Everything else is written in English.** This covers source code, identifiers, comments, log and error messages, MCP tool names and descriptions, tests, commit messages, `README.md`, this file, skills under `.claude/`, and configuration files.
- When English text names a concept registered in `docs/glossary.md`, use that entry's English counterpart rather than a fresh translation.

## Documentation

- design: docs/design
- requirements: docs/prd
- adr: docs/adr
- glossary: docs/glossary.md
- references: docs/references.md
- language: en

`docs/roadmap.md` records priorities and milestones and belongs to none of the documentation trees above. ADRs are part of the design: a design change that adopts, replaces or reverses a significant decision adds or revises the record under `docs/adr` in the same change.

## Checks

`make check` runs every gate in order: `lint`, `proto-lint`, `gen-check`, `test`, `e2e`, `cover-check` and `docs-check`. Each is also a target of its own. Run the gates that cover what a change touches before committing it, and leave them passing with no warning ignored. `make check` collects coverage into `.cover` in the repository root ($(CURDIR)/.cover) unless the caller sets `COVER_DIR` to an absolute directory, so it needs `setfacl` from the `acl` package. An empty or relative `COVER_DIR` makes `make check` fail before any gate runs. `test` and `e2e` run alone collect coverage only when `COVER_DIR` names an absolute directory.

`cover-check` enforces the coverage thresholds over the data `make test` writes as `unit` and `testenv` and `make e2e` writes as `e2e`: total statement coverage of at least `COVER_TOTAL` (90) and at least `COVER_PACKAGE` (80) for every package. Inside `make check` both floors are read only from the command line, so an exported value cannot loosen the gate; a standalone `make cover-check` honours them from the environment too. The denominator is every package under `internal/` and `cmd/peers`, excluding generated code (files with the `// Code generated ... DO NOT EDIT.` header), `internal/testkit`, `test/`, `scripts/` and `third_party/`; the data `make test-root` writes is not counted. `make cover-report` prints the same table without enforcing the floors, and `COVER_FORMAT=markdown` prints the Markdown table CI appends to the step summary.

`make site-check` builds the website under `docs/site/` strictly and checks its internal links. It also fails when a page under `docs/site/content/` writes a bare release number that equals a `vX.Y.Z` tag: a page that needs the release its install commands fetch uses the `release` shortcode, which renders the build's `params.release`, and a release named as history keeps its `v` prefix. The build takes `params.release` from the newest `vX.Y.Z` tag, so the checkout needs its tags. It is not part of `make check`; run it when a change touches the website.

`make third-party-check` compares every vendored library under `third_party/` with the upstream module version `third_party/UPSTREAM` records, and `make vulncheck` scans the module graph for known vulnerabilities on both supported platforms. Both reach the network, so neither is part of `make check`; run them when a change touches the vendored source or the module graph.

## Signals

Sending a signal to a real process must go through `procsig.SendSignal`, and asking whether a pid names a live process must go through `procsig.Alive`, both in `internal/procsig/signal.go`. They refuse pids at or below 1, which would otherwise reach init, the caller's process group or every process the account may signal. `make lint` checks every Go file outside `third_party/`, test files included, and fails on a direct `syscall.Kill` or `unix.Kill` call outside that file. A direct call that is genuinely intended is exempt only with `//procsig:allow <reason>` on the same line or the line above, and the reason must not be empty. This covers signaling the caller's own process, signaling a process group on purpose with a negative pid, and a signal-0 probe that must distinguish EPERM from success rather than only check liveness.

## Commit Convention

Supplements the `commit-convention` skill.

### Scopes

- a module name, such as `broker` or `link`: the module's Go package under `internal/` and its design document `docs/design/modules/design-<module>.md`. The module table in `docs/design/README.md` defines which modules exist.
- `cmd`: `cmd/peers`, the entry point and the command wiring that belongs to no module
- `e2e`: `test/e2e/`, the end-to-end tests and their container topologies
- `realsession`: `test/realsession/`, the probes that run real Claude Code sessions, and their workflow `.github/workflows/real-session.yml`
- `site`: `docs/site/`, the project website, and its workflow `.github/workflows/site.yml`
- `deps`: vendored source under `third_party/` and dependency versions
- `architecture`: `docs/design/architecture.md`
- `adr`: `docs/adr/`
- `prd`: `docs/prd/`
- `glossary`: `docs/glossary.md`
- `references`: `docs/references.md`
- `roadmap`: `docs/roadmap.md`
- `design`: cross-cutting changes
- `repo`: repository-level configuration, such as `CLAUDE.md`, `.claude/`, `.github/`, `Makefile`, `scripts/`, `.golangci.yml`, the `buf` configuration and `.gitignore`

Shared test support under `internal/testkit/` takes the scope of the module it simulates.

### Rules

- A change is breaking when it removes, renames or changes the meaning of an MCP tool name or parameter, a configuration key, the one-time pairing key format, a link frame, the local interface version, a requirement id, or a glossary term used by an interface.
