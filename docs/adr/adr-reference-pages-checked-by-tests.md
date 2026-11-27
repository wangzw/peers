# Check the reference pages against the code with package tests

## Context

The website's reference pages document the command line (`docs/site/content/{en,zh}/docs/reference/cli.md`), the MCP tools (`mcp-tools.md`), the project configuration file (`project-config.md`) and the Broker configuration keys (`configuration.md`). Each page is written by hand in English and in Chinese, and carries prose, examples and explanations that no structure in the code holds.

The identifiers these pages document come from the code: the command tree that `newRootCommand` in `cmd/peers/app.go` builds, the tools that `newMCPServer` in `internal/mcpserver/tools.go` registers together with their input structs, the struct `projectConfigDocument` in `internal/mcpserver/project.go`, and the table `keyTable` in `internal/config/keys.go` that `config.KeyNames` exposes. The command tree is in package main; the tool registry, the project configuration struct and the key table are unexported.

A command, flag, tool, parameter or key that is added, removed or renamed in the code, a parameter whose required mark changes, or a key whose default changes leaves the pages describing an interface the binary does not have, and readers and generative engines quote the pages as current. The comparison is required on every change, in CI, not only before a release. `.github/workflows/ci.yml` runs `make test` on every push to main and every pull request with no path filter; the Site workflow runs only for changes under its path filter, which excludes the code.

## Options Considered

### Generate the reference pages from the code

Render the identifier lists, or the whole pages, from the code when the site is built.

**Rejected**: the Chinese pages and the prose around each identifier cannot be generated from the code. Generating only the identifier lists splits each page into a generated part and a hand-written part that still have to agree with each other.

### A program under `scripts/`

A Go program reads the pages and an inventory of the code and reports the differences, run by its own Make target.

**Rejected**: a program outside the owning packages cannot reach package main or the unexported registries, so it needs a new exported inventory or a parse of the generated man pages, and it needs its own Make target wired into `make check` and into CI.

### Compare only before a release

Run the comparison as part of the pre-tag release check.

**Rejected**: it breaks the requirement that every change is compared. Drift stays on main, and on the development version of the website, until the next release, which it then blocks.

### Package tests in the owning packages

**Adopted**; see Decision.

## Decision

Adopt package tests in the packages that own each identifier set. The command-line entry point's tests compare the command reference page with the command tree: a command is covered by a `## peers <path>` heading that names it, by the heading of its nearest ancestor below the root, or by the heading of one of its descendants, except that a command with visible flags of its own is covered only by a heading naming it or naming its nearest ancestor below the root; the flags documented under a heading are the flags of the commands it names and of their descendants that no heading names. The MCP server's tests compare the MCP tool reference page with the registered tools, their parameters and each parameter's required or optional mark, and the project configuration reference page with the project configuration keys. The configuration's tests compare the configuration reference page with the configuration keys and their defaults. Each test reads both the English and the Chinese page through one shared test-support reader, outside the coverage denominator. Names, flags, parameters, required marks and defaults are compared; prose, types and descriptions are not.

## Consequences

`make test`, and therefore every CI run, fails when a reference page in either language disagrees with the code on a compared item, so a change to a compared item updates the English and the Chinese page in the same change.

**Costs accepted:**

- The pages are bound to the structure the tests read: a command is a `## peers <path>` heading, a flag is a list item that begins with its code span, and the identifiers of `mcp-tools.md`, `project-config.md` and `configuration.md` are in the first table of the page. A page rewritten into another structure fails the tests until the tests change with it.
- A code-only change to a compared item no longer passes `make test` until both language versions of the page are edited.
- Prose, types, descriptions and examples are not compared and can still disagree with the code; review is their only check.
- The page reader recognises only the Markdown constructs these pages use, not Markdown in general.

## Invalidation Conditions

If the reference pages are generated from the code, or the Chinese pages stop being written by hand, the reason for comparing hand-written pages no longer holds. If keeping the pages in the structure the tests read starts to dictate how the pages explain the interface, the reader or the decision needs revisiting.
