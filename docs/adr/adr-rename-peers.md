# Rename the project to peers

## Context

The repository upstream of this project has changed to `wangzw/peers`, and the website has changed to <https://zhanwei.wang/peers/>. The user required the project name to be unified to peers in this version, in these words: "Rename the project to peers; the upstream is already done" and "You need to change the corresponding agent-peer wording, from the command-line name and the configuration file to the docs, and tell the site to change along." The name is therefore an external constraint: if the command line, installation paths, configuration files, service definitions and documentation keep the old name, they will disagree with the release channel and the website.

The user also ruled out compatibility. The installation layout, configuration files and protocol constants are not interchangeable between 0.3.0 and this version; keeping compatibility would leave the old name in paths, the protocol and the code, conflicting with the constraint of a unified name.

## Options Considered

### Keep agent-peer and do not rename

**Rejected**: it disagrees with the names of the upstream repository, the website and the release artifacts, and the user explicitly required the rename.

### Rename and keep compatibility with 0.3.0

Migrate the old paths, old services and old admin group during installation, read the old project configuration file name, and keep the old protocol constants and pairing labels.

**Rejected**: the user ruled out compatibility and ruled out upgrading from an old version. A compatibility layer would leave the old name in paths, the protocol and the code, and would require maintaining two sets of installation paths.

### Rename without compatibility

**Adopted**; see Decision.

## Decision

The project name is unified to peers. The binary, the release packages and the entries inside them, the Go module path, the installation paths, the system and user data directories, the configuration and log directories, the runtime directory, the service unit name and launchd label, the default admin group name, the project configuration file name, the environment variables, the build tags, the HMAC domain separation label of the pairing protocol, the certificate CN, and the repository queried for self-updates all use the corresponding names under peers.

This version is incompatible with agent-peer 0.3.0 and provides no migration path: the old version's installation, configuration and pairing state are not read, the old paths, old services and old admin group are not migrated, and the old protocol constants and pairing labels are not kept.

The MCP server name `peer` and the tool names are unchanged; only the product name in the usage instructions and tool descriptions changes.

## Consequences

All of the project's external names match the upstream repository and the website, and release packages, installation paths and documentation no longer show two names.

**Costs accepted:**

- This version is incompatible with agent-peer 0.3.0: a 0.3.0 instance cannot pair with this version.
- The old version's installation, configuration, identity, audit and service logs are not read or migrated; the old service unit, launchd job and admin group are not removed by this version.
- The old name in the documentation, the website and the release artifacts must change together, and any omission leaves the user seeing two names.

## Invalidation Conditions

If the upstream repository or the website is renamed again, the name premise of this decision no longer holds and a rename is needed. If the project must interoperate with an old version again, the incompatibility conclusion needs to be re-evaluated.
