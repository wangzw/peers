---
title: Uninstall
linkTitle: Uninstall
weight: 9
---

To remove this account's installation, run:

```sh
peers uninstall
```

A system-level installation, which serves every account on the host, is removed as root:

```sh
sudo peers uninstall
```

The account that runs the command decides the level. Root removes the system-level installation. Another account removes its own user-level installation; when the host has only the system-level installation, the command asks whether to remove it and runs `sudo` when you answer `y`. When an ordinary account has both levels installed, it removes the user-level one and then says how to remove the system-level one.

## Keep or delete the data

The command asks `Also remove configuration, identity, audit and service logs? [y/N]` and keeps them unless you answer `y`. Without a terminal, add `--yes`: it skips the question and keeps the data, and together with `--purge` it deletes the data. Without `--yes` the command exits with code 1. [`peers uninstall`]({{< relref "/docs/reference/cli" >}}#peers-uninstall) describes `--purge` and the rest of the behaviour.

## Remove the MCP registration of other accounts

The command removes the MCP registration of the account that ran `sudo`, or of the account that runs it when it is not run through `sudo`. Every other account that registered the MCP server removes its own with `claude mcp remove -s user peer`; when the command runs as root directly, it prints that command instead.

## Remove only the service

To stop and remove the service but keep the binary, run `peers service remove`, or `sudo peers service remove --system` for a system-level service. [`peers uninstall`]({{< relref "/docs/reference/cli" >}}#peers-uninstall) gives the exit codes and the rest of the behaviour.
