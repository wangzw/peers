---
title: 卸载
linkTitle: 卸载
weight: 9
---

要卸载本账户的安装，运行：

```sh
peers uninstall
```

系统级安装为主机上的所有账户服务，要以 root 卸载：

```sh
sudo peers uninstall
```

作用的级别按执行命令的账户确定。root 卸载系统级安装；其他账户卸载自己的用户级安装；主机上只有系统级安装时，命令询问是否卸载它，回答 `y` 后经 `sudo` 完成卸载。普通账户两个级别都装了时，命令卸载用户级安装，然后说明怎样卸载系统级安装。

## 保留还是删除数据

命令询问 `Also remove configuration, identity, audit and service logs? [y/N]`，除非回答 `y`，这些数据都会保留。没有终端时要加 `--yes`：它跳过询问并保留数据，与 `--purge` 同用则删除数据；不加 `--yes` 时命令以退出码 1 结束。[`peers uninstall`]({{< relref "/docs/reference/cli" >}}#peers-uninstall) 说明 `--purge` 与其余行为。

## 删除其他账户的 MCP 注册

命令删除执行 `sudo` 的账户（未经 `sudo` 时即执行命令的账户）的 MCP 注册。其他注册过 MCP 服务的账户，各自用 `claude mcp remove -s user peer` 删除；以 root 直接运行时只打印这条命令。

## 只卸载服务

只想停止并移除服务、保留二进制时，运行 `peers service remove`，系统级服务用 `sudo peers service remove --system`。退出码与其余行为见 [`peers uninstall`]({{< relref "/docs/reference/cli" >}}#peers-uninstall)。
