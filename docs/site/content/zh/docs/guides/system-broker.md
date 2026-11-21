---
title: 运行系统级 Broker
linkTitle: 系统级 Broker
weight: 6
---

系统级 Broker 以 root 特权作为服务运行，服务一台主机上多个账户的 Agent。用户级 Broker 只服务一个账户。一台主机至多运行一个系统级 Broker。

## 安装并启用

把二进制装进 root 拥有、组和其他人不可写的目录，并以 root 安装服务。安装命令会一次完成这两件事：对它的级别询问回答 y。请在需要使用系统级 Broker 的 Claude Code 所属的账户下执行。安装只为执行安装的账户注册 MCP 服务，即使它经 `sudo` 重新执行自己也是这个账户。安装会立即启动 Broker，此后它随开机启动。

```sh
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh
```

[安装 peers]({{< relref "install.md" >}})介绍这个询问以及其他安装方式；手动命令见[把 Broker 安装为服务]({{< relref "run-as-service.md" >}})。

系统级 Broker 取的是 root 的账户锁。如果 root 名下已有用户级 Broker 在运行，系统级安装会被拒绝，并提示先停掉它。

系统级 Broker 使用 `/etc/peers/config.yaml`、`/var/log/peers/audit.log` 和 `/var/lib/peers`。配置文件不存在时，安装会生成它，内容是一份列出全部键的模板，属主为 root，属组为默认的 `peers`，权限 `0660`；已存在的文件不改动。该组的成员在本账户没有运行自己的用户级 Broker 时，不用 `sudo` 就能管理 Broker；这个组及它的限制见[以配置文件权限管理 Broker]({{< relref "management-permissions.md" >}}#管理组)。

## 允许账户使用它

每个账户各自注册 MCP 服务；注册按账户进行，不按主机进行。系统级安装已经为执行安装的账户注册好。其他账户只需注册 MCP 服务，用安装后二进制的绝对路径：

```sh
claude mcp add --scope user peer -- /usr/local/bin/peers mcp
```

不要在这些账户上执行安装命令。主机上已有系统级服务时，安装命令会改问是否升级系统级安装，回答 n 不改动主机；只有带 `--user`，或者在没有终端时执行，才会装出用户级 Broker，装了以后本账户的会话改用它。已经有用户级安装的，用 [`peers uninstall`]({{< relref "/docs/reference/cli" >}}#peers-uninstall) 卸掉；卸载会删掉本账户的 MCP 注册，之后要重新执行上面的注册命令。

会话通过本机 socket 访问系统级 Broker：Linux 上是 `/run/peers/broker.sock`，macOS 上是 `/var/run/peers/broker.sock`。两个设置决定哪些账户能连接：

| 键 | 默认值 | 作用 |
|---|---|---|
| `broker.local_socket_group` | 空 | socket 的属组。空表示沿用运行时目录的属组。组必须存在。 |
| `broker.local_socket_mode` | `0666` | socket 的权限位。 |

要限定到某个组，就把 socket 的属组设为这个组，并把 socket 权限设为允许该组访问的值，例如 `broker.local_socket_group: peers-users` 配 `broker.local_socket_mode: 0660`。这个组不要用管理组，也就是拥有配置文件、能管理 Broker 的那个组：管理组的成员同时也能管理 Broker，而能不能连接和能不能管理是两件事。组外的账户连不上，需要自己启动用户级 Broker，安装为服务或在终端运行都可以。命令见[把 Broker 安装为服务]({{< relref "run-as-service.md" >}})。

## 会话或命令使用哪个 Broker

会话的 MCP 服务进程按以下顺序选定 Broker：

1. 本账户正在运行的用户级 Broker。
2. 本账户有权使用的系统级 Broker。

两者都不可用时，工具调用返回说明下一步怎么做的错误，MCP 服务进程不会启动 Broker；详见 [FAQ 的没有 Broker 一问]({{< relref "/docs/faq" >}}#没有-broker-在运行时会怎样)。因此，如果希望所有账户都用系统级 Broker，这些账户就不需要用户级 Broker；在这些账户上安装用户级服务，这些账户的会话就会改连用户级 Broker，不再使用系统级 Broker。

一个账户有权使用系统级 Broker，但它自己的用户级 Broker 正在运行，会话会登记到用户级 Broker 上。要让它的会话改用系统级 Broker，就不要在这个账户上运行用户级 Broker。

终端命令 `peers config`、`peers key` 和 `peers broker` 按同样的顺序选择 Broker，只查找正在运行的 Broker，不会启动 Broker。`peers service` 下的命令不按这个顺序，只作用于 `--system` 或 `--user` 选中的级别，不回退到另一级。本账户有用户级 Broker 在运行时，用 `peers config`、`peers key` 或 `peers broker` 管理系统级 Broker 要加 `sudo`；root 直接选中系统级 Broker。

## 多个账户共用一个身份

同一个系统级 Broker 服务的所有 Agent 共用它的指纹、别名和对端。不同账户的 Agent 只要加入同一个 topic，就能互相看见并互发消息。因此系统级 Broker 配对一次，所有被服务的账户就都连到了对方；另一台主机只需要这一次配对，就能访问这些账户。系统级 Broker 的默认别名是主机名的第一段，不带账户名，所以它服务的每个账户的 Agent 地址都形如 `name@workstation`。账户 alice 另起用户级 Broker 后，该账户会话的地址变成 `name@workstation.alice`，因为这些会话登记到了另一个 Broker。

Broker 的管理权限由 `/etc/peers/config.yaml` 的文件权限决定，与运行服务的账户无关。管理它有两种方式：成为拥有该文件的组的成员，或以 `sudo` 执行终端命令，后者由 Broker 按 root 的权限判定。这个组及它的限制见[以配置文件权限管理 Broker]({{< relref "management-permissions.md" >}}#管理组)。
