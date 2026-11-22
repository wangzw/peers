---
title: 把 Broker 安装为服务
linkTitle: 把 Broker 安装为服务
weight: 5
description: "把 Broker 安装为用户级或系统级服务，让它随登录或开机启动、持续运行并在重启后恢复，还可以启动、停止和移除。"
---

Claude Code 会话只连接 Broker，不会启动它。启动 Broker 有两种办法：安装为服务，或者在终端里运行。安装为服务后，它立刻启动，此后随登录或开机启动，持续运行，机器重启后也会恢复。

安装命令已经安装并启动服务，所以本页介绍手动安装，以及用 `--no-service` 安装后补装服务。先停掉自己在终端启动的 Broker、卸载服务这两节，对所有安装方式都适用。

## 先停掉自己在终端启动的 Broker

如果你在这个账户下用终端运行过 `peers service run`，先在那个终端里停掉它，再安装服务。安装时如果已有 Broker 正在应答，而且不是这个服务启动的，安装会失败，因为这个 Broker 占着账户锁。

## 安装用户级服务

主机上已经有你能使用的系统级 Broker 时，不需要安装用户级服务。

用拥有会话的账户运行安装命令：

```sh
peers service install
```

命令写入单元文件或 plist，启用服务，并立即启动 Broker。它会等 Broker 应答，最多等 20 秒；Broker 没有应答时，安装失败，并给出服务日志的位置。重新安装会用新的二进制替换正在运行的 Broker。用户级安装不能用 root 运行，因为单元属于一个账户。位于临时目录里的二进制会被拒绝安装，用户级与系统级都如此。

`~/.peers/config.yaml` 不存在时，安装会生成它，内容是一份列出全部键的模板：有固定默认值的键写出取值，其余键写成注释并附示例值或空占位。文件属主是执行安装的账户，权限为 `0600`；已存在的文件不改动。安装汇总的 `config:` 一行以 `(generated)` 或 `(kept)` 结尾。[配置键]({{< relref "docs/reference/configuration" >}})介绍生成的文件。

在装有 systemd 的 Linux 上，单元文件是 `~/.config/systemd/user/peers.service`（或位于 `$XDG_CONFIG_HOME` 下）。在 macOS 上，LaunchAgent 是 `~/Library/LaunchAgents/io.github.wangzw.peers.plist`，安装只能在图形登录会话里执行；没有图形会话的主机改用系统级服务。

单元或 plist 以该账户身份运行 `peers service run --user --allow-degraded`，所以 Broker 无法绑定监听地址时会进入仅本机降级，继续服务这个账户。它在账户登录时启动，Broker 退出后重新启动。

## 安装系统级服务

系统级安装以 root 运行，让 Broker 在开机时启动。对安装命令的级别询问回答 y，或者以 root 运行，就能一步装好二进制与服务；这个询问见[安装 peers]({{< relref "install.md" >}})。下面的步骤则手动完成这两部分。脚本还会询问是否把执行安装的账户加入管理组。[管理组]({{< relref "management-permissions.md" >}}#管理组)介绍这个组、对这个询问的回答，以及控制这个询问的选项。

二进制必须放在 root 拥有、且组和其他人不可写的目录里，例如 `/usr/local/bin`。服务以 root 身份运行这个二进制，能改动它或其上级目录的普通账户可能借此接管服务：

```sh
VERSION=0.6.0
sudo install -o root -g 0 -m 0755 peers-${VERSION}/peers /usr/local/bin/peers
sudo /usr/local/bin/peers service install --system
```

Intel Mac 上用 Homebrew 时，`/usr/local/bin` 属于普通用户，安装会被拒绝；改选一个 root 拥有、组和其他人不可写的目录。

安装会检查二进制以及它上面的每一层目录，然后立即启动 Broker。

在 Linux 上，命令写入 `/etc/systemd/system/peers.service`，启用并启动它；在 macOS 上写入并加载 `/Library/LaunchDaemons/io.github.wangzw.peers.plist`。系统级安装还会在 `/etc/peers/config.yaml` 不存在时，用系统级模板生成它，属主为 root，属组为 `peers`，权限 `0660`；已存在的文件不改动，安装会打印把已有文件交给该组的命令。[管理组]({{< relref "management-permissions.md" >}}#管理组)介绍这个组。

## 卸载服务

用同一个账户运行 `peers service remove`，可以卸载用户级服务。卸载系统级服务用 `sudo /usr/local/bin/peers service remove --system`。命令停止 Broker 并只删除服务定义；二进制与 MCP 注册都会保留。没有安装过服务时，命令照常成功，并报告未安装，所以可以放心重复执行。要卸载整个安装，运行 [`peers uninstall`]({{< relref "/docs/reference/cli" >}}#peers-uninstall)，系统级安装加 `sudo`。

## 启停服务

用 `peers service stop` 与 `peers service start` 停止和启动服务，用 `peers service restart` 重启；系统级服务加 `--system`，并用 `sudo` 运行。退出码见[命令行]({{< relref "/docs/reference/cli" >}}#peers-service-start-stop-and-restart)。

## 检查服务

在终端运行 `peers service status`，系统级服务加 `--system`。输出包含一张 `Agents` 表和一张 `Peers` 表，没有内容时分别写 `Agents: none` 和 `Peers: none`。服务管理器里也能看到 Broker：`systemctl --user status peers.service`、`systemctl status peers.service`，或者用 `launchctl print` 查看标签 `io.github.wangzw.peers`。

每个账户只保留一个 Broker。用同一个账户再启动一个 Broker 会失败，错误说明已有实例在运行，无论它要绑定哪个地址。另一个账户的 Broker 已占了端口时，安装失败；已装好的服务之后才遇到端口被占用时，进入仅本机降级。
