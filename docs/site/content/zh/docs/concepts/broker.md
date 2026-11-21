---
title: Broker
weight: 1
---

Broker 是主机上的守护进程。它登记自己所服务的 Agent 会话，维护与其他 Broker 的链路，并把收到的消息写入目标会话的 inbox socket。启用了 peers 工具的会话，在名字可用的前提下登记到本机的一个 Broker。

## 两种形态

Broker 以两种形态之一运行。

**系统级 Broker** 以特权身份作为系统服务运行。一台主机上至多一个，服务主机上所有获准使用它的账户下的会话。它由管理员安装。账户与组取自系统的账户数据库：Linux 上 Broker 经 `getent` 按 NSS 配置查询，`getent` 不存在、执行失败或输出不合要求时，退回读取本地文件，并记一条警告。

**用户级 Broker** 以普通账户运行，只服务这个账户下的会话。一个账户至多运行一个实例。它由用户的服务管理器启动，或由用户在终端里启动；MCP 服务进程只负责连接。能使用本机系统级 Broker 的账户不需要用户级 Broker；正在运行的用户级 Broker 优先于系统级 Broker。

以服务方式安装的 Broker 无法绑定监听地址时不会退出，而是进入仅本机降级，继续服务本机会话，见 [FAQ 的端口一问]({{< relref "/docs/faq" >}}#端口被占用时会怎样)。

每个实例有自己的指纹、对端和配置。同一台主机上的两个 Broker 在网络中是彼此独立的 Broker，不是同一个进程的两个部分。它们的默认别名各不相同，规则见下文[地址与身份](#地址与身份)。在主机名为 `workstation.example.com` 的主机上，系统级 Broker 取 `workstation`，账户 alice 与 bob 的用户级 Broker 分别取 `workstation.alice` 与 `workstation.bob`。这是三个别名互不相同的 Broker，按别名排序时同一主机上的 Broker 排在一起。

## 地址与身份

默认情况下，Broker 监听所有本机地址上的 17443 端口，启用 IPv6 时也包括 IPv6 地址。其他 Broker 经它的对外地址连到它；对外地址默认取承载默认路由的网络接口的地址。监听地址为空的 Broker 不监听，只能主动连接其他 Broker。

指纹就是身份：它是 Broker 公钥的摘要，Broker 重启、改名或更换监听地址之后都不变。别名是显示名，出现在 Agent 地址的 `@` 之后。没有配置 `broker.alias` 时，系统级 Broker 取主机名的第一段，即第一个 `.` 之前的部分；用户级 Broker 取这一段、一个 `.` 和运行它的账户名。别名与监听地址、端口、对外地址都无关，改端口或停止监听都不改变别名。主机名为 `workstation.example.com` 时，系统级 Broker 是 `workstation`，账户 alice 的用户级 Broker 是 `workstation.alice`。完整规则见[配置键]({{< relref "/docs/reference/configuration" >}})，包括别名字符集之外的字符如何替换、账户名查不到时怎么办、别名过长时如何截短。

Broker 默认不公开。公开的 Broker 发布一张名片，写明自己的指纹、别名和对外地址列表，连通网络内的每个 Broker 都能得知。是否与公开 Broker 配对，仍要单独决定。

## 配置

配置是一个 YAML 文件。用户级 Broker 读取 `~/.peers/config.yaml`，系统级 Broker 读取 `/etc/peers/config.yaml`。读取或修改配置的工具和终端命令遵循操作系统对这个文件的权限：读权限允许读取配置、完整状态和审计日志，写权限允许修改设置、生成一次性配对密钥、配对和解除配对。列出 Agent 与 topic、加入 topic、发送消息都不需要这些权限。

Broker 还可以设为下线，此时拒绝其他主机发来的消息，链路与配对都照常；两个命令见[命令行]({{< relref "/docs/reference/cli" >}}#peers-service-online-and-offline)。

Broker 如何决定可见范围，见[topic]({{< relref "/docs/concepts/topics" >}})；如何信任另一个 Broker，见[配对]({{< relref "/docs/concepts/pairing" >}})；对端之间的连接，见[链路]({{< relref "/docs/concepts/links" >}})。安装服务见[把 Broker 安装为服务]({{< relref "/docs/guides/run-as-service" >}})，多账户部署见[运行系统级 Broker]({{< relref "/docs/guides/system-broker" >}})；配置键与文件路径见[配置键]({{< relref "/docs/reference/configuration" >}})和[文件与路径]({{< relref "/docs/reference/files" >}})。
