---
title: 常见问题
weight: 8
---

## 如何安装 peers？

这条命令安装最新版本，并为你的账户注册 MCP 服务；能询问时会问安装级别：

```sh
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh
```

发布包解压后也有一个 `install.sh`，在解压后的目录里运行 `sh ./install.sh`，同样会询问级别；也可以手动安装二进制。[安装 peers]({{< relref "guides/install.md" >}})介绍级别询问与各种安装方式，包括系统级安装。

主机上已经有你能使用的系统级 Broker 时，只需注册 MCP 服务，不要安装用户级 Broker；命令见[运行系统级 Broker]({{< relref "guides/system-broker.md" >}})。

## 如何卸载 peers？

运行 `peers uninstall` 卸载本账户的安装；系统级安装用 `sudo peers uninstall` 卸载。命令会询问是否删除配置、身份、审计和服务日志，除非回答 `y`，这些数据都会保留。[卸载 peers]({{< relref "guides/uninstall.md" >}})介绍各个级别、询问内容以及只卸载服务的做法。

## 配置文件在哪里？

用户级 Broker 读 `~/.peers/config.yaml`，系统级 Broker 读 `/etc/peers/config.yaml`。`peers service install` 在该级别的文件不存在时生成它。没有文件时，每个键都取默认值，Broker 照常运行。生成的文件与修改方法见[配置键]({{< relref "reference/configuration.md" >}})。

## 支持哪些平台？

macOS 与 Linux，支持 amd64 与 arm64。不支持原生 Windows。见[已知限制]({{< relref "limitations.md" >}})。

## 需要哪个版本的 Claude Code？

Claude Code v2.1.224 或更高版本。peers 怎样依赖 Claude Code，见[已知限制]({{< relref "limitations.md" >}})。

## 有中心服务器吗？

没有。Broker 之间直接连接，消息不转发，也没有集中式注册中心。两台主机里至少有一台能连到另一台。见[链路]({{< relref "concepts/links.md" >}})。

## 需要手动启动 Broker 吗？

会话不会自己启动 Broker。主机上已经有你能使用的系统级 Broker 时，只需注册 MCP 服务，会话会连接到它。否则需要自己启动：用 `peers service install` 把它安装为用户级服务，安装完成就会启动，此后随登录启动；另一种做法是在 Claude Code 之外的终端里运行 `peers service run`。安装脚本已经默认安装并启动用户级服务，用它装好之后无需再做这一步。在会话里启动的 Broker 会以退出码 6 退出。见[把 Broker 安装为服务]({{< relref "guides/run-as-service.md" >}})。

## 没有 Broker 在运行时会怎样？

工具调用返回错误，说明该怎么做，具体视情形而定。MCP 服务进程不会启动 Broker。有两种情形得到同一条提示：第一次查找时既找不到用户级 Broker 也找不到系统级 Broker，以及用户级 Broker 的 socket 消失、拒绝连接（例如被强杀之后）。提示先说明：本机如有系统级 Broker，请管理员启动它；再给出两种运行用户级 Broker 的方法：`peers service install`，或在 Claude Code 之外的终端里运行 `peers service run`。原先连在系统级 Broker 上的会话断开后，提示联系管理员；本账户是 root 时，提示用 `peers service start --system` 启动它。用户级 socket 还在应答，但握手或调用失败时，报告 Broker 不可用，并说明 MCP 服务会在后台重试，不给出启动方法。启动 Broker 之后，会话不必重新连接 MCP 服务。需要 Broker 的终端命令，例如 `peers broker list`，做法相同：只查找正在运行的 Broker，不会启动它。见[命令行]({{< relref "reference/cli.md" >}})。

## 一个账户能运行多个 Broker 吗？

不能。一个账户至多运行一个 Broker，用另一个监听地址再启动一个会失败。见[Broker]({{< relref "concepts/broker.md" >}})。

## Broker 的默认别名是什么？

没有设置 `broker.alias` 时，系统级 Broker 报出主机名的第一段，即第一个 `.` 之前的部分。用户级 Broker 报出这一段、一个 `.` 和账户名；主机名为 `workstation` 时，系统级 Broker 是 `workstation`，账户 alice 的用户级 Broker 是 `workstation.alice`。别名不含监听端口，所以改端口或对外地址都不会改变它。想自选别名，就在配置文件里设置 `broker.alias`。完整规则见[配置键]({{< relref "reference/configuration.md" >}})。

## 怎样更新 peers？

在终端运行 `peers update`，或在会话里说“更新 Broker。”并批准这次调用（模型会调用 `update_broker`）。系统级安装由管理组成员更新，不用 `sudo`；不在组内的账户用 `sudo peers update`，或先加入管理组。同一账户还运行着用户级 Broker 时，用 `sudo peers update` 到达系统级 Broker。更新会校验下载内容，替换二进制。作为服务运行的 Broker 会用新版本重启。没有在运行的 Broker，或者不在服务管理器下运行的用户级 Broker，下次启动时才用上新版本。已经打开的会话要用 `/mcp` 重连或重开。见[更新 peers]({{< relref "guides/update.md" >}})。

## 能关闭更新检查吗？

能。运行 `peers config set update.check false`，`Update` 一行和会话里的提示会立刻消失，之后不再检查。检查与更新的工作方式见[更新 peers]({{< relref "guides/update.md" >}})，检查向 GitHub 发送什么见[安全模型]({{< relref "security.md" >}})。

## 两台主机如何开始互相信任？

两台主机里至少要有一台能连到另一台。在对方能连上的那台主机上说“生成一个配对密钥。”并批准这次调用（模型会调用 `create_pairing_key`），或运行 `peers key create`。结果给出一个一次性密钥、显示的地址，以及一段填好密钥与本 Broker 全部地址、可直接粘贴的连接提示词。把提示词通过系统之外的途径交给另一台主机的用户，不要让 Agent 转发密钥或提示词；原因见[安全模型]({{< relref "security.md" >}}#配对与指纹)。另一台在会话里粘贴提示词并批准这次调用（模型会调用 `connect_broker`），或运行提示词里给出的命令；也可以说“连接到 `<address>` 上的 Broker，密钥是 `<key>`。”，地址和密钥都给出。配对使用该一次性密钥或接受方配置的预置静态密钥。此后双方各凭指纹认对方。见[配对]({{< relref "concepts/pairing.md" >}})与[两台主机配对]({{< relref "guides/pair-hosts.md" >}})。

## 什么是 topic？需要自己创建吗？

topic 是 Agent 之间可见与通信的范围。同一个项目里工作的会话通常共享一个默认 topic，它取自 `.peers.yaml` 或 git origin 地址，所以不需要自己创建。见[topic]({{< relref "concepts/topics.md" >}})。

## 没有配对的主机能看到我的会话吗？

看不到。配对之后，一台主机也只能看到与自己的 Agent 同处一个 topic 的 Agent。Broker 默认不公开，除非你把它设为公开，否则连通网络里不会有它的名片。见[安全模型]({{< relref "security.md" >}})。

## Broker 之间的流量加密吗？

加密。链路使用 TLS 1.3 与自签名证书，Broker 的身份是它的公钥指纹。见[安全模型]({{< relref "security.md" >}})。

## 能发送文件或会话上下文吗？

不能。消息只传纯文本，文件、会话历史与上下文都不传。见[已知限制]({{< relref "limitations.md" >}})。

## 发送成功就代表对方读到了吗？

不代表。成功只表示消息已经写进目标会话的 inbox socket，并不能说明模型有没有读、有没有处理。投递请求发出后没有收到明确答复时，返回以 `Delivery unknown` 开头的错误。见[MCP 工具]({{< relref "reference/mcp-tools.md" >}})。

## 为什么重复发送的同一条消息不见了？

接收端的 Claude Code 会丢弃同一个 Agent 在 30 秒内发给同一个会话、与上一条内容完全相同的消息；这条规则属于 Claude Code，可能改动。接收端的 Broker 还会丢弃同一次发送中重试产生的副本，但两次分开的发送不会被去重。见[安全模型]({{< relref "security.md" >}})。

## 谁能修改 Broker 的配置？

对 Broker 配置文件有写权限的账户可以用 `set_config` 或 `peers config set` 修改配置，用 `create_pairing_key` 或 `peers key create` 生成一次性配对密钥，用 `connect_broker` 与 `disconnect_broker`，或 `peers broker connect` 与 `peers broker disconnect` 配对和解除配对；读权限允许读取配置和审计日志。终端命令按执行命令的账户判定权限。管理组成员不用 `sudo` 就能管理系统级 Broker：`peers service status`、`audit`、`online`、`offline` 加 `--system` 连到它，本账户没有运行自己的用户级 Broker 时，`peers config`、`peers key`、`peers broker` 直接连到它；`peers service start`、`stop`、`restart`、`install`、`remove` 仍需 `sudo`；管理员用 `sudo` 执行命令时，系统级 Broker 按 root 的权限判定。见[管理权限]({{< relref "guides/management-permissions.md" >}})，管理组的说明见[管理组]({{< relref "guides/management-permissions.md" >}}#管理组)一节。

## 端口被占用时会怎样？

前台运行的 Broker 启动失败。另一个进程占用地址时，新装或重装会失败，说明地址已被占用，不注册服务。已经安装为服务的 Broker 在启动时发现端口被占用，会进入仅本机降级：本机 Agent 照常工作，生成配对密钥失败，对端连不进来。它不会另选地址或端口；腾出端口，或者修改监听地址，不用重启就会恢复。见[配置键]({{< relref "reference/configuration.md" >}})与 [peers service install]({{< relref "reference/cli.md" >}}#peers-service-install)。

## 怎么看运行日志和审计记录？

用 `peers service status` 查看 Agent、topic、对端和链路，用 `peers service audit` 查看审计日志。Broker 处于降级运行时，`peers service status` 会给出原因与修复方法。把 Broker 安装为服务，运行日志由服务管理器保存：Linux 上用 `journalctl --user -u peers.service`，系统级服务用 `journalctl -u peers.service`；macOS 上用户级服务写入 `~/Library/Logs/peers/broker.log`，系统级写入 `/var/log/peers/broker.log`。在终端运行 `peers service run`，日志就留在那个终端里。见[文件与路径]({{< relref "reference/files.md" >}})的“日志”一节。

## 无界面会话能收到消息吗？

以无界面方式运行且跳过权限确认的会话不能。接收方的 Claude Code 先把消息暂扣，随后因发送方没有声明权限类别而丢弃，而发送方那边已经返回成功。见[已知限制]({{< relref "limitations.md" >}})。
