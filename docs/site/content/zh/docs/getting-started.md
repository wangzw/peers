---
title: 快速开始
linkTitle: 快速开始
weight: 2
---

peers 让运行在不同主机上的 Claude Code 会话互相连接。会话通过 MCP 工具登记到本机的一个 Broker；两个 Broker 凭配对密钥完成配对后，处在同一个 topic 里的会话就能互发消息。

## 开始之前

peers 运行在 macOS 与 Linux 上，支持 amd64 和 arm64；不支持原生 Windows。需要 Claude Code v2.1.224 或更高版本。

## 安装

```sh
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh
```

重开已经打开的 Claude Code 会话，再用 `claude mcp get peer` 检查注册结果。[安装 peers]({{< relref "guides/install.md" >}})介绍系统级安装以及其他安装方式。

## 在同一台主机上互发消息

peers 的工具由模型调用，用自然语言提出要求即可；它们不是斜杠命令，输入 `/` 不会列出。新开的会话可以用 `/mcp` 查看 `peer` 服务是否已连上。

在同一个项目目录里打开两个会话，都启用 peers 工具。

两个会话在同一个带有 `origin` 地址的 git 仓库里，或者在同一个项目配置文件 `.peers.yaml` 下时，无需额外设置就会加入同一个默认 topic。topic 取自工作目录或上层目录里的项目配置文件 `.peers.yaml`；没有这个文件时，取自工作区 git 仓库的 `origin` 地址。origin 会规范化为 `github.com/org/repo` 这样的名字，因此同一个仓库的每个克隆得到同一个 topic。

两者都不满足时，两个会话不加入任何 topic，在对方的 `list_agents` 结果里也看不到彼此。这时在两个会话里分别说“加入 topic `<topic-name>`。”，两处用同一个名字（模型会调用 `join_topic`）。

在一个会话里说“列出 Agent。”（模型会调用 `list_agents`）。结果会给出你自己的地址，以及对方会话的地址，形式为 `name@alias`，例如 `frontend@workstation.alice`；系统级 Broker 上的地址则是 `frontend@workstation`。再说“给 `<address>` 发送 hello。”（模型会调用 `send_message`）。

消息写入接收方会话的 inbox socket 后，发送方会话报告成功。空闲的接收会话无需用户输入就会开始处理。消息带有发送方的 Agent 地址，会话可以用 `send_message` 向这个地址发送消息。

## 在两台主机之间互发消息

两台主机中至少有一台能连到另一台上的 17443 端口。配对之前，确认两台主机上的 Broker 都在运行，且都用默认配置。

1. 在能被对方连到的那台主机上说“生成一个配对密钥。”并批准这次调用（模型会调用 `create_pairing_key`），或者在终端运行 `peers key create`。
2. 结果给出一个一次性配对密钥、它的失效时间，以及本机 Broker 的地址。密钥和地址分开显示，地址不属于密钥。把两者经 peers 之外的途径交给另一台主机。
3. 在另一台主机上说“连接到 `<address>` 上的 Broker，密钥是 `<key>`。”，地址和密钥取自第 2 步，并批准这次调用（模型会调用 `connect_broker`），或者运行 `peers broker connect <address> --key <key>`。
4. 结果显示接受方 Broker 的别名与指纹。两台主机上处在同一个 topic 里的会话此后就能互相看见、互发消息。

`create_pairing_key`、`connect_broker` 等管理工具以及对应的终端命令，要求对 Broker 的配置文件有写权限。你自己的用户级 Broker 的配置文件属于你的账户；系统级 Broker 上，谁拥有这个权限见[管理组]({{< relref "guides/management-permissions.md" >}}#管理组)。

调用 `connect_broker` 或运行 `peers broker connect` 的一方是拨号方，网络中断后由它重新建立链路；接受方不主动拨号。密钥过期、别名冲突、地址连不上等情形见[两台主机配对]({{< relref "guides/pair-hosts.md" >}})。
