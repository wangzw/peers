---
title: 快速开始
linkTitle: 快速开始
weight: 2
description: "安装 peers、注册 MCP 服务，并在同一主机或两台已配对主机上的会话之间发出第一条消息。"
---

peers 让运行在不同主机上的 Claude Code 会话互相连接。会话通过 MCP 工具登记到本机的一个 Broker；两个 Broker 凭配对密钥完成配对后，处在同一个 topic 里的会话就能互发消息。

## 开始之前

peers 运行在 macOS 与 Linux 上，支持 amd64 和 arm64；不支持原生 Windows。需要 Claude Code v2.1.224 或更高版本。

## 安装

```sh
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh
```

重开已经打开的 Claude Code 会话，再用 `claude mcp get peer` 检查注册结果。[安装 peers]({{< relref "guides/install.md" >}})介绍系统级安装以及其他安装方式。

## 谁能和谁通信

**两个 Agent 只有同在至少一个 topic 里，才能互相看见、互发消息。** 同一台主机上是这样，两台已配对的主机之间也是这样：仅在同一台主机上，或者仅有两个已连接的 Broker，还不够。没有共同 topic 时，对方不会出现在 `list_agents` 的结果里，向对方发消息也会失败。

默认情况下，会话登记后会加入一个默认 topic，在同一个项目里工作通常不用额外设置。默认 topic 取自工作目录或上层目录里的项目配置文件 `.peers.yaml` 所声明的 topic 列表；没有这个文件，或者文件没有声明列表时，默认 topic 取工作区 git `origin` 地址的规范化结果，例如 `github.com/org/repo`。因此同一个仓库的每个克隆，无论在哪台主机上，都得到同一个 topic。

目录不在 git 仓库里，仓库没有 `origin` 远端，或者 `origin` 是本地路径时，会话不加入任何 topic。这样的会话看不见其他 Agent，别的 Agent 也看不见它。

解决办法是在两边会话里分别说“加入 topic `<topic-name>`。”，两处用同一个名字（模型会调用 `join_topic`）。要为一个项目长期固定 topic，提交一份列出该 topic 的 `.peers.yaml`；见[在项目里指定 topic]({{< relref "guides/collaborate-in-topic.md" >}}#在项目里指定-topic)。完整的规则见[默认 topic]({{< relref "concepts/topics.md" >}}#默认-topic)。

## 在同一台主机上互发消息

peers 的工具由模型调用，用自然语言提出要求即可；它们不是斜杠命令，输入 `/` 不会列出。新开的会话可以用 `/mcp` 查看 `peer` 服务是否已连上。

在同一个项目目录里打开两个会话，都启用 peers 工具，两个会话就在同一个默认 topic 里。不是这样时，按[谁能和谁通信](#谁能和谁通信)里的办法，在两边加入同一个 topic。

在一个会话里说“列出 Agent。”（模型会调用 `list_agents`）。结果会给出你自己的地址，以及对方会话的地址，形式为 `name@alias`，例如 `frontend@workstation.alice`；系统级 Broker 上的地址则是 `frontend@workstation`。再说“给 `<address>` 发送 hello。”（模型会调用 `send_message`）。

消息写入接收方会话的 inbox socket 后，发送方会话报告成功。空闲的接收会话无需用户输入就会开始处理。消息带有发送方的 Agent 地址，会话可以用 `send_message` 向这个地址发送消息。

## 在两台主机之间互发消息

两台主机中至少有一台能连到另一台上的 17443 端口。配对之前，确认两台主机上的 Broker 都在运行，且都用默认配置。

1. 在能被对方连到的那台主机上说“生成一个配对密钥。”并批准这次调用（模型会调用 `create_pairing_key`），或者在终端运行 `peers key create`。
2. 结果给出一个一次性配对密钥、它的失效时间、本机 Broker 的地址，以及一段填好密钥与本机 Broker 全部地址、可直接粘贴的连接提示词。密钥和地址分开显示，地址不属于密钥。把这段提示词经 peers 之外的途径交给另一台主机的用户，不要让 Agent 转发；原因见[安全模型]({{< relref "security.md" >}}#配对与指纹)。
3. 在另一台主机的会话里粘贴这段提示词并批准这次调用（模型会调用 `connect_broker`，地址与密钥取自提示词），或者运行提示词给出的 `peers broker connect` 命令。
4. 结果显示接受方 Broker 的别名与指纹。配对只连通两台 Broker；两边的会话还要在同一个 topic 里才能互相看见，例如都开在同一仓库的克隆里（`origin` 规范化后相同），或者都加入了同名 topic。看不到对方时，先检查这一点，见[谁能和谁通信](#谁能和谁通信)。

`create_pairing_key`、`connect_broker` 等管理工具以及对应的终端命令，要求对 Broker 的配置文件有写权限。你自己的用户级 Broker 的配置文件属于你的账户；系统级 Broker 上，谁拥有这个权限见[管理组]({{< relref "guides/management-permissions.md" >}}#管理组)。

调用 `connect_broker` 或运行 `peers broker connect` 的一方是拨号方，网络中断后由它重新建立链路；接受方不主动拨号。密钥过期、别名冲突、地址连不上等情形见[两台主机配对]({{< relref "guides/pair-hosts.md" >}})。
