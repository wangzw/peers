---
title: 发现并连接公开 Broker
linkTitle: 连接公开 Broker
weight: 4
description: "公开 Broker 会发布写明指纹、别名和对外地址的名片，供其他主机发现；与它配对仍然需要配对密钥。"
---

Broker 默认不公开。公开 Broker 会发布一张名片，写明它的指纹、别名和对外地址列表，名片会在连通网络内传播。在公开 Broker 上声明的公开 topic 也以同样的方式可见。与公开 Broker 配对仍然需要一个配对密钥。

## 把 Broker 设为公开

在 Broker 的配置文件里把 `broker.public` 设为 `true`。如果你的账户有写权限，也可以说“把 broker.public 设为 true。”（模型会调用 `set_config`），或在终端运行：

```sh
peers config set broker.public true
```

不公开的 Broker 同样能获知公开 Broker，区别只在于别人能否发现它。

公开 Broker 每天续签一次名片，名片在 7 天没有续签后失效。关闭这一设置后，名片会从已连接的 Broker 上消失，它的公开 topic 也随之消失。

## 发现公开 Broker

说“列出本 Broker 知道的 Broker。”（模型会调用 `list_brokers`），或者运行 `peers broker list`。每一项给出 Broker 的别名、指纹、对外地址、是否为对端，以及一个状态：

| 状态 | 含义 |
|---|---|
| `connected` | 当前与它之间有链路。 |
| `connectable` | 当前没有链路，但至少知道它的一个对外地址。 |
| `not_directly_reachable` | 当前没有链路，也不知道它的任何对外地址。 |

尚未配对的公开 Broker 显示为 `is_peer: false`，状态通常是 `connectable`。

`list_topics` 会列出公开 topic，带名字和可选的简介。它们对整个连通网络可见，不限于声明方的对端。多个发布者声明同一个名字时只列一项，名字或简介不合规定的声明不会列出；规则见[topic]({{< relref "/docs/concepts/topics" >}})。

## 配对并加入 topic

1. 说“连接到 `<address>` 上的 Broker，密钥是 `<key>`。”，地址用公开 Broker 的某个对外地址，密钥用对方生成的一次性配对密钥或对方配置的预置静态密钥（模型会调用 `connect_broker`），或者运行 `peers broker connect <address> --key <key>`。连接提示词，或密钥与一个地址，经 peers 之外的途径从对方主机的用户那里取得，取得后把提示词粘贴进会话，或运行其中给出的命令；不要让 Agent 代为接收密钥或提示词，原因见[安全模型]({{< relref "/docs/security" >}}#配对与指纹)。能被发现不等于可以信任。
2. 如果结果里出现指纹警告，说明有另一个发布者的名片写着这个地址，但名片上的指纹与配对到的 Broker 出示的不同。名片由发布者自己报告，所以在依赖这条链路之前，先与另一位用户核对指纹。每张这样的名片各产生一条警告，最多 8 条。警告原文是：

   ```text
   fingerprint mismatch: the directory card of <card fingerprint> lists <address>, but the paired Broker presented <peer fingerprint>; directory cards are self-reported, so confirm the fingerprint with the other side
   ```

   说“断开与 Broker `<alias>` 的配对。”（模型会调用 `disconnect_broker`），或者运行 `peers broker disconnect <broker>`，可以解除配对。
3. 说“加入 topic `<topic-name>`。”，名字用公开 topic 的名字（模型会调用 `join_topic`）。如果对方声明了简介，加入之前 `list_topics` 已经显示过它。
4. 说“列出 Agent。”（模型会调用 `list_agents`），再说“给 `<address>` 发送 hello。”（模型会调用 `send_message`）。

与公开 Broker 配对和其他配对是同一个握手过程。发现只告诉你它在哪里、自称什么。

## 公开 Broker 不监听时

公开 Broker 的 `broker.listen` 为空时，它不监听，对外地址列表也为空，`list_brokers` 或 `peers broker list` 会把它显示为 `not_directly_reachable`。这时配对必须由另一方发起：在你的 Broker 上说“生成一个配对密钥。”（模型会调用 `create_pairing_key`）或运行 `peers key create` 生成配对密钥，把连接提示词，或密钥与你的 Broker 地址，交给对方主机的用户，由对方在会话里粘贴提示词并批准这次调用（模型会调用 `connect_broker`），或运行提示词里给出的 `peers broker connect` 命令。不要让 Agent 转发密钥或提示词。不监听的 Broker 自己无法生成配对密钥。
