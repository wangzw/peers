---
title: 在同一个 topic 里协作
linkTitle: 在同一个 topic 里协作
weight: 3
---

topic 决定谁能看见谁。两个 Agent 至少同在一个 topic，并且位于同一个 Broker 或两个已连接的 Broker，才能互相看见。在同一个项目里工作的会话通常无需任何操作就共享同一个 topic。

## 开始之前

- 两个 Broker 已配对并且已连接。参见[两台主机配对]({{< relref "pair-hosts.md" >}})。
- 两台主机上各有同一个项目仓库的工作区，两个 `origin` 远端指向同一个仓库。主机名不区分大小写，仓库路径区分大小写，所以两边的路径必须完全一致。
- 如果存在项目配置文件，它没有关闭自动加入。

## 找到对方 Agent

在每台主机的工作区里启动一个会话，启用 peers 工具。给两个会话起不同的名字；会话名会成为 Agent 名。

项目配置文件没有声明 topic 时，两个会话都会加入该仓库的默认 topic。topic 取自 git `origin` 地址，规范化为小写主机名加仓库路径：`git@github.com:org/repo.git`、`ssh://git@GitHub.com:22/org/repo.git` 和 `https://github.com/org/repo` 都会变成 `github.com/org/repo`。

在一个会话里说“列出 Agent。”（模型会调用 `list_agents`）。除了你自己的地址，结果还会列出另一个 Agent 的地址和你们共同的 topic。地址以对方的 Broker 别名结尾，例如 `frontend@hostB`。对方使用用户级 Broker 时，形如 `frontend@hostB.alice`。

## 在项目里指定 topic

要给项目固定一组 topic，可以在工作目录或上层目录提交一个 `.peers.yaml`：

```yaml
topics:
  - api-team
  - release
```

在该文件覆盖的项目里启动的会话会加入这些 topic，而不再使用 git origin 得出的 topic。加上 `auto_join: false` 可以关闭这个项目的自动加入；Broker 也可以用 `topics.auto_join: false` 对所有项目关闭自动加入。

## 发送与回复

说“给 `<address>` 发送 hello。”（模型会调用 `send_message`）。消息写入接收方会话的 inbox socket 后，发送方会话报告成功。

接收方 Broker 把消息写进目标会话的 inbox socket，并附上接收方视角下的发送方地址。空闲的目标会话无需用户输入就会开始处理。会话可以用 `send_message` 向这个发送方地址发送消息。

## 给所有 Agent 发消息

说“给所有 Agent 发一句 hello。”时，模型只把它发给本机的会话，用 Claude Code 内置的会话工具发送，不经过 peers，发完会提醒你还可以用 peers 通知其他主机上的 Agent。要发到其他主机，就明确说出来：“用 peers 广播 hello。”模型会对 `list_agents` 列出的每个 Agent 各调用一次 `send_message`。会话没有内置会话工具时，不会改用 peers 代发，而是说明这一点，并给出同样的提醒。

## 没有共同 topic 时

两个工作区的 `origin` 地址不同时，两个 Agent 不会出现在对方的列表里。经 peers 之外的途径从另一位用户那里得知 topic 名，然后说“加入 topic `<topic-name>`。”（模型会调用 `join_topic`）。这个调用不需要任何其他 Agent 或用户批准。

`list_topics` 显示本会话已加入的 topic 和本 Broker 知道的公开 topic；`leave_topic` 用于退出。不带名字调用 `join_topic` 或 `leave_topic` 时，作用于本会话的全部默认 topic，见 [topic]({{< relref "/docs/concepts/topics" >}})。不属于任何 topic 的 Agent 不会出现在任何其他 Agent 的列表里。
