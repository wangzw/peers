---
title: 已知限制
weight: 7
---

有些事你可能以为 peers 能做，它其实做不到。

- **发送文件或共享会话上下文。** 消息只传纯文本。见[Agent]({{< relref "concepts/agents.md" >}})。
- **为没有运行的会话保留消息。** 消息不排队、不存储、不重投：只有发送时正在运行的会话才能收到，要不要再发由发送方 Agent 决定。见[链路]({{< relref "concepts/links.md" >}})。
- **连接两台互相连不到的主机。** Broker 之间直接连接，没有中转，所以两台主机里至少要有一台能连到另一台。见[链路]({{< relref "concepts/links.md" >}})。
- **在原生 Windows 上运行。** peers 运行在 macOS 与 Linux 上，支持 amd64 与 arm64。见[安全模型]({{< relref "security.md" >}})。
- **与其他 A2A agent 对话。** Broker 之间的消息使用 A2A 的 Message 格式，但 peers 不处理 A2A 的任务、流式传输与回调，所以不能与外部 A2A agent 交换消息。
- **覆盖每一个 Claude Code 会话。** 以无界面方式运行且跳过权限确认的会话收不到消息，发送方却会看到成功；跳过权限确认又没有设置入站策略的会话，会把每条消息暂扣等批准；从另一个会话的命令行启动的会话不能成为 Agent。见[Agent]({{< relref "concepts/agents.md" >}})与[安全模型]({{< relref "security.md" >}})。
- **在每次 Claude Code 升级后照常工作。** peers 依赖 Claude Code 的两个接口，即 inbox socket 的消息格式与会话注册表，Claude Code 没有公开承诺保持它们兼容，所以升级可能改变登记或投递的方式。
