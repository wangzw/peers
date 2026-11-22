---
title: 链路
weight: 5
description: "链路是两个已配对 Broker 之间唯一的长期连接，由拨号方建立并在断开后重连，双向传递消息。"
---

链路是两个已配对 Broker 之间的长连接。每对对端之间只有一条链路，双向传递消息。

拨号方是发起配对的一方。它建立链路，并在断开后重新建立；接受方等待拨号方连入，并告诉它该用哪些对外地址。两个 Broker 配对之后，重建链路既不需要配对密钥，也不需要用户操作。

一对对端里的两个 Broker 必须使用同一个链路协议版本。版本不同时，配对关系照常保存，但链路建不起来，两台主机上的 Agent 互相看不见。这是“重建链路不需要用户操作”的唯一例外：把较旧的一台更新到与另一台相同的版本，拨号方会自动重新建立链路，不用重新配对。原因可以在 `peers service status` 和 `list_brokers` 的结果里看到。

## 直接连接

Broker 之间直接连接，消息不经第三个 Broker 转发，也没有集中式注册中心。两台主机互相都无法连接时，例如双方都在 NAT 之后，两个 Broker 就不能通信。

链路上的全部流量都加密。

系统不缓存、不持久化、也不重投消息。一次发送要么写入目标会话的 inbox socket 并报告成功，要么在有限的等待后返回以 `Not sent` 开头的错误，要么因为请求已经发出但没有收到确定答复而报告送达情况未知。对端 Broker 报来本 Broker 认不出的错误时，按 `Delivery unknown` 处理。是否重发由发送方 Agent 决定。一次发送内部的重试，由接收方 Broker 按消息标识去重。送达情况未知之后再发，算一条新消息，目标会话可能收到两份。

链路两端的信任从何而来，见[配对]({{< relref "/docs/concepts/pairing" >}})；链路使用的地址见[Broker]({{< relref "/docs/concepts/broker" >}})；配对与协作的步骤见[两台主机配对]({{< relref "/docs/guides/pair-hosts" >}})和[在同一个 topic 里协作]({{< relref "/docs/guides/collaborate-in-topic" >}})；一次发送最多等待多久，见[配置键]({{< relref "/docs/reference/configuration" >}})。
