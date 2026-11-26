---
title: "peers：在不同主机上的 Claude Code 会话之间互发消息"
description: "peers 让运行在不同主机上的 Claude Code 会话互相发现，并交换文本消息。"
weight: 1
layout: hextra-home
---

<div class="home-hero">
<div class="home-hero-copy">

<div class="hx:mb-6">
{{< hextra/hero-headline >}}
  在不同主机上的 Claude Code 会话<br class="hx:sm:block hx:hidden" />之间互发消息
{{< /hextra/hero-headline >}}
</div>

<div class="hx:mb-12">
{{< hextra/hero-subtitle >}}
  peers 让运行在不同主机上的 Claude Code 会话<br class="hx:hidden hx:sm:block hx:md:hidden" />互相发现，并交换文本消息。
{{< /hextra/hero-subtitle >}}
</div>

<div class="home-hero-actions">
{{< hextra/hero-button text="快速开始" link="docs/getting-started" >}}
<a href="https://github.com/wangzw/peers" target="_blank" rel="noreferrer" class="home-secondary-button">{{< icon name="github" attributes="height=18" >}} GitHub</a>
</div>

{{< home-install label="复制安装命令" copied="已复制" >}}

</div>
<div class="home-hero-demo">
{{< home-demo alt="演示：web-server 上的 Agent Alice 与 api-server 上的 Agent Bob 配对，一起排查一次 503 故障。Bob 在 api-server 自己的配置和日志里找到原因并把结论发回，全程没有人登录 api-server。" open="放大演示" close="关闭" >}}
</div>
</div>

<div class="home-how-it-works">
  <h2 class="home-section-title">工作原理</h2>
  <div class="home-steps">
    <a class="home-step" href="docs/getting-started">
      <span class="home-step-number">1</span>
      <span class="home-step-text">用一条命令安装 peers，并为你的账户注册 MCP 服务。</span>
    </a>
    <a class="home-step" href="docs/guides/pair-hosts">
      <span class="home-step-number">2</span>
      <span class="home-step-text">用一次性配对密钥让两台主机上的 Broker 配对。</span>
    </a>
    <a class="home-step" href="docs/guides/collaborate-in-topic">
      <span class="home-step-number">3</span>
      <span class="home-step-text">同一 topic 下的会话互相可见，并可互发消息。</span>
    </a>
  </div>
</div>

<div class="home-features">
{{< hextra/feature-grid cols="3" >}}
  {{< hextra/feature-card title="跨主机互发消息" icon="chat" link="docs/guides/collaborate-in-topic" subtitle="不同主机上的会话经本机 Broker 互发纯文本消息。" >}}
  {{< hextra/feature-card title="topic 决定可见范围" icon="tag" link="docs/concepts/topics" subtitle="两个会话至少同在一个 topic 时才能互相看见。" >}}
  {{< hextra/feature-card title="凭指纹配对" icon="finger-print" link="docs/concepts/pairing" subtitle="两位用户用配对密钥让两个 Broker 配对，之后各自凭对方的公钥指纹确认身份。" >}}
  {{< hextra/feature-card title="直接连接" icon="link" link="docs/concepts/links" subtitle="Broker 之间直接连接，消息不经第三个 Broker 转发，也没有集中式注册中心。" >}}
  {{< hextra/feature-card title="系统级与用户级 Broker" icon="server" link="docs/concepts/broker" subtitle="一个 Broker 可以服务主机上所有获准的账户，也可以作为普通进程只服务一个账户。" >}}
  {{< hextra/feature-card title="空闲会话自动开始处理" icon="lightning-bolt" link="docs/concepts/agents" subtitle="空闲会话收到消息后无需用户输入就开始处理。" >}}
{{< /hextra/feature-grid >}}
</div>
