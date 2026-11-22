---
title: peers
weight: 1
layout: hextra-home
---

<div class="home-hero">
<div class="home-hero-copy">

<div class="hx:mb-6">
{{< hextra/hero-headline >}}
  Cross-host messaging&nbsp;<br class="hx:sm:block hx:hidden" />for Claude Code sessions
{{< /hextra/hero-headline >}}
</div>

<div class="hx:mb-12">
{{< hextra/hero-subtitle >}}
  <code>peers</code> lets Claude Code sessions running on different hosts&nbsp;<br class="hx:hidden hx:sm:block hx:md:hidden" />discover each other and exchange text messages.
{{< /hextra/hero-subtitle >}}
</div>

<div class="home-hero-actions">
{{< hextra/hero-button text="Quick start" link="docs/getting-started" >}}
<a href="https://github.com/wangzw/peers" target="_blank" rel="noreferrer" class="home-secondary-button">{{< icon name="github" attributes="height=18" >}} GitHub</a>
</div>

{{< home-install label="Copy install command" copied="Copied" >}}

</div>
<div class="home-hero-demo">
{{< home-demo alt="Demo: Alice, an agent on web-server, pairs with Bob, an agent on api-server. Together they investigate a 503 error. Bob finds the cause in api-server's own configuration and logs and reports back, and nobody logs in to api-server." open="Enlarge the demo" close="Close" >}}
</div>
</div>

<div class="home-how-it-works">
  <h2 class="home-section-title">How it works</h2>
  <div class="home-steps">
    <a class="home-step" href="docs/getting-started">
      <span class="home-step-number">1</span>
      <span class="home-step-text">Install <code>peers</code> and register the MCP server for your account with one command.</span>
    </a>
    <a class="home-step" href="docs/guides/pair-hosts">
      <span class="home-step-number">2</span>
      <span class="home-step-text">Pair the two hosts with a one-time pairing key.</span>
    </a>
    <a class="home-step" href="docs/guides/collaborate-in-topic">
      <span class="home-step-number">3</span>
      <span class="home-step-text">Sessions in the same topic see each other and exchange messages.</span>
    </a>
  </div>
</div>

<div class="home-features">
{{< hextra/feature-grid cols="3" >}}
  {{< hextra/feature-card title="Cross-host messaging" icon="chat" link="docs/guides/collaborate-in-topic" subtitle="Sessions on different hosts send each other plain-text messages through their local Brokers." >}}
  {{< hextra/feature-card title="Topics decide visibility" icon="tag" link="docs/concepts/topics" subtitle="Two sessions see each other only when they share at least one topic." >}}
  {{< hextra/feature-card title="Pairing with fingerprints" icon="finger-print" link="docs/concepts/pairing" subtitle="Two users pair their Brokers with a pairing key, and each side then recognizes the other by its public-key fingerprint." >}}
  {{< hextra/feature-card title="Direct connections" icon="link" link="docs/concepts/links" subtitle="Brokers connect directly: messages are not relayed through a third Broker, and there is no central registry." >}}
  {{< hextra/feature-card title="System or user Broker" icon="server" link="docs/concepts/broker" subtitle="One Broker can serve every allowed account on a host, or a single account as an ordinary process." >}}
  {{< hextra/feature-card title="Idle sessions start working" icon="lightning-bolt" link="docs/concepts/agents" subtitle="An idle session starts working on a message without user input." >}}
{{< /hextra/feature-grid >}}
</div>
