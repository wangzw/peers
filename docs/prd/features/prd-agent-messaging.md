# Agent 消息

## 问题陈述

分布在不同主机上的 Claude Code 会话需要知道有哪些会话可以通信，并把文本消息送进对方的会话。

## 范围

本文档覆盖会话如何登记为 [Agent](../../glossary.md)、Agent 的命名与地址、Agent 列表、点对点消息的发送与投递结果、用户要求给所有 Agent 发消息或广播时消息的发送范围，以及 Broker 下线时对端消息的拒绝。

### 范围外

- 可见性规则本身，由 [topic](prd-topics.md) 规定。
- 系统自身的群发能力、文件传递、送达之后的状态回报、离线补发、发送端的限速与循环检测。

## 参与者

| 参与者 | 权限 |
|---|---|
| Agent | 列出可见的 Agent，向可见的 Agent 发送消息。 |
| 用户 | 经 Claude Code 的权限确认批准或拒绝 Agent 的发送，并经接收方会话的入站控制决定是否接收消息。 |

## 功能需求

### REQ-agent-messaging-001

会话启用本系统的工具且会话名可用的，系统必须在无需用户额外操作的情况下把它登记为本机 Agent。

- **验收**：启动一个启用了工具的会话，该会话调用列出 Agent 的能力时，返回结果里包含它自己的 Agent 地址。
- **状态**：已实现
- **端到端测试**：`test/e2e/registration_test.go` 的 `TestLocalRegistration/REQ-agent-messaging-001 self address in list`

### REQ-agent-messaging-002

未启用本系统工具的会话不得出现在任何 Agent 的列表中，也不得成为发送目标。

- **验收**：同一主机上启动一个未启用工具的会话，其他 Agent 的列表不含它，以它的会话名为目标的发送返回错误。
- **状态**：已实现
- **端到端测试**：`test/e2e/registration_test.go` 的 `TestLocalRegistration/REQ-agent-messaging-002 session without tools`

### REQ-agent-messaging-003

Agent 的名字必须是所在 Claude Code 会话的会话名按 Claude Code 匹配会话名的规则规范化后的结果：去掉控制字符与格式字符，去掉首尾空白，转为小写，空白换成 `-`。

- **验收**：以会话名 `api-worker` 启动会话，其 Agent 地址的名字部分是 `api-worker`；以会话名 ` API Worker ` 启动会话，名字部分是 `api-worker`。
- **状态**：已实现
- **端到端测试**：`test/e2e/registration_test.go` 的 `TestLocalRegistration/REQ-agent-messaging-003 name normalization`

### REQ-agent-messaging-004

Agent 地址必须由下列部分依次组成：Agent 的名字；名字在所在 Broker 上不足以唯一确定该 Agent，或名字不能原样出现在署名中、放不下时的消歧标识；`@`；看到该地址的一方的 Broker 对该 Agent 所在 Broker 使用的别名。

- **验收**：主机 A 以别名 `hostB` 称呼主机 B 的 Broker。B 上只有一个名为 `api-worker` 的 Agent 时，A 上看到的地址是 `api-worker@hostB`；B 上有两个同名的 Agent 时，A 上看到的两个地址都以 `api-worker` 开头、以 `@hostB` 结尾，中间各带一个不同的消歧标识。
- **状态**：已实现
- **端到端测试**：`test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-agent-messaging-004 remote refs`

### REQ-agent-messaging-005

会话改名后，系统必须以新名字作为该 Agent 的名字，旧地址随之失效。

- **验收**：会话改名后，同一主机上的另一个 Agent 随即列出 Agent，结果显示新名字；以旧地址为目标的发送返回错误。
- **状态**：已实现
- **端到端测试**：`test/e2e/session_lifecycle_test.go` 的 `TestLocalSessionChanges/REQ-agent-messaging-005 rename`

### REQ-agent-messaging-006

Agent 的消歧标识必须采用与 Claude Code 在其会话列表中为同一会话显示的标识相同的算法，由会话的 inbox socket 路径算出；候选集合相同时两者取值相同，候选集合不同时本 Broker 的取值是该标识的前缀。它在会话改名和清空对话之后必须保持不变。

- **验收**：在 Claude Code 的默认配置下以会话名 `a<b` 启动一个启用了工具的会话，列表中它的地址带消歧标识，该标识是 Claude Code 会话列表中该会话方括号内标识的前缀；把会话改名为 `c<d`，再清空对话，它的消歧标识不变。
- **状态**：已实现
- **端到端测试**：`test/e2e/registration_test.go` 的 `TestLocalRegistration/REQ-agent-messaging-006 ref algorithm`

### REQ-agent-messaging-007

发送目标的名字在目标 Broker 上对应多个 Agent 且未带消歧标识时，发送必须失败并说明存在歧义。

- **验收**：对两个同名 Agent 只用名字发送，返回的错误指出歧义；带上其中一个的消歧标识后发送成功，且只有该 Agent 收到。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_local_test.go` 的 `TestLocalSend/REQ-agent-messaging-007 ambiguous`

### REQ-agent-messaging-008

Agent 必须能列出所有对自己可见的在线 Agent，包括本机上的，以及已连接的对端 Broker 上的。可见性由 `REQ-topics-005` 规定。

- **验收**：两台已连接的主机上各有一个同在某个 topic 的 Agent，任一方的列表都包含对方。
- **状态**：已实现
- **端到端测试**：`test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-agent-messaging-008 lists remote`

### REQ-agent-messaging-009

列表中的每一项必须给出该 Agent 的地址，以及它与调用方共同所在的 topic。

- **验收**：调用方与某个 Agent 同在 topic `t1` 与 `t2`，列表中该项的地址正确，共同 topic 恰为 `t1` 与 `t2`。
- **状态**：已实现
- **端到端测试**：`test/e2e/registration_test.go` 的 `TestLocalRegistration/REQ-agent-messaging-009 shared topics`

### REQ-agent-messaging-010

列表给出某个 Agent 的状态时，取值必须原样取自 Claude Code 会话注册表中该会话的状态。

- **验收**：一个 Agent 的会话正在执行耗时的工具调用，另一个 Agent 的会话空闲；列表给出这两项的状态时，正在执行工具调用的那一项为忙碌，空闲的那一项为空闲。
- **状态**：已实现
- **端到端测试**：`test/e2e/session_lifecycle_test.go` 的 `TestLocalSessionChanges/REQ-agent-messaging-010 status`

### REQ-agent-messaging-011

列表结果必须包含调用方自己的 Agent 地址。

- **验收**：任一 Agent 调用列表，结果中有一个标明为调用方自身的地址，其名字部分等于该会话名规范化后的结果。
- **状态**：已实现
- **端到端测试**：`test/e2e/registration_test.go` 的 `TestLocalRegistration/REQ-agent-messaging-011 self entry`

### REQ-agent-messaging-012

会话结束后，系统必须在 5 秒内把它的 Agent 从本机和已连接对端的列表中移除。

- **验收**：结束一个 Agent 的会话，5 秒后在本机和对端各调用一次列表，结果都不含该 Agent。
- **状态**：已实现
- **端到端测试**：`test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-agent-messaging-012 removal reaches peer`；`test/e2e/session_lifecycle_test.go` 的 `TestLocalSessionChanges/REQ-agent-messaging-012 local end`

### REQ-agent-messaging-013

Agent 必须能向一个 Agent 地址发送一条纯文本消息。

- **验收**：以某个可见 Agent 的地址为目标发送文本，该 Agent 的会话中出现这段文本。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_local_test.go` 的 `TestLocalSend/REQ-agent-messaging-013 plain text`

### REQ-agent-messaging-014

发送方与目标互不可见时，发送必须失败，且消息不得进入目标会话。

- **验收**：两个没有任何共同 topic 的 Agent 之间发送，发送方得到错误，目标会话中不出现该消息。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_local_test.go` 的 `TestLocalSend/REQ-agent-messaging-014 not visible`

### REQ-agent-messaging-015

发送调用只能在消息已经交给目标会话的 inbox socket 之后返回成功。成功不表示目标会话中的模型已读或已处理该消息。

- **验收**：把目标会话的入站策略设为留待用户批准，向它发送；调用返回成功的那一刻，该消息已经出现在目标会话的待批准列表中。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_local_test.go` 的 `TestLocalSend/REQ-agent-messaging-015 success only after inbox`

### REQ-agent-messaging-016

发送调用返回后，系统不得就这条消息向发送方发出任何后续通知。

- **验收**：向一个把消息留待用户批准的会话发送，发送方得到成功；无论用户随后批准、拒绝还是不处理，发送方会话都不出现来自系统的通知。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_local_test.go` 的 `TestLocalSend/REQ-agent-messaging-016 no follow-up`

### REQ-agent-messaging-017

目标所在的 Broker 不可达时，发送调用必须在发送时限之内返回未送达的错误；发送时限由 Broker 的配置给出。

- **验收**：把双方 Broker 的发送时限都配置为 10 秒，先断开两台主机之间的网络并等待链路断开，再从拨号方和接受方的主机上各发送一条消息，两次调用都在 10 秒之内返回说明对端不可达的错误，网络恢复后目标会话中不出现这两条消息。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_remote_test.go` 的 `TestRemoteSendFaults/REQ-agent-messaging-017 unreachable within budget`

### REQ-agent-messaging-018

目标所在的 Broker 无法把消息写入目标会话时，发送调用必须在发送时限之内返回未送达的错误。

- **验收**：把发送方 Broker 的发送时限配置为 10 秒，目标会话已退出但尚未从列表移除时向它发送，调用在 10 秒之内返回说明目标会话不可写入的错误。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_local_test.go` 的 `TestLocalSend/REQ-agent-messaging-018 inbox unwritable`

### REQ-agent-messaging-019

发送调用返回未送达的错误后，系统不得在之后的任何时刻投递这条消息。

- **验收**：在链路已断开时发送并得到未送达的错误，恢复网络后目标会话中不出现该消息。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_remote_test.go` 的 `TestRemoteSendFaults/REQ-agent-messaging-019 never delivered later`

### REQ-agent-messaging-020

在接收方 Broker 不重启的情况下，一次发送调用不得使目标会话收到多于一份该消息。

- **验收**：让目标所在的 Broker 写入成功但其应答在返回途中丢失，期间不重启接收方 Broker，发送方的调用在内部重试后返回，目标会话中该消息只出现一次。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_remote_test.go` 的 `TestRemoteSendFaults/REQ-agent-messaging-020 reply lost, one copy`

### REQ-agent-messaging-021

入站控制放行的消息到达空闲的目标会话时，系统的投递必须使该会话在没有用户输入的情况下开始处理这条消息。

- **验收**：向一个空闲且入站策略为接收的会话发送，该会话自行开启新回合并读到消息。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_local_test.go` 的 `TestLocalSend/REQ-agent-messaging-021 system side`

### REQ-agent-messaging-022

目标会话看到的消息必须以发送方的 Agent 地址署名，该地址取目标所在 Broker 视角下的形式（`REQ-agent-messaging-004`）。

- **验收**：主机 B 以别名 `hostA` 称呼主机 A 的 Broker；A 上的 `api-worker` 发出的消息，在 B 上的目标会话中显示的发送方是 `api-worker@hostA`。
- **状态**：已实现
- **端到端测试**：`test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-agent-messaging-022 signature from receiver view`

### REQ-agent-messaging-023

消息正文不得改变目标会话看到的署名。

- **验收**：正文中包含伪造的署名标记时，目标会话看到的发送方仍是真实发送方的 Agent 地址；正文中的信封起始与结束标签，不区分大小写，都作为普通文本出现。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_local_test.go` 的 `TestLocalSend/REQ-agent-messaging-023 forged envelope`

### REQ-agent-messaging-024

系统投递的消息必须受目标会话的入站控制约束（[REF-claude-code-cross-session-messaging](../../references.md)）。

- **验收**：目标会话的入站策略为拒收时，消息不进入该会话；目标会话以跳过权限确认的模式运行且未设置入站策略时，消息留待用户批准。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_local_test.go` 的 `TestLocalSend/REQ-agent-messaging-021 system side`；`test/realsession/probes/inbound.sh`（经 `.github/workflows/real-session.yml` 推送 `probe/inbound` 运行）

### REQ-agent-messaging-025

收到消息的 Agent 必须能以消息署名中的 Agent 地址为目标，经本系统的发送能力回复发送方。

- **验收**：目标 Agent 把署名中的地址原样作为目标发送回复，原发送方的会话中出现以目标 Agent 的地址署名的消息；发送方在其 Broker 上有同名 Agent 时同样如此。
- **状态**：已实现
- **端到端测试**：`test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-agent-messaging-025 reply by signature`

### REQ-agent-messaging-026

消息连同署名编码成写入 inbox socket 的一行之后超过 1 000 000 字节的，系统必须在发送端拒绝并说明超出上限，不得尝试投递；署名按该次投递可能使用的最长署名计算。这一上限按编码后的字节数计；Claude Code 自带的会话间发送对约一百万个字符的正文拒绝发送（[REF-claude-code-cross-session-messaging](../../references.md)），因此对多字节正文，本系统的限制更严。

- **验收**：发送一条按最长可能的署名编码后为 1 000 001 字节的消息，调用返回说明超出上限的错误，目标所在的 Broker 没有收到投递请求；按最长可能的署名编码后为 1 000 000 字节的消息发送成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_remote_test.go` 的 `TestRemoteSendFaults/REQ-agent-messaging-026 size limit`

### REQ-agent-messaging-027

系统无法确定消息是否已经写入目标会话时，发送调用必须返回与未送达不同的结果，并说明消息可能已经送达。

- **验收**：让目标所在的 Broker 写入成功，并使它的应答和此后全部重试的应答都无法到达发送方；调用返回的结果标明送达情况未知，而不是未送达。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_remote_test.go` 的 `TestRemoteSendFaults/REQ-agent-messaging-027 delivery unknown`

### REQ-agent-messaging-028

同一主机上互相可见的两个 Agent 之间，发送行为必须与跨主机时一致。

- **验收**：同一主机上同在一个 topic 的两个 Agent 经本系统互发消息，署名、成功与错误的含义都与跨主机时相同。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_local_test.go` 的 `TestLocalSend/REQ-agent-messaging-028 same as remote`

### REQ-agent-messaging-029

Agent 的名字经接收方会话对署名的清洗后与原名不同时，该 Agent 的地址与署名都必须带消歧标识。

- **验收**：以会话名 `a<b` 启动的 Agent 向另一台主机上的 Agent 发送消息，目标会话看到的署名带消歧标识，目标 Agent 按署名原样回复，回复送达 `a<b` 的会话；同一主机上另有会话名为 `ab` 的 Agent 时，回复不会到达它。
- **状态**：已实现
- **端到端测试**：`test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-agent-messaging-029 cleaned name gets ref`

### REQ-agent-messaging-030

会话名不可用的会话不得登记为 Agent，系统不得以其他名字代替，并必须向该会话说明原因。会话名不可用是指它规范化后为空、经署名清洗后为空，或含有 `@` 或 `*`。会话名规范化后超过 128 字节的部分截去。已登记的会话改名后名字变得不可用的，其登记随之撤销。

- **验收**：以会话名 `a@b` 启动一个启用了工具的会话：其他 Agent 的列表中不出现对应它的任何一项；它调用列出 Agent 时得到错误，说明会话名含有 `@` 因而不能登记。把会话改名为 `ab` 后，其他 Agent 的列表中出现名字为 `ab` 的一项。以超过 128 字节的会话名启动一个启用了工具的会话：其他 Agent 的列表中出现对应它的一项，名字不超过 128 字节，截断落在 UTF-8 字符边界上；与它配对的对端 Broker 上的 Agent 列表中出现同一个截断后的名字。
- **状态**：已实现
- **端到端测试**：`test/e2e/registration_test.go` 的 `TestLocalRegistration/REQ-agent-messaging-030 unusable names`，以及 `test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-agent-messaging-030 long name is truncated for both sides`

### REQ-agent-messaging-031

以一个 Agent 的曾用名为目标的发送失败时，错误必须给出该 Agent 现在的名字。

- **验收**：把 Agent `api-worker` 的会话改名为 `api-v2`，随即以 `api-worker@hostA` 为目标发送，返回的错误中给出 `api-v2`，消息不进入该会话。
- **状态**：已实现
- **端到端测试**：`test/e2e/session_lifecycle_test.go` 的 `TestLocalSessionChanges/REQ-agent-messaging-031 old name error`

### REQ-agent-messaging-032

列出 Agent 时，改名后 10 分钟内的 Agent 必须注明其曾用名。

- **验收**：把一个 Agent 的会话从 `api-worker` 改名为 `api-v2`，另一个与它可见的 Agent 随即列出 Agent，该项的名字是 `api-v2`，并注明曾用名 `api-worker`。
- **状态**：已实现
- **端到端测试**：`test/e2e/session_lifecycle_test.go` 的 `TestLocalSessionChanges/REQ-agent-messaging-032 previous name`

### REQ-agent-messaging-033

会话清空对话后，其 Agent 的地址与 topic 成员关系必须保持不变，此前已到达而尚未处理的消息不得因清空而丢失。

- **验收**：一个加入了 topic `t1` 的 Agent 在会话中清空对话，其他 Agent 的列表中它的地址与共同 topic 不变，以原地址发送的消息进入该会话；会话忙碌时有消息到达，随后清空对话，清空要等这条已到达的消息处理完才生效，这条消息不丢失。
- **状态**：已实现
- **端到端测试**：`test/realsession/probes/ref-clear.sh`（经 `.github/workflows/real-session.yml` 推送 `probe/ref-clear` 运行）

### REQ-agent-messaging-034

用户在会话中停用本系统的 MCP 服务后，该会话的 Agent 必须被注销；重新启用后必须以相同的 Agent 身份和名字重新登记，地址的消歧标识按当时的 Agent 集合计算，且只重新加入默认 topic，停用前手动加入的 topic 不恢复。

- **验收**：在会话中停用本系统的 MCP 服务，其他 Agent 随即列出 Agent，结果不含它；重新启用后，列表中重新出现它，名字与停用前相同，停用前手动加入的 topic 不再出现。
- **状态**：已实现
- **端到端测试**：`test/e2e/session_lifecycle_test.go` 的 `TestLocalSessionChanges/REQ-agent-messaging-034 disable and enable`

### REQ-agent-messaging-035

子代理经本系统工具发起的调用必须以所属主会话的 Agent 身份进行，子代理不得成为独立的 Agent。

- **验收**：会话中的子代理调用发送，目标会话看到的署名是主会话的 Agent 地址；子代理运行期间，其他 Agent 的列表中对应该会话的只有一项。子代理的定义中自行声明本系统的 MCP 服务时同样如此。
- **状态**：已实现
- **端到端测试**：`test/e2e/registration_test.go` 的 `TestLocalRegistration/REQ-agent-messaging-035 second MCP process`

### REQ-agent-messaging-036

以无界面方式运行，或在 IDE 等宿主中运行的会话，启用本系统的工具后，必须与交互式会话同样成为 Agent。

- **验收**：以无界面方式或作为 IDE 中的主会话运行一个启用了工具的会话，它运行期间，同在一个 topic 的另一个 Agent 的列表包含它，它向该 Agent 发送的消息进入对方会话。
- **状态**：已实现
- **端到端测试**：`test/realsession/probes/headless.sh`（经 `.github/workflows/real-session.yml` 推送 `probe/headless` 运行）

### REQ-agent-messaging-037

系统生成的署名必须能被接收方会话识别为发送方。Agent 地址超出可署名的长度时，系统必须截短名字部分并附上消歧标识，使署名仍能唯一指向发送方。

- **验收**：以 70 个字符的会话名启动的 Agent 向另一台主机上的 Agent 发送消息，目标会话显示出发送方，署名带消歧标识，名字部分是会话名的开头部分；目标 Agent 按署名原样回复，回复送达原发送方。
- **状态**：已实现
- **端到端测试**：`test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-agent-messaging-037 truncated signature`

### REQ-agent-messaging-038

本机 Broker 不可用或登记未完成时，工具调用必须返回说明原因的错误，会话不得因此需要用户重新连接 MCP 服务。

- **验收**：让另一个进程占用默认监听地址，在终端启动该账户的 Broker，启动失败并说明监听地址不可用；再启动一个启用了工具的会话：Claude Code 显示本系统的 MCP 服务已连接，工具调用返回说明没有可用 Broker 及如何启动的错误。结束占用该地址的进程并启动 Broker 后，不在 Claude Code 中重新连接 MCP 服务，下一次列出 Agent 成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/broker_process_test.go` 的 `TestBrokerProcessInContainer/REQ-agent-messaging-038 recover without reconnect`

### REQ-agent-messaging-039

下线的 Broker 必须以 `broker_offline` 拒绝对端发来的每一条消息，不写入任何会话。

- **验收**：B 下线后，A 上与 B 上 Agent 同在一个 topic 的 Agent 向它发送，调用返回以 `Not sent` 开头、错误码为 `broker_offline` 的错误，说明由 B 报告；B 上的目标会话没有收到消息。B 上线后同一发送成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/offline_test.go` 的 `TestBrokerOffline/REQ-agent-messaging-039 peer delivery is refused`

### REQ-agent-messaging-040

Broker 下线期间，它与对端之间的链路、链路的重新建立、topic 成员关系的交换与名录对账必须照常进行，新的配对也必须照常完成。

- **验收**：B 下线后，A 列出 Broker 时 B 仍为已连接；断开再恢复两台主机之间的网络，链路重新建立；B 上新加入共同 topic 的 Agent 出现在 A 上 Agent 的列表中；B 公开时，C 经 A 仍能收到 B 的名片。另一台主机凭有效配对密钥向 B 发起配对成功，链路建立；向 B 上 Agent 的发送仍按 `REQ-agent-messaging-039` 被拒绝。
- **状态**：已实现
- **端到端测试**：`test/e2e/offline_test.go` 的 `TestBrokerOffline/REQ-agent-messaging-040 links and topics continue`、`TestBrokerOffline/pairing works while offline` 与 `TestBrokerOfflineDirectory`（三节点：公开的对端下线后，第三个 Broker 仍经共同对端收到它的名片与新公开的 topic）

### REQ-agent-messaging-041

Broker 下线不得影响本机 Agent 之间的发送，也不得影响本机 Agent 向对端上 Agent 的发送。

- **验收**：B 下线后，B 上的两个 Agent 互发消息成功；B 上的 Agent 向 A 上的 Agent 发送成功；A 上的 Agent 回复 B 上的 Agent，被 `REQ-agent-messaging-039` 拒绝。
- **状态**：已实现
- **端到端测试**：`test/e2e/offline_test.go` 的 `TestBrokerOffline/REQ-agent-messaging-041 local and outgoing sends continue`

### REQ-agent-messaging-042

用户要求给所有 Agent 发消息或广播、且没有提到 peers 时，系统必须使这次群发只到达本机会话，不经本系统发给任何 Agent，并在群发之后给用户一句提示：还可以用 peers 通知其他主机上的 Agent。

- **验收**：在一个有内置会话间发送工具的会话里要求给所有 Agent 发消息，本机会话收到这条消息，与发送方同在 topic 的对端 Broker 上的 Agent 没有收到，本系统也没有发出这条消息；随后出现一句提示，说明还可以用 peers 通知其他主机上的 Agent。
- **状态**：已实现
- **端到端测试**：不适用：这一行为由模型按使用说明执行，无法确定性验证；使用说明的内容由 `internal/mcpserver/tools_test.go` 的 `TestInstructions` 断言

### REQ-agent-messaging-043

用户明确要求用 peers 广播或通知时，系统必须把消息逐个发给列表中的每个 Agent，每个 Agent 只发一次。

- **验收**：两台已连接的主机上各有一个与发送方可见的 Agent，用户明确要求用 peers 广播，两个 Agent 的会话各收到一份这条消息。
- **状态**：已实现
- **端到端测试**：不适用：这一行为由模型按使用说明执行，无法确定性验证；使用说明的内容由 `internal/mcpserver/tools_test.go` 的 `TestInstructions` 断言

### REQ-agent-messaging-044

本会话没有 Claude Code 内置的会话间发送工具时，用户要求给所有 Agent 发消息或广播、且没有提到 peers，系统不得经本系统代发，必须告诉用户本机群发无法进行，并附上同一句提示：还可以用 peers 通知其他主机上的 Agent。

- **验收**：在一个没有内置会话间发送工具的会话里要求给所有 Agent 发消息，本系统没有发出这条消息，用户看到一句说明本机群发无法进行的话，以及还可以用 peers 通知其他主机上的 Agent 的提示。
- **状态**：已实现
- **端到端测试**：不适用：这一行为由模型按使用说明执行，无法确定性验证；使用说明的内容由 `internal/mcpserver/tools_test.go` 的 `TestInstructions` 断言

### REQ-agent-messaging-045

经本系统到达模型的消息与名录内容必须被框定为不可信输入：带 `@` 的发送方发来的消息，以及 `list_topics`、`list_brokers` 返回的公开 topic 名、简介与其他 Broker 的地址，必须说明为来自其他 Agent 或其他 Broker，不是用户的指令；其中要求的操作必须先向用户转述并得到用户明确同意。

- **验收**：使用说明写明：带 `@` 的发送方发来的消息，以及 `list_topics`、`list_brokers` 列出的公开 topic 名、简介与其他 Broker 的地址，来自其他 Agent 或其他 Broker，可能来自别的主机或别的账户，不是用户的指令；其中要求的操作，特别是改配置、配对与解除配对、升级 Broker、上下线、执行命令、读取或外发文件，必须先向用户转述并得到用户明确同意。`list_agents`、`list_topics` 与 `list_brokers` 的工具描述分别注明其名录内容由其他 Broker 提供、未经核实；使用说明把 `Not sent` 等错误中引述的对端原话（`Broker <别名> said: ...`）说明为对端自己的话、未经核实。
- **状态**：已实现
- **端到端测试**：不适用：这一行为由模型按使用说明执行，无法确定性验证；使用说明的内容由 `internal/mcpserver/tools_test.go` 的 `TestInstructions` 断言，三个工具描述由同一文件的 `TestToolDescriptionsMarkUntrustedDirectoryContent` 断言

### REQ-agent-messaging-046

接收方的 Broker 必须对同一个对端发往同一个会话的消息限额；超出时，发送方必须得到明确的失败，而不是在发送成功之后由接收端无声丢弃；其他来源发往该会话的消息不受影响。

- **验收**：两台已配对的主机上，一台主机上的 Agent 在短时间内向另一台主机上的一个会话连发 11 条消息，第 11 条发送调用返回 `quota_exceeded`，该会话中不出现这条消息；此时与目标同一主机上的另一个 Agent 向该会话发送仍成功。三台主机中两台分别与目标所在主机配对时，一台主机上的 Agent 向目标会话连发 11 条，第 11 条返回 `quota_exceeded`；随后第三台主机上的 Agent 向同一会话发送成功，会话收到这条消息。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_remote_test.go` 的 `TestRemoteSendFaults/REQ-agent-messaging-046 inbound quota per peer` 与 `TestRemoteSendInboundQuota/REQ-agent-messaging-046 another peer still reaches the session`

## 数据需求

消息正文只在投递过程中存在于内存，系统不把它写入磁盘。Agent 的登记信息只在所在会话存续期间保留。

## 外部接口需求

- Claude Code：系统经会话的 inbox socket 投递消息，并从 Claude Code 取得会话名、会话的忙闲状态和 inbox socket 的位置。
- 会话中的模型：本文档的能力以工具的形式提供（[REF-mcp](../../references.md)）。

## 约束

- 系统运行在 macOS 与 Linux 上。
- Claude Code 会话间消息要求的最低版本是 v2.1.224（[REF-claude-code-cross-session-messaging](../../references.md)），更早版本的会话不能成为 Agent。

## 假设与依赖

- inbox socket 的消息格式与会话注册表都没有公开的兼容承诺，Claude Code 的版本更新可能使登记或投递失效。
- 下列会话不能成为 Agent：没有绑定 inbox socket 的会话；Claude Code 没有为其保存会话注册表记录的会话，包括从另一个会话的命令行里启动的交互式会话；会话名不可用的会话（`REQ-agent-messaging-030`）。
- 系统以账户为隔离的边界：同一账户下的任何进程都能把该账户的任一会话登记为 Agent，并以该 Agent 的身份调用。
- 以无界面方式运行且跳过权限确认的会话收不到本系统的消息：接收方的 Claude Code 会暂扣未声明权限类别的发送方发来的消息，等用户批准；无界面会话没有批准界面，暂扣的消息因发送方没有声明权限类别而被丢弃，而发送方的发送已经返回成功。
- 下线的 Broker 上的 Agent 发出的消息照常送达，对方的回复被拒绝；需要双向通信时先上线。
- 对端只能通过 `broker_offline` 得知本 Broker 下线；链路状态、名录与 `list_brokers` 的结果不反映下线。
- 发送限速、重复消息的丢弃和待处理消息的排队上限由接收方的 Claude Code 负责，系统不改变这些行为。规则如下，数值可由 Claude Code 调整：接收方按发送方限速与去重，写入的消息没有回复地址时以写入进程识别发送方；每个发送方的额度至多 30 条，每秒恢复 0.5 条；同一发送方在 30 秒内发来与上一条内容完全相同的消息时，后一条被丢弃；未处理的会话间消息至多排队 50 条；回合进行中入队的消息在回合结束时以 `remove` 结束，不成为用户记录，因此排队上限只能按队列深度观察。由此有下列后果：
  - 经同一个 Broker 写入同一个会话的全部消息，不论来自哪个 Agent，在接收方那里属于同一个发送方，共用一份额度。
  - 同一个 Agent 在 30 秒内向同一个会话连续发送两条内容完全相同的消息时，后一条被接收方丢弃，而发送已经返回成功。署名不同则内容不同，不同 Agent 之间不会因此互相导致消息被丢弃。
  - Claude Code 自带的消息循环检测对本系统的消息不起作用，能约束 Agent 之间往复的消息的只有上述限速。
