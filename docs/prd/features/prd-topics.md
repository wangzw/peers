# topic

## 问题陈述

配对后的两台主机上可能各有多个会话在做互不相关的工作。Agent 需要一个范围来限定谁能看见自己、谁能给自己发消息，并且在同一个项目里工作的 Agent 应当不经额外操作就处在同一个范围内。

## 范围

本文档覆盖 [topic](../../glossary.md) 的加入与退出、可见性规则、topic 的公开与不公开、默认 topic 的确定。

### 范围外

- topic 的管理员、邀请与审批。知道名字即可加入。
- 向整个 topic 群发消息。
- 按对端区分的可见性。一个 Agent 在某个 topic 里，就对所有已连接且在该 topic 有成员的对端可见。

## 参与者

| 参与者 | 权限 |
|---|---|
| Agent | 加入和退出 topic，把 topic 声明为公开，列出自己所在的 topic 和已知的公开 topic。 |
| 用户 | 在项目配置文件里声明该项目的 topic，在 Broker 配置或项目配置文件里关闭自动加入。 |

## 功能需求

### REQ-topics-001

Agent 必须能按名字加入一个 topic。

- **验收**：Agent 以名字 `release` 加入后，列出自己所在的 topic，结果包含 `release`。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-001 join`

### REQ-topics-002

加入 topic 不得要求任何其他 Agent 或用户的批准，加入一个此前无人加入的 topic 时同样必须成功。

- **验收**：以一个此前无人使用的名字加入，调用成功；另一个 Agent 以同名加入，调用成功，且两者互相可见。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-002 join new name without approval`

### REQ-topics-003

Agent 必须能退出一个已加入的 topic。

- **验收**：退出后，列出自己所在的 topic，结果不含该 topic；与它只有该 topic 这一个共同 topic 的 Agent，其列表中不再出现它。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-003 leave`

### REQ-topics-004

一个 Agent 必须能同时属于多个 topic。

- **验收**：Agent 先后加入 `t1` 和 `t2`，列出自己所在的 topic，结果同时包含 `t1` 与 `t2`。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-004 many topics`

### REQ-topics-005

两个 Agent 当且仅当至少同在一个 topic，并且位于同一个 Broker 或位于两个已连接的 Broker 时，互相可见。

- **验收**：同在一个 topic 的两个 Agent 互相出现在对方的列表中；其中一个退出该 topic 且两者没有其他共同 topic 后，互相从对方的列表中消失；两者所在的 Broker 之间的链路断开后同样消失。
- **状态**：已实现
- **端到端测试**：`test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-topics-005 iff shared and connected`

### REQ-topics-006

不属于任何 topic 的 Agent 不得出现在任何其他 Agent 的列表中。

- **验收**：一个已登记但未加入任何 topic 的 Agent，不出现在本机和对端任何 Agent 的列表中。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-006 no topic means invisible`；`test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-topics-006 no topic invisible`

### REQ-topics-007

不同 Broker 上名字相同的 topic 必须视为同一个 topic。

- **验收**：两台已连接的主机上各有一个 Agent 以相同的名字加入 topic，两者互相可见。
- **状态**：已实现
- **端到端测试**：`test/e2e/visibility_remote_test.go` 的 `TestRemoteVisibility/REQ-topics-007 same name across brokers`

### REQ-topics-008

topic 在未声明为公开时必须是不公开的。

- **验收**：Agent 加入一个新 topic 且未声明公开，其他 Broker 上列出公开 topic 的结果不含它。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestDiscoveryPrivateByDefault`

### REQ-topics-009

系统不得向没有该 topic 成员的 Broker 透露不公开 topic 的名字。

- **验收**：主机 A 上有 Agent 加入不公开的 topic `secret-x`，已连接的主机 B 上没有 Agent 加入它；B 上任何工具的返回结果和 B 的日志中都不出现 `secret-x`。
- **状态**：已实现
- **端到端测试**：`test/e2e/security_test.go` 的 `TestPrivateTopicNamesStayHome/REQ-topics-009 private topic name`

### REQ-topics-010

所在 Broker 为公开时，Agent 必须能在加入 topic 时把它声明为公开。声明可以附一段简介，也可以不附。

- **验收**：公开 Broker 上的 Agent 以公开方式加入 topic 并给出简介后，已连接的其他 Broker 上列出公开 topic 的结果包含该名字和简介；不给简介时，结果包含该名字，简介为空。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestPublicTopicVisibleAcrossNetwork`；`test/e2e/journey_public_broker_test.go` 的 `TestJourneyConnectPublicBroker/main`

### REQ-topics-011

公开 topic 的名字和简介必须对连通网络内所有 Broker 上的 Agent 可见，不限于声明方的对端。

- **验收**：A 与 B 已连接，B 与 C 已连接，A 与 C 之间没有链路；A 上声明的公开 topic 出现在 C 上列出公开 topic 的结果中。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestPublicTopicVisibleAcrossNetwork`；`test/e2e/journey_public_broker_test.go` 的 `TestJourneyConnectPublicBroker/main`

### REQ-topics-012

公开 topic 的声明必须遵守与名片相同的有效期和续签规则（`REQ-broker-discovery-007`、`REQ-broker-discovery-008`）。

- **验收**：声明方离线且不再续签，超过有效期后，其他 Broker 上列出公开 topic 的结果不含该 topic。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_expiry_test.go` 的 `TestPublicTopicExpiresWithDeclarer`

### REQ-topics-013

Agent 必须能列出自己已加入的 topic 和本 Broker 已知的公开 topic。

- **验收**：结果区分“已加入”和“公开但未加入”，分别与实际情况一致。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestPublicTopicVisibleAcrossNetwork`；`test/e2e/journey_public_broker_test.go` 的 `TestJourneyConnectPublicBroker/main`

### REQ-topics-014

一个 topic 的成员名单不得发送给任何 Broker，但已连接且在该 topic 也有成员的对端除外。

- **验收**：主机 A 上名为 `api-worker` 的 Agent 在 topic `t1` 中，已连接的主机 B 上没有 Agent 在 `t1` 中；B 上任何工具的返回结果和 B 的日志中都不出现 `api-worker`。B 上有 Agent 加入 `t1` 后，A 上的该 Agent 出现在它的列表中。
- **状态**：已实现
- **端到端测试**：`test/e2e/security_test.go` 的 `TestPrivateTopicNamesStayHome/REQ-topics-014 member list`；`test/e2e/security_test.go` 的 `TestEchoedTopicDigestsRevealNothing/REQ-topics-014 echoed digests`

### REQ-topics-015

自动加入未关闭时，会话所在工作区的项目配置文件声明了 topic 列表的，Agent 登记后必须自动加入列表中的每个 topic。

- **验收**：项目配置文件声明 `t1` 与 `t2`，在该项目中启动的 Agent 未做任何加入操作，列出自己所在的 topic，结果恰为 `t1` 与 `t2`。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-015 project topics`

### REQ-topics-016

自动加入未关闭时，项目配置文件没有声明 topic 列表，且会话的工作区是带有 `origin` 远端的 git 仓库的，Agent 登记后必须自动加入以该远端地址的规范化结果命名的 topic。

- **验收**：在 `origin` 为 `https://github.com/org/repo` 的仓库中启动的 Agent，未做任何加入操作，所在的 topic 恰为 `github.com/org/repo`。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-016 origin topic`

### REQ-topics-017

同一个仓库的不同地址写法必须规范化为同一个 topic 名，形式为小写的主机名后接仓库路径，不含协议、用户信息、端口和 `.git` 后缀。

- **验收**：`git@github.com:org/repo.git`、`ssh://git@GitHub.com:22/org/repo.git` 与 `https://github.com/org/repo` 的规范化结果都是 `github.com/org/repo`。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-017 spellings`

### REQ-topics-018

项目配置文件没有声明 topic 列表，且工作区不是带有 `origin` 远端的 git 仓库时，系统不得让 Agent 自动加入任何 topic。

- **验收**：在一个非 git 目录中启动的 Agent，所在的 topic 为空，且不出现在任何其他 Agent 的列表中。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-018 non-git`

### REQ-topics-019

自动加入的 topic 必须是不公开的。

- **验收**：Agent 自动加入默认 topic 后，其他 Broker 上列出公开 topic 的结果不含它。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestDiscoveryPrivateByDefault`

### REQ-topics-020

Broker 配置中关闭自动加入后，该 Broker 上新登记的 Agent 不得自动加入任何 topic。

- **验收**：关闭后在一个带 `origin` 的仓库中启动 Agent，其所在的 topic 为空。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-020 broker auto_join off`

### REQ-topics-021

项目配置文件中关闭自动加入后，在该项目中登记的 Agent 不得自动加入任何 topic。

- **验收**：项目配置文件关闭自动加入，Broker 配置保持开启；在该项目中启动的 Agent 所在的 topic 为空，在其他项目中启动的 Agent 仍自动加入其默认 topic。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-021 project auto_join off`

### REQ-topics-022

Agent 的 topic 成员关系必须在所在会话结束时全部消失。Broker 重启后，各 Agent 的 topic 成员关系由 MCP 服务进程重新登记后恢复。

- **验收**：Agent 手动加入 `t1` 后结束会话；在同一工作区重新启动会话，新 Agent 所在的 topic 只有自动加入的那些，不含 `t1`。重启 Broker 后，重启前已加入 topic 的 Agent 在其 MCP 服务进程重新登记后恢复原来的成员关系。
- **状态**：已实现
- **端到端测试**：`test/e2e/session_lifecycle_test.go` 的 `TestLocalSessionChanges/REQ-topics-022 membership ends with session and returns after broker restart`

### REQ-topics-023

所在 Broker 不公开时，把 topic 声明为公开的调用必须失败并说明需要先把 Broker 设为公开，Agent 不得因这次调用加入该 topic。

- **验收**：不公开的 Broker 上的 Agent 以公开方式加入 topic，调用返回错误；该 Agent 所在的 topic 不变，其他 Broker 上列出公开 topic 的结果不含该 topic。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestPublicTopicRejectedOnPrivateBroker`

### REQ-topics-024

Broker 由公开改为不公开后，它此前的公开 topic 声明必须从连通网络内所有 Broker 上列出公开 topic 的结果中消失。

- **验收**：A 与 B 已连接，B 与 C 已连接，A 与 C 之间没有链路；A 上的 Agent 以公开方式加入 `infra-help`，C 上列出公开 topic 的结果包含它；A 改为不公开后，在该 Agent 仍在 `infra-help` 中的情况下，C 上列出公开 topic 的结果不再包含 `infra-help`。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestPublicTopicGoneWhenBrokerPrivate`

### REQ-topics-025

一个 Broker 上已没有以公开方式加入某个 topic 的 Agent 时，该 Broker 对这个 topic 的公开声明必须从连通网络内其他 Broker 列出公开 topic 的结果中消失。

- **验收**：A 与 B 已连接，A 上只有一个 Agent 以公开方式加入 `infra-help`，网络中没有其他 Broker 声明它；该 Agent 退出 `infra-help` 后，B 上列出公开 topic 的结果不再包含它；重新以公开方式加入后再次出现，结束该 Agent 的会话后再次消失。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestPublicTopicFollowsLastMember`

### REQ-topics-026

不带名字加入 topic 时，Agent 必须加入本会话的默认 topic。

- **验收**：在 `origin` 为 `https://github.com/org/repo` 的仓库中启动的 Agent 先按名字退出 `github.com/org/repo`，再不带名字加入，列出自己所在的 topic，结果含 `github.com/org/repo`；项目配置文件声明 `t1` 与 `t2` 时，不带名字加入后结果含这两个。会话没有默认 topic 时，调用返回错误，说明本会话没有默认 topic。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-026 join default without name` 与 `TestLocalTopics/REQ-topics-026 join default with broker auto_join off`

### REQ-topics-027

不带名字退出 topic 时，Agent 必须退出本会话的默认 topic，此后在本会话内不得自动重新加入，新的会话照常自动加入。

- **验收**：在 `origin` 为 `https://github.com/org/repo` 的仓库中启动的 Agent 不带名字退出，列出自己所在的 topic，结果不含 `github.com/org/repo`；重启 Broker，MCP 服务进程重新登记后仍不含；在同一工作区新开一个会话，新 Agent 所在的 topic 含 `github.com/org/repo`。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_local_test.go` 的 `TestLocalTopics/REQ-topics-027 leave default without name`

### REQ-topics-028

本机有成员的 topic 数、每个 topic 的本机成员数，以及同一账户在短时间内变更 topic 的次数，都必须有上限；达到上限时系统必须拒绝超出的调用，说明稍后再试，并且不改变成员关系。系统级 Broker 还须把本机 topic 的预算按账户细分，使一个账户不能占满全部 topic。

- **验收**：本机已有 512 个有成员的 topic 时，加入第 513 个 topic 失败并说明数量已达上限；一个 topic 已有 4096 个本机成员时，再加入一个 Agent 失败；系统级 Broker 下一个账户已有 128 个有成员的 topic 时，该账户再加入一个新的 topic 失败，已在该账户某个 topic 中的 Agent 重复加入仍成功；同一账户连续变更 topic 超过 64 次后，调用失败并说明稍后再试，成员关系不变，等待一秒后再变更 4 次成功；另一个账户的变更不受影响。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_limits_test.go` 的 `TestTopicChangeLimits` 覆盖同一账户的 topic 变更限速、拒绝后成员关系不变与等待后恢复；本机 topic 数、每个 topic 的本机成员数以及系统级 Broker 的每账户上限由单元测试 `internal/topic/locallimit_test.go` 的 `TestJoinTopicCountLimit`、`TestJoinMemberLimit`、`internal/topic/linkstability_test.go` 的 `TestLinkStaysUpSharingTopicsToLimit`、`internal/broker/topics_rpc_test.go` 的 `TestJoinTopicQuotaExceeded`、`TestJoinTopicPerAccountLimit`、`TestUserBrokerHasNoPerAccountLimit` 与 `internal/broker/topicratelimit_test.go` 的 `TestJoinTopicRateLimited`、`TestLeaveTopicRateLimited` 覆盖

### REQ-topics-029

任何对端发来的成员名单都不得使本机为该对端保存的条目无限增长；名字或曾用名超过长度上限的成员条目必须不予保存。

- **验收**：已配对的对端在同一个共同 topic 下连发多帧成员名单，累计超过 4096 个新成员；本机为该对端保存的成员不超过 4096 个，名字为 129 字节的条目不出现在任何结果中；五个共同 topic 各有 4096 条成员时，第五个 topic 的名单被整帧丢弃，该对端保存的条目合计不超过 16384；对端随后发来一个可识别的成员，该成员可见，链路仍为已连接，Broker 日志记录名单帧与成员条目被丢弃。
- **状态**：已实现
- **端到端测试**：`test/e2e/topics_limits_test.go` 的 `TestTopicRosterBounds` 覆盖对端批量名单的上限、129 字节名字的丢弃、拒绝后链路仍为已连接；累计条数的精确边界由单元测试 `internal/topic/rosterlimit_test.go` 的 `TestRosterDeltaCumulativeLimit`、`TestRosterMemberNameLength`、`TestPeerRosterCumulativeLimit` 覆盖

## 数据需求

topic 成员关系只在内存中保留，不写入磁盘。项目的 topic 声明保存在项目配置文件里，随仓库管理。

## 假设与依赖

- 不公开 topic 的保密程度以名字不易猜中为限。一个对端可以让它自己的 Agent 以猜测的名字加入，从而得知本 Broker 上是否有成员在这个 topic 中。
- 默认 topic 的名字由仓库地址决定，能访问同一仓库的人都能得出它。
