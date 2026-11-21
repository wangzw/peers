# Broker 发现

## 问题陈述

用户想连接一台 [Broker](../../glossary.md) 时，需要知道网络里有哪些 Broker、各自能否连上。同时，不愿被人知道的 Broker 不应因为与别人配对就被传播出去。

## 范围

本文档覆盖 Broker 的公开与不公开、名片在连通网络内的传播与失效、已知 Broker 的列出与状态。

### 范围外

- Agent 与成员名单的传播，由 [topic](prd-topics.md) 规定。
- 对 Broker 是否可达的主动探测。
- 依据发现结果自动发起配对。

## 参与者

| 参与者 | 权限 |
|---|---|
| 用户 | 决定本机 Broker 是否公开。 |
| Agent | 列出本机 Broker 已知的全部 Broker 及其状态。 |
| 对端 | 向本 Broker 传递它持有的名片。 |

## 功能需求

### REQ-broker-discovery-001

Broker 未在配置中明确设为公开时必须是不公开的。

- **验收**：以默认配置启动的 Broker A 与 B 配对，C 与 B 相连而与 A 之间没有链路；C 列出已知 Broker 的结果不含 A。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestDiscoveryPrivateByDefault`

### REQ-broker-discovery-002

Broker 设为公开后必须发布名片，名片的内容是它的指纹、别名和对外地址列表，列表可以为空。

- **验收**：设为公开后，与它没有链路的 Broker 列出已知 Broker 的结果包含它，指纹、别名和对外地址列表与其实际值一致；把监听地址配置为空的公开 Broker，结果中它的对外地址列表为空。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestDiscoveryCardReachesWholeNetwork`；`test/e2e/discovery_test.go` 的 `TestDiscoveryPublicWithoutListen`

### REQ-broker-discovery-003

公开 Broker 的名片必须到达连通网络内的每一个 Broker，不限于它的对端。

- **验收**：A 与 B 已连接，B 与 C 已连接，C 与 D 已连接，其余各对之间没有链路；A 设为公开后，D 列出已知 Broker 的结果包含 A。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestDiscoveryCardReachesWholeNetwork`；`test/e2e/journey_public_broker_test.go` 的 `TestJourneyConnectPublicBroker/main`

### REQ-broker-discovery-004

Broker 不得把另一个不公开 Broker 的任何信息传给该 Broker 的对端以外的 Broker。

- **验收**：不公开的 A 与 B 已连接，B 与 C 已连接，A 与 C 之间没有链路；C 上任何工具的返回结果和 C 的日志中都不出现 A 的指纹、别名或对外地址。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestDiscoveryPrivateByDefault`

### REQ-broker-discovery-005

不公开的 Broker 必须同样能获知连通网络内其他 Broker 的名片。

- **验收**：不公开的 A 与 B 已连接，B 持有公开 Broker C 的名片；A 列出已知 Broker 的结果包含 C。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestDiscoveryPrivateBrokerLearnsCards`

### REQ-broker-discovery-006

Broker 必须拒绝内容被传递者改动过的名片，以及并非由名片所述 Broker 发布的名片。

- **验收**：一个对端传来对外地址列表被改动的名片，以及一张冒用他人指纹的名片，接收方列出已知 Broker 的结果不含这两张名片。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_hostile_test.go` 的 `TestDirectoryRejectsTamperedCards`

### REQ-broker-discovery-007

名片的有效期必须为 7 天，自发布或最近一次续签起算。

- **验收**：一张名片在发布后第 6 天仍出现在其他 Broker 的结果中；发布者此后一直离线，第 8 天该名片不再出现。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_expiry_test.go` 的 `TestCardExpiresAfterSevenDays`

### REQ-broker-discovery-008

在线的公开 Broker 必须每天续签一次自己的名片。

- **验收**：公开 Broker 持续在线 10 天，期间它的名片始终出现在其他 Broker 的结果中。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_expiry_test.go` 的 `TestCardRenewedDaily`

### REQ-broker-discovery-009

Broker 由公开改为不公开后，它的名片必须在 60 秒内从与它持续连通的各 Broker 的已知 Broker 结果中消失，且不得因其他 Broker 仍持有旧名片而重新出现。当时没有与它连通的 Broker 在重新连通并对账之后才收到撤回，不在此限。

- **验收**：A 公开后名片到达 C；A 改为不公开，60 秒后 C 的结果不再含 A；此后让一台持有 A 旧名片的 Broker 重新接入网络，C 的结果仍不含 A。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestDiscoveryRevokeWithin60s`

### REQ-broker-discovery-010

Broker 不得接受对端以外的来源传来的名片。

- **验收**：未配对的 Broker 无法使任何名片出现在本 Broker 列出已知 Broker 的结果中。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_hostile_test.go` 的 `TestDirectoryRejectsUnpairedSource`

### REQ-broker-discovery-011

名录保存的记录数量必须有可配置的上限。达到上限后，尚无名片的 Broker 再传来名片时必须拒绝，已存名片的续签不受影响。

- **验收**：把上限配置为 3，让对端传来 5 张互不相同的有效名片，本 Broker 列出已知 Broker 的结果含其中 3 张；这 3 张名片的续签到达后被接受，它们在超过原有效期后仍在结果中。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_expiry_test.go` 的 `TestDirectoryTotalLimitKeepsRenewals`

### REQ-broker-discovery-012

Agent 必须能列出本机 Broker 已知的全部 Broker，包括全部对端，以及本 Broker 持有其名片的非对端。每一项给出该 Broker 的对外地址列表；是对端的，另标明本方是它的拨号方还是接受方。

- **验收**：本 Broker 有两个不公开的对端，一个由本方发起配对，另一个向本方发起配对；本 Broker 另持有一张非对端的名片。结果同时包含这三个 Broker，每一项都带有该 Broker 的对外地址列表，不知道其对外地址的，列表为空；由本方发起配对的对端标明本方是拨号方，向本方发起配对的对端标明本方是接受方。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-discovery-012 peers listed` 覆盖对端与角色；非对端名片那一半由 `test/e2e/discovery_test.go` 的 `TestDiscoveryCardReachesWholeNetwork` 覆盖。

### REQ-broker-discovery-013

列出的每个 Broker 必须标明且只标明一种状态：已连接、可连接或不可直达。已连接指当前与它之间有链路；可连接指当前没有链路且知道它至少有一个对外地址；不可直达指当前没有链路且不知道它的任何对外地址。

- **验收**：分别构造这几种情形的 Broker，结果中的状态与情形一一对应。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-discovery-013 three statuses`

### REQ-broker-discovery-014

列出的每个 Broker 必须标明它是否为本 Broker 当前的对端。

- **验收**：结果中标为对端的 Broker 与本 Broker 当前的对端完全一致；解除配对后，该 Broker 不再标为对端。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-discovery-014 peer flag` 覆盖对端；非对端一半由 `test/e2e/discovery_test.go` 的 `TestDiscoveryCardReachesWholeNetwork` 覆盖。

### REQ-broker-discovery-015

经同一个对端传入的记录数量必须有单独的可配置上限，使单个对端无法占满名录。

- **验收**：把名录上限配置为 10、单个对端的上限配置为 4；一个对端传来 10 条有效记录，只有 4 条被接受，另一个对端此后传来的名片仍被接受。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_hostile_test.go` 的 `TestDirectoryPerLinkLimit`

### REQ-broker-discovery-016

公开 Broker 的对外地址列表变化后，它必须重新发布名片，使连通网络内的其他 Broker 得到新的列表。

- **验收**：公开 Broker A 与 B 已连接，B 与 C 已连接，A 与 C 之间没有链路；不重启 A，把 A 所在主机默认路由所在接口的地址由 `198.51.100.4` 改为 `198.51.100.9`，此后 C 列出已知 Broker 的结果中 A 的对外地址列表变为 `198.51.100.9:17443`。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_address_test.go` 的 `TestCardFollowsAddressChange`

### REQ-broker-discovery-017

名片中含有本 Broker 不认识的地址种类时，Broker 必须忽略该地址而仍然接受并传播这张名片，名片中其余的地址照常可用。

- **验收**：B 与 C 已连接；B 的对端 A 传来一张有效的名片，其对外地址列表含一项 B 不认识的地址种类和一项 `198.51.100.4:17443`。B 列出已知 Broker 的结果包含该名片的发布者，对外地址列表为 `198.51.100.4:17443`，状态为可连接；C 列出已知 Broker 的结果同样包含该发布者。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_hostile_test.go` 的 `TestDirectoryUnknownAddressKind`

### REQ-broker-discovery-018

名片中含有书写不合规定的直连地址项的，本 Broker 必须忽略该地址项而仍然接受并传播这张名片，名片中其余地址照常可用。

- **验收**：B 与 C 已连接；B 的对端 A 传来一张有效的名片，其对外地址列表含一项带 ESC 字符的地址和一项 `198.51.100.4:17443`。B 列出已知 Broker 的结果包含该名片的发布者，对外地址列表只有 `198.51.100.4:17443`；C 列出已知 Broker 的结果同样包含该发布者。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_hostile_test.go` 的 `TestDirectoryMalformedDirectAddress`；单元测试 `internal/directory/validate_test.go` 的 `TestValidDirectAddr`、`internal/directory/views_test.go` 的 `TestBrokersSkipsMalformedDirectAddresses`

## 数据需求

名录保存在本机磁盘上，Broker 重启后仍可列出。名片只含指纹、别名和对外地址列表，不含 Agent、topic 成员关系或配置。

## 假设与依赖

- 公开 Broker 的名片把它的对外地址列表公布给连通网络内的所有 Broker。
- Broker 是否可直达，以本 Broker 是否知道它的对外地址为准，不保证知道对外地址的 Broker 实际连得上。
- 本机地址变化之后、新的名片传到各处之前，其他 Broker 持有的是旧的对外地址列表。名片不构成信任依据，与公开 Broker 配对仍需它的配对密钥。
