# Broker 配对与连接

## 问题陈述

两台主机上的 [Broker](../../glossary.md) 互相通信之前，各自的用户需要确认对方就是想连接的那一台，并且之后的通信不能被第三方读取，对方的身份也不能被第三方冒充。这个确认过程不应要求事先建立证书颁发机构，也不能要求双方都有对方能连上的对外地址。

## 范围

本文档覆盖 Broker 的身份与别名、配对密钥的生成与使用、配对、链路的保持与恢复、解除配对。

### 范围外

- 经第三个 Broker 中转的连接。
- 由证书颁发机构签发证书的信任方式。
- 系统自行决定连接哪个 Broker。首次连接一律由用户发起。

## 参与者

| 参与者 | 权限 |
|---|---|
| 用户 | 决定与哪个 Broker 配对，把本机 Broker 的地址和配对密钥经系统之外的途径交给对方用户，经 Claude Code 的权限确认批准 Agent 发起的操作。 |
| Agent | 应用户要求生成配对密钥、发起配对、解除配对。这些操作以 Agent 所属的账户对 Broker 的配置文件有写权限为前提，由 `REQ-broker-config-021` 规定。 |
| 对端 | 配对后与本 Broker 保持链路。 |

## 功能需求

### REQ-broker-pairing-001

每个 Broker 必须有一个指纹作为身份，指纹在 Broker 重启、别名变更和监听地址变更后保持不变。

- **验收**：记录 Broker 的指纹，依次重启它、修改它的别名、更换它的监听地址，每次之后读到的指纹都与最初相同。
- **状态**：已实现
- **端到端测试**：`test/e2e/aliases_test.go` 的 `TestAliases/REQ-broker-pairing-001 fingerprint stable`

### REQ-broker-pairing-002

Broker 的别名在未配置时，系统级 Broker 必须取所在主机系统主机名的第一段（第一个 `.` 之前的部分），用户级 Broker 必须由主机名的第一段、`.` 和所属账户名依次组成。别名与监听地址、监听端口和对外地址都无关。主机名和账户名中不属于别名字符集的字符替换为 `-`。

- **验收**：在主机名为 `hostA.example.com` 的主机上，以默认配置启动系统级 Broker，对端看到它名下 Agent 的地址以 `@hostA` 结尾；以账户 `alice` 启动用户级 Broker，地址以 `@hostA.alice` 结尾。把用户级 Broker 的监听端口改为 17444、把监听地址配置为空、把对外地址配置为 `a.example:443`，三种情况下地址的结尾都不变。
- **状态**：已实现
- **端到端测试**：`test/e2e/aliases_test.go` 的 `TestAliases`、`TestLongHostnameAlias`；`test/e2e/system/alias_default_test.go` 的 `TestREQBrokerPairing002_DefaultAliasHostAndAccount`

### REQ-broker-pairing-003

Broker 的别名必须可以经配置修改。

- **验收**：把别名改为 `build-box` 后，本机 Agent 看到的自身地址以 `@build-box` 结尾，没有为它设本地别名的对端看到的地址同样如此。
- **状态**：已实现
- **端到端测试**：`test/e2e/aliases_test.go` 的 `TestAliases/REQ-broker-pairing-003 configured alias`

### REQ-broker-pairing-004

发起配对的一方发现对方的别名与本机别名或已有的某个对端的别名相同时，系统必须在用户为新对端指定本地别名之后才完成配对。

- **验收**：已有别名为 `macbook` 的对端，再向另一台别名也是 `macbook` 的 Broker 发起配对，系统返回要求指定本地别名的结果；指定 `macbook-2` 后配对完成，两台对端分别以 `@macbook` 和 `@macbook-2` 寻址。
- **状态**：已实现
- **端到端测试**：`test/e2e/aliases_test.go` 的 `TestAliases/REQ-broker-pairing-004 initiator conflict`

### REQ-broker-pairing-005

Agent 必须能让本机 Broker 生成一次性配对密钥。

- **验收**：调用后得到一个一次性配对密钥及其失效时间；另一台主机凭本机 Broker 的地址和这个密钥即可发起配对，不需要其他信息。
- **状态**：已实现
- **端到端测试**：`test/e2e/journey_pair_test.go` 的 `TestJourneyPairBrokers`

### REQ-broker-pairing-006

一次性配对密钥必须在生成 10 分钟后失效。

- **验收**：生成后第 9 分钟用它配对成功；另生成一个，在第 11 分钟用它配对，返回密钥已过期的错误。
- **状态**：已实现
- **端到端测试**：`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-006 key within validity`；时限部分由 `internal/pairing/pairkey_expired_test.go` 的 `TestREQBrokerPairing006_NineMinutesSucceedsElevenMinutesExpired` 与 `internal/pairing/pairkey_test.go` 的 `TestREQBrokerPairing006_KeyExpiredAtElevenMinutes` 覆盖

### REQ-broker-pairing-007

一次性配对密钥必须在成功完成一次配对后失效。

- **验收**：用同一个一次性配对密钥先后从两台主机向生成它的 Broker 发起配对，先发起的成功，后发起的返回密钥无效的错误。
- **状态**：已实现
- **端到端测试**：`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-007 one-time key used once`

### REQ-broker-pairing-008

配置文件中没有设置预置静态密钥时，只有有效的一次性配对密钥能使配对成功。

- **验收**：以默认配置启动的 Broker 没有设置预置静态密钥，另一台主机以任意不是有效一次性配对密钥的字符串作为密钥向它发起配对，都返回密钥无效的错误，双方都不保存对方的身份。
- **状态**：已实现
- **端到端测试**：`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-008 no static key`

### REQ-broker-pairing-009

配置文件中设置了预置静态密钥后，它必须可以用于任意多次配对，且不随时间失效。

- **验收**：在 Broker 的配置文件中设置预置静态密钥，三台不同的主机先后凭本机 Broker 的地址和该密钥配对，全部成功；设置 11 分钟之后，第四台主机凭它配对，同样成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-009 static key many times`；“11 分钟后仍可用”由 `internal/pairing/pairkey_test.go` 的 `TestREQBrokerPairing009_StaticKeyNotExpired` 覆盖

### REQ-broker-pairing-010

Agent 必须能凭对方的地址和配对密钥，让本机 Broker 向对方发起配对。

- **验收**：以对方的地址和一次性配对密钥调用，返回配对成功以及对方的别名与指纹；以对方的地址和对方配置的预置静态密钥调用，结果相同；把展示的地址原文（`主机名(地址:端口)`）原样作为地址，结果相同，展示中的主机名不用于连接；密钥仍单独传入。
- **状态**：已实现
- **端到端测试**：`test/e2e/journey_pair_test.go` 的 `TestJourneyPairBrokers`；`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-010 static key and plain address`

### REQ-broker-pairing-011

配对密钥错误、过期或已被使用时，配对必须失败并说明原因，双方都不得保存对方的身份。

- **验收**：以错误的密钥发起配对，调用返回说明密钥无效的错误；之后双方列出对端的结果都不含对方。
- **状态**：已实现
- **端到端测试**：`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-011 wrong key stores nothing`

### REQ-broker-pairing-012

不知道配对密钥的第三方即使能截获并改写双方之间的网络流量，也不得使配对成功。

- **验收**：在两个 Broker 之间放置一个能读写双方之间全部流量但不知道配对密钥的中间节点，以正确的配对密钥发起配对，配对失败，双方都不保存对方或中间节点的身份。
- **状态**：已实现
- **端到端测试**：`test/e2e/security_test.go` 的 `TestMITM/REQ-broker-pairing-012 man in the middle`

### REQ-broker-pairing-013

Broker 之间传输的全部内容必须加密。

- **验收**：抓取两个已连接 Broker 之间的网络流量，其中不出现任何消息正文、Agent 名字或 topic 名字的明文。
- **状态**：已实现
- **端到端测试**：`test/e2e/security_test.go` 的 `TestLinkEncrypted/REQ-broker-pairing-013 encrypted`

### REQ-broker-pairing-014

系统不得以尚未配对的 Broker 为目标发起连接，除非这次连接是由 Agent 的发起配对调用直接触发的。

- **验收**：Broker 获知一个公开 Broker 的名片后，在没有任何发起配对调用的情况下，不产生指向该 Broker 对外地址的连接。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestDiscoveryNoDialToCardHolder`；`test/e2e/discovery_address_test.go` 的 `TestNoDialToCardAddress`

### REQ-broker-pairing-015

来自既未持有有效配对密钥，也不是对端的 Broker 的连接请求必须被拒绝。

- **验收**：一个未配对的 Broker 不带密钥连接，连接被拒绝，被连接方列出对端的结果不含它。
- **状态**：已实现
- **端到端测试**：`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-015 unpaired without key rejected`

### REQ-broker-pairing-016

配对完成后，两个对端之间再次建立链路不得需要配对密钥。

- **验收**：配对成功后断开网络再恢复，链路重新建立，期间没有任何一方提供配对密钥，且原一次性配对密钥已失效。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-pairing-016 relink without key`

### REQ-broker-pairing-017

配对关系必须在 Broker 重启后保留。

- **验收**：配对后重启其中一个 Broker，重启后它列出对端的结果仍包含对方。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-pairing-017 survives restart`

### REQ-broker-pairing-018

对端之间的链路断开后，拨号方必须在没有用户操作的情况下自网络恢复起 120 秒内重新建立链路。

- **验收**：断开两台主机之间的网络再恢复，无任何用户操作，恢复后 120 秒内双方列出对端的结果重新显示已连接。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-pairing-018 redial after restore`

### REQ-broker-pairing-019

Broker 重启后，系统必须在没有用户操作的情况下在重启后 60 秒内恢复与全部对端的链路。

- **验收**：重启一个有两个对端的 Broker，其中一个对端由它发起配对，另一个向它发起配对；无任何用户操作，重启后 60 秒内它与两个对端的链路都恢复。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinksRestartWithTwoPeers`

### REQ-broker-pairing-020

发起配对的一方能主动连到另一方时，双方必须能互相发送消息。

- **验收**：主机 B 位于 NAT 之后，没有主机 A 能连上的对外地址，主机 A 有 B 能连上的对外地址；B 向 A 发起配对后，A 上的 Agent 向 B 上的 Agent 发送消息成功，反向同样成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/send_remote_test.go` 的 `TestRemoteSendFaults/REQ-broker-pairing-020 both directions behind NAT`

### REQ-broker-pairing-021

Agent 必须能让本机 Broker 解除与一个对端的配对，解除后链路断开，该 Broker 不再是本机的对端。

- **验收**：解除配对后，本机列出对端的结果不含对方，对方 Broker 上的 Agent 从本机 Agent 的列表中消失。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-pairing-021 disconnect`

### REQ-broker-pairing-022

解除配对后，被解除配对的 Broker 再次连接时必须被拒绝，直到重新配对。

- **验收**：B 凭 A 的地址和配对密钥向 A 发起配对。A 解除与 B 的配对后，B 的自动重连被 A 拒绝；B 凭新的有效配对密钥重新配对后，链路恢复。A 设有预置静态密钥时，B 凭它重新配对同样恢复链路。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-pairing-022 re-pair after unpair`

### REQ-broker-pairing-023

配对时对方出示的指纹，与本 Broker 此前从名片获知的、该对外地址对应的指纹不一致时，系统必须在配对结果中向用户给出警告。

- **验收**：本 Broker 已持有某公开 Broker 的名片；向该名片所载对外地址发起配对而对方实际出示了另一个指纹时，调用结果包含指纹不一致的警告。
- **状态**：已实现
- **端到端测试**：`test/e2e/journey_public_broker_test.go` 的 `TestJourneyConnectPublicBroker/fingerprint_mismatch`

### REQ-broker-pairing-024

接受配对的一方发现对方的别名与本机别名或已有的某个对端的别名相同时，系统必须为新对端生成一个不与它们重复的本地别名，并完成配对。

- **验收**：A 已有别名为 `macbook` 的对端，另一台别名也是 `macbook` 的 Broker 凭有效密钥向 A 发起配对，配对成功；A 列出 Broker 的结果中两台对端的别名互不相同。
- **状态**：已实现
- **端到端测试**：`test/e2e/aliases_test.go` 的 `TestAliases/REQ-broker-pairing-024 acceptor conflict`

### REQ-broker-pairing-025

系统不得生成预置静态密钥，生成配对密钥的操作只产生一次性配对密钥。

- **验收**：在没有设置预置静态密钥的 Broker 上多次生成配对密钥，每次得到的都是带失效时间的一次性配对密钥，Broker 的配置文件与 Broker 保存在磁盘上的数据中都没有出现可重复使用的配对密钥；在配置文件中设置预置静态密钥后，凭它配对成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-025 never generates a static key`

### REQ-broker-pairing-026

对端自报的别名变为与本机别名或其他对端的别名相同时，系统必须为它生成不重复的本地别名，已有的 Agent 地址不得因此指向另一个 Broker。

- **验收**：A 有别名分别为 `hostB` 和 `hostC` 的两个对端；B 把自己的别名改为 `hostC` 之后，A 上以 `@hostC` 结尾的地址仍只指向 C 上的 Agent，B 上的 Agent 在 A 上以另一个别名出现，它们发来的消息的署名不以 `@hostC` 结尾。
- **状态**：已实现
- **端到端测试**：`test/e2e/aliases_test.go` 的 `TestAliases/REQ-broker-pairing-026 peer renames into conflict`

### REQ-broker-pairing-027

接受方不得主动连接对方。

- **验收**：两台主机都以默认配置运行，A 生成配对密钥，B 凭 A 的地址和这个密钥发起配对。断开再恢复两台主机之间的网络，链路重新建立，期间 A 没有向 B 发起任何连接。用防火墙禁止 B 主动连接 A 后再断开并恢复网络，链路不再恢复，期间 A 仍没有向 B 发起任何连接。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-pairing-027 acceptor never dials`

### REQ-broker-pairing-028

生成一次性配对密钥的结果必须在密钥之外另行展示本机的系统主机名与对外地址列表中的每一项，首项在前，展示的内容不是密钥的一部分；对外地址列表为空而 Broker 在监听时，必须仍然生成密钥并说明没有可展示的地址。

- **验收**：在系统主机名为 `hostA`、默认路由所在接口的地址为 `192.0.2.10`、未配置对外地址的 Broker 上生成一次性配对密钥，结果中密钥与 `hostA(192.0.2.10:17443)` 分开给出，密钥本身不含主机名或地址；把对外地址配置为 `a.example:443` 后生成，展示的是 `hostA(a.example:443)`；再追加对外地址 `a.vpn.example` 后生成，依次展示 `hostA(a.example:443)` 与 `hostA(a.vpn.example:443)`；主机没有默认路由、监听全部地址且未配置对外地址时，生成照常成功，结果说明没有可展示的地址、需要用户自行告知对方连接地址。
- **状态**：已实现
- **端到端测试**：`test/e2e/journey_pair_test.go` 的 `TestJourneyPairBrokers`；`test/e2e/addresses_test.go` 的 `TestAdvertisedAddresses/REQ-broker-pairing-028 shown addresses`

### REQ-broker-pairing-029

长度不足 16 个字符或超过 128 个字符的预置静态密钥必须被拒绝，Broker 不得以它启动，运行期间改成这样的值时也不得采用。

- **验收**：在配置文件中把预置静态密钥设为 15 个字符后启动 Broker，启动失败，错误指出这一配置项；设为 16 个字符或 128 个字符时都能启动，并能凭它配对。运行期间在配置文件中把它改为 129 个字符，Broker 不采用新值，凭原来的密钥配对仍成功，凭新值配对失败。
- **状态**：已实现
- **端到端测试**：`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-029 static key length`

### REQ-broker-pairing-030

读取配置的结果与运行日志都不得包含预置静态密钥的取值。

- **验收**：在配置文件中设置预置静态密钥，经工具读取配置，结果只说明它已设置；随后在配置文件中更换它，再凭新值完成一次配对。在读取配置的结果与运行日志中搜索新旧两个取值，都搜不到。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_cluster_test.go` 的 `TestConfigToolsCluster/StaticKeyReadHasNoSecret`

### REQ-broker-pairing-031

系统不得允许经工具设置或修改预置静态密钥。

- **验收**：所属账户对配置文件有写权限的 Agent 经工具设置、修改或删除预置静态密钥，每次都返回错误，说明该项只能在配置文件中设置；配置文件的内容与生效的密钥都没有变化。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_cluster_test.go` 的 `TestConfigToolsCluster/StaticKeyToolsDoNotChange`

### REQ-broker-pairing-032

从配置文件中删除预置静态密钥后，Broker 必须在不重启的情况下（`REQ-broker-config-017`）停止接受凭该密钥的配对。

- **验收**：依次检查：配置文件中设有预置静态密钥时，另一台主机凭它配对成功；从配置文件中删除该项，不重启 Broker，此后第三台主机凭同一个密钥发起配对，被拒绝，返回的原因是密钥无效，双方都没有保存对方的身份；读取配置的结果显示该项未设置；审计日志新增一条该配置项发生变化的记录，记录中没有密钥的取值；先前凭它配对成功的对端仍是对端，它与本 Broker 的链路不受影响。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_cluster_test.go` 的 `TestConfigToolsCluster/StaticKeyRemovedRejects`

### REQ-broker-pairing-033

把配置文件中的预置静态密钥改为另一个合法的值后，Broker 必须在不重启的情况下（`REQ-broker-config-017`）只接受新值，凭旧值的配对必须被拒绝。

- **验收**：配置文件中设有预置静态密钥，并已有一个凭它配对的对端；把它改为另一个合法的值，不重启 Broker。随后分别凭旧值与新值发起配对，凭旧值的被拒绝，返回的原因是密钥无效，凭新值的成功；更换之前已配对的对端仍是对端，链路不受影响。
- **状态**：已实现
- **端到端测试**：`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-033 replace static key`

### REQ-broker-pairing-034

接受方的对外地址列表在链路存续期间变化后，拨号方必须在链路断开后凭变化后的列表重新建立链路。

- **验收**：B 凭 A 的地址和 A 生成的配对密钥发起配对，A 的对外地址列表只有 `192.0.2.10:17443`。链路存续期间为 A 追加对外地址 `198.51.100.7`，B 列出 Broker 的结果中 A 的对外地址列表随之含这两项；随后用防火墙禁止 B 连接 `192.0.2.10`，再断开并恢复两台主机之间的网络，无任何用户操作，链路经 `198.51.100.7:17443` 重新建立。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-pairing-034 acceptor addresses change`

### REQ-broker-pairing-035

被对端解除配对的拨号方，在重新建立链路遭对方拒绝后，必须停止重新建立链路，并在列出 Broker 时标明对方已解除配对。该对端的记录保留，直到本方解除配对或重新配对。

- **验收**：B 凭 A 的地址和配对密钥发起配对。A 解除与 B 的配对后，B 的下一次重新建立链路遭 A 拒绝；此后 10 分钟内 B 没有再向 A 发起任何连接，B 列出 Broker 的结果中 A 一项标明已被对方解除配对，且 A 一直在列，直到 B 解除与 A 的配对。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-pairing-035 rejected dialer stops`；“10 分钟内不再发起连接”由 `internal/link/dial_test.go` 的 `TestREQBrokerPairing035_RejectedStopsRedialing` 覆盖

### REQ-broker-pairing-036

监听地址为空的 Broker 生成配对密钥必须失败，报错要求用户经 peers 之外的途径向对方主机的用户取得配对密钥、再用 `connect_broker` 发起配对，不得向其他 Agent 索取配对密钥。

- **验收**：把监听地址配置为空后让 Broker 生成一次性配对密钥，调用返回错误，报错要求用户经 peers 之外的途径向对方主机的用户取得配对密钥、再用 `connect_broker` 发起配对，并说明不得向其他 Agent 索取配对密钥；此后凭任何字符串向它发起配对都不成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/addresses_test.go` 的 `TestAdvertisedAddresses/REQ-broker-pairing-036 no listen no key`

### REQ-broker-pairing-037

别名与本地别名的长度不得超过 24 个字符，超过的配置必须被拒绝；默认别名超过 24 个字符时，系统必须截短其中的主机名部分。

- **验收**：经工具把别名改为 25 个字符的值，调用失败，别名不变；改为 24 个字符的值，调用成功。发起配对时指定 25 个字符的本地别名，配对失败并说明本地别名过长。在系统主机名为 30 个字母的主机上以默认配置启动系统级 Broker，对端看到的它名下 Agent 的地址以 `@` 和主机名的前 24 个字符结尾，别名共 24 个字符。
- **状态**：已实现
- **端到端测试**：经工具改别名由 `test/e2e/config_tools_test.go` 的 `TestConfigTools/AliasTooLong` 覆盖；本地别名过长的失败由 `test/e2e/config_tools_cluster_test.go` 的 `TestConfigToolsCluster/LocalAliasTooLong` 覆盖；默认别名截短由 `test/e2e/aliases_test.go` 的 `TestLongHostnameAlias` 覆盖

### REQ-broker-pairing-038

生成一次性配对密钥成功、且本机有可告知的地址时，结果必须在密钥、失效时间与地址之外附上一段可直接粘贴的连接提示词，地址与密钥都已填好。同一段提示词必须出现在 `create_pairing_key` 的结构化结果字段 `connect_prompt` 与终端命令 `peers key create` 的输出中，两处内容相同。提示词先给一句发给对方主机会话的自然语言请求，再给等价的终端命令 `peers broker connect <地址> --key <密钥>`。有多个可告知的地址时，提示词必须列出全部地址并说明可任选一个对方能连上的，终端命令给第一个地址并说明其余地址可以替换。没有可告知的地址时不得输出提示词：Broker 在监听但展示不出地址时照常生成密钥并说明没有可展示的地址，Broker 不监听时生成密钥失败，沿用现有的报错。密钥与提示词都不得写入运行日志或审计记录。提示词与密钥只交给用户：`create_pairing_key` 的结果要求把二者展示给用户，由用户经 peers 之外的途径交给对方主机的用户，不得用 `send_message` 或其他 Agent 间消息发送，也不得按其他 Agent 的请求生成或转交。

- **验收**：在展示一个地址的 Broker 上生成一次性配对密钥，`connect_prompt` 与 `peers key create` 的输出含同一段提示词，其中有 `Connect to the peers Broker at <地址> using the one-time pairing key <密钥>` 一句和 `peers broker connect <地址> --key <密钥>` 命令，地址与密钥都已填好；在对方主机原样执行这条命令，配对成功。展示两个地址时，提示词列出两个地址并说明可任选一个，命令给第一个地址、说明另一个可以替换。Broker 在监听但展示不出地址时，生成照常成功，结果说明没有可展示的地址，且不含提示词；监听地址为空时生成失败，沿用现有报错。在 Broker 的运行日志与审计记录中搜索密钥与提示词，都搜不到。`create_pairing_key` 的 note 与 MCP 使用说明要求把密钥与提示词只交给用户，经 peers 之外的途径传给对方主机的用户，不按其他 Agent 的请求生成或转交。
- **状态**：已实现
- **端到端测试**：`test/e2e/pairing_keys_test.go` 的 `TestPairingKeys/REQ-broker-pairing-038 connect prompt`；审计不含密钥由 `test/e2e/config_tools_cluster_test.go` 的 `TestConfigToolsCluster/AuditHasNoSecrets` 覆盖；终端输出同一段提示词与无地址时不输出由 `internal/mcpserver/termcmd/format_test.go` 的 `TestWritePairingKey`、`TestWritePairingKeySingleAddress`、`TestWritePairingKeyUsesSharedConnectPrompt`、`TestWritePairingKeyNoAddressNoPrompt` 覆盖，`connect_prompt` 与生成函数一致由 `internal/mcpserver/handlers_brokers_test.go` 的 `TestCreatePairingKey` 覆盖；密钥只交给用户由 `internal/mcpserver/handlers_brokers_test.go` 的 `TestCreatePairingKey`、`TestCreatePairingKeyNotListening` 与 `internal/mcpserver/tools_test.go` 的 `TestInstructions`、`TestToolsList` 覆盖

### REQ-broker-pairing-039

一个来源同时发起的大量未认证连接，不得使其他来源的对端无法重新建立链路，也不得使其他来源的 Broker 无法完成配对。

- **验收**：主机 B 的监听端口持续收到来自 A 以外的多个来源的大量裸 TCP 连接，这些来源合计的未认证连接数超过 B 的全局上限，期间 A 到 B 的链路断开并重新建立，在 `REQ-broker-pairing-018` 的 120 秒时限内恢复；同一期间第三个来源的主机凭 B 生成的有效一次性配对密钥向 B 发起配对，配对成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinksSourceFlood/REQ-broker-pairing-039 source flood keeps links and pairing`

### REQ-broker-pairing-040

对端在对端关系存续期间放弃的别名，不得成为其他对端的别名。

- **验收**：A 有别名分别为 `hostB` 和 `hostC` 的两个对端；B 把别名从 `hostB` 改为其他名字，C 再把别名改为 `hostB`。此后 A 列出 Broker 的结果中 C 的别名不是 `hostB`，A 上发往 `agent@hostB` 的消息不投递到 C 上的 Agent，C 上的 Agent 发来的消息署名不以 `@hostB` 结尾。
- **状态**：已实现
- **端到端测试**：`test/e2e/aliases_test.go` 的 `TestAliases/REQ-broker-pairing-040 released alias stays with its peer`

## 数据需求

Broker 的身份密钥、对端的指纹与别名、本方在每对对端中的角色、由本方发起配对的对端的对外地址保存在本机磁盘上，只允许运行该 Broker 的账户读取。一次性配对密钥只在内存中保留。预置静态密钥保存在 Broker 的配置文件中，能读取配置文件的账户都能读到它。

## 假设与依赖

- 配对密钥由用户经系统之外的可信途径交给对方，该途径被窃听等同于密钥泄露。Broker 的地址不是秘密，可以与密钥经不同的途径告知。
- 生成配对密钥时展示的地址未必能被对方连上；连不上时由用户告知对方能连上的地址。展示中的主机名只供用户辨认，不用于连接。
- 预置静态密钥的强度由设置它的人自行保证。凡用它配对过的主机都知道它；这样的主机能截获新主机的连接或把它引向错误的一方时，可以冒充接受配对的一方完成配对。对配置文件只有读权限的账户也能读到它，从而凭它让本 Broker 增加对端。设置预置静态密钥即接受这些风险。
- 两台主机只有一方能连到另一方时，配对必须由能连到对方的一方发起，链路此后由它重新建立。
- 接受方在链路断开之后、重新建立之前更换了地址，而拨号方又没有从名片得到新地址时，拨号方连不上它，恢复的办法是重新配对。
- 与一个 Broker 配对，就信任它对自己名下 Agent 的名字与 topic 成员关系的陈述。系统不验证对端主机上的事实。
- 运行 Broker 的账户下的其他进程可以读取该 Broker 保存在磁盘上的数据。
- 拨号方解除配对时只关闭链路，接受方得不到通知，只看到该对端未连接，记录一直保留；接受方的用户自行解除这条记录。
