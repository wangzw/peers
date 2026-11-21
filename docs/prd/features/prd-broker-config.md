# Broker 配置、审计与运行

## 问题陈述

用户在会话里工作时需要查看和调整 [Broker](../../glossary.md) 的配置，不必离开会话去编辑文件。配置中有影响安全的配置项。谁能读、谁能改，应当与操作系统对配置文件的权限一致，经工具操作只是便捷途径；改了什么也要能在事后查到。

Broker 有两种部署形态。生产部署时由管理员把它作为系统服务运行，一个 Broker 服务主机上的多个账户；个人使用时它以普通账户运行，只服务这个账户；用户把它安装为服务一次之后，它随登录启动并持续运行，不需要每次手动启动。同一台主机上的多个 Broker 不能互相干扰，一个账户也不应同时有两个 Broker。

## 范围

本文档覆盖 Broker 的两种部署形态及各自的服务范围、Broker 的安装、启动、持续运行与卸载、降级运行、下线与上线、监听地址与对外地址的确定、配置的读取与修改及其权限、审计日志、本机的状态查看。

### 范围外

- 独立于 Claude Code 权限机制的二次确认。一次工具调用是否需要用户确认，由所在会话的 Claude Code 权限设置决定；它与本文档规定的文件权限前提各自独立，两者都满足时调用才被执行。
- 文件权限之外另设的管理员名单或角色。
- 同一账户下运行多个 Broker 实例。
- 监听地址被占用时自动改用其他地址或端口。
- 对端读取或修改本 Broker 的配置。
- 分布式追踪与指标导出。

## 参与者

本文档中的账户指操作系统账户。

| 参与者 | 权限 |
|---|---|
| Agent | 列出 Agent、列出 topic、加入与退出 topic、发送消息不需要对配置文件的任何权限。读取配置要求所属账户对 Broker 的配置文件有读权限；修改配置、生成配对密钥、发起配对、解除配对要求有写权限。 |
| 用户 | 经 Claude Code 的权限设置决定哪些工具调用需要确认。对自己账户下的用户级 Broker，可以直接编辑配置文件、在本机终端查看状态与审计日志、把它安装为服务（安装后立即启动并随登录启动），或在终端手动启动它。 |
| 管理员 | 安装并启动系统级 Broker，以文件权限决定哪些账户可以使用它、哪些账户可以读取或修改它的配置。 |

## 功能需求

### REQ-broker-config-001

所属账户对 Broker 的配置文件有读权限的 Agent，必须能读取该 Broker 的当前配置。权限前提由 `REQ-broker-config-022` 规定。

- **验收**：读取结果中的别名、是否公开、监听地址、是否自动加入默认 topic 与 Broker 的实际行为一致。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_test.go` 的 `TestDiscoveryCardReachesWholeNetwork`

### REQ-broker-config-002

所属账户对 Broker 的配置文件有写权限的 Agent，必须能修改该 Broker 的配置项，`REQ-broker-pairing-031` 与 `REQ-broker-config-029` 规定不能经工具修改的情形除外。权限前提由 `REQ-broker-config-021` 规定。

- **验收**：把别名改为新值，调用成功后再读取配置，别名是新值。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_test.go` 的 `TestConfigTools/SetAliasIsAudited`

### REQ-broker-config-003

读取配置与修改配置必须能分别授权。

- **验收**：在 Claude Code 权限设置中只放行读取配置的工具，Agent 读取配置不触发确认，修改配置触发确认。
- **状态**：已实现
- **端到端测试**：`test/realsession/probes/permissions.sh`（经 `.github/workflows/real-session.yml` 推送 `probe/permissions` 运行）

### REQ-broker-config-004

没有副作用的工具必须声明为只读（[REF-mcp](../../references.md)），有副作用的工具不得声明为只读。没有副作用是指不改变 Broker 的状态，也不投递消息。

- **验收**：检查全部工具的声明，列出 Agent、列出 topic、列出 Broker、读取配置带只读标记；加入与退出 topic、发送消息、生成配对密钥、发起配对、解除配对、修改配置、设置上线与下线、升级 Broker 不带只读标记。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_test.go` 的 `TestConfigToolAnnotations`

### REQ-broker-config-005

生成配对密钥必须能独立于修改配置获得授权。

- **验收**：在 Claude Code 权限设置中只放行生成配对密钥的工具，Agent 生成一次性配对密钥不触发确认，修改配置触发确认。
- **状态**：已实现
- **端到端测试**：`test/realsession/probes/permissions.sh`（经 `.github/workflows/real-session.yml` 推送 `probe/permissions` 运行）

### REQ-broker-config-006

Agent 对配置的每次修改必须写入审计日志，记录发起修改的 Agent 及其所属账户、被修改的配置项、修改前的值、修改后的值和时间。预置静态密钥只记录发生了变化，不记录取值。

- **验收**：修改别名后，审计日志新增一条记录，各项内容与这次修改一致。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_test.go` 的 `TestConfigTools/SetAliasIsAudited`

### REQ-broker-config-007

每次生成一次性配对密钥、配对成功、配对失败、链路建立、链路断开和解除配对必须写入审计日志，记录事件类型、相关 Broker 的指纹与别名和时间；由 Agent 发起时，还记录发起的 Agent 及其所属账户。

- **验收**：依次执行生成一次性配对密钥、一次失败的配对、一次成功的配对、断开网络、恢复网络、解除配对，审计日志按顺序各新增一条对应类型的记录；其中由 Agent 发起的记录带有该 Agent 的地址和它所属的账户。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_cluster_test.go` 的 `TestConfigToolsCluster/PairingAndLinkEventsInOrder`

### REQ-broker-config-008

审计日志不得记录配对密钥的值和消息正文。

- **验收**：完成一次配对和一次消息发送后，在审计日志中搜索所用的配对密钥和消息正文，都搜不到。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_cluster_test.go` 的 `TestConfigToolsCluster/AuditHasNoSecrets`

### REQ-broker-config-009

所属账户对 Broker 的配置文件有读权限的用户，必须能在本机终端查看该 Broker 的审计日志。

- **验收**：以有读权限的账户在终端执行查看审计日志的命令，输出包含此前产生的全部审计记录；以没有读权限的账户执行，得到权限不足的错误。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/permission_test.go` 的 `TestREQBrokerConfig009_AuditNeedsReadPermissionOnSystemBroker`（系统级：有读权限的账户看到的输出与 Broker 的全部审计记录逐条对应；无读权限的账户被拒）；`test/e2e/config_tools_test.go` 的 `TestConfigTools/TerminalAuditCommand`（用户级：输出与 `Oracle.Audit` 逐条对应）

### REQ-broker-config-010

用户必须能在本机终端查看 Broker 的状态，包括已登记的 Agent、各 Agent 所在的 topic、全部对端及其连接状态。所属账户对配置文件没有读权限的用户，只能看到自己账户下的 Agent，对端及其连接状态照常可见。

- **验收**：登记两个 Agent 并让它们加入不同的 topic，与一个对端配对后断开网络；以有读权限的账户在终端执行查看状态的命令，输出列出这两个 Agent 及各自的 topic，并显示该对端未连接。在系统级 Broker 上以没有读权限的账户执行，输出只含该账户自己的 Agent，并同样列出该对端及其连接状态。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/permission_test.go` 的 `TestREQBrokerConfig010_StatusWithoutReadPermissionShowsOwnAgents`（无读权限的账户只看到自己的 Agent，并看到对端未连接；有读权限的非 root 账户看到两个 Agent 及各自 topic，与对端配对后停掉对端 Broker 代替“断开网络”，对端仍列出且显示为未连接）

### REQ-broker-config-011

Broker 必须以结构化格式输出运行日志。

- **验收**：运行日志的每一行都能被解析为一条记录，记录中有时间、级别和事件名这几个字段。
- **状态**：已实现
- **端到端测试**：`test/e2e/broker_process_test.go` 的 `TestBrokerProcessInContainer/REQ-broker-config-011 structured log`

### REQ-broker-config-038

MCP 服务进程不得启动任何 Broker。会话按 `REQ-broker-config-026` 的顺序找不到可用的 Broker 时，工具调用必须返回说明如何启动 Broker 的错误；Broker 随后启动的，会话不必重新连接 MCP 服务即可使用。

- **验收**：在没有任何 Broker 进程的主机上启动一个启用了工具的会话，工具调用返回的错误说明可以安装服务或在终端启动 Broker，主机上不出现 Broker 进程；随后在终端启动该账户的 Broker，不在 Claude Code 中重新连接 MCP 服务，下一次列出 Agent 成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/broker_process_test.go` 的 `TestBrokerProcessLocal/REQ-broker-config-038 no Broker until started`；`test/e2e/broker_process_test.go` 的 `TestBrokerProcessInContainer/REQ-broker-config-024 default address taken`、`TestBrokerProcessInContainer/REQ-agent-messaging-038 recover without reconnect`（终端启动失败、会话报错、手工启动后恢复）

### REQ-broker-config-039

用户必须能在本机终端执行 Broker 的每一项查询与管理操作：查看状态、读取审计日志、列出 Broker、读取配置、修改配置、设置上线与下线、生成一次性配对密钥、发起配对、解除配对。终端命令与 Agent 的对应操作遵守同一权限规则，按执行命令的账户判定，并写入同样的审计记录。

- **验收**：在系统级 Broker 上，以对配置文件没有写权限的账户在会话中生成配对密钥，被拒绝；以管理员身份在终端执行生成配对密钥的命令，输出密钥、失效时间和本机对外地址，另一台主机在终端凭这些信息发起配对成功，两边在终端列出 Broker 时都显示对方已连接。同样以管理员身份在终端读取并修改一个配置项、解除这个配对，都成功，审计日志依次记录这些操作和执行它们的账户。以没有写权限的账户执行修改配置的命令，返回权限不足，并给出配置文件的路径。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/term_admin_test.go` 的 `TestREQBrokerConfig039_TerminalAdminCommands`

### REQ-broker-config-040

安装服务时，该级别的配置文件如果还不存在，必须生成一份列出全部配置项的配置文件：有默认值的配置项写出默认值，没有固定默认值的配置项以注释形式给出，每个配置项附一行说明。已存在的配置文件不得改动。

- **验收**：在没有配置文件的主机上分别安装用户级和系统级服务，生成的配置文件中，设计规定的每个配置项恰好出现一次；写出的取值与读取配置时得到的当前值一致。取消某个注释掉的配置项的注释并填入合法值，Broker 不重启即采用。用修改配置的工具修改一个注释掉的配置项后，文件中这一项只有一处生效的值，其他配置项和注释不变。配置文件已存在时再次安装，文件内容不变。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/config_template_test.go` 的 `TestREQBrokerConfig040_SystemInstallWritesTemplate`、`TestREQBrokerConfig040_UserInstallWritesTemplate`

### REQ-broker-config-041

发布包必须附带安装脚本，用户解压后执行一条命令即可完成安装：把二进制装到合适的位置，把 Broker 安装为服务并启动，为执行安装的用户在 Claude Code 中注册本系统的 MCP 服务。系统级安装由脚本自行经 sudo 取得管理员身份。主机上装有 fish 时，安装脚本还必须安装 fish 的补全；主机上没有 fish 时跳过，也不创建 fish 的配置目录。重复执行安装脚本不得出错，也不得重复注册。

- **验收**：在没有安装过本系统的 Linux 主机上，解压发布包，以普通账户执行安装脚本。之后在新的终端中，`peers --version` 输出发布版本，该账户的用户级 Broker 正在运行，`claude mcp get peer` 显示已注册。以普通账户执行安装脚本并对级别询问回答 y，输入 sudo 密码后，系统级 Broker 正在运行，二进制位于 root 所有、组与其他人不可写的目录，执行 sudo 的账户在 Claude Code 中已注册 MCP 服务。每种方式各再执行一次，都成功，MCP 服务只注册一次。在装有 fish 的主机上安装，fish 的补全文件存在；在没有 fish 的主机上安装，fish 的补全文件不存在，fish 的配置目录也没有被创建。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/install_script_test.go` 的 `TestREQBrokerConfig041_UserInstallScript`、`TestREQBrokerConfig041_SystemInstallScript`、`TestREQBrokerConfig044_InstallScriptLevelPromptAcceptsSystemLevel`（对级别询问回答 y 的系统级安装）

### REQ-broker-config-042

用户必须能用一条命令从本项目的 GitHub 正式发布在线安装：命令下载最新正式版本中本平台的发布包，校验后用发布包中的安装脚本完成安装。系统级安装执行同一条命令并对级别询问回答 y。用户可以指定要安装的版本。校验和不符或下载失败时，安装必须中止，主机上不得留下任何改动。

- **验收**：在没有安装过本系统的 Linux 主机上，以普通账户执行一条在线安装命令，结果与在本地执行发布包内安装脚本相同；执行同一条命令并对级别询问回答 y，结果与本地系统级安装相同。指定一个已发布的版本时，安装的是该版本；指定一个不存在的版本时，命令以非零退出，报错 `version <v> not found`，主机上没有改动。让下载的发布包与校验和不符，命令以非零退出，二进制、服务、MCP 注册都没有改动，也没有留下临时文件。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/remote_install_test.go` 的 `TestREQBrokerConfig042_RemoteInstallUser`、`TestREQBrokerConfig042_RemoteInstallSystem`、`TestREQBrokerConfig042_RemoteInstallPinnedVersion`、`TestREQBrokerConfig042_RemoteInstallUnknownVersion`、`TestREQBrokerConfig042_RemoteInstallArchiveWithoutInstallScript`、`TestREQBrokerConfig042_RemoteInstallTruncatedScript`、`TestREQBrokerConfig042_RemoteInstallChecksumMismatch`

### REQ-broker-config-013

同一账户下至多运行一个 Broker 实例。已有实例在运行时，以任何监听地址或配置再启动一个都必须失败，并说明已有实例在运行。

- **验收**：在没有 Broker 进程的主机上，以同一个账户同时运行五次启动 Broker 的命令，该账户名下只有一个 Broker 进程持续运行，其余都以说明已有实例在运行的错误退出；随后在该账户下启动五个启用了工具的会话，它们都登记在这个 Broker 上；再以同一个账户指定另一个监听地址手动启动一个 Broker，启动失败，错误说明已有实例在运行，原有的 Broker 不受影响。
- **状态**：已实现
- **端到端测试**：`test/e2e/broker_process_test.go` 的 `TestBrokerProcessLocal/REQ-broker-config-013 single instance`

### REQ-broker-config-014

该 Broker 上的最后一个 Agent 的会话结束后，Broker 必须继续运行并保持与对端的链路。

- **验收**：结束主机上全部会话，Broker 进程仍在，对端列出 Broker 的结果仍显示它已连接。
- **状态**：已实现
- **端到端测试**：`test/e2e/links_test.go` 的 `TestLinks/REQ-broker-config-014 broker outlives last session`

### REQ-broker-config-015

用户必须能把自己账户下的用户级 Broker 安装为服务：安装后立即启动，此后随该账户登录而启动。

- **验收**：安装后不重新登录，该账户的 Broker 进程已在运行；重新登录后，在未启动任何会话的情况下，该账户的 Broker 进程已在运行，并已恢复与对端的链路。macOS 与 Linux 都满足。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/user_service_test.go` 的 `TestREQBrokerConfig015_UserServiceStartsAtLoginAndRestoresLink`（Linux：安装后立即运行、重新登录后启动并恢复链路）；`test/e2e/darwinservice/darwin_test.go` 的 `TestUserServiceRestoresLinkAfterRelogin`（macOS：以 bootout/bootstrap 模拟登录，断言出现新 pid 且对端链路恢复为已连接）

### REQ-broker-config-016

会话的 MCP 服务进程与正在运行的 Broker 版本不兼容时，工具调用必须失败并提示用户重启 Broker，系统不得自行终止正在运行的 Broker。

- **验收**：让一个不兼容版本的 MCP 服务进程连接正在运行的 Broker，工具调用返回提示重启 Broker 的错误，原 Broker 进程和已登记在它上面的其他 Agent 不受影响。
- **状态**：已实现
- **端到端测试**：`test/e2e/version_local_test.go` 的 `TestLocalVersionMismatchReq016`、`TestLocalVersionMismatchRecoveryReq016`

### REQ-broker-config-017

用户直接修改 Broker 的配置文件后，Broker 必须在不重启的情况下采用其中合法的新值，不合法的配置项保持原值，并为每个采用了新值的配置项写一条审计记录；预置静态密钥只记录发生了变化，不记录取值。以启动参数给定监听地址的运行期间，监听地址一项的变化不在此列。

- **验收**：Broker 运行期间在配置文件中把别名改为新值，不重启 Broker，再读取配置得到新值，审计日志新增一条写有该配置项前后取值的审计记录；同一次修改中把别名改为新值、同时把另一个配置项改为不合法的值时，别名采用新值，另一个配置项保持原值。以启动参数指定监听 `:17444` 运行时，在配置文件中同时修改监听地址和别名：Broker 仍在 17444 端口上监听，审计日志只新增别名一项的记录。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_test.go` 的 `TestConfigTools/FileEditHotReload`

### REQ-broker-config-018

以特权身份作为系统服务运行的 Broker，即系统级 Broker，必须能同时服务本机多个账户下的 Agent。

- **验收**：主机上运行着一个系统级 Broker，两个不同的账户各启动一个启用了工具的会话，两个会话都登记在这个 Broker 上；两者加入同一个 topic 后互相可见，并能互发消息。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/multi_account_test.go` 的 `TestREQBrokerConfig018_TwoAccountsShareSystemBroker`

### REQ-broker-config-019

用户级 Broker 必须拒绝其他账户的登记与调用。

- **验收**：账户甲运行着一个用户级 Broker；账户乙的会话的 MCP 服务进程和账户乙在终端执行的命令都连不上它，甲的 Broker 上不出现乙的 Agent，乙也读不到它的状态、配置和审计日志。
- **状态**：已实现
- **端到端测试**：`test/e2e/accounts_test.go` 的 `TestOtherAccountRejected/REQ-broker-config-019 filesystem layer`、`TestOtherAccountRejected/REQ-broker-config-019 credential layer`、`TestOtherAccountRejected/REQ-broker-config-019 account paths`

### REQ-broker-config-020

系统级 Broker 必须拒绝把不属于调用方账户的会话登记为 Agent。

- **验收**：账户乙的进程向系统级 Broker 要求登记账户甲的一个会话，所报出的会话信息与该会话自己的 MCP 服务进程报出的完全相同，登记被拒绝，任何 Agent 的列表中都不出现由此产生的 Agent。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/foreign_session_test.go` 的 `TestREQBrokerConfig020_ForeignSocketOwnerRejected/unreadable record`、`TestREQBrokerConfig020_ForeignSocketOwnerRejected/record readable but socket owned by another account`

### REQ-broker-config-021

修改配置、设置上线与下线、生成配对密钥、发起配对、解除配对，只有在调用方账户对该 Broker 的配置文件有写权限时才能执行；没有写权限时必须返回权限不足，并且不改变任何状态。

- **验收**：在系统级 Broker 上，以对配置文件没有写权限的账户依次调用这些操作，每次都返回权限不足，配置、对端和有效的配对密钥都没有变化；管理员把配置文件的写权限授予该账户所属的组之后，同样的调用全部成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/permission_test.go` 的 `TestREQBrokerConfig021_WriteNeedsConfigWritePermission`

### REQ-broker-config-022

读取配置，只有在调用方账户对该 Broker 的配置文件有读权限时才能返回配置内容；没有读权限时必须返回权限不足。

- **验收**：在系统级 Broker 上，以对配置文件没有读权限的账户读取配置，返回权限不足，结果中不含任何配置项的值；以有读权限的账户读取，返回当前配置。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/permission_test.go` 的 `TestREQBrokerConfig022_ReadNeedsConfigReadPermission`

### REQ-broker-config-023

登记、列出 Agent、列出 topic、列出 Broker、加入与退出 topic、发送消息，不得以对配置文件的任何权限为前提。

- **验收**：在系统级 Broker 上，一个对配置文件既没有读权限也没有写权限的账户启动会话，该会话完成登记，并成功执行列出 Agent、列出 topic、列出 Broker、加入 topic、退出 topic 和发送消息。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/permission_test.go` 的 `TestREQBrokerConfig023_NoPermissionStillWorksOnSystemBroker`

### REQ-broker-config-024

未允许降级运行而启动的 Broker，在配置或启动参数给出的监听地址无法绑定时必须启动失败并报告该地址不可用，不得改用其他地址或端口。

- **验收**：先让另一个进程在本机全部地址的 17443 端口上监听，再以默认配置在终端执行 `peers service run`：启动失败，退出码非零，错误指出地址 `:17443` 不可用，主机上没有留下这个 Broker 的进程，也没有它在其他端口上的监听。
- **状态**：已实现
- **端到端测试**：`test/e2e/broker_process_test.go` 的 `TestBrokerProcessInContainer/REQ-broker-config-024 default address taken`

### REQ-broker-config-025

启动 Broker 时必须能指定监听地址，以取代配置中的值。

- **验收**：配置保持默认，启动时指定监听 `:17444`：Broker 在本机全部地址的 17444 端口上监听，不在 17443 端口上监听，配置文件的内容没有变化。
- **状态**：已实现
- **端到端测试**：`cmd/peers/broker_process_test.go`: `TestBrokerListenFlagOverridesConfig`

### REQ-broker-config-026

会话尚未选定 Broker 时，存在多个可用的 Broker，MCP 服务进程必须按以下顺序选择第一个可用的：本账户下正在运行的用户级 Broker；本账户有权使用的系统级 Broker。

- **验收**：主机上同时运行着系统级 Broker 和账户甲的用户级 Broker，系统级 Broker 监听 `:17443`，甲的用户级 Broker 监听 `:17444`。甲的会话登记在甲的用户级 Broker 上；甲的用户级 Broker 停止后新开的会话登记在系统级 Broker 上；没有用户级 Broker 的账户乙的会话登记在系统级 Broker 上。会话的环境中设置已被移除的旧版变量 `AGENT_PEER_SOCKET`，不影响选择。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/select_reregister_test.go` 的 `TestSelectOrderReq026`

### REQ-broker-config-027

系统级 Broker 的管理员必须能以文件权限限定哪些账户可以使用它。

- **验收**：管理员把系统级 Broker 的使用权限限定到某个组。组内账户的会话照常登记在它上面；组外账户已在自己的配置中把监听地址设为 `:17444`，它的会话不能登记在系统级 Broker 上，按 `REQ-broker-config-038` 返回说明没有可用 Broker 的错误；该账户启动自己的用户级 Broker 后，会话登记在它上面。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/socket_group_test.go` 的 `TestREQBrokerConfig027_GroupRestrictsUse`、`TestSystemSocketGroupReappliedOnReload`、`TestSystemSocketGroupUnknownRejected`

### REQ-broker-config-028

Broker 在未经配置时必须在本机全部地址的 17443 端口上监听，启用了 IPv6 时包括 IPv6 地址；把监听地址配置为空时不监听。

- **验收**：在启用了 IPv6 的主机上以默认配置启动 Broker，另一台主机经该主机的 IPv4 地址和 IPv6 地址都能连到它的 17443 端口；把监听地址配置为空后重新启动，它不在任何端口上监听，仍能主动连接由它发起配对的对端。
- **状态**：已实现
- **端到端测试**：`test/e2e/addresses_test.go` 的 `TestAdvertisedAddresses/REQ-broker-config-028 dual stack and no listen`

### REQ-broker-config-029

以启动参数给定监听地址的运行期间，经工具修改监听地址的调用必须失败并说明原因，配置文件不得被改动。

- **验收**：以启动参数指定监听 `:17444` 启动 Broker，经工具把监听地址改为 `:17445`：调用返回错误，说明本次运行的监听地址由启动参数给定；配置文件的内容没有变化，Broker 仍在 17444 端口上监听。不带该参数重新启动后，同样的修改成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_test.go` 的 `TestConfigTools/ListenFlagBlocksSet`

### REQ-broker-config-030

Broker 监听全部地址且未配置含主机的对外地址时，对外地址列表必须只包含每个默认路由所在接口的地址，以及用户配置追加的地址。

- **验收**：B 以默认配置运行，所在主机默认 IPv4 路由所在接口的地址是 `192.0.2.20`，另有一个虚拟网络接口，地址是 `10.211.55.2`，没有默认 IPv6 路由。A 凭 B 的地址和 B 生成的配对密钥与 B 配对后，A 列出 Broker 的结果中 B 的对外地址列表只有 `192.0.2.20:17443`。为 B 的主机加上默认 IPv6 路由，该路由所在接口的全局地址是 `2001:db8::20` 后，列表变为 `192.0.2.20:17443` 与 `[2001:db8::20]:17443`，仍不含 `10.211.55.2`；把 B 的对外地址配置为只含端口的 `:18443` 后，列表中的端口都变为 18443；再为 B 追加对外地址 `b.vpn.example`，列表末尾多出 `b.vpn.example:18443`。
- **状态**：已实现
- **端到端测试**：`test/e2e/addresses_test.go` 的 `TestAdvertisedAddresses/REQ-broker-config-030 default route addresses`

### REQ-broker-config-031

配置了含主机的对外地址时，对外地址列表必须以该地址为首项，其后只有用户配置追加的地址。

- **验收**：公开 Broker B 在 17443 端口上监听，把对外地址配置为 `b.example:443`：B 的拨号方列出 Broker 的结果中 B 的对外地址列表首项是 `b.example:443`，持有 B 的名片的其他 Broker 列出的同样如此，列表中没有默认路由所在接口的地址。把对外地址改为只含主机的 `b.example` 后，列表首项是 `b.example:17443`；再追加对外地址 `b.vpn.example`，列表依次是 `b.example:17443` 与 `b.vpn.example:17443`。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_address_test.go` 的 `TestCardShowsConfiguredAddresses`

### REQ-broker-config-032

监听地址含具体主机而对外地址只含端口时，或监听地址为空而对外地址、追加的对外地址非空时，Broker 必须拒绝启动并指出这两项配置；运行期间的此类修改必须失败并保留原值。

- **验收**：把监听地址配置为 `192.0.2.20:17443`、对外地址配置为只含端口的 `:18443` 后启动 Broker，启动失败，错误同时指出监听地址与对外地址这两项配置。以监听 `192.0.2.20:17443`、不配置对外地址运行时，经工具把对外地址改为 `:18443`，调用返回错误，再读取配置得到原值；在配置文件中做同样的修改，Broker 不采用，读取配置仍得到原值。监听地址配置为空而对外地址或追加的对外地址非空时，结果相同。
- **状态**：已实现
- **端到端测试**：运行期间经工具修改由 `test/e2e/config_tools_test.go` 的 `TestConfigTools/RuntimeAddressRulesThroughTools` 覆盖，配置文件路径由 `TestConfigTools/FileEditRejectsCombination` 覆盖，监听地址为空时工具与配置文件两条路径由 `TestConfigTools/EmptyListenRejectsAddresses` 覆盖；启动失败一半由 `test/e2e/addresses_test.go` 的 `TestAdvertisedAddresses/REQ-broker-config-032 and 036 startup half` 覆盖

### REQ-broker-config-033

用户必须能配置追加的对外地址，追加的地址出现在对外地址列表中。

- **验收**：以默认配置运行的 Broker B 追加对外地址 `b.vpn.example` 与 `198.51.100.7:8443` 后，B 的拨号方列出 Broker 的结果中，B 的对外地址列表在默认路由所在接口的地址之后依次是 `b.vpn.example:17443` 与 `198.51.100.7:8443`；B 设为公开后，持有 B 的名片的其他 Broker 列出的是同样的列表。
- **状态**：已实现
- **端到端测试**：`test/e2e/discovery_address_test.go` 的 `TestCardShowsConfiguredAddresses`

### REQ-broker-config-034

运行期间把监听地址改为无法绑定的地址时，修改必须失败，Broker 必须继续在原来的监听地址上监听。

- **验收**：Broker 在 `:17444` 上监听，另一个进程占用着本机全部地址的 17445 端口；经工具把监听地址改为 `:17445`，调用返回说明该地址不可用的错误，再读取配置得到 `:17444`，另一台主机仍能连到它的 17444 端口；在配置文件中做同样的修改，结果相同。
- **状态**：已实现
- **端到端测试**：`test/e2e/config_tools_test.go` 的 `TestConfigTools/UnbindableListen`（工具与配置文件两条路径；本机拨号）

### REQ-broker-config-035

Broker 监听特定地址且未配置对外地址时，对外地址列表必须以监听地址为首项，其后只有用户配置追加的地址。

- **验收**：把监听地址配置为 `192.0.2.20:17443`、不配置对外地址，B 的拨号方列出 Broker 的结果中 B 的对外地址列表只有 `192.0.2.20:17443`；再追加对外地址 `b.vpn.example`，列表依次是 `192.0.2.20:17443` 与 `b.vpn.example:17443`。
- **状态**：已实现
- **端到端测试**：`test/e2e/addresses_test.go` 的 `TestAdvertisedAddresses/REQ-broker-config-035 specific listen`

### REQ-broker-config-036

对外地址的配置使对外地址列表超过 8 项时，Broker 必须拒绝启动并指出该配置项；运行期间的此类修改必须失败并保留原值。

- **验收**：配置含主机的对外地址，并追加 8 项对外地址后启动 Broker，启动失败，错误指出追加的对外地址这一项；以只追加 7 项的配置运行时，经工具再追加 1 项，调用返回错误，再读取配置得到原值。
- **状态**：已实现
- **端到端测试**：运行期间经工具修改由 `test/e2e/config_tools_test.go` 的 `TestConfigTools/RuntimeAddressRulesThroughTools` 覆盖，配置文件路径由 `TestConfigTools/FileEditRejectsAddressOverflow` 覆盖；启动失败一半由 `test/e2e/addresses_test.go` 的 `TestAdvertisedAddresses/REQ-broker-config-032 and 036 startup half` 覆盖

### REQ-broker-config-037

会话所登记的 Broker 进程退出后，系统必须把该会话重新登记到同一个 Broker，不得改登记到另一个 Broker。

- **验收**：主机上同时运行着系统级 Broker 和账户甲的用户级 Broker，两者监听不同的端口，甲的会话登记在用户级 Broker 上；终止用户级 Broker 进程，在它重新启动之前，会话的列出 Agent 返回说明没有可用 Broker 的错误，主机上不出现别的 Broker；它由用户级的服务管理机制或用户重新启动之后，不重新连接 MCP 服务，会话的下一次列出 Agent 成功，会话仍登记在指纹不变的这个用户级 Broker 上，系统级 Broker 上不出现该会话的 Agent。账户乙的会话登记在系统级 Broker 上时，终止系统级 Broker 进程，在它由系统的服务管理机制重新启动之前，主机上不出现乙的用户级 Broker；重新启动之后，乙的会话重新登记在系统级 Broker 上。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/select_reregister_test.go` 的 `TestReregisterUserBrokerReq037`、`TestReregisterSystemBrokerReq037`

### REQ-broker-config-043

安装系统级服务时，系统必须提供一个管理组：组不存在时创建，新生成的配置文件以该组为属组并授予组读写权限。管理组成员不经 root 或 sudo，即可执行需要配置文件读写权限的全部操作。用发布包内的安装脚本安装系统级服务时，脚本必须询问是否把执行安装的管理员账户加入管理组，默认不加入；无法询问时按不加入处理并给出加入的命令。

- **验收**：在全新主机上以普通账户执行 `sh ./install.sh`，对级别询问回答 y 并输入 sudo 密码，对管理组询问回答 y，执行 sudo 的账户重新登录后，不经 sudo 在终端成功执行生成配对密钥、修改配置项、发起配对，审计记录的是该账户；对询问直接回车或在没有终端时执行，该账户不加入管理组，输出给出加入管理组的命令；带 `--join-admin-group` 执行时不询问直接加入；不在组内的普通账户执行同样的命令返回权限不足。配置文件已存在时重新安装，文件的属组与权限不变。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/admin_group_test.go` 的 `TestREQBrokerConfig043_SystemInstallAdminGroup`（测试环境以 NOPASSWD 配置 sudo；“输入 sudo 密码”一步由静态阅读安装脚本与 sudo 重新执行的用例共同覆盖）

### REQ-broker-config-044

不带 `--system` 与 `--user`、能在终端询问的非 root 安装，必须询问 `Install for every account on this host? [y/N]`，回答 y 时经 sudo 完成系统级安装，其他回答完成用户级安装。

- **验收**：在没有安装过本系统的主机上，以普通账户执行 `curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh`：出现这句询问；回答 y 并输入 sudo 密码后，系统级 Broker 正在运行，二进制位于 root 所有的目录，执行安装的账户在 Claude Code 中已注册 MCP 服务；在另一台主机上直接回车，该账户的用户级 Broker 正在运行，主机上没有系统级服务。解压发布包后执行 `sh ./install.sh`，结果相同。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/install_script_test.go` 的 `TestREQBrokerConfig044_InstallScriptLevelPromptAcceptsSystemLevel`、`TestREQBrokerConfig044_InstallScriptLevelPromptDefaultsToUser`；`test/e2e/system/remote_install_test.go` 的 `TestREQBrokerConfig042_RemoteInstallSystem`

### REQ-broker-config-045

以 root 执行的安装必须不经询问完成系统级安装。

- **验收**：以 root 执行一条在线安装命令，不带任何参数，安装过程不出现级别询问，结束后系统级 Broker 正在运行，主机上没有 root 的用户级服务。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/install_script_test.go` 的 `TestREQBrokerConfig045_InstallScriptRootDoesNotAsk`

### REQ-broker-config-046

无法在终端询问的非 root 安装必须不经询问完成用户级安装。

- **验收**：以普通账户在没有控制终端的环境中执行一条在线安装命令，例如经 `setsid` 并把标准输入接到 `/dev/null`：命令不等待输入，结束后该账户的用户级 Broker 正在运行，主机上没有系统级服务。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/install_script_test.go` 的 `TestREQBrokerConfig046_InstallScriptWithoutTerminal`

### REQ-broker-config-047

主机上已安装系统级服务时，`REQ-broker-config-044` 的询问必须改为 `Upgrade the system installation? [Y/n]`，直接回车或回答 y 时经 sudo 升级系统级安装；回答 n 时不得改动主机，命令以 0 退出，并说明系统安装没有改动。

- **验收**：在已安装旧版本系统级服务的主机上，以普通账户执行一条在线安装命令：询问为这一句；直接回车并输入 sudo 密码后，系统级 Broker 以新版本运行，配置文件内容不变。在另一台同样的主机上回答 n：命令以 0 退出，二进制、服务与配置都没有变化，输出说明系统安装没有改动，不提示改用 `--user`。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/install_script_test.go` 的 `TestREQBrokerConfig047_InstallScriptUpgradePrompt`

### REQ-broker-config-048

带 `--user` 的安装必须不经询问完成用户级安装；带 `--system` 的安装必须不经级别询问完成系统级安装。

- **验收**：以普通账户执行 `curl ... | sh -s -- --user`，不出现级别询问，结束后用户级 Broker 正在运行；以普通账户执行 `curl ... | sh -s -- --system`，不出现级别询问，脚本经 sudo 完成系统级安装；同时带两者时以用法错误退出，主机上没有改动；以 root 执行带 `--user` 的安装被拒绝，提示去掉 `--user`。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/install_script_test.go` 的 `TestREQBrokerConfig048_InstallScriptBothFlags`、`TestREQBrokerConfig048_InstallScriptUserFlag`、`TestREQBrokerConfig048_InstallScriptSystemFlag`

### REQ-broker-config-049

安装、升级与卸载失败时，输出必须写明具体原因，不得只说连不上或没有启动。

- **验收**：分别制造下列失败，每种情况命令都以非零退出，输出含括号中的内容：监听地址被另一个进程占用（地址、配置文件路径与配置项 `broker.listen`）；`systemctl` 或 `launchctl` 返回失败（执行的命令与它的错误输出）；对安装目录或配置文件没有权限（路径与所缺的权限）；Broker 在启动阶段退出（运行日志中 `startup_failed` 记录的原因）。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/install_port_conflict_test.go` 的 `TestSystemServiceInstallListenPortConflict`、`test/e2e/darwinservice/install_port_test.go` 的 `TestUserServiceInstallRefusesAnOccupiedListenPort`（地址被占时错误含地址、配置文件路径与 `broker.listen`）；其余失败路径由单元测试覆盖：`internal/broker/service/install_test.go` 的 `TestInstallLinuxStepFailures`、`TestInstallDarwinBootstrapFailureReportsTheOutput`、`TestCreateConfigRemovesPartialFileOnWriteFailure`、`TestInstallReportsTheStartupFailureOnTimeout`、`TestInstallReportsTheServiceManagerStateOnTimeout`（命令失败、权限不足、启动阶段退出与服务管理器状态），`cmd/peers/uninstall_cmd_test.go` 的 `TestUninstallStopsAfterAFailedStep`、`TestUninstallReportsPathsRemovedBeforeAPurgeFailure`（卸载失败的步骤、路径与原因），`internal/broker/update/apply_test.go` 的 `TestApplyFailureLeavesTargetUnchanged` 与 `api_test.go` 的 `TestLatestRejectsAPIError`、`TestLatestRejectsInvalidContent`（升级失败的原因）

### REQ-broker-config-050

安装或升级成功后，输出必须给出安装级别、二进制路径、版本、配置文件路径、运行日志的位置、审计日志路径、启动、停止与重启 Broker 的命令，以及卸载命令。

- **验收**：分别完成一次用户级安装与一次系统级安装，输出中逐项出现上述内容，其中的路径在主机上存在，给出的启动、停止、重启与卸载命令照抄执行都成功。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/install_summary_test.go` 的 `TestREQBrokerConfig050_SystemServiceInstallPrintsSummary`、`TestREQBrokerConfig050_UserServiceInstallPrintsSummary`（Linux：系统级与用户级，路径存在与沿用配置，并照抄汇总里的 start、stop、restart、uninstall 命令逐条执行成功）；`test/e2e/darwinservice/install_summary_test.go` 的 `TestREQBrokerConfig050_UserServiceInstallPrintsSummary`、`TestREQBrokerConfig050_SystemServiceInstallPrintsSummary`（macOS：用户级与系统级，同一组断言与照抄执行）；`test/e2e/system/install_script_test.go` 的 `TestREQBrokerConfig050_InstallScriptUserSummary`、`TestREQBrokerConfig050_InstallScriptSystemSummary`（Linux：经发布包内的安装脚本完成用户级与系统级安装，汇总出现在脚本输出里，路径存在并照抄执行）

### REQ-broker-config-051

`peers uninstall` 必须判别已安装的级别，并删除该级别的服务定义、服务定义所启动的二进制、执行卸载的账户在 Claude Code 中的 MCP 注册、该级别的运行时目录，以及安装脚本写入的补全文件；系统级还要删除本项目的 man 页。

- **验收**：在完成用户级安装的主机上执行 `peers uninstall` 并对数据询问直接回车：之后该账户的 Broker 进程不存在，服务定义文件、二进制、运行时目录都不存在，`claude mcp get peer` 报告未注册；配置文件、身份、审计日志与服务日志仍在。对系统级安装执行，结果相同。主机上同时有系统级与用户级安装时，非 root 账户只卸载用户级，结束后输出说明主机上仍有系统级安装及 `sudo peers uninstall`。再执行一次，命令成功并报告没有安装。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/uninstall_test.go` 的 `TestUninstallREQBrokerConfig051UserLevel`、`TestUninstallREQBrokerConfig051SystemLevel`、`TestUninstallREQBrokerConfig051NoService`、`TestUninstallREQBrokerConfig051UnrecordedGroup`；`test/e2e/darwinservice/uninstall_test.go` 的 `TestUninstallREQBrokerConfig051UserLevel`

### REQ-broker-config-052

非 root 账户只有系统级安装时，`peers uninstall` 必须经 sudo 重跑：给了 `--yes` 时直接重跑；有终端时先询问，回答 y 后重跑。没有终端又没有 `--yes` 时报错并以状态 1 退出，错误信息给出可以直接重跑的 `sudo peers uninstall ... --yes`。

- **验收**：主机上只有系统级安装，以普通账户在没有终端的情况下执行 `peers uninstall`：命令报错，状态为 1，错误信息含 `sudo peers uninstall` 与 `--yes`，主机上没有改动；照抄这条命令执行并输入 sudo 密码后，系统级安装按 `REQ-broker-config-051` 删除。在终端上执行同一命令，回答 y 后命令经 sudo 重跑，系统级安装同样删除；以 `--yes` 执行时不提问，直接经 sudo 重跑。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/uninstall_test.go` 的 `TestUninstallREQBrokerConfig052NonRootSystemOnly`、`TestUninstallREQBrokerConfig052TwoLevels`

### REQ-broker-config-053

卸载必须询问 `Also remove configuration, identity, audit and service logs? [y/N]`，只有回答 y 时才删除这些数据；`--yes` 跳过询问并保留这些数据。系统级安装同时删除其安装记录能够证明由本项目创建的管理组；没有安装记录时保留管理组，并输出保留原因与手动删除命令。不带 `--purge` 的卸载在其他情况下不得删除它们。没有终端又没有 `--yes` 时，命令报错并以状态 1 退出，不做任何删除，`--purge` 也一样。

- **验收**：在系统级安装的主机上卸载，对这句询问直接回车：配置文件、身份与对端表、审计日志都在，管理组仍存在；重新安装后，Broker 的指纹与卸载前相同，原有对端仍是对端。另一台主机上回答 y：这些数据都不存在；有安装记录时管理组也不存在，没有记录时管理组保留并输出保留原因与手动删除命令。没有终端又没有 `--yes` 时，命令以状态 1 退出，不做任何删除。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/uninstall_test.go` 的 `TestUninstallREQBrokerConfig051UserLevel`、`TestUninstallREQBrokerConfig051SystemLevel`（保留数据与管理组的断言）

### REQ-broker-config-054

`peers uninstall --purge` 必须不经数据询问删除 `REQ-broker-config-051` 与 `REQ-broker-config-053` 所列的全部内容；删除后这些路径下都没有本系统的文件或目录，服务定义、二进制与 MCP 注册都不存在；管理组只在安装记录证明由本项目创建时删除，没有记录时保留并输出保留原因与手动删除命令。

- **验收**：在分别完成用户级与系统级安装、配对过一个对端、产生过审计记录的主机上执行 `peers uninstall --purge`：命令不询问数据去留；之后配置文件与配置目录、数据目录、审计日志、运行时目录都不存在，服务管理器中查不到 peers 服务，`claude mcp get peer` 报告未注册；有安装记录时 `getent group peers`（macOS 上 `dscl . -read /Groups/peers`）报告不存在，没有记录时管理组保留并输出保留原因与手动删除命令。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/uninstall_test.go` 的 `TestUninstallREQBrokerConfig051UserLevel`、`TestUninstallREQBrokerConfig051SystemLevel`、`TestUninstallREQBrokerConfig051UnrecordedGroup`、`TestUninstallREQBrokerConfig054OtherAccountBroker`

### REQ-broker-config-055

用户必须能用 `peers service start`、`stop` 与 `restart` 启动、停止与重启 Broker 服务，作用的级别由 `--system` 或 `--user` 指定，两者互斥；都不给时按执行命令的账户确定：root 作用于系统级服务，其他账户作用于本账户的用户级服务。没有安装该级别的服务时，命令必须失败并说明该级别没有安装服务；非 root 账户只有系统级安装时，命令必须失败并给出 `sudo peers service <命令> --system`。`start` 与 `restart` 之后服务在运行而 Broker 处于降级运行时，命令必须输出故障与修复命令，并以退出码 3 结束。

- **验收**：在同时有系统级服务与账户甲的用户级服务的主机上，甲执行 `peers service stop`，甲的用户级 Broker 停止，系统级 Broker 不受影响；甲执行 `sudo peers service restart`，系统级 Broker 以新的进程号运行，甲的用户级 Broker 仍是停止状态；甲执行 `peers service start`，用户级 Broker 重新运行。没有安装该级别的服务时，命令以非零退出并说明该级别没有安装服务；甲在只有系统级服务的主机上执行，命令以 1 退出并给出 `sudo peers service <命令> --system`。先停止甲的用户级服务，让另一个进程占用配置中的监听端口，甲执行 `peers service start`：输出降级原因与修复命令，退出码为 3，服务处于运行状态。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/service_control_test.go` 的 `TestServiceControl`（`stop and start act on one level only`、`a user command with only the system service names sudo`、`no installed service names the level`、`start reports a degraded Broker and keeps the service running`；最后一例保留 `service start` 的退出码 3，并断言 `peers service status` 的 stdout 含 `Broker: degraded (local only):` 与 `Fix:`）；`test/e2e/darwinservice/control_test.go` 的 `TestUserServiceControl`、`TestSystemServiceControl`、`TestServiceControlNotInstalled`、`TestUserServiceControlDegraded`（同样断言 `peers service status` 的 stdout 含状态行与 `Fix:`）

### REQ-broker-config-056

不带 `--allow-degraded` 启动的 Broker，在启动的任何一步失败时必须在标准错误输出原因并以非零退出码结束。

- **验收**：在终端以 `peers service run` 分别制造配置文件无法解析、监听地址被占用、身份文件不可读三种失败，每次进程都退出，退出码非零，标准错误的最后一行写明原因，运行时目录中没有留下本机 socket。
- **状态**：已实现
- **端到端测试**：`test/e2e/degraded_run_test.go` 的 `TestDegradedRun/REQ-broker-config-056 foreground failures exit`

### REQ-broker-config-057

带 `--allow-degraded` 启动的 Broker 取得账户锁之后，监听地址无法绑定，或数据目录、审计日志不可用时，必须不退出，并在本机 socket 上应答；配置文件读取或解析失败、取值不合法，以及其他与数据目录无关的启动失败，仍必须像不带该参数时一样退出。

- **验收**：以 `peers service run --allow-degraded` 启动，把数据目录中的身份文件改成当前账户不可读，或让审计日志所在目录不可写：进程持续运行，`peers service status` 能连上它并输出故障；监听地址被占用时同样如此。把配置文件写成无法解析的内容再以同一个参数启动，进程以非零退出码结束，与不带该参数时相同。
- **状态**：已实现
- **端到端测试**：`test/e2e/degraded_run_test.go` 的 `TestDegradedRun/REQ-broker-config-057 diagnostics entry and recovery`

### REQ-broker-config-058

降级运行中只有监听地址无法绑定时，Broker 必须照常提供本机的登记、列出、topic 与本机消息功能，生成配对密钥必须失败，错误写明无法绑定的地址与配置项 `broker.listen`。系统级 Broker 的监听端口小于 1024 而配置文件可被其他账户写入时，按本条处理。

- **验收**：另一个进程占用 17443 端口时以 `--allow-degraded` 和默认配置启动 Broker：两个会话登记成功、加入同一 topic 后互发消息成功；会话调用 `create_pairing_key` 返回 `broker_degraded`，说明含 `:17443` 与 `broker.listen`；本机不在任何端口上监听；`peers service status` 输出该故障；向本方作为接受方的对端发送时返回 `peer_unreachable`，说明末尾附降级原因。另把系统级 Broker 的监听端口设为 1024 以下并让配置文件可被组写入，启动后同样进入仅本机降级，`peers service status` 的修复提示含 `chmod g-w` 或改用 1024 以上的端口。
- **状态**：已实现
- **端到端测试**：`test/e2e/degraded_run_test.go` 的 `TestDegradedRun/REQ-broker-config-058 local only keeps local features`；peer_unreachable 附带降级原因由单元测试 `internal/broker/degradedrpc_test.go` 的 `TestPeerUnreachableReasonOnlyForAcceptor` 覆盖；低端口修复提示含 `chmod g-w` 由单元测试 `internal/broker/degradedstate_test.go` 的 `TestDegradedFixHints` 覆盖

### REQ-broker-config-059

降级运行中数据目录或审计日志不可用时，`peers service status` 必须输出故障与修复提示，会话中每个工具调用都必须返回 `broker_degraded` 及原因。

- **验收**：以 `--allow-degraded` 启动、审计日志所在目录不可写时，`peers service status` 输出审计日志的路径、原因与修复方法，退出码非零；会话中调用 `list_agents`、`send_message`、`get_config` 都返回 `broker_degraded`，说明与 `peers service status` 输出的原因一致；会话不需要重新连接 MCP 服务。
- **状态**：已实现
- **端到端测试**：`test/e2e/degraded_run_test.go` 的 `TestDegradedRun/REQ-broker-config-059 diagnostics from the audit log`

### REQ-broker-config-060

带 `--allow-degraded` 的 Broker 在取得账户锁之前失败时，必须与不带该参数时一样退出。

- **验收**：以 `--allow-degraded` 分别在会话的进程树内启动、以非 root 带 `--system` 启动、在已有实例持有账户锁时启动，三种情况进程都以 `REQ-broker-config-056` 的方式退出，原有实例不受影响。
- **状态**：已实现
- **端到端测试**：`test/e2e/degraded_run_test.go` 的 `TestDegradedRun/REQ-broker-config-060 before-lock failures exit`

### REQ-broker-config-061

降级运行中，Broker 必须在配置文件发生变化时立即重试失败的那一步，并每分钟重试一次；重试成功后不重启进程即退出降级运行。重试失败时只在原因发生变化时写运行日志。

- **验收**：以 `--allow-degraded` 启动、审计日志所在目录不可写；恢复该目录的权限后不重启 Broker，60 秒内 `peers service status` 不再报告故障。另以默认配置启动、17443 端口被占用；把 `broker.listen` 改为空闲端口后，不重启 Broker，60 秒内 `peers service status` 不再报告故障，Broker 在新端口上监听，进程号不变。
- **状态**：已实现
- **端到端测试**：`test/e2e/degraded_run_test.go` 的 `TestDegradedRun/REQ-broker-config-059 diagnostics from the audit log`、`TestDegradedRun/REQ-broker-config-061 recovery listens on the new port`

### REQ-broker-config-062

降级运行中监听地址无法绑定时，Broker 必须以不超过 1 分钟的间隔重试绑定，地址可用后不重启进程即开始监听。

- **验收**：以 `--allow-degraded` 启动时 17443 端口被占用；结束占用该端口的进程，不改配置，2 分钟内另一台主机能连上该 Broker 的 17443 端口，`peers service status` 不再报告故障，Broker 的进程号不变。
- **状态**：已实现
- **端到端测试**：`test/e2e/degraded_run_test.go` 的 `TestDegradedRun/REQ-broker-config-062 listen retry reaches the address`

### REQ-broker-config-063

`peers service install` 生成的服务定义必须以允许降级运行的方式启动 Broker。

- **验收**：分别安装用户级与系统级服务，Linux 上单元文件的 `ExecStart` 与 macOS 上 plist 的 `ProgramArguments` 都含 `--allow-degraded`；在已完成安装的主机上停止服务，让另一个进程占用配置中的监听端口，再执行 `peers service start`：Broker 进入降级运行，服务管理器报告服务处于运行状态，至少 30 秒（`RestartSec=5` 的 6 倍）内进程 pid 不变、服务管理器不重启它，`peers service status` 报告降级运行。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/service_control_test.go` 的 `TestServiceControl/start reports a degraded Broker and keeps the service running`（并断言用户级与系统级单元文件含 `--allow-degraded`，保留 `service start` 的退出码 3，并断言 `peers service status` 的 stdout 含 `Broker: degraded (local only):` 与 `Fix:`）；`test/e2e/darwinservice/control_test.go` 的 `TestUserServiceControlDegraded`（并断言 plist 含 `--allow-degraded`，同样的状态行断言）；服务定义模板的两级内容另由 `internal/broker/service/units_test.go` 覆盖；终端状态行的逐字输出由“终端与会话显示降级与下线”单元覆盖

### REQ-broker-config-064

服务注册之后 Broker 进入降级运行时，`peers service install` 必须保留已安装并在运行的服务，输出故障与修复命令，并以退出码 3 结束。注册之前预检发现监听地址被占用时，安装必须拒绝并不注册服务。

- **验收**：在已完成安装的主机上让审计日志所在目录不可写，再执行 `peers service install`：命令以 3 退出，输出写明原因、路径与修复命令；服务定义与二进制都在，服务管理器报告服务处于运行状态，`peers service status` 报告降级运行。另在安装前让 17443 端口被另一个进程占用：安装失败并说明地址不可用，服务定义没有写入，主机上没有本服务。
- **状态**：已实现
- **端到端测试**：`test/e2e/system/install_summary_test.go` 的 `TestREQBrokerConfig064_ReinstallReportsDegradedAuditLog`、`test/e2e/system/install_port_conflict_test.go` 的 `TestSystemServiceInstallListenPortConflict/an occupied port fails the install and registers nothing`（Linux：审计日志不可用后的降级重装与预检端口占用）；`test/e2e/darwinservice/install_summary_test.go` 的 `TestREQBrokerConfig064_ReinstallReportsDegradedAuditLog`、`test/e2e/darwinservice/install_port_test.go` 的 `TestUserServiceInstallRefusesAnOccupiedListenPort`（macOS：同一验收）

### REQ-broker-config-065

Broker 的下线状态必须保存在配置中，Broker 重启后保持重启前的状态。

- **验收**：把 Broker 设为下线后重启它，`peers service status` 仍报告下线，对端向它的 Agent 发送仍被拒绝；在配置文件中把 `broker.offline` 改为 `false`，不重启，对端的发送成功，审计日志新增一条该配置项的变更记录。
- **状态**：已实现
- **端到端测试**：`test/e2e/offline_test.go` 的 `TestBrokerOffline/REQ-broker-config-065 state survives restart and a file edit`

### REQ-broker-config-066

用户必须能在终端执行 `peers service offline` 与 `peers service online` 使 Broker 下线与上线，权限与修改配置相同。

- **验收**：以对配置文件有写权限的账户执行 `peers service offline`，命令成功，`peers service status` 报告下线；执行 `peers service online` 后报告上线；以没有写权限的账户执行，返回权限不足并给出配置文件路径，状态不变；审计日志记录每次变化与执行的账户。
- **状态**：已实现
- **端到端测试**：`test/e2e/offline_test.go` 的 `TestBrokerOffline/REQ-broker-config-066 terminal commands`；`test/e2e/system/permission_test.go` 的 `TestREQBrokerConfig021_WriteNeedsConfigWritePermission`（没有写权限时 `offline` 与 `online` 返回 `permission_denied` 并给出配置文件路径，配置文件不变）

### REQ-broker-config-067

Agent 必须能用工具 `set_broker_online` 使所连 Broker 下线或上线，该工具必须能独立于修改配置获得授权。

- **验收**：会话调用 `set_broker_online`，参数 `online` 为假，调用成功，Broker 下线；为真时上线。在 Claude Code 权限设置中只放行该工具时，调用它不触发确认，`set_config` 触发确认。对配置文件没有写权限的账户调用返回 `permission_denied`。工具声明不带只读标记。
- **状态**：已实现
- **端到端测试**：`test/e2e/offline_test.go` 的 `TestBrokerOffline/REQ-broker-config-067 tool changes the state`；`test/e2e/system/permission_test.go` 的 `TestREQBrokerConfig021_WriteNeedsConfigWritePermission`（没有写权限时返回 `permission_denied`）；`test/e2e/config_tools_test.go` 的 `TestConfigToolAnnotations`（声明为非只读）。Claude Code 按工具单独授权、只放行它时不触发确认，属于 Claude Code 的权限行为，不在端到端中验证，由 `internal/mcpserver/tools_test.go` 的工具声明测试覆盖声明层面

### REQ-broker-config-068

本机终端查看状态与列出 Broker 时，必须显示本 Broker 是否下线。

- **验收**：Broker 下线后，`peers service status` 与 `peers broker list` 的输出各有一行说明本 Broker 已下线及上线的命令，会话中 `list_brokers` 的结果带本 Broker 的下线状态；上线后这些说明消失。
- **状态**：已实现
- **端到端测试**：`test/e2e/offline_test.go` 的 `TestBrokerOffline/REQ-broker-config-068 status shows the state`（`Status.offline`、`ListBrokers.local.offline`，以及 `peers service status` 与 `broker list` 的下线状态行；上线后状态行消失）

### REQ-broker-config-069

同一来源在合并窗口内被拒绝的未认证连接，不论对方更换多少证书，审计日志中只新增一条记录；同一时段内被拒绝的来源再多，审计日志新增的记录数也有上限。

- **验收**：一个未配对的对端在同一来源地址上用五张不同证书各发起一次连接，每次都被拒绝；随后读取审计日志，只有一条 `connection_rejected` 记录，`count` 为 5，`detail` 带该来源的前缀。伪造大量不同来源的连接时，一个 10 分钟窗口内所有可合并事件合计写入审计日志的普通记录不超过 256 条，超出部分合并为一条 `detail` 为 `other sources` 的汇总记录。
- **状态**：已实现
- **端到端测试**：`test/e2e/security_test.go` 的 `TestREQBrokerConfig069_RejectedConnectionsMergeBySource`

## 数据需求

Broker 的配置和审计日志保存在本机磁盘上，谁能读、谁能改由它们的文件权限决定。用户级 Broker 的配置文件和审计日志属于运行它的账户，只有该账户可以读写。系统级 Broker 的配置文件默认只有 root 与管理组可以读写。审计日志只追加，不由系统删除。

每个 Broker 实例各有自己的配置和审计日志，同一台主机上的多个 Broker 互不共用。Broker 的下线状态保存在配置文件中；卸载删除哪些数据由 `REQ-broker-config-051` 至 `REQ-broker-config-054` 规定。

## 外部接口需求

- Claude Code：工具调用是否需要用户确认，由 Claude Code 依据会话的权限设置决定。
- 操作系统：调用方属于哪个账户、该账户对配置文件有什么权限，以操作系统给出的为准，不以调用方的陈述为准。系统级 Broker 由 macOS 或 Linux 的系统服务管理机制启动，用户级 Broker 安装为服务时由用户级的服务管理机制启动。

## 约束

- 默认配置在本机全部地址的 17443 端口上监听；17443 只是默认配置中的取值，系统不对这个端口作任何区别对待。
- 同一台主机上的第二个 Broker 按默认配置启动时，监听地址已被占用：新装或重装时，预检发现地址被其他进程占用，安装失败且不注册服务（`REQ-broker-config-064`）；已经装好的服务之后再启动，服务进入仅本机降级，在前台启动则启动失败。降级运行只能提供本机功能，需要用户或管理员为它指定另一个地址或端口。

## 假设与依赖

- 系统识别的是发起调用的进程所属的账户，不是人，也不是模型。同一账户下的任何进程都能以该账户的权限调用 Broker。
- 账户对配置文件有写权限，并且把修改配置、生成配对密钥、发起配对或升级 Broker 的工具加入了 Claude Code 的自动批准名单时，这些操作不再有任何确认。审计日志是此时的事后依据。
- 会话中的模型能以所在会话拥有的文件访问权限直接读写该账户有权访问的配置文件和审计日志。系统不防范这一途径，由 Claude Code 对文件操作的权限确认承担。
- 生成配对密钥、发起配对和解除配对并不写入配置文件，它们同样以对配置文件的写权限为前提。
- 默认路由所在接口的地址在经过地址转换的网络之外连不上；这种情形需要配置含主机的对外地址，或追加对外地址。
- 升级只替换二进制，不改写服务定义。已安装的服务要重新执行安装，才采用当前版本的服务定义与启动方式。
- 能写 Broker 配置文件的账户可以修改 `broker.offline`，与执行上线、下线命令等价。
