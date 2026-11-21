---
title: 命令行
weight: 2
---

`peers` 二进制负责运行 Broker 和 MCP 服务，并提供终端命令。用 `peers -h`、`peers --help` 或 `peers help` 打印下面的用法。每个命令也接受 `-h` 与 `--help`。不带参数运行 `peers` 会把用法写到标准错误，并以退出码 2 结束。用法错误，包括未知命令、未知选项与参数个数不对，同样以退出码 2 结束。

```
peers runs an MCP server for one Claude Code session, the local Broker, and the terminal commands that manage pairings and the installation.

Usage:
  peers [flags]
  peers [command]

Pairing:
  broker      Manage the Brokers this host is paired with
  key         Manage the one-time pairing keys

Local Broker:
  config      Read and change the Broker configuration
  service     Run and manage the local Broker service

Installation:
  completion  Generate the autocompletion script for the specified shell
  uninstall   Remove the peers installation
  update      Install the latest release of peers

Flags:
  -h, --help      help for peers
      --version   print the version and exit

Use "peers [command] --help" for more information about a command.
```

## peers --version

打印 `peers <version>` 后退出。构建时未注入版本号的二进制报告 `peers dev`。

## peers broker list

```
peers broker list [flags]
```

把正在运行的 Broker 已知的全部 Broker 列成表格，包括还不是对端的 Broker。列为 `ALIAS`、`FINGERPRINT`、`PEER`（`yes` 或 `no`）、`STATUS`、`DIRECTION` 与 `ADDRESSES`，按别名排序。`ADDRESSES` 列用逗号连接各地址。没有任何 Broker 时输出 `Brokers: none`。本 Broker 下线时，表格之前先输出一行 `Broker: offline; messages from other hosts are refused. Run peers service online to accept them.`。Broker 处于降级运行时，表格之前先输出 `Broker: degraded …` 与 `Fix: …` 两行；仅诊断降级时改为把原因写到标准错误，并以退出码 1 结束。

除 `-h`/`--help` 外没有选项。不需要权限。用法错误以退出码 2 结束，其余失败以退出码 1 结束。

## peers broker connect

```
peers broker connect <address> [flags]
```

把本 Broker 与另一个 Broker 配对，并打印一行 `Paired with <alias> (<fingerprint>)`。

- `address` 写成 `host:port` 或 `hostname(host:port)`。它可以写在选项之前或之后。
- `--key <key>` 是对方 Broker 上生成的一次性配对密钥，或对方配置的预置静态密钥。必须给出，且只接受双横线写法。
- `--alias <alias>` 指定本地别名。对方的自报别名与已有别名冲突、又没有给出本地别名时，配对失败，并提示提供本地别名。

应答中的警告逐条写到标准错误，每行前缀 `warning: `，命令仍以退出码 0 结束。

需要 Broker 配置文件的写权限。用法错误以退出码 2 结束，其余失败以退出码 1 结束。

## peers broker disconnect

```
peers broker disconnect <broker> [flags]
```

解除与一个 Broker 的配对，并打印一行 `Disconnected <broker>`。

- `broker` 写对端的别名或完整的 64 位十六进制指纹。shell 补全会给出已配对 Broker 的别名与指纹。

除 `-h`/`--help` 外没有选项。需要 Broker 配置文件的写权限。用法错误以退出码 2 结束，其余失败以退出码 1 结束。

## peers key create

```
peers key create [flags]
```

在正在运行的 Broker 上生成一次性配对密钥，并输出密钥、失效时间与本 Broker 的对外地址：

```text
Key: <key>
Expires: <RFC 3339 UTC>
Addresses:
  <address>
```

每个地址单独一行，缩进两格。`broker.listen` 为空、且启动时没有给 `--listen` 时，Broker 不监听，会拒绝请求，命令以退出码 1 结束。

除 `-h`/`--help` 外没有选项。需要 Broker 配置文件的写权限。用法错误以退出码 2 结束，其余失败以退出码 1 结束。

## Broker 管理命令

`config` 与 `broker` 两组命令以及 `key create` 连接正在运行的 Broker，不会启动 Broker。它们按执行命令的操作系统账户选择 Broker，没有按级别指定 Broker 的命令行选项：先找本账户正在运行的用户级 Broker，再找本账户有权使用的系统级 Broker。本账户运行着自己的用户级 Broker、又要管理系统级 Broker 时，必须以 `sudo` 执行命令。`peers update` 遵循同一账户选择规则。与正在运行的 Broker 通信的 `service` 命令，即 `status`、`audit`、`online` 与 `offline`，改用 `--system` 与 `--user` 选择级别，规则见各自小节。选择规则与相关设置见[运行系统级 Broker]({{< relref "/docs/guides/system-broker" >}})。

Broker 按执行命令的账户判定每个命令的权限，规则与对应的工具相同。写操作留下的审计记录中，`account` 是执行命令的账户，`actor` 为空。管理员以 `sudo` 执行时，系统级 Broker 按 root 的权限判定。系统级 Broker 管理组的成员在本账户没有运行自己的用户级 Broker 时，不用 `sudo` 也能管理它；管理组见[以配置文件权限管理 Broker]({{< relref "/docs/guides/management-permissions" >}})。这些命令不经过 Claude Code 的权限确认。Broker 拒绝某个命令时，它给出的错误写到标准错误，其中包含配置文件的路径，命令以退出码 1 结束。

这些命令的输出沿用 `peers service status` 的约定：空值写 `-`。

## peers config list

```
peers config list [flags]
```

打印正在运行的 Broker 的全部配置项，每项一行，写成 `<key> = <value>`，按 `key` 排序。

`pairing.static_key` 只打印 `set` 或 `unset`，不打印取值。

除 `-h`/`--help` 外没有选项。需要 Broker 配置文件的读权限；用户级 Broker 下，本账户本来就具备该权限。用法错误以退出码 2 结束，其余失败以退出码 1 结束。

## peers config get

```
peers config get <key> [flags]
```

打印指定的配置项，写成 `<key> = <value>`。只读取一个键；要打印全部，用 `peers config list`。

`pairing.static_key` 只打印 `set` 或 `unset`，不打印取值。未知的键会失败。shell 补全会给出配置键。

需要 Broker 配置文件的读权限；用户级 Broker 下，本账户本来就具备该权限。用法错误以退出码 2 结束，其余失败以退出码 1 结束。

## peers config set

```
peers config set <key> <value> [flags]
```

修改一个配置项，并打印改动，写成 `<key>: <old> -> <new>`。

- `key` 与 `value` 都必须给出。shell 补全会给出配置键。
- `pairing.static_key` 不能在这里修改，只能在配置文件里设置。

Broker 以 `--listen` 运行时，该选项固定了 `broker.listen`，`config set` 不能修改它；规则见[配置键]({{< relref "docs/reference/configuration" >}})。

需要 Broker 配置文件的写权限。用法错误以退出码 2 结束，其余失败，包括 Broker 拒绝的键，都以退出码 1 结束。

## peers service run

```
peers service run [flags]
```

在前台运行 Broker，直到收到 `SIGINT` 或 `SIGTERM`。在会话的进程树内启动的 Broker 会以退出码 6 失败。服务定义以这种方式启动 Broker。

- `--system` 与 `--user` 选择级别，二者互斥。都不给时，root 选系统级，其他账户选用户级。
- `--system` 运行系统级 Broker，需要 root。
- `--listen <address>` 只对本次运行覆盖 `broker.listen`。地址写法与配置键相同，例如 `:17443` 或 `127.0.0.1:17443`。空值作为用法错误拒绝；要停止监听，把配置文件里的 `broker.listen` 设为空值。
- `--allow-degraded` 让 Broker 在某些启动失败后继续运行，而不是退出。监听地址无法绑定，包括被低端口规则拒绝时，它进入仅本机降级，继续服务本机会话；数据目录或审计日志不可用时，它进入仅诊断降级。两种情况下它都在本机 socket 上应答并报告故障。不带该参数时，任何启动失败都在标准错误写明原因并退出，其中监听地址不可用以退出码 4 结束。配置错误、`broker.local_socket_group` 指向不存在的组，以及取得账户锁之前的失败，带不带该参数都会退出。服务定义总会带上该参数，前台运行不带。仅本机降级时，地址变为可用或改动 `broker.listen` 后会自动开始监听。仅诊断降级时，配置文件变化时重试，并每分钟重试一次；修好后也可以执行 `peers service restart`。两档恢复都不重启进程。

不给 `--listen` 时使用配置的 `broker.listen`。用法错误以退出码 2 结束。启动失败按原因使用对应退出码：本账户已有另一个 Broker 在运行并持有账户锁时为 3，监听地址不可用时为 4，配置无效时为 5，在会话的进程树内启动时为 6，不以 root 运行却给了 `--system` 时为 7，运行时目录不可用时为 8。其余失败以退出码 1 结束。

## peers service install

```
peers service install [flags]
```

写入并启用服务定义，然后立即启动 Broker。它会等 Broker 应答，最多等 20 秒；超时会使安装失败，给出服务日志的位置，并附上该日志中最后一条 `startup_failed` 记录的原因。重新安装会用新的二进制替换正在运行的 Broker。安装报告的内容见[安装 peers]({{< relref "/docs/guides/install" >}}#安装输出)。

- `--system` 与 `--user` 选择级别，二者互斥。都不给时，root 安装系统级，其他账户安装用户级。
- 用户级服务在账户登录时启动 Broker。不能以 root 运行。在 macOS 上只能在图形登录会话里运行。`~/.peers/config.yaml` 不存在时，生成这个文件，属主为执行安装的账户，权限 `0600`；已存在的文件不改动。
- 系统级服务在开机时启动 Broker。必须以 root 运行。组 `peers` 不存在时，命令新建它；已存在时直接使用，不改动成员。`/etc/peers/config.yaml` 不存在时，生成这个文件，属主为 `root`，组为 `peers`，权限 `0660`；已存在的文件不改动。`--admin-group <name>` 只在系统级安装时有效；安装落到用户级时，无论是因为给了 `--user` 还是因为执行安装的不是 root，都属于用法错误，以退出码 2 结束。它用于改用另一个组；组名只能以小写字母或下划线开头，其后只能是小写字母、数字、下划线或连字符。

生成的文件是一份列出全部键的模板：有固定默认值的键写出取值，其余键写成注释并附示例值或空占位。安装汇总的 `config:` 一行以 `(generated)` 结尾表示本次写入，以 `(kept)` 结尾表示文件已存在；系统级安装保留已有文件时，还会输出 `To delegate administration, run: chgrp <group> <path> && chmod 0660 <path>`。执行这条命令就把已有安装的配置文件交给该组，组成员随后不用 `sudo` 就能管理 Broker；管理组见[以配置文件权限管理 Broker]({{< relref "/docs/guides/management-permissions" >}})。`broker.listen` 使用小于 1024 的端口时，不要把文件交给这个组；规则与修复方法见[配置键]({{< relref "docs/reference/configuration" >}}#类型与约束)。

如果这一级别已有 Broker 正在应答，而且不是这个服务启动的，安装会失败，并提示先停掉那个 Broker。

安装写入或启用服务之前，会预检本次安装将要使用的监听地址。地址已被占用时，安装失败，不注册服务，错误中给出地址、配置文件与 `broker.listen`。重装时认出自己的服务，先停掉它并最多等 20 秒让它释放地址；地址被别的进程占用时，已安装的服务保持原样。安装失败时写入或启用的服务，不会在开机或登录时自动启动；如果停过原有的服务，错误给出恢复它的命令。

服务定义以 `service run --system` 或 `service run --user` 启动 Broker，并且总会带上 `--allow-degraded`。服务注册之后 Broker 进入降级运行时，安装会保留已安装并在运行的服务，输出故障原因与修复命令，并以退出码 3 结束。`peers service start` 与 `peers service restart` 遇到同样情况也以退出码 3 结束。

在 Linux 上，命令使用 systemd，执行 `systemctl daemon-reload`、`systemctl enable` 和 `systemctl restart`。在 macOS 上写入并加载 launchd 作业。`--system` 还要求可执行文件及其上每一级目录属于 root、组和其他人不可写，各级目录对其他账户可进入，并且可执行文件对其他账户可执行。位于临时目录下的可执行文件会被拒绝。Intel Mac 上用 Homebrew 时，`/usr/local/bin` 属于普通用户，要改选一个 root 拥有、组和其他人不可写的目录。

[文件与路径]({{< relref "docs/reference/files" >}})列出服务的文件和位置，[使用指南]({{< relref "docs/guides" >}})介绍怎样安装和运行服务。

发布包里的安装脚本就是用这个命令安装服务的。

用法错误以退出码 2 结束；降级运行以退出码 3 结束；其余失败以退出码 1 结束。

## peers service remove

```
peers service remove [flags]
```

只停止并卸载已安装的服务。二进制与 MCP 注册都保留，配置、身份与审计日志也保留；要卸载整个安装，用 [peers uninstall](#peers-uninstall)。没有安装过服务时，命令输出 `The peers service is not installed.` 并照常成功，所以可以重复执行。

- `--system` 与 `--user` 选择级别，二者互斥。都不给时，root 选系统级，其他账户选用户级。
- `--system` 卸载系统级服务，必须以 root 运行。

用法错误以退出码 2 结束，其余失败以退出码 1 结束。

## peers service start, stop and restart

```
peers service start [flags]
peers service stop [flags]
peers service restart [flags]
```

启动、停止或重启已安装的服务。

- `--system` 与 `--user` 选择级别，二者互斥。都不给时，root 选系统级，其他账户选用户级。

成功时输出 `The user Broker service has started.`、`The user Broker service has stopped.` 或 `The user Broker service has restarted.`；系统级服务把 `user` 换成 `system`。

`stop` 不禁用服务，下次登录或开机时仍会启动。该级别没有安装服务时，命令以退出码 1 结束，并说明该级别没有安装服务；只有系统级服务的账户还会收到提示，改用 `sudo peers service <command> --system`。

`start` 与 `restart` 和安装一样等待 Broker 应答并检查是否处于降级运行；退出码规则见 [peers service install](#peers-service-install)。用法错误以退出码 2 结束。

## peers service status

```
peers service status [flags]
```

连接正在运行的 Broker，不会启动 Broker，打印两张表：

- `Agents`：每个已登记 Agent 的地址、账户、状态、topic 与曾用名。
- `Peers`：每个对端的别名、指纹、本 Broker 的角色、状态、链路状态、对外地址与详细信息。

- `--system` 与 `--user` 选择级别，二者互斥。都不给时，root 选系统级，其他账户选用户级。命令只与选中的级别通信，不会回退到另一级别；只使用系统级 Broker 的账户必须加 `--system`。

某一节为空时打印为 `Agents: none` 或 `Peers: none`。没有 Broker 配置文件的读权限时，只列出调用方自己的 Agent；Peers 表照常显示。本 Broker 下线时，两张表之前先输出一行 `Broker: offline; messages from other hosts are refused. Run peers service online to accept them.`。

Broker 处于降级运行时，两张表之前先输出 `Broker: degraded (local only): <reason>` 或 `Broker: degraded (diagnostics only): <reason>`，再输出 `Fix: <fix>`。仅本机降级时照常输出两张表。仅诊断降级时只输出这两行，并以退出码 1 结束。

有更新的正式发布版本时，`peers service status` 在最后多打印一行。这一行里，`<latest>` 是最新版本，`<installed>` 是已安装版本。

```text
Update: <latest> available (current <installed>); run peers update
```

没有可用版本时不打印这一行。关闭 `update.check` 后它立即消失：Broker 不再检查，也不再报告可用版本。两个更新相关的键见[配置键]({{< relref "docs/reference/configuration" >}})。

用法错误以退出码 2 结束，其余失败以退出码 1 结束。

## peers service online and offline

```
peers service online [flags]
peers service offline [flags]
```

`peers service offline` 让本 Broker 拒绝其他主机发来的消息，`peers service online` 恢复接收。两个命令分别输出 `The Broker is now offline.` 与 `The Broker is now online.`。

- `--system` 与 `--user` 选择级别，二者互斥。都不给时，root 选系统级，其他账户选用户级。命令只与选中的级别通信，不会回退到另一级别。

其他主机发往已下线 Broker 上 Agent 的消息被拒绝，错误码为 `broker_offline`，由本 Broker 报告。本机 Agent 之间照常互发，本机 Agent 也能发往其他主机，但对方的回复会被拒绝；需要双向通信时先执行 `peers service online`。链路、topic 成员关系、名录与新的配对都照常工作。对端只有在发送被拒绝时才知道本 Broker 已下线，它的 Broker 列表与链路状态都不显示这一点。

这一状态就是配置键 `broker.offline`，重启后保留。命令需要配置文件的写权限，与修改配置相同；没有写权限时以 `permission_denied` 失败，错误中包含配置文件的路径。每次变化都写入审计日志，并记录执行命令的账户。用法错误以退出码 2 结束，其余失败以退出码 1 结束。

## peers service audit

```
peers service audit [flags]
```

按时间顺序逐行打印正在运行的 Broker 的审计日志，然后退出。它不启动 Broker，运行期间也不跟随新事件。

- `--system` 与 `--user` 选择级别，二者互斥。都不给时，root 选系统级，其他账户选用户级。命令只与选中的级别通信，不会回退到另一级别。
- `--limit <n>` 最多读取 `n` 条，取最新的那些，仍按从旧到新打印。`0` 表示全部，也是默认值。上限为 `4294967295`；不是数字或超过上限的值属于用法错误。

读取审计日志需要 Broker 配置文件的读权限。用户级 Broker 下，本账户本来就具备该权限。用法错误以退出码 2 结束，其余失败以退出码 1 结束。

## peers update

```
peers update [flags]
```

安装最新的正式发布版本：要么请选中的 Broker 自己更新，要么在本地替换二进制。

命令只安装比当前版本更新的正式发布版本：草稿与预发布一律忽略，不能指定版本，也不会降级。

- `--yes`（`-y`）跳过确认问题。不给时，命令询问 `peers <installed> is installed. Update to the latest release? [y/N]`（`<installed>` 是已安装版本），输入 `y` 或 `yes`（不区分大小写）即确认。标准输入不是终端时，没有 `--yes` 命令就失败，以退出码 1 结束。用户回答 `n` 时，命令输出 `Update canceled; nothing was changed.`，以退出码 0 结束。
- 用法错误以退出码 2 结束，其余失败以退出码 1 结束。更新成功、已是最新与取消更新都以退出码 0 结束。

命令先检查本二进制是不是正式发布版本，未给 `--yes` 时再请求确认，之后才选择 Broker。只有没有 Broker 在运行，或者选中的用户级 Broker 不在服务管理器下运行时，命令才在本地替换二进制；系统级 Broker 不在服务管理器下运行时，命令报错。

### 由 Broker 执行

命令选中正在运行的 Broker 时，由它自己安装更新。系统级 Broker 管理组的成员不用 `sudo` 就能更新它；本账户运行着自己的用户级 Broker 时，用 `sudo peers update` 作用于系统级 Broker。

1. 把请求发给选中的 Broker。Broker 按对配置文件的写权限判定权限，命令不先行检查。权限被拒时，Broker 的错误写到标准错误，命令以退出码 1 结束，没有发出任何下载请求。
2. 已经是最新版本时，命令输出 `peers <installed> is up to date`，以退出码 0 结束。
3. Broker 下载本平台与架构的发布包和 `checksums.txt`，核对发布包的 SHA-256，取出其中名为 `peers` 的普通文件，并运行新二进制的 `--version`，确认它报告的版本符合预期。然后它替换自己的二进制；符号链接仍指向更新后的文件。
4. Broker 先应答再重启。命令输出 `The Broker is restarting with version <latest>.`，并等到重启后的 Broker 报告该版本，因为只有新进程在运行，更新才算完成。成功时输出 `The Broker is running peers <latest>.`，随后输出会话提示。重启后的 Broker 未及时报告新版本时，命令失败，并给出该级别服务日志的位置。
5. 会话提示为 `Sessions that are already open keep the old MCP server until they are reopened; this Broker's connections recover automatically.`

在终端里启动、而不是由服务管理器启动的用户级 Broker 不能替换并重启自己，会把命令交给下面的本地替换。

### 本地替换二进制

没有正在运行的 Broker，或上面的用户级 Broker 无法自我更新时，命令在本地替换二进制：

1. 定位正在运行的可执行文件，并解析符号链接。只替换它们指向的文件。
2. 检查能否写入目标所在的目录，这一步先于任何下载。没有写权限时输出 `cannot write to <dir>: ...`，随后输出 `run "sudo peers update" to update a system installation`，以退出码 1 结束。
3. 向 GitHub 查询最新的正式发布版本。已经是最新版本时输出 `peers <installed> is up to date`，以退出码 0 结束。
4. 先输出 `peers <installed> is installed; version <latest> is available.`；发布填了地址时再输出 `Release: <url>`。然后下载本平台与架构的发布包和 `checksums.txt`，核对发布包的 SHA-256，取出其中名为 `peers` 的普通文件，并运行新二进制的 `--version`，确认它报告的版本符合预期。
5. 在目标所在的目录写入新二进制，把属主与权限改成与原文件相同，再改名到目标路径。旧版本不保留。
6. 不启动也不重启任何服务，因为服务可能是有意停掉的。它输出 `The new binary takes effect the next time the Broker starts.`。

更新成功的输出是下面两种之一。只有发布填了地址时才出现 `Release:` 一行。

由 Broker 执行：

```text
peers <installed> is installed. Update to the latest release? [y/N] y
The Broker is restarting with version <latest>.
The Broker is running peers <latest>.
Sessions that are already open keep the old MCP server until they are reopened; this Broker's connections recover automatically.
```

本地替换：

```text
peers <installed> is installed. Update to the latest release? [y/N] y
peers <installed> is installed; version <latest> is available.
Release: https://github.com/wangzw/peers/releases/tag/v<latest>
The new binary takes effect the next time the Broker starts.
```

失败信息写到标准错误，前缀为 `peers update: `，包括：

- `version "dev" is not a release build; only release builds can update themselves`
- `cannot write to <dir>: ...`，随后是 `run "sudo peers update" to update a system installation`
- `standard input is not a terminal; pass --yes to proceed without asking`
- `checksum mismatch for <archive>`
- `downloaded binary reported "<x>", want "peers <latest>"`
- `the Broker did not report version <latest> within <duration>; check the service log: <command or path>`；最后一次查询出错时，原因放在括号里跟在时长后面：`within <duration> (last error: <error>)`
- `the Broker is not running under a service manager, so it cannot restart itself with the new binary; install it as a service and run the update against that service (for example, "peers service install" or "sudo peers service install --system")`
- `another update is already in progress`

经 Broker 更新时，校验和不符、版本不符这类报错出现在 `installing <latest> failed: ` 之后，查询最新发布版本失败出现在 `looking up the latest release failed: ` 之后。

## peers uninstall

```
peers uninstall [flags]
```

卸载一个安装：服务、服务启动的二进制、执行 `sudo` 的账户（未经 `sudo` 时为执行命令的账户）的 MCP 注册、安装脚本放置的 shell 补全文件与 man 页，以及运行时目录。

- 以 root 执行时，卸载系统级安装。其他账户卸载自己的用户级安装；主机上同时有系统级安装时，命令先卸载用户级安装，结束后输出 `A system installation is still present; run: sudo peers uninstall --yes`；给过 `--purge` 时命令里也带 `--purge`。
- 主机上只有系统级安装时，命令询问 `Remove the system installation through sudo? [y/N]`。回答 `y` 时命令经 `sudo` 重新执行；给出 `--yes` 时跳过询问，直接经 `sudo` 执行。回答其他内容时输出 `Run: sudo peers uninstall --yes`，以退出码 0 结束。没有终端又没有 `--yes` 时命令失败，输出可以直接重跑的完整 `sudo peers uninstall --yes` 命令，以退出码 1 结束。给过 `--purge` 时，这两处给出的命令都带 `--purge`，即 `sudo peers uninstall --purge --yes`。
- 不给 `--purge` 时，命令询问 `Also remove configuration, identity, audit and service logs? [y/N]`，除回答 `y` 外都保留这些数据。
- `--yes`（`-y`）跳过询问。除同时给出 `--purge` 外，配置、身份、审计与服务日志都保留。
- `--purge` 跳过删除数据的询问，一并删除这些数据。
- 没有终端时，无论是否给出 `--purge`，命令都需要 `--yes` 才能继续；否则在删除任何内容之前失败，以退出码 1 结束。
- 系统级安装中，回答 `y` 或给出 `--purge` 时，只有管理组是安装时创建的才会删除它；其余情况，或该组仍在使用时，管理组保留，命令打印手动删除的方法。
- 其他账户的 MCP 注册会保留，各自由相应账户用 `claude mcp remove -s user peer` 删除。系统级安装删除执行 `sudo` 的账户的注册；以 root 直接运行时只打印命令。

删除系统级二进制之前，命令会列出用户级 Broker 正在运行的其他账户，然后照常删除。

中途失败时命令停下，不回滚已完成的步骤；错误写明是哪一步失败，再次执行可以接着完成。两个级别都没有安装服务、但 MCP 注册指向这个二进制时，卸载本账户所属的安装：以 root 执行时卸载系统级安装，其他账户卸载用户级安装；什么都没有安装时，命令报告没有安装，以退出码 0 结束。

退出码：成功、没有安装、拒绝经 sudo 卸载系统级安装都是 0；失败为 1，其中包括没有终端又没有 `--yes`；用法错误为 2。

## peers completion

```
peers completion [flags]
peers completion [command]
```

把指定 shell 的自动补全脚本输出到标准输出。支持的 shell 为 `bash`、`zsh`、`fish` 与 `powershell`；每个 shell 子命令都接受 `--no-descriptions`，让脚本不带描述。不带 shell 的 `peers completion` 与未知的 shell 名都属于用法错误，以退出码 2 结束。成功时以退出码 0 结束。

安装脚本会按安装的级别写入补全文件：bash 两个级别都装，zsh 只在系统级装，主机装有 fish 时才装 fish。[安装 peers]({{< relref "/docs/guides/install" >}}#补全与-man-页)说明安装脚本写入哪些文件，[文件与路径]({{< relref "/docs/reference/files" >}}#补全与-man-页)列出路径。

部分参数支持动态补全：`broker disconnect` 给出已配对 Broker 的别名与指纹，`config get` 与 `config set` 给出配置键，`completion` 给出 shell 名。

## peers mcp

```
peers mcp
```

在进程的标准输入与标准输出上为一个 Claude Code 会话运行 MCP 服务。Claude Code 按 MCP 注册启动这个进程，通常不必手动运行。这个命令不出现在帮助列表里。本账户有用户级 Broker 在运行时，会话使用这个用户级 Broker；否则，本账户有权连接系统级 Broker 时，会话使用系统级 Broker。选择规则与相关设置见[运行系统级 Broker]({{< relref "/docs/guides/system-broker" >}})。正在运行的用户级 Broker 优先于系统级 Broker；在能使用系统级 Broker 的账户上安装用户级服务，这些账户的会话就会改连用户级 Broker，不再使用系统级 Broker。没有 Broker 可用时，工具调用返回说明下一步怎么做的错误，详见 [FAQ 的没有 Broker 一问]({{< relref "/docs/faq" >}}#没有-broker-在运行时会怎样)。

用法错误以退出码 2 结束；运行服务失败以退出码 1 结束。
