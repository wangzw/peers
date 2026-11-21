---
title: 配置键
weight: 3
---

两种部署形态各读一个 YAML 文件：用户级 Broker 读 `~/.peers/config.yaml`，系统级 Broker 读 `/etc/peers/config.yaml`。没有文件时 Broker 照常运行，每个键都可选，未出现的键取默认值。`peers service install` 如何生成这个文件，见下文[生成的配置文件](#生成的配置文件)一节。Broker 会监视该文件，所以无论是手工修改、经 `set_config` 修改，还是经 `peers config set` 修改，合法的改动都会在运行期间生效。两个保活键是例外：它们在建立监听或链路时读取，改动只对之后建立的监听或链路生效。

## 生成的配置文件

`peers service install` 只在该级别的配置文件不存在时写入模板，已存在的文件不改。模板列出全部键，每个键上方有一行说明。有固定默认值的键直接写出取值。默认值为空或在运行时计算的 5 个键写成注释并附示例：`broker.alias`（默认值在运行时计算），以及默认为空的 `broker.advertise`、`broker.advertise_extra`、`broker.local_socket_group` 和 `pairing.static_key`。取消注释并填入合法值，改动就会像其他合法改动一样在 Broker 运行期间生效。没有配置文件就启动的 Broker 不会得到模板。

两份模板有三处不同。第一行写明级别，分别是 `# peers user Broker configuration.` 与 `# peers system Broker configuration.`。开头注释不同：用户级模板说明这个文件只属于本账户，系统级模板则说明，把文件的读权限授予某个组，该组的账户就能读取配置、完整状态与审计日志，把写权限授予某个组，该组的账户就能修改配置、管理配对。用户级模板还在 `broker.local_socket_group` 与 `broker.local_socket_mode` 这两个键的说明后注明，它们只对系统级 Broker 生效。

模板以一段说明注释开头：

```yaml
# peers user Broker configuration.
#
# Every key is optional; a key that is absent takes its default value.
# YAML anchors, aliases and merge keys are not supported.
# This file belongs to this account only: only this account runs the Broker
# and reads or writes the file.
```

文件按键分成若干段。开头注释与 `pairing` 段之间是 `broker` 段，`pairing` 之后还有 `topics`、`directory`、`send`、`link` 和 `log` 段；下面的节选省略了这些段，只给出 `pairing` 段：

```yaml
pairing:
  # Pairing requests per minute that match no pairing key; successful matches of the static key are not counted.
  max_attempts_per_minute: 10
  # Failed verifications allowed before a one-time pairing key is invalidated.
  max_failures: 5
  # Static pairing key of 16 to 128 characters; a non-empty value takes effect.
  # static_key: ""
  # Fill in 16 to 128 random characters, for example the output of `openssl rand -hex 32`. An empty value leaves the static key unset.
```

## 配置键

| 键 | 类型 | 默认值 | 含义 | `set_config`、`peers config set` |
|---|---|---|---|---|
| `broker.alias` | 字符串 | 推导得出 | 本 Broker 自报的别名。未设置时由主机名推导，用户级 Broker 还取运行它的账户名。取值为 1 至 24 个字符，限字母、数字、`.`、`-` 和 `_`。细则见表后的说明。 | 可以 |
| `broker.public` | 布尔 | `false` | 是否发布名片，使本 Broker 为整个连通网络所知。 | 可以 |
| `broker.listen` | 字符串 | `:17443` | 绑定的地址与端口，写作 `host:port`、`:port` 或 `[v6]:port`。主机部分为空表示本机全部地址；整个值为空表示不监听。 | 可以，`--listen` 运行时除外 |
| `broker.advertise` | 字符串 | 空 | 取代自动探测到的对外地址。合法写法为空、`host`、`host:port` 和 `:port`。`host` 使用监听端口；`:port` 取默认路由所在接口的地址，并要求监听本机全部地址。非空值要求有监听地址。 | 可以 |
| `broker.advertise_extra` | 字符串列表 | 空 | 追加的对外地址。每项为 `host` 或 `host:port`；不带端口的项在 `broker.advertise` 写出了端口时取它，否则取监听端口。要求有监听地址。 | 可以 |
| `broker.address_check_interval` | 时长 | `1m` | Broker 重新读取本机地址的间隔。 | 可以 |
| `broker.local_socket_group` | 字符串 | 空 | 系统级 Broker 本机 socket 的属组。空值沿用运行时目录的属组。组必须存在于账户数据库，取值不能含空白、`:` 或 `/`。用户级 Broker 忽略此项。 | 可以 |
| `broker.local_socket_mode` | 字符串 | `0666` | 系统级 Broker 本机 socket 的文件权限，写成 3 或 4 位八进制数。用户级 Broker 固定为 `0600`。 | 可以 |
| `broker.offline` | 布尔 | `false` | 本 Broker 是否拒绝其他主机发来的消息。链路与名录照常工作，运行中修改立即生效。 | 可以 |
| `pairing.static_key` | 字符串（密钥） | 空 | 预置静态密钥，16 至 128 个字符。有值即生效。可用 `openssl rand -hex 32` 生成，输出 64 个十六进制字符。 | 不可以，只能改文件 |
| `pairing.max_failures` | 整数 | `5` | 一次性配对密钥作废前允许的验证失败次数。 | 可以 |
| `pairing.max_attempts_per_minute` | 整数 | `10` | 每分钟允许的、没有匹配上任何配对密钥的配对请求数。匹配上预置静态密钥的请求不计入。 | 可以 |
| `topics.auto_join` | 布尔 | `true` | 会话是否自动加入默认 topic。 | 可以 |
| `directory.reconcile_interval` | 时长 | `10m` | 对端之间对账名录的间隔。 | 可以 |
| `directory.max_records` | 整数 | `1000` | 名录记录总数的上限。 | 可以 |
| `directory.max_records_per_link` | 整数 | `250` | 经一条链路接受的记录数上限。 | 可以 |
| `directory.max_topics_per_publisher` | 整数 | `16` | 同一发布者的公开 topic 声明与未知种类记录合计上限。名片与撤销不计。 | 可以 |
| `send.link_retries` | 整数 | `5` | 链路未建立时的重试次数。 | 可以 |
| `send.link_backoff` | 时长 | `500ms` | 首次重试前的等待；之后每次翻倍，单次不超过 8 秒。 | 可以 |
| `send.request_timeout` | 时长 | `20s` | 一次发送的总时限，包含重试与等待。 | 可以 |
| `send.inbox_retries` | 整数 | `2` | inbox 确定未写入时的重试次数。 | 可以 |
| `send.inbox_backoff` | 时长 | `200ms` | inbox 写入重试之间的等待。 | 可以 |
| `send.inbox_timeout` | 时长 | `5s` | 单次 inbox 写入的期限。 | 可以 |
| `send.dedupe_window` | 时长 | `5m` | 去重表条目的保留时间。 | 可以 |
| `link.dial_timeout` | 时长 | `5s` | 拨号时对每个地址的连接期限。 | 可以 |
| `link.reconnect_min` | 时长 | `1s` | 重建链路的初始退避间隔。 | 可以 |
| `link.reconnect_max` | 时长 | `1m` | 重建链路的最大退避间隔。 | 可以 |
| `link.keepalive_interval` | 时长 | `15s` | 保活探测的间隔。gRPC 会把小于 10 秒的间隔提高到 10 秒。 | 可以 |
| `link.keepalive_timeout` | 时长 | `10s` | 保活探测的应答期限。 | 可以 |
| `log.level` | 字符串 | `info` | 运行日志级别：`debug`、`info`、`warn` 或 `error`。 | 可以 |
| `update.check` | 布尔 | `true` | Broker 是否周期检查 GitHub 上是否有更新的正式发布版本。为 `false` 时不发出检查请求。 | 可以 |
| `update.check_interval` | 时长 | `24h` | Broker 检查新版本的间隔。下限为 `1h`。 | 可以 |

更新检查会访问 GitHub。[安全模型]({{< relref "docs/security" >}})说明它发送什么、接收什么。`peers update` 也访问 GitHub，但只在你运行它时。

未设置 `broker.alias` 时，系统级 Broker 取系统主机名的第一段，即第一个 `.` 之前的部分；用户级 Broker 取这一段、`.` 和运行它的账户名，例如 `workstation` 与 `workstation.alice`。别名与监听地址、监听端口和对外地址都无关。不在别名字符集内的字符换成 `-`。账户名查不到或为空时用十进制 uid 代替，例如 `workstation.1000`。主机名读不到或为空时，主机名部分取 `broker`。别名超过 24 个字符时先截短主机名部分，主机名部分至少保留 1 个字符，仍然超长再截短账户名部分。截短后去掉末尾的 `.` 与 `-`；账户名部分改用 uid 时即截短 uid。想自选别名，就设置 `broker.alias`。

## 类型与约束

- 时长写成 Go 时长语法，例如 `10m`、`500ms`、`1s` 或 `1h30m`。时长必须大于零。
- 整数必须大于零，`send.link_retries` 和 `send.inbox_retries` 例外，可以取 `0`。
- `link.reconnect_max` 不得小于 `link.reconnect_min`；`link.keepalive_timeout` 必须小于 `link.keepalive_interval`。修改其中一对里的一个键时，会与另一个一起校验。
- `broker.listen` 写作 `host:port`、`:port` 或 `[v6]:port`，端口为 1 至 65535。修改为另一个值时，系统会先探测该地址，无法绑定的地址会被拒绝。系统级 Broker 的端口小于 1024 时，只有在配置文件只能由 root 写时才生效。配置文件可被组或其他人写入时，前台运行的 Broker 会拒绝启动，作为服务运行的 Broker 会进入仅本机降级；执行 `chmod g-w <path>`，或把端口改为不小于 1024，即可恢复。root 经 `set_config` 或 `peers config set` 把端口设为小于 1024 时，配置文件可被 root 以外的账户写入则请求被拒绝，错误说明文件必须具备的条件，并给出同样的两种办法。调用者不是 root 时一律拒绝，报错为 `changing broker.listen to a port below 1024 requires root`。启动或热加载被拒绝时，提示会写出文件中当前的 `broker.listen` 取值，要求 root 先核对该值是不是自己想要的，再收紧权限，因为能写这个文件的账户可能已经改过它。这条规则见[以配置文件权限管理 Broker]({{< relref "management-permissions.md" >}}#管理组)。
- `broker.advertise` 与 `broker.advertise_extra` 要求有监听地址；只有端口的 `broker.advertise` 还要求监听本机全部地址。
- 配置的对外地址，连同监听特定地址时隐含的那一项，最多 8 项。默认路由所在接口的地址会使列表超过 8 项时，从这部分末尾舍去；配置的项全部保留。
- `broker.local_socket_group` 必须指向存在的组，不存在时启动失败，带 `--allow-degraded` 也一样。
- `broker.local_socket_mode` 接受 3 或 4 位八进制数，最大 `0777`。
- `pairing.static_key` 接受 16 至 128 个字符；空值表示不设静态密钥。
- `update.check_interval` 不得小于 `1h`。

未通过检查的值会被拒绝，配置保持原样。启动时，配置文件读不出、无法解析，或某个值未通过检查，Broker 都会退出，前台运行与作为服务运行都一样。只有数据目录或审计日志不可用时才会进入仅诊断降级。

## 文件格式与上限

配置文件是一份 YAML 文档。它不接受 YAML 的锚点、别名（`&name`、`*name`）和合并键（`<<`），映射与序列最多嵌套 32 层，文件最大 1 MiB。不符合其中任何一条的文件无法加载：

- `config file parse error: anchors and aliases are not supported at line <line>, column <column>`
- `config file parse error: merge keys are not supported at line <line>, column <column>`
- `config file parse error: nesting is deeper than 32 levels at line <line>, column <column>`
- `cannot read <path>: the file is larger than 1 MiB`

启动时遇到这几种错误，Broker 都会退出，带 `--allow-degraded` 也一样。运行中热加载遇到时，全部配置保留原值，错误写入运行日志。`set_config` 与 `peers config set` 在文件无法解析时失败，配置保持原样。

## 修改配置

`set_config` 以文本接收新值，返回旧值与新值。`peers config set` 在终端做同样的事，规则相同。两者修改模板中写成注释的键时，都在注释所在位置取消注释并写入新值，文件其余内容不变。文件不存在时，新建的文件只写这一项。两者都不能修改 `pairing.static_key`，在 Broker 以 `--listen` 运行时也都不能修改 `broker.listen`。修改 `update.check_interval` 不会中断当前检查周期，新值从下一个周期起生效。把 `update.check` 设为 `false` 会停止检查，并清除已报告的可用版本。手工编辑文件时，Broker 采纳其中合法的改动；某个键的值不合法时，该键保留旧值，Broker 向日志写入一条错误。

文件位置与权限见[文件与路径]({{< relref "docs/reference/files" >}})。会话默认加入哪些 topic 见[项目配置文件]({{< relref "docs/reference/project-config" >}})。
