# config

`config` 保存 [Broker](../../glossary.md) 的配置和审计日志，并给出两种部署形态下各类文件的位置。配置是一组有名字、有类型、有默认值的键，可以由用户编辑文件设定，也可以经本机接口读取和修改；审计日志是只追加的事件记录。其他包从这里读取自己需要的值和路径，并把需要留痕的事件交给它写入。它不引用本项目的任何其他包。

## 职责

- 按账户与部署形态给出运行时目录、配置文件、数据目录、审计日志和账户锁的路径（[ADR-broker-deployment-modes](../../adr/adr-broker-deployment-modes.md)）。
- 启动时加载配置文件，对缺失的键取默认值，键的类型或取值不合法时拒绝启动并指出该键；监听地址与对外地址配置的组合非法时拒绝启动并同时指出这两项，对外地址列表超过 8 项时拒绝启动并指出该项，运行期间的这类修改不提交（`REQ-broker-config-032`、`REQ-broker-config-036`）。
- 把启动参数给出的监听地址作为本次运行的取值，不把它写入配置文件，配置文件中的 `broker.listen` 在本次运行中不起作用；其间经本机接口修改 `broker.listen` 的调用失败（`REQ-broker-config-025`、`REQ-broker-config-029`）。
- 在没有配置别名时按系统主机名与运行 Broker 的账户得出默认别名（`REQ-broker-pairing-002`）。
- 由监听地址、`broker.advertise` 与 `broker.advertise_extra` 得出本次运行的对外地址列表，监听全部地址时取默认路由所在接口的地址，供 [link](design-link.md)、[pairing](design-pairing.md) 与 [directory](design-directory.md) 使用（[ADR-advertised-address](../../adr/adr-advertised-address.md)，`REQ-broker-config-030`、`REQ-broker-config-031`、`REQ-broker-config-033`、`REQ-broker-config-035`）。
- 每隔 `broker.address_check_interval` 重新枚举本机地址，对外地址列表变化时通知订阅者（`REQ-broker-discovery-016`）。
- 给出系统主机名，供 `pairing` 在生成配对密钥的结果中展示本机地址（`REQ-broker-pairing-028`）。
- 保存密钥类配置项，即只经配置文件设置、取值不经本机接口返回的配置项，读取时不返回取值，审计与日志不记录取值，并校验预置静态密钥的长度（[ADR-static-pairing-key](../../adr/adr-static-pairing-key.md)，`REQ-broker-pairing-029` 至 `REQ-broker-pairing-031`）。
- 提供按键读取，以及经校验的修改：校验通过后持久化、更新内存、写一条审计记录、通知订阅该键的调用方（`REQ-broker-config-001`、`REQ-broker-config-002`、`REQ-broker-config-006`）。
- 监视配置文件，文件由其他程序改动后逐项采用合法的新值，不合法的项保留原值，并为每个采用了新值的键写一条审计记录（`REQ-broker-config-017`）。
- 按部署形态生成配置文件模板，列出全部配置项，供服务安装时写入尚不存在的配置文件（`REQ-broker-config-040`）。
- 追加审计记录，合并同一来源的重复拒绝事件，并提供按时间顺序读取（`REQ-broker-config-007` 至 `REQ-broker-config-009`）。

### 非职责

- 不决定工具的读写划分与只读标记，由 [mcpserver](design-mcpserver.md) 负责。
- 不要求用户确认。修改是否需要确认由 Claude Code 的权限设置决定（[ADR-config-through-tools](../../adr/adr-config-through-tools.md)）。
- 不判定调用方有没有权限读写配置。配置文件的文件权限就是管理权限的依据（[ADR-management-by-config-file-permission](../../adr/adr-management-by-config-file-permission.md)），判定由 [broker](design-broker.md) 在调用本包之前完成；本包只给出配置文件的路径。
- 不绑定监听地址，由 [link](design-link.md) 负责；绑定失败时的处理由 `broker` 负责。
- 不保存对端表与身份密钥，由 [pairing](design-pairing.md) 负责；不保存名录，由 [directory](design-directory.md) 负责。
- 不读取项目配置文件 `.peers.yaml`，由 `mcpserver` 负责。
- 不了解某个键的取值在别的包看来是否可行。这类检查由 [broker](design-broker.md) 以校验函数的形式注册进来。
- 不输出运行日志。运行日志由各包经标准日志接口写出，本包只提供其级别设置。

## 外部接口契约

供本项目其他各包调用，签名稳定。配置键的名字与含义是对用户的稳定承诺，新增键不改变既有键的含义。

```go
// Load 按形态确定各路径并加载配置；listenOverride 非空时取代 broker.listen，只对本次运行有效，
// 其间 Set 拒绝修改 broker.listen，配置文件中这一项的变化不采用。
func Load(mode Mode, listenOverride string) error
func Paths() PathSet                     // 运行时目录、配置文件、数据目录、审计日志
func AccountLock() string                // 账户锁的路径，不依赖 Load

// Template 按形态生成配置文件模板：有固定默认值的键写出取值，没有固定默认值的键写成注释。
func Template(mode Mode) []byte

func Get(key string) (Value, error)
func All() []Entry
func Set(actor Actor, key string, v Value) (old Value, err error)

// Secret 返回密钥类配置项的取值，未设置时为空；只供 pairing 在握手查找密钥时调用。
func Secret(key string) string

// AdvertisedAddr 是对外地址列表中的一项。Kind 是地址种类，决定其他 Broker 怎样经这一项连到本 Broker；
// Addr 是该种类下的地址写法。本版本产生的 Kind 只有 "direct"，此时 Addr 为 "主机:端口"，IPv6 主机带方括号。
type AdvertisedAddr struct {
    Kind string
    Addr string
}

// Advertised 返回本次运行的对外地址列表，至多 8 项；不监听时为空。列表中的每一项都是直连的地址项。
func Advertised() []AdvertisedAddr

// Hostname 返回系统主机名，其中不属于别名字符集的字符替换为 "-"。
func Hostname() string

// SubscribeAdvertised 注册对外地址列表的变更通知；各次通知按发生的顺序逐个送达。
func SubscribeAdvertised(onChange func(old, new []AdvertisedAddr))

// Actor 是一次修改或一个事件的发起者：系统账户，以及经 Agent 发起时的 Agent 地址。
type Actor struct {
    Account string
    Agent   string
}

// Validate 注册某个键的附加校验；Set 与文件重载在提交之前调用它，返回错误则不提交。
// 校验在包内互斥锁持有期间执行，校验函数不得调用本包的其他导出函数；需要别的包判断时
// 只可调用对方只取自身锁的查询，例如 pairing 的 AliasConflict 与 ResolveAlias。
func Validate(key string, check func(old, new Value) error)

// Subscribe 注册某个键的变更通知；各次通知按提交的顺序逐个送达。
func Subscribe(key string, onChange func(old, new Value))

func Audit(e Event)
func ReadAudit(since time.Time) ([]Event, error)

// Close 停止监视与通知协程并关闭审计日志；可重复调用，未加载时无动作。
func Close()

// ParseValue 把 SetConfig 请求中的文字按键的类型解析成 Value，只检查类型，不校验取值与组合。
func ParseValue(key, text string) (Value, error)

// CheckRuntimeDir 创建并检查运行时目录的属主与权限，供 broker 在取账户锁之前调用。
func CheckRuntimeDir() error

// ProbeListen 探测 addr 能否绑定，addr 使用 broker.listen 的写法。它绑定后立即关闭，
// 不保留 listener；校验通过后端口仍可被真正的监听绑定。空地址表示不监听，总是成功。
func ProbeListen(addr string) error

type ValueKind uint8
const (
    KindUnset ValueKind = iota
    KindString
    KindBool
    KindInt
    KindDuration
    KindStringList
    KindSecret
)

// KeyKind 是配置键的类型，取值由包内的键表定义；包外不需要构造它。
type KeyKind uint8

// Value 是配置值的带类型表示，只有 Kind 对应的字段有意义。Strings 由调用方持有副本。
// 密钥类配置项的取值只保存在包内，Get 与 All 返回 KindSecret 与 SecretIsSet。
type Value struct {
    Kind        ValueKind
    String      string
    Bool        bool
    Int         int64
    Duration    time.Duration
    Strings     []string
    SecretIsSet bool
}

type Entry struct {
    Key   string
    Value Value
}

// Event 是一条审计记录。MergeKey 只参与合并判断，不属于审计文件的公开字段。
type Event struct {
    Time     time.Time
    Event    EventKind
    Actor    string
    Account  string
    Peer     *PeerRef
    Key      string
    Old      *Value
    New      *Value
    Count    uint64
    Detail   string
    // MergeKey 只参与合并判断，不属于审计文件的公开字段；connection_rejected 优先于 Peer.Fingerprint。
    MergeKey string
}

type PathSet struct {
    RuntimeDir string
    ConfigFile string
    DataDir    string
    AuditLog   string
}
```

加载之前调用导出函数的语义：`Get`、`Set`、`ReadAudit` 返回 `ErrNotLoaded`；`All` 与 `Advertised` 返回 `nil`；`Secret` 返回空字符串；`Paths` 返回零值；`Validate`、`Subscribe`、`SubscribeAdvertised` 是装配错误，直接 panic；`Audit` 以错误级别写一条运行日志并丢弃该事件；`Close` 无动作。`Load` 只能成功一次，第二次返回 `ErrAlreadyLoaded`。`Close` 之后本包回到未加载的状态，各导出函数按上面的语义处理：`Set` 返回 `ErrNotLoaded`，`Secret` 返回空字符串，其余同未加载时。错误以哨兵值导出（`ErrNotLoaded`、`ErrAlreadyLoaded`、`ErrUnknownKey`、`ErrSecretKey`、`ErrListenOverridden`、`ErrInvalidValue`、`ErrInvalidCombination`、`ErrTooManyAddresses`、`ErrFileWrite`），调用方用 `errors.Is` 区分。`Load` 在审计日志不可用时把错误包上可判别的哨兵值，并导出这个哨兵；数据目录不可用时，`broker` 的装配器在打开身份、对端表与名录时把错误包上同一个哨兵值。`broker` 用 `errors.Is` 按这个哨兵决定是否进入仅诊断；`Load` 的其余失败不带这一标记。

启动时的低端口检查不属于 `Load`。`Load` 成功之后、绑定监听地址之前，由 `broker` 调用本包导出的检查函数；`broker.listen` 的端口小于 1024 而配置文件可被组或其他人写入时返回错误，按监听地址不可用处理。

下表是配置键的完整集合：

| 键 | 类型 | 默认值 | 含义 |
|---|---|---|---|
| `broker.alias` | 字符串 | 见“默认别名” | 本机自报的别名，1 至 24 个字符，取自字母、数字、`.`、`-`、`_`，超过 24 个字符或含其他字符的配置非法（`REQ-broker-pairing-002`、`REQ-broker-pairing-003`、`REQ-broker-pairing-037`） |
| `broker.public` | 布尔 | `false` | 是否公开（`REQ-broker-discovery-001`） |
| `broker.listen` | 字符串 | `:17443` | 实际绑定的地址与端口；主机部分为空表示本机的全部地址，明确配置为空表示不监听（[ADR-listen-by-default](../../adr/adr-listen-by-default.md)，`REQ-broker-config-028`） |
| `broker.advertise` | 字符串 | 空 | 对外地址的配置，合法写法只有空、`主机`、`主机:端口`、`:端口`，见“对外地址”（[ADR-advertised-address](../../adr/adr-advertised-address.md)，`REQ-broker-config-030` 至 `REQ-broker-config-032`） |
| `broker.advertise_extra` | 字符串列表 | 空 | 追加的对外地址，每项是 `主机` 或 `主机:端口`（`REQ-broker-config-033`） |
| `broker.address_check_interval` | 时长 | `1m` | 重新枚举本机地址的间隔（`REQ-broker-discovery-016`） |
| `broker.local_socket_group` | 字符串 | 空 | 系统级 Broker 本机 socket 的属组；空表示沿用运行时目录的属组，用户级忽略；组必须存在于账户数据库（Linux 上经 getent 按 NSS 配置查询，见 [ADR-account-database-source](../../adr/adr-account-database-source.md)），运行期间修改立即生效（`REQ-broker-config-027`） |
| `broker.local_socket_mode` | 字符串 | `0666` | 系统级 Broker 本机 socket 的文件权限；用户级的固定为 `0600`（`REQ-broker-config-027`） |
| `broker.offline` | 布尔 | `false` | 是否下线；为真时拒绝对端发来的消息，链路与名录照常，运行期间修改立即生效（`REQ-broker-config-065`、`REQ-agent-messaging-039`） |
| `pairing.static_key` | 字符串，密钥类 | 空 | 预置静态密钥，长度 16 至 128 个字符，有值即生效（[ADR-static-pairing-key](../../adr/adr-static-pairing-key.md)，`REQ-broker-pairing-008`、`REQ-broker-pairing-009`、`REQ-broker-pairing-029`） |
| `pairing.max_failures` | 整数 | `5` | 一次性配对密钥作废前允许的验证失败次数 |
| `pairing.max_attempts_per_minute` | 整数 | `10` | 每分钟允许的、没有匹配上任何配对密钥的配对请求数；匹配上预置静态密钥的成功请求不计入 |
| `topics.auto_join` | 布尔 | `true` | 是否自动加入默认 topic（`REQ-topics-020`） |
| `directory.reconcile_interval` | 时长 | `10m` | 定期对账的间隔 |
| `directory.max_records` | 整数 | `1000` | 名录记录总数的上限（`REQ-broker-discovery-011`） |
| `directory.max_records_per_link` | 整数 | `250` | 经同一条链路接受的记录数上限（`REQ-broker-discovery-015`） |
| `directory.max_topics_per_publisher` | 整数 | `16` | 同一发布者的公开 topic 声明与未知种类记录合计上限（名片与撤回不计） |
| `send.link_retries` | 整数 | `5` | 链路未建立时的重试次数（`REQ-agent-messaging-017`） |
| `send.link_backoff` | 时长 | `500ms` | 首次重试前的等待，之后每次翻倍，单次不超过 8 秒 |
| `send.request_timeout` | 时长 | `20s` | 一次发送的预算，即发送调用的总时限（`REQ-agent-messaging-017`、`REQ-agent-messaging-018`） |
| `send.inbox_retries` | 整数 | `2` | inbox 确定未写入时的重试次数（`REQ-agent-messaging-018`） |
| `send.inbox_backoff` | 时长 | `200ms` | inbox 写入重试之间的等待 |
| `send.inbox_timeout` | 时长 | `5s` | 单次 inbox 写入的期限 |
| `send.dedupe_window` | 时长 | `5m` | 去重表条目的保留时间 |
| `link.dial_timeout` | 时长 | `5s` | 拨号时对每个地址的连接期限 |
| `link.reconnect_min` | 时长 | `1s` | 重建链路的初始退避间隔 |
| `link.reconnect_max` | 时长 | `60s` | 重建链路的最大退避间隔 |
| `link.keepalive_interval` | 时长 | `15s` | 保活探测的间隔；gRPC 客户端的实际下限 10 秒，小于 10 秒按 10 秒探测 |
| `link.keepalive_timeout` | 时长 | `10s` | 保活探测的应答期限 |
| `log.level` | 字符串 | `info` | 运行日志级别 |
| `update.check` | 布尔 | `true` | 是否周期检查 GitHub 上是否有更新版本（periodically check GitHub for a newer release）；关闭后 Broker 不发出检查请求（`REQ-update-001`） |
| `update.check_interval` | 时长 | `24h` | 周期检查的间隔，下限为 `1h`，低于下限的取值不合法（`REQ-update-001`） |

`broker.listen` 的默认值只是默认配置中的一个取值，代码不对这个端口做任何特殊处理。一次性配对密钥的有效期、名录记录的有效期和续签间隔由需求固定（`REQ-broker-pairing-006`、`REQ-broker-discovery-007`、`REQ-broker-discovery-008`），不是配置键；消息编码成 inbox 帧后的字节数上限 1 000 000 同样固定（`REQ-agent-messaging-026`），Agent 曾用名的保留时间 10 分钟是常量，两者见 [broker](design-broker.md)。表中其余默认值是实现选定的初值。

两组跨键约束由本包自己检查：`link.reconnect_max` 不小于 `link.reconnect_min`，`link.keepalive_timeout` 小于 `link.keepalive_interval`。它们是纯值规则，`Set` 与文件重载都把一组键放在一起按候选值检查：组合合法时组内各键一起采用新值，非法时组内各键都保留原值。逐个键与尚未更新的同组键比较会使结果取决于键名的字典序，例如把合法的 `15s`、`10s` 改成 `5s`、`3s` 时只采用其中一项，得到用户从未写过的组合，因此按组判定。需要其他包判断的约束（监听地址能否绑定、别名是否与对端冲突）仍由 `broker` 注册的校验函数完成。

### SetConfig 的取值

`SetConfig` 请求中的 `value` 是一个字符串，原样携带模型给出的文字，不按键的类型预先编码。Broker 按键的类型把它交给 `ParseValue` 解析：字符串、布尔、整数与时长键的取值是一个 YAML 标量，例如 `10m`、`true`；字符串列表键的取值是一个 YAML 流式序列（flow sequence），例如 `[a, b]`。`ParseValue` 要求输入是单个 YAML 文档、一个根节点，节点的标签与键的类型一致；未知键返回 `ErrUnknownKey`，多余文档、类型不符或列表不是流式序列返回 `ErrInvalidValue`，错误指出键与原因。密钥类配置项按字符串标量解析，随后由 `Set` 以 `ErrSecretKey` 拒绝。解析只检查类型，别名长度、端口范围、8 项上限与组合规则由 `Set` 判断。解析结果转成 `Value` 后交给 `Set`；字符串不能按该键的类型解析时，调用以 `invalid_argument` 失败，不改动任何状态。

`GetConfig` 的应答、`SetConfig` 的应答与审计记录中的取值仍用 `ConfigValue` 表示。`Set` 接收解析后的 `Value`，不接收字符串。

## 依赖

`config` 不引用本项目的其他包，符合模块依赖图的方向。

## 核心数据结构

各类文件的位置由部署形态决定：

| 内容 | 用户级 | 系统级 |
|---|---|---|
| 配置文件 | `~/.peers/config.yaml` | `/etc/peers/config.yaml` |
| 数据目录：身份、对端表、名录 | `~/.peers/` | `/var/lib/peers/` |
| 审计日志 | `~/.peers/audit.log` | `/var/log/peers/audit.log` |

运行时目录内有本机 socket `broker.sock` 和账户锁 `broker.lock`。运行时目录只取决于进程的账户与文件系统的状态，与形态、环境变量、配置内容和监听地址无关：root 的是 Linux 上的 `/run/peers/`、macOS 上的 `/var/run/peers/`，系统级 Broker 与 root 的用户级 Broker 因此争用同一把锁；其他账户的首选位置是 `/tmp/peers-<uid>/`，`<uid>` 是进程的账户。首选位置已存在且是符号链接、不是目录或属主不是本账户时，视为被其他账户占用，改用后备位置：Linux 的是 `/run/user/<uid>/peers/`，macOS 的是 `getconf` 的 `DARWIN_USER_TEMP_DIR` 给出的每用户临时目录下的 `peers/`。后备位置的上级目录必须是本账户拥有、不是符号链接、组与其他人都没有任何权限的目录；首选位置的属主是本账户而权限过宽时不算被占用，仍按权限规则拒绝启动，因为账户自己能改正。两处都不可用时拒绝启动，错误写出首选位置的占用者与原因，退出码沿用运行时目录失败时的取值。这条规则只取决于文件系统的状态与进程的账户，不读环境变量；Broker、MCP 服务、终端命令、`service install` 的预检与 `service remove` 使用同一组规则。占用者可以删除并重建首选位置，因此同一账户的不同进程在不同时刻可能算出不同的位置；为此 Broker 启动时在首选与后备两处都检查账户锁，任一处被本账户运行中的实例持有就拒绝启动，MCP 服务与终端命令在同样的两处探测本机 socket，只采用通过进程凭据检查的那个，两处都通过时采用账户锁被持有的那个，都没有持有时采用位置规则选出的那个（`REQ-broker-config-013`）。root 的运行时目录权限 `0755`；系统级 Broker 的本机 socket 在绑定后按 `broker.local_socket_group` 与 `broker.local_socket_mode` 设置属组与权限，哪些账户能连接由它决定；用户级的 socket 固定为 `0600`；其他账户的运行时目录权限 `0700`。运行时目录的属主必须是本账户，不符时拒绝启动。以上位置规则对发布构建成立。带 `peers_testenv` 构建标签的测试构建例外：`PEERS_TEST_RUNTIME_ROOT` 非空时运行时目录的根取它的值，`PEERS_TEST_HOME` 非空时账户家目录取它的值；两者为空时不生效，发布构建不含这段代码。

用户级的数据目录权限 `0700`，其中的文件权限 `0600`。系统级的配置文件由 `service install --system` 在不存在时以 root 为属主、管理组为属组、`0660` 为权限创建，内容为 `Template(ModeSystem)`，已存在时安装不改动；管理组默认为 `peers`，可由 `--admin-group` 指定（[ADR-admin-group](../../adr/adr-admin-group.md)，`REQ-broker-config-040`、`REQ-broker-config-043`）。用户级的配置文件由 `service install` 在不存在时以本账户为属主、`0600` 为权限创建，内容为 `Template(ModeUser)`，已存在时安装不改动。管理员把它的读或写权限授予某个组，即把读取配置或管理操作委托给该组的账户。Broker 的 `Set` 在配置文件不存在而新建时，系统级也用 `0600` 的文件权限：系统级 Broker 不知道管理组，不能照安装的属组与 `0660` 加组。

配置文件的结构与键名一一对应，例如 `broker.alias` 对应顶层 `broker` 下的 `alias`。文件中没有出现的键取默认值，文件不存在等同于空文件。`Set` 只改动文件中它所修改的那个键，文件里的其他内容原样保留；文件不存在时，以运行 Broker 的账户创建它，文件权限 `0600`，所在目录不存在时一并创建，用户级目录 `0700`，系统级目录 `0755`。

默认别名：没有配置 `broker.alias` 时，系统级 Broker 取所在主机系统主机名的第一段（第一个 `.` 之前的部分），用户级 Broker 取主机名的第一段、`.` 与所属账户名依次组成的字符串。主机名取自 `os.Hostname()`，保留原有大小写；账户名取运行 Broker 的账户，经账户数据库查出，与审计 `account` 字段的来源相同；账户名查不到或为空时，账户名部分改用 `os.Geteuid()` 的十进制数字 uid，例如 `hostA.1000`，同一主机上唯一且每次结果相同，同时以 Warn 级别写一条运行日志，说明账户名查不到、改用 uid。别名与监听地址、监听端口和对外地址都无关。主机名和账户名中不属于别名字符集的字符替换为 `-`。主机名取不到或为空时，主机名部分取 `broker`。总长不超过 24 个字符（`REQ-broker-pairing-037`）：超长时先截短主机名部分，至少保留 1 个字符；仍然超长时再截短账户名部分，账户名部分改用 uid 时即截短 uid。截短后去掉末尾的 `.` 与 `-`，结果为空时取 `broker`。配置了 `broker.alias` 时取配置值，本节规则不适用。

监听地址：`broker.listen` 的主机部分为空时，在本机的全部地址上监听，启用了 IPv6 时包括 IPv6 地址；写出了主机部分的，按字面绑定该地址，其中 `0.0.0.0` 与 `::` 在计算对外地址时按全部地址对待。端口不得为 0。

系统级 Broker 的 `broker.listen` 端口小于 1024 时，只有在配置文件只有 root 可写时才生效：属组与其他人都没有写权限，访问控制列表里也没有额外的写授权。经本机接口把 `broker.listen` 设为低端口而调用方不是 root 的，由 `broker` 返回 `permission_denied`，说明低端口需要 root 修改，并且配置文件只能由 root 写。配置文件可被组或其他人写入时，热加载不采用新的低端口，保留旧值，运行日志以 Warn 级记录并追加一条 `config_rejected` 审计记录。启动时这条规则不改变 `Load` 的结果，而在绑定监听地址前由 `broker` 检查：前台运行以退出码 4 结束，带 `--allow-degraded` 的服务进入仅本机降级。拒绝时的提示分两种。`SetConfig` 的拒绝说明低端口需要 root 与只有 root 可写的配置文件，配置文件现在可被其他账户写入，修复办法是 `chmod g-w <配置文件>`（并去掉授予写权限的访问控制列表），或改用 1024 及以上的端口。启动与热加载共用一个提示：写出文件中 `broker.listen` 的当前取值，说明这个值可能由能写该文件的其他账户写入，要先核对该取值是否是想要的，再收紧权限；修复办法同样是 `chmod g-w` 或改用 1024 及以上的端口。用户级 Broker 不受这条规则影响（[ADR-admin-group](../../adr/adr-admin-group.md)，`REQ-broker-config-043`、`REQ-broker-config-058`）。

对外地址：本次运行的对外地址列表由生效的监听地址与 `broker.advertise` 按下表得出，`broker.advertise_extra` 的各项追加在其后（[ADR-advertised-address](../../adr/adr-advertised-address.md)，`REQ-broker-config-030`、`REQ-broker-config-031`、`REQ-broker-config-033`）。

| 监听地址 | `broker.advertise` | 列表中由此得出的部分 |
|---|---|---|
| 不为空 | `主机:端口` | 仅此一项 |
| 不为空 | `主机` | 该主机配上监听端口 |
| 特定地址 | 空 | 监听地址配上监听端口（`REQ-broker-config-035`） |
| 全部地址 | 空 | 默认路由所在接口的地址，每个地址都配上监听端口 |
| 全部地址 | `:端口` | 默认路由所在接口的地址，每个地址都配上该端口 |
| 空，不监听 | 空 | 空 |

表外的组合非法，例如监听特定地址而 `broker.advertise` 只有端口，或不监听而 `broker.advertise` 非空；不监听时 `broker.advertise_extra` 也必须为空（`REQ-broker-config-032`）。组合按本次运行生效的监听地址判定，监听地址来自启动参数时以它为准。`broker.advertise_extra` 中不带端口的项取对外地址所用的端口：`broker.advertise` 写出了端口时取它，否则取监听端口。主机是 IP 地址或域名，IPv6 地址写成带方括号的形式。列表中重复的项只保留首次出现的。

对外地址列表中的每一项有一个种类，种类决定其他 Broker 怎样经这一项连到本 Broker。Go 接口以 `AdvertisedAddr` 表示一项，`Kind` 是种类，`Addr` 是该种类下的地址写法。本版本产生的种类只有直连，取值为 `direct`，其地址写作 `主机:端口`，表示可以直接向该地址建立连接，IPv6 主机带方括号；本包得出的列表只含直连的项。配置文本没有表示种类的语法：`broker.advertise` 与 `broker.advertise_extra` 的合法写法只有上表列出的形式，其他写法一律按写法不合法处理，因此本版本的配置文本没有可表达的未知种类。线路上种类的具体表示法属于实现。其他 Broker 告知的列表与名片所载的列表中出现不认识的种类时怎样处理，见 [link](design-link.md)、[pairing](design-pairing.md) 与 [directory](design-directory.md)。

默认路由所在接口的地址：度量值最小的默认 IPv4 路由所在接口上的 IPv4 地址，不含 `169.254.0.0/16` 中的链路本地地址；存在默认 IPv6 路由时，另加度量值最小的那条所在接口上的全局单播 IPv6 地址，不含链路本地地址与临时地址。默认 IPv4 路由按目的地址与掩码都为零且路由处于启用状态判定，不要求带网关标志，因此只指定设备的默认路由（例如 `default dev wg0`）也算。没有默认路由时这部分为空。主机接入的其他网络上的地址不列入。启动时枚举默认路由失败不拒绝启动：这部分地址取空，运行日志以错误级别记录，周期枚举在后续轮次补上。

列表至多 8 项。`broker.advertise` 含主机时的一项加上 `broker.advertise_extra` 的项数超过 8 的配置非法（`REQ-broker-config-036`）；默认路由所在接口的地址使列表超过 8 项时，舍去这部分中排在后面的地址，`broker.advertise_extra` 的各项全部保留。

本包每隔 `broker.address_check_interval` 重新枚举本机地址，并在 `broker.listen`、`broker.advertise`、`broker.advertise_extra` 变化后重新计算列表；列表与此前不同时通知 `SubscribeAdvertised` 的订阅者（`REQ-broker-discovery-016`）。对端在连接上看到的本方地址不参与计算。

名片中的列表与生成配对密钥时的展示分别见 [directory](design-directory.md) 与 [pairing](design-pairing.md)。

密钥类配置项：`pairing.static_key` 属于这一类，这一类配置项适用下列规则（[ADR-static-pairing-key](../../adr/adr-static-pairing-key.md)）：

- 只能通过编辑配置文件设置。`Set` 对它的修改一律以 `invalid_argument` 拒绝，说明该项只能在配置文件中设置（`REQ-broker-pairing-031`）。
- `Get` 与 `All` 只给出它是否已设置，不返回取值；取值只经 `Secret` 交给 `pairing`（`REQ-broker-pairing-030`）。
- 配置文件被修改后的重载对它照常生效，设置、更换、删除都不需要重启（`REQ-broker-config-017`、`REQ-broker-pairing-032`、`REQ-broker-pairing-033`）。它的 `config_file_changed` 审计记录只记这一项发生了变化，不带 `old` 与 `new`；运行日志、审计日志和错误信息都不出现它的取值（`REQ-broker-config-008`、`REQ-broker-pairing-030`）。
- 预置静态密钥的长度必须在 16 至 128 个字符之间：启动时不合法即拒绝启动并指出该键，重载时不合法即这一项保留原值（`REQ-broker-pairing-029`）。取值原样保存，不做大小写或格式的规范化。
- 取值必须写成 YAML 字符串，写成数字等其他类型的视为不合法；含 ` #` 等会被 YAML 另作解释的字符时要加引号。

审计日志每行一个 JSON 对象：

| 字段 | 含义 |
|---|---|
| `time` | 事件时间，RFC 3339 格式 |
| `event` | 事件类型，取值见下表 |
| `actor` | 发起的 Agent 地址；不是经 Agent 发起，或由对端、文件改动、系统触发时为空 |
| `account` | 发起的系统账户；由对端、文件改动或系统触发时为空（`REQ-broker-config-007`） |
| `peer` | 相关 Broker 的指纹与别名；与 Broker 无关时为空 |
| `key`、`old`、`new` | 配置变更的键与前后的值；密钥类配置项只有 `key` |
| `count` | 合并的事件次数；没有合并时不出现 |
| `detail` | 失败原因等补充信息 |

事件类型是封闭集合：

| 事件 | 触发 |
|---|---|
| `config_set` | 经本机接口完成的一次配置修改 |
| `config_file_changed` | 配置文件由其他程序改动后，某个键采用了新值 |
| `pairing_key_created` | 生成一次性配对密钥 |
| `pairing_succeeded` | 配对成功，发起方与接受方各记一条 |
| `pairing_failed` | 配对失败，记录原因 |
| `link_up` | 链路建立 |
| `link_down` | 链路断开 |
| `unpaired` | 解除配对 |
| `connection_rejected` | 拒绝了一个未知指纹的连接 |
| `rejected_by_peer` | 本方的连接遭对方以“不是对端”拒绝 |
| `local_alias_assigned` | 系统为某个对端生成了本地别名 |
| `config_rejected` | 配置文件的重载拒绝了一个会让管理组取得超出编辑文件能力的值 |
| `update_applied` | Broker 完成了自我更新，记录调用方账户与新旧版本 |
| `update_failed` | Broker 的自我更新失败，记录原因；第二段失败时 Detail 以 `second phase:` 开头 |

直接编辑配置文件触发的 `config_file_changed` 与 `config_rejected` 没有账户归属，`account` 与 `actor` 为空；经本机接口的修改记录调用方账户。审计日志不记录配对密钥的值、消息正文和消息的收发（`REQ-broker-config-008`）。密钥类配置项的变化只记键名，见“密钥类配置项”。

## 核心算法与流程

### 生成配置文件模板

`Template(mode Mode) []byte` 按部署形态生成一份列出全部配置项的配置文件模板，供服务安装时写入尚不存在的配置文件（`REQ-broker-config-040`）。

模板由包内的键表生成，键表是它唯一的来源，不存在手写的模板常量。键表中的每个键带一行英文说明，意思与该键在配置键表中“含义”一栏一致。

段按固定顺序排列：`broker`、`pairing`、`topics`、`directory`、`send`、`link`、`log`，段内按键名排序。文件开头是说明注释：系统级沿用现有的一段关于文件权限的说明，用户级另写一段，说明该文件只供本账户使用。

有固定默认值的键写成两行：上一行是 `# <说明>`，下一行是 `<leaf>: <默认值>`。取值经 YAML 编码器写出，或按同等规则加引号，使 `Load(Template(mode))` 得到的每个有效值都等于该键的默认值，例如 `broker.listen` 的 `:17443` 要加引号。默认值为空的 `broker.advertise`、`broker.advertise_extra`、`broker.local_socket_group`、`pairing.static_key` 与默认值在运行时计算的 `broker.alias` 写成注释，形式是 `# <leaf>: <写法示例>`，下一行说明“没有固定默认值”或“默认值在运行时计算”。写法示例必须能通过该键的校验。模板不把空串写成生效值：空串在某些键上有别的含义，例如 `broker.listen` 的空串表示不监听，与默认值不同。

用户级模板同样列出 `broker.local_socket_group` 与 `broker.local_socket_mode`，并注明这两项只对系统级生效。

### 修改一个键

`Set` 依次做下面的事，前一件失败则不做后面的：

- 先按“配置文件的监视”中的同一套规则采用磁盘上尚未采纳的外部改动，并为每个采用了新值的键写 `config_file_changed` 审计记录。校验因此从磁盘上的当前值出发，去抖期内保存的外部改动不会被这次写入吞掉。
- 检查键存在且不是密钥类配置项，值的类型与取值范围合法，与其他键的组合合法，再调用为该键注册的校验函数。修改 `broker.listen`、`broker.advertise` 或 `broker.advertise_extra` 时，以新值与另外两项的当前值核对“对外地址”中的组合规则与项数上限。
- 读取配置文件的当前内容，只替换这个键，在原文件上就地改写：截断后写入完整内容并同步到磁盘。写回要重写整个文件，文件中的其他内容原样保留，原文件的属主、组、权限位与访问控制列表因此保持不变。文件不存在时以本账户的身份创建它，文件权限 `0600`；所在目录不存在时一并创建，用户级目录 `0700`，系统级目录 `0755`。键在文件中不存在时，按下一段的占位行规则写入。写路径以 `O_NOFOLLOW` 打开配置文件，它是符号链接时写入失败、原文件不变；这条规则依赖配置目录 `/etc/peers` 以 root 为属主、组不可写（[ADR-admin-group](../../adr/adr-admin-group.md)，`REQ-broker-config-043`）。
- 更新内存中的值。
- 追加 `config_set` 审计记录。
- 把这次变更放入通知队列。

键在文件中不存在时，`Set` 先在键所属的段内找一行与 `^\s*#\s*<leaf>:` 匹配的占位行。找到第一处时，把这一行替换为生效的 `<leaf>: <新值>`，位置与缩进不变，它上面的说明注释保留；找不到时，按原有做法追加到段末或文件末尾。除这次替换外，文件其余内容一字不改（`REQ-broker-config-040`）。

读取与写回之间不是原子的：另一个程序在这段窗口内写入的内容会被本次写回覆盖而丢失，这是就地改写接受的代价。磁盘上的配置文件为空或不存在时，写回的内容只包含本次修改的键和文件原有的注释；其他键只留在内存中，本次运行按内存值生效，重启后回到默认值。

写文件失败时返回 `internal`，内存与审计不变：本包尽力把原字节写回文件；无论写回是否成功，都把内存中记录的文件哈希改成磁盘当前内容的哈希，使本进程不把这次留下的空文件或残缺内容当作外部改动采纳。

通知队列由一个协程按提交的顺序逐个调用订阅者，对同一个键的两次修改因此按修改的先后生效。校验函数用于需要别的包判断的情形，由 `broker` 注册：`broker.listen` 的新值只做绑定探测，校验函数绑定后立即关闭，不保留 listener，变更提交后由订阅者实际绑定；探测失败则修改不提交，原有的监听不受影响（`REQ-broker-config-034`）。新值与当前值相同时不探测，直接接受这次修改：正在运行的监听仍占着该地址，探测必然失败。探测与真正的绑定之间存在时间差，期间端口可能被其他进程占用。校验函数在包内互斥锁持有期间执行，不得调用本包的其他导出函数。`broker.alias` 的新值与某个已提交对端的生效别名或一次进行中的握手预占相同时修改不提交；校验函数调用 `pairing` 的 `AliasConflict`，后者只取 `pairing` 的锁。本次运行的监听地址来自启动参数时，`Set` 对 `broker.listen` 的修改在校验之前即以 `invalid_argument` 拒绝，错误说明本次运行的监听地址由启动参数给定，配置文件与内存中的值都不变（`REQ-broker-config-029`）。

订阅者使修改立即生效，订阅在 `broker` 装配完成后交给各包：`broker.alias` 的变更使各链路收到 `PeerInfo` 帧，并使 `directory` 重新签发名片，同时调用 `pairing` 的 `SetOwnAlias` 推入新的本机别名，`broker` 在装配时也推入一次当前值；`broker.listen` 的变更使 `link` 改在新地址上监听；对外地址列表的变化，不论来自 `broker.listen`、`broker.advertise`、`broker.advertise_extra` 的修改还是定期的重新枚举，都经 `SubscribeAdvertised` 送达，使本方是[接受方](../../glossary.md)的各条链路收到 `PeerInfo` 帧，并使 `directory` 重新签发名片；`broker.public` 的变更使 `directory` 发布或撤回本机的记录；改为公开时，`broker` 另行按各 Agent 的公开 topic 集合重新声明。`pairing.static_key` 没有订阅者，`pairing` 在下一次握手查找密钥时读取新值；`Secret` 返回取值之前读取配置文件并比较内容哈希，内容不同即重新加载，文件变化通知丢失时删除或更换仍能生效。各项间隔、次数与上限在下一次使用时取新值。

`link.keepalive_interval` 与 `link.keepalive_timeout` 的生效时机与其余间隔不同：[link](design-link.md) 在接受方一侧于第一次监听、创建 gRPC 服务端时读取，修改后接受方一侧重启才生效；拨号方一侧对下一条拨出的连接生效。只修改 `broker.listen` 的主机部分（例如把 `:17443` 改为 `127.0.0.1:17443`）会被拒绝：校验以新值探测绑定时，`link` 的旧监听仍占着同一端口，探测因此失败；修改这一项需要同时改端口，或改配置文件后重启 Broker，修改失败时继续在原地址监听（`REQ-broker-config-034`）。

### 配置文件的监视

本包监视配置文件，重载只由文件系统通知驱动（REF-fsnotify），没有周期性重扫。文件内容变化时重新解析，与内存中的值逐键比较：

- 解析失败或读到空文件：不采用任何改动，内存中的值不变，运行日志以错误级别记录原因。空文件包括其他程序就地改写时落在截断与写入之间的窗口；只有启动加载把空文件当作空配置。截断恰好落在行边界时，剩下的一段仍可能是合法的 YAML 文档，重载会把它当作删除了其余键的外部改动采用，不再出现的键因此取默认值；这是只凭文件内容判断改动所接受的代价。
- 解析成功：逐键判定。取值合法并通过校验函数的键采用新值；不合法或未通过校验函数的键保留原值，运行日志以错误级别记录。成对约束的键作为一组判定，组合合法时一起采用，否则整组保留原值，结果与键名的字典序无关。`broker.listen`、`broker.advertise` 与 `broker.advertise_extra` 另作为一组判定：按改动后的整体检查组合规则与项数上限，合法则一起采用，否则三项都保留原值。`broker.listen` 的新值端口小于 1024 而配置文件可被组或其他人写入时，不采用新值，保留旧值，运行日志以 Warn 级记录并追加一条 `config_rejected` 审计记录（`REQ-broker-config-043`）。
- 对每个采用了新值的键更新内存、追加一条 `config_file_changed` 审计记录、放入通知队列。

本包自己写入文件期间不处理文件变化；写入完成后把文件内容的哈希记为已采纳，不产生记录。文件变化通知丢失时由 `Secret` 在返回取值前比对文件内容哈希兜底，因此没有周期性重扫也不会让旧密钥一直生效。

本次运行的监听地址来自启动参数时，`broker.listen` 不参与比较与校验：文件中这一项的值变化时不采用，不写审计记录，运行日志记录一次；其他键照常重载（`REQ-broker-config-017`）。

`Load` 失败时本包不启动监视。降级运行中的 Broker 自己监视配置文件所在目录，文件变化时重新执行启动步骤，`Load` 成功后由本包照常监视（[broker](design-broker.md) 的“降级运行”）。

### 审计日志的表示与合并

每行一个 JSON 对象，UTF-8。`time` 是 UTC 的 RFC 3339 纳秒时间；`event` 是事件类型的字符串值；`actor`、`account`、`key`、`detail` 为空时省略。`peer` 是 `{"fingerprint":"<64 位小写十六进制>","alias":"..."}`。`old` 与 `new` 是带类型的对象 `{"kind":"...","value":...}`，`kind` 取 `string`、`bool`、`int`、`duration`、`string_list`：时长的 `value` 是 `10m` 这样的字符串，整数的 `value` 是 JSON 数字。审计文件是 config 的私有格式；`GetConfig` 与 `SetConfig` 的应答使用 `ConfigValue`，与此处的类型对应，密钥类配置项另以是否已设置表示。密钥类配置项的变化只保留 `key`，不写 `old` 与 `new`。打开审计文件时，如果文件末尾不是换行，先追加一个换行；崩溃留下的半行仍然无法解析，但不会吞掉下一条记录。

`connection_rejected` 与由尝试过于频繁引起的 `pairing_failed` 在写入前于内存中合并。合并键由事件类型与来源标识组成：`connection_rejected` 用调用方给出的来源，即 IPv4 地址或 IPv6 /64 前缀，没有来源时用对端指纹；这类 `pairing_failed` 用调用方给出的合并键，`pairing` 在尝试频率超限时填入来源主机，写作 `source:<host>`。同一合并键上，第一个事件开启一个 10 分钟窗口，窗口从这条事件的时间起算，后续事件不顺延窗口。窗口内的后续事件只在内存中累加，不写文件。窗口到期或 `Close` 时，窗口汇总为一条记录追加到文件，记录的时间是窗口第一条事件的时间：窗口内只有一条事件时不带 `count`，多于一条时 `count` 是事件条数。内存中的普通窗口至多 256 个，各种可合并事件合计。已有窗口的事件照常并入；合并键不在表中而普通窗口已满时，事件并入该事件种类的汇总窗口。每种可合并事件至多一个汇总窗口，不计入 256。汇总窗口从第一条事件起算同样的 10 分钟，到期时写一条记录，时间与事件类型取第一条事件，`peer` 为空，`detail` 为 `other sources`，`count` 是并入的事件条数。汇总窗口开启时写一条 Warn 级运行日志 `config_audit_merge_overflow`，字段 `audit_event` 是事件类型，`limit` 是 256。`ReadAudit` 返回文件中的记录加上尚在内存中的窗口记录，按时间排序。到期调度按截止时间与合并键排序，同一截止时间的窗口按合并键顺序落盘；追加事件与安排定时器都不随待合并窗口数做全量排序。到期由一个定时器驱动，定时器始终挂在最早的截止时间上。定时器按单调时钟触发，可能比墙上时间的截止点早一点；这一次触发没有窗口到期时，实现按剩余时间重新挂一个定时器，窗口仍在截止点之后落盘。落盘因此不依赖后续事件：只要还有待合并窗口，就有一个定时器挂在最早的截止时间上。审计文件始终只追加，不改写已有行。合并键只存在于内存，不写入文件。进程在窗口落盘前崩溃时，该窗口的记录丢失，包括窗口中只有一条的事件；这是保持文件只追加所接受的代价。`Close` 期间与之后产生的审计事件不得静默丢弃：审计日志已关闭或尚未打开时，事件至少以错误级别写入运行日志。

`link_up`、`link_down` 不合并：对端已经认证，属于受信任范围（[architecture](../architecture.md) 的“认证与授权边界”）。

### 实现约定

以下约定是设计未写死、由实现确定的细节：

- `All` 按键名的字典序返回；`Get`、`All`、`Set` 返回的列表与字符串切片都是副本。
- `Validate` 与 `Subscribe` 的键必须在键表中且 `Load` 已完成；未知键或未加载是装配错误，直接 panic。
- `Load` 第二次调用返回 `ErrAlreadyLoaded`。
- `log.level` 的取值集合是 `debug`、`info`、`warn`、`error`，不认识的取值非法。
- `broker.advertise` 与 `broker.advertise_extra` 拒绝 `0.0.0.0` 与 `::` 作为主机，其他 Broker 无法连接通配地址。
- `broker.local_socket_mode` 接受不带引号的整数写法（如 `0666`），取值按八进制校验；其他字符串键写成数字仍然非法。
- 同值 `Set` 照常写文件、写审计、发通知，因为这是一次经本机接口完成的修改。
- 当前配置文件在结构上无法解析时 `Set` 失败，不覆盖文件；单个键的取值不合法不阻止其他键的修改。
- 任何段中重复出现的键（包括未知段与未知键）、顶层已知段不是映射、YAML 合并键 `<<` 都按解析错误处理；其余未知段与未知键保留在文件里，不参与加载，`Set` 也不改动它们。
- 配置文件中的锚点与别名（`&name`、`*name`）按解析错误处理，映射与序列的嵌套超过 32 层同样按解析错误处理；检查只遍历节点树、不跟随别名指针，因此自引用的别名不会成环，展开后呈指数增长的别名也不会被展开。配置文件的每一处读取最多读到 1 MiB 加 1 字节，超过 1 MiB 按读取错误处理。
- 特定地址监听且未配置 `broker.advertise` 时，监听地址这一隐含项计入 8 项上限；默认路由来源的地址超过上限时舍去排在后面的项，`broker.advertise_extra` 的项全部保留。
- 审计合并的分组依据包含事件类型，两种不同事件不会因字符串相同而合并。
- 汇总窗口按事件种类分开，每种可合并事件至多一个。
- 文件监视的去抖时长与地址枚举的周期 ticker 都通过包内字段注入；测试用假 ticker 驱动重载与周期枚举，不依赖真实等待。
- 每个平台的 `defaultRouteAddresses` 都有一条冒烟测试直接调用它，只断言 `err == nil`；macOS 的 IPv4 与 IPv6 路由夹具取自 CI 运行 44329785438 在 macOS runner 上采集的真实 `route -n get default` 输出：IPv4 是带 `gateway: 192.168.64.1` 与 `interface: en0` 的正常路由描述；IPv6 没有默认路由，输出为 `route: writing to routing socket: not in table`，退出码为零；并另有 `ifconfig` 失败分支的测试。

## 错误与失败语义

| 情形 | 行为 |
|---|---|
| 启动时配置文件无法解析或含不合法的值 | 拒绝启动，带 `--allow-degraded` 时同样拒绝；无法解析时错误信息给出行号与列号，或指出出错的段名、键名，或说明失败的原因，不引用文件中的取值；含不合法的值时指出键与原因，不含密钥类配置项的取值 |
| 配置文件超过 1 MiB | 读取在 1 MiB 加 1 字节处停止，超过时按读取错误处理：启动时拒绝启动，带 `--allow-degraded` 时同样拒绝；运行期间保留全部原值，运行日志以错误级别记录 |
| 启动时数据目录或审计日志不可用 | `Load` 返回错误；带 `--allow-degraded` 时 `broker` 进入仅诊断降级，否则拒绝启动 |
| 启动时 `broker.listen` 端口小于 1024 且配置文件可被组或其他人写入 | 由 `broker` 在绑定监听地址前拒绝：前台运行退出码为 4，带 `--allow-degraded` 时进入仅本机降级 |
| 启动时监听地址与对外地址配置的组合非法，或对外地址的配置超过 8 项 | 拒绝启动；组合非法时错误信息同时指出 `broker.listen` 与 `broker.advertise` 或 `broker.advertise_extra`，超过 8 项时指出 `broker.advertise_extra` |
| 启动时枚举默认路由失败 | 不拒绝启动，这部分地址取空，运行日志以错误级别记录，周期枚举在后续轮次补上 |
| 运行时目录或账户锁所在目录的属主、权限不符 | 拒绝启动，错误信息指出目录 |
| `SetConfig` 请求中的 `value` 无法按该键的类型解析 | 返回 `invalid_argument`，指出该键与原因，不改动任何状态 |
| `Set` 的键不存在，值不合法，或校验函数拒绝 | 返回 `invalid_argument`，不改动任何状态 |
| `Set` 修改密钥类配置项 | 返回 `invalid_argument`，说明该项只能在配置文件中设置，不改动任何状态 |
| `Set` 使监听地址与对外地址配置的组合非法，或使对外地址的配置超过 8 项 | 返回 `invalid_argument`，说明与哪一项冲突，不改动任何状态 |
| 本次运行的监听地址来自启动参数时 `Set` 修改 `broker.listen` | 返回 `invalid_argument`，说明本次运行的监听地址由启动参数给定，不改动任何状态 |
| 配置文件写入失败 | 返回 `internal`，内存中的值不变，不写审计记录；本包尽力把原字节写回，写回失败时文件可能为空或不完整。无论写回是否成功，本包都把内存记录的文件哈希改为磁盘当前内容的哈希，本进程不采纳这次留下的内容；进程崩溃或写回也失败时，下次启动按文件的现有内容加载，截断恰好落在行边界时，文件可能被读成一份合法的残缺配置，这是就地改写所接受的代价 |
| 运行期间读到的配置文件为空 | 按无法解析处理，保留全部原值，运行日志以错误级别记录 |
| 运行期间配置文件改成了不合法的内容，包括组合非法 | 不合法的项保留原值，其余合法的项照常采用，运行日志记录 |
| 本次运行的监听地址来自启动参数时，配置文件中的 `broker.listen` 改变 | 不采用，其他键照常重载，运行日志记录 |
| 审计日志写入失败 | 触发它的操作照常完成，运行日志以错误级别记录 |
| 审计日志已关闭或尚未打开时收到事件 | 事件以错误级别写入运行日志后丢弃，不静默丢失 |

能够写配置文件的账户不经本机接口也能直接改写它，这正是管理权限以该文件的写权限为准的含义；用户级形态下审计日志与配置文件属于同一账户，该账户下的其他进程能够改写它们，本包不防范这一途径。系统级形态下审计日志只有 root 可写。

## 并发与事务边界

`Set` 与文件重载的各项动作由一把互斥锁串行化，订阅者在锁外由通知协程调用。为某个键注册的校验函数在 `Set` 与文件重载提交之前、持锁期间执行，因此不得调用本包的其他导出函数。`Get` 读取内存中的值，不访问磁盘。审计日志的追加由独立的锁保护，每条记录以一次写调用写入完整的一行。

`config` 的锁与 `pairing` 的锁有固定顺序：先取 `config` 锁，再取 `pairing` 锁；反向禁止。`pairing` 持有自己的锁时不调用 `config` 的任何函数。`config` 的校验函数在持锁期间只调用 `pairing` 的 `AliasConflict` 与 `ResolveAlias`，这两个方法只取 `pairing` 的锁。`broker.alias` 的校验通过到订阅者把新值推入 `pairing` 之间存在窄窗口：窗口内提交的握手可能拿到与新本机别名相同的生效别名，窗口结束后 `pairing` 的提交复核恢复拒绝这个名字。窗口的长度取决于通知队列的排队情况，这是消除锁序反转所接受的代价。

## 实现状态

| 能力 | 状态 | 代码 |
|---|---|---|
| 两种形态的路径与账户锁路径 | 已实现 | `internal/config/paths.go`（`resolvePaths`、`prepareDataDirs`、`CheckRuntimeDir`、`AccountLock`） |
| 配置的加载、默认值与校验 | 已实现 | `internal/config/keys.go`、`internal/config/file.go`、`internal/config/load.go`（`load`、`adoptValues`、`validateLoadedValues`） |
| 启动参数对监听地址的取代与默认别名 | 已实现 | `internal/config/load.go`、`internal/config/alias.go`（`adoptValues`、`defaultAlias`） |
| 对外地址列表的计算与组合校验 | 已实现 | `internal/config/advertise.go`、`internal/config/routes_common.go`、`internal/config/routes_linux.go`、`internal/config/routes_darwin.go`、`internal/config/routes_other.go`（`computeAdvertised`、`platformAddressSource`、`defaultRouteAddresses`） |
| 密钥类配置项的读取、修改限制与长度校验 | 已实现 | `internal/config/keys.go`、`internal/config/state.go`、`internal/config/api.go`（`validateValue`、`secretLocked`、`Secret`、`setLocked`） |
| 本机地址的定期重新枚举与变更通知 | 已实现 | `internal/config/state.go`、`internal/config/load.go`、`internal/config/notify.go`（`startAddressTickerLocked`、`reevaluateAdvertised`、`newNotifier`） |
| 配置的读取与修改 | 已实现 | `internal/config/api.go`、`internal/config/state.go`、`internal/config/file.go`（`Get`、`Set`、`setLocked`、`setKey`） |
| 附加校验与变更通知 | 已实现 | `internal/config/api.go`、`internal/config/notify.go`（`Validate`、`ProbeListen`、`Subscribe`、`newNotifier`） |
| 配置文件的监视 | 已实现 | `internal/config/watch.go`、`internal/config/state.go`（`startWatch`、`startWatchWithDebounce`、`reloadFromDisk`、`reloadLocked`） |
| 配置文件模板的生成 | 已实现 | `internal/config/template.go`（`Template`、`templateExamples`、`templateSections`） |
| 修改配置项时替换注释占位行 | 已实现 | `internal/config/file.go`（`replacePlaceholderLine`、`setKey`） |
| 审计日志的追加、合并与读取 | 已实现 | `internal/config/audit.go`（`openAudit`、`append`、`mergeLocked`、`flushExpiredLocked`、`read`、`terminatePartialLine`） |
| `broker.offline` 配置项 | 已实现 | `internal/config/keys.go`（`keyTable` 中的 `broker.offline`） |
| `Load` 失败的分类与绑定前的低端口检查 | 已实现 | `internal/config/api.go`（`ErrDataUnavailable`、`DataUnavailableError`）、`internal/config/lowport.go`（`CheckLowPort`、`checkLowPortAtStartup`） |
