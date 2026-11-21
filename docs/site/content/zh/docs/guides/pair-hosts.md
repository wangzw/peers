---
title: 两台主机配对
linkTitle: 两台主机配对
weight: 2
---

配对让两个 Broker 凭指纹互相信任。这是连接两台主机的唯一途径，起点是一方生成、双方共有的配对密钥。

## 开始之前

- 两个 Broker 都在运行，彼此尚未配对。
- 在接受配对的那台主机上，Broker 按默认地址监听：本机全部地址的 17443 端口。另一台主机能连到接受方 Broker 的某个对外地址；另一台主机可以位于 NAT 之后。
- 两位用户之间有一条 peers 之外的可信途径，用来传递连接提示词与密钥。
- 运行会话或终端命令的账户对各自的 Broker 配置文件有写权限。用户级 Broker 的配置文件属于该账户，所以自然满足。系统级 Broker 上，管理组拥有写权限：组成员在该侧直接执行命令；不在组内的账户请管理员把它加入组，或由管理员代为执行。本账户的用户级 Broker 正在运行时，用 `sudo` 访问系统级 Broker。管理组与权限规则见[管理组]({{< relref "management-permissions.md" >}}#管理组)。

## 生成配对密钥

在接受方主机上说“生成一个配对密钥。”并批准这次调用（模型会调用 `create_pairing_key`），或者在终端运行：

```sh
peers key create
```

两种做法都会给出：

- 一个一次性配对密钥，10 分钟后失效，成功配对一次后也失效；
- 本机 Broker 的对外地址，每项都带主机名；
- 一段填好密钥与地址、可直接粘贴的连接提示词。

把连接提示词经 peers 之外的途径交给对方主机的用户，不要让 Agent 转发；原因见[安全模型]({{< relref "/docs/security" >}}#配对与指纹)。地址不是秘密，可以与密钥分开传递。

## 在另一台主机上发起配对

在另一台主机的会话里粘贴连接提示词并批准这次调用（模型会调用 `connect_broker`），或者运行：

```sh
peers broker connect <address> --key <key>
```

提示词里就有这条命令，地址与密钥已经填好，可以直接在另一台主机上运行，也可以把整段提示词粘贴进那里的会话。地址写成 `host:port`，或者按对方展示的原样写成 `hostname(host:port)`；后一种写法里的主机名只供用户辨认，不用于连接。手写 `hostname(host:port)` 放进 shell 命令时要加引号，提示词里的命令已经加好。

两种做法都会给出接受方 Broker 的别名和指纹。

## 检查链路

在两台主机上分别说“列出本 Broker 知道的 Broker。”（模型会调用 `list_brokers`），或者运行 `peers broker list`。两边都会把对方列为对端，状态为 `connected`。对端一项还会给出本 Broker 的角色：调用 `connect_broker` 或运行 `peers broker connect` 的一方是 `dialer`，生成密钥的一方是 `acceptor`。

网络中断后，拨号方无需用户操作就会重新建立链路。接受方从不主动拨号，因此这条双向链路只需要一个方向可达。

## 特殊情况

- 密钥过期或已经用过一次。一次性配对密钥有效期 10 分钟，成功配对一次后失效。超过时间再使用会返回密钥无效或已过期的错误，双方都不保存对方的身份。重新生成一个密钥，从头再来。
- 别名已被占用。常见起因是默认别名只取主机名的第一段：主机名第一段相同的两台主机，例如 `workstation.example.com` 与 `workstation.example.net`，其系统级 Broker 会得到相同的别名，两台主机上同名账户的用户级 Broker 也是如此。如果接受方 Broker 的别名与本 Broker 自己的别名、另一个对端的别名，或另一个对端仍然保留的旧别名相同，`connect_broker` 会要求指定本地别名。说“连接到 `<address>` 上的 Broker，密钥是 `<key>`，本地别名是 `<alias>`。”，别名不超过 24 个字符（模型会调用 `connect_broker`）；也可以运行 `peers broker connect <address> --key <key> --alias <alias>`，名字同样不超过 24 个字符。这个别名只在本 Broker 上有效。
- 展示的地址连不上。随密钥给出的地址未必能从对方连上，例如接受方主机位于端口转发之后。提示词列出多个地址时，最后一行给出其余可选地址。改用一个对方能连上的地址，说“连接到 `<address>` 上的 Broker，密钥是 `<key>`。”（模型会调用 `connect_broker`），或者在 `peers broker connect` 里使用它。
- 网络中断。配对完成后，拨号方不需要密钥就会重连。稍等片刻，再说一次“列出本 Broker 知道的 Broker。”（模型会调用 `list_brokers`），或者运行 `peers broker list` 检查，对端会回到 `connected`。
- 链路一直建不起来，详情显示版本不兼容。两个 Broker 只有使用同一个链路协议版本才能建立链路。`peers service status` 的 `DETAIL` 列与 `list_brokers` 的 `status_detail` 显示 `link protocol version incompatible with this peer; upgrade the older Broker`；本方是接受方时，后面还有 `: peer speaks link protocol version <N>`。拨号方以 `backoff` 状态持续重试，接受方停在 `waiting`。`peers broker list` 不显示原因，运行日志里有 `link: protocol version mismatch`。把较旧的一台更新到与另一台相同的版本，拨号方会自动重新建立链路，不用重新配对。
- 重复部署。接受方 Broker 也可以在配置文件里预置静态密钥，键为 `pairing.static_key`。取值 16 至 128 个字符，不过期，可以反复使用。工具不会生成、设置或返回它；部署之前在配置文件里取消 `static_key` 一行的注释，填入密钥，例如 `openssl rand -hex 32` 的输出。直接运行 Broker、用 `peers config set` 写出的文件没有这一行；这时在 `pairing:` 段下加上 `static_key: "<key>"`。配对时说“连接到 `<address>` 上的 Broker，密钥是 `<key>`。”，密钥用这个值（模型会调用 `connect_broker`），或者运行 `peers broker connect <address> --key <key>`。
