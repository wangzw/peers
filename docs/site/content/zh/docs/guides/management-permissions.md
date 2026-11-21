---
title: 以配置文件权限管理 Broker
linkTitle: 管理权限
weight: 7
---

工具和终端命令能改动的范围，不超出调用方账户直接修改 Broker 配置文件所能改动的范围。Broker 从操作系统为本机 socket 另一端进程提供的凭据里取得账户，不采信调用方的陈述。

## 规则

| 操作 | 对配置文件所需的权限 |
|---|---|
| `list_agents`、`list_topics`、`join_topic`、`leave_topic`、`send_message` | 无 |
| `list_brokers`、`peers broker list` | 无 |
| `get_config`、`peers config list`、完整的 `peers service status` 输出、`peers service audit` | 读 |
| `set_config`、`peers config set` | 写 |
| `set_broker_online`、`peers service offline`、`peers service online` | 写 |
| `create_pairing_key`、`peers key create` | 写 |
| `connect_broker`、`peers broker connect` | 写 |
| `disconnect_broker`、`peers broker disconnect` | 写 |
| `update_broker`、经 Broker 执行的 `peers update` | 写 |

`peers update` 在本地替换二进制时，改为检查二进制所在目录的写权限，情形是没有正在运行的 Broker，或选中的用户级 Broker 不在服务管理器下运行。

用户级 Broker 的配置文件属于运行它的账户，所以这些检查对该账户自然通过。系统级 Broker 的文件是 `/etc/peers/config.yaml`。新安装会以属主 root、属组为默认的 `peers`、权限 `0660` 创建它；已存在的文件不改动。读权限还允许账户查看完整状态和审计日志；写权限允许它修改设置、管理配对。两级权限都由普通的文件权限决定，不另设管理员名单。

拥有配置文件的组的成员不用 `sudo` 就能管理 Broker。`peers service status`、`audit`、`online`、`offline` 加 `--system` 连到系统级 Broker。本账户没有用户级 Broker 时，`peers config`、`peers key`、`peers broker` 也直接连到系统级 Broker；本账户运行着用户级 Broker 时，这三条连到那个用户级 Broker，要管理系统级 Broker 就得加 `sudo`。`peers service start`、`stop`、`restart` 直接调用 `systemctl` 或 `launchctl`，`peers service install`、`remove` 创建或删除系统级服务，这几条仍需 `sudo`。管理员用 `sudo` 执行终端命令时按 root 的权限判定，所以无论文件的属组是什么，管理员都能用这种方式管理 Broker。[会话或命令使用哪个 Broker]({{< relref "system-broker.md" >}}#会话或命令使用哪个-broker)一节介绍选择顺序。

检查识别的是操作系统账户，不是人，也不是模型。同一账户下的任何进程，无论经工具还是从终端，都能发起同样的调用。

## 管理组

`peers service install --system` 默认会创建 `peers` 组（不存在时），已存在的组直接使用，不改成员。用 `--admin-group <name>` 可以改用别的组名：组名只能以小写字母或下划线开头，其后只能是小写字母、数字、下划线或连字符。系统级安装新建的配置文件属于该组，权限为 `0660`，所以组成员不用 `sudo` 就能管理 Broker。这个组的可信程度等同于“能手工编辑配置文件”，它不等于 root。

已有同名组且组内已有成员时，如果本次安装要新生成配置文件、管理员又没有用 `--admin-group` 指定这个组、此前也没有安装记录过这个组，安装会拒绝：否则这些成员会在管理员不知情的情况下获得系统级 Broker 的管理权。安装不写任何文件，以退出码 1 结束，报错为

```
peers service install: cannot use the existing group "<group>" as the administration group: its members <a, b and c> would be able to manage the system Broker; pass --admin-group <group> to accept them, or pass --admin-group with another name
```

只有一个成员时写成 `member <a>`。用 `--admin-group` 指定这个组，表示接受这些成员。成员名单包括附加成员和主组为该组的账户，macOS 上还包括嵌套组里的账户；名单读不出来时，安装同样拒绝，报错为 `cannot read the members of group "<group>": …`。组 id 为 0 的组一律拒绝，指定与否都一样，因为组内每个账户都会拿到配置文件的权限。用 `--admin-group` 指定了组，或配置文件已存在时，安装不读成员名单，所以账户数据库损坏也不会挡住这样的重装。先前安装记录在 `/var/lib/peers/admin-group` 里的组不会被拒绝，但成员仍会读取并列入汇总。安装脚本在 `--system` 下被拒时，打印 `install.sh: cannot install the system service`，以退出码 1 结束。

安装脚本会询问是否把执行安装的账户加入该组，默认不加入。`--join-admin-group` 不询问直接加入，`--no-admin-group` 既不询问也不加入。没有终端时，脚本不加入任何账户，并打印加入组的命令。Broker 每次判定权限都从账户数据库读取账户的组，所以经 Broker 做的管理操作，加入组后下一次调用就生效。要让本账户自己的进程带上新的成员关系，例如直接编辑配置文件，先重新登录或执行 `newgrp peers`。要把别的账户加入组，Linux 上执行 `sudo usermod -aG peers <account>`，macOS 上执行 `sudo dseditgroup -o edit -a <account> -t user peers`；设置过 `--admin-group` 时，把命令里的 `peers` 换成那个组名。

已有安装时，以 root 运行安装输出给出的这条命令：

```sh
chgrp peers /etc/peers/config.yaml && chmod 0660 /etc/peers/config.yaml
```

只授予读权限时，把 `0660` 换成 `0640`。

这个组带有这些限制：

- `broker.listen` 的端口小于 1024 时，只有在配置文件只能由 root 写时才生效；执行 `chmod g-w <path>`，或把端口改为不小于 1024，即可恢复；规则见[配置键]({{< relref "configuration.md" >}}#类型与约束)。
- 系统级服务的安装与卸载仍然需要 root。

只授读权限（`0640`）不受低端口规则限制，因为这条规则只看组和其他账户能否写这个文件。

直接编辑配置文件引起的改动，审计日志里没有账户归属，因为只有经本机 socket 的修改才带有调用方账户。

能使用 Broker 和能管理 Broker 是两件事。[运行系统级 Broker]({{< relref "system-broker.md" >}})里的设置决定哪些账户能连接本机 socket，配置文件的权限决定其中哪些账户能读取或修改 Broker。

## 其他前提

- Claude Code 自己的权限设置决定一次工具调用是否需要你批准，这只管工具；终端命令不经过 Claude Code 的权限确认。文件权限检查与此独立，工具调用要文件权限检查和会话里的批准都允许才会执行。
- 不要把 `update_broker` 加入 Claude Code 的自动批准名单。这条确认是升级前的最后一道人工检查；系统级安装时，Broker 替换的是以 root 运行的二进制。
- 工具和终端命令都不能设置 `pairing.static_key`，也不能读取它的取值。`get_config` 与 `peers config list` 只报告它是否已设置，`set_config` 与 `peers config set` 对它总是失败。要设置、更换或删除它，直接编辑配置文件；Broker 无需重启就会采用改动。
- 权限不足的错误不会改变任何状态：配置、对端和有效的配对密钥都保持原样。终端命令权限不足时，Broker 给出的错误会写到标准错误，其中包含配置文件的路径，命令以退出码 1 结束。
