---
title: 文件与路径
weight: 4
---

Broker 把配置、身份、对端表、名录和审计日志放在固定位置。位置取决于部署形态：用户级 Broker 只服务一个账户；系统级 Broker 以 root 运行，服务本机上获准使用它的各个账户。

## 用户级 Broker

| 路径 | 内容 |
|---|---|
| `~/.peers/config.yaml` | 配置文件，权限 `0600`。文件不存在时由 `peers service install` 生成。 |
| `~/.peers/identity.key` | Broker 的私钥，权限 `0600`。 |
| `~/.peers/identity.crt` | 与私钥配对的自签名证书，权限 `0600`。 |
| `~/.peers/peers.json` | 已配对的对端，权限 `0600`。 |
| `~/.peers/directory.json` | 本 Broker 已知的名片与公开 topic 声明。 |
| `~/.peers/audit.log` | 审计日志，权限 `0600`。 |
| `/tmp/peers-<uid>/broker.sock` | 本机 socket，权限 `0600`。 |
| `/tmp/peers-<uid>/broker.lock` | 账户锁，保证每个账户至多一个 Broker。 |

`<uid>` 是账户的数字 ID。运行时目录权限为 `0700`。身份文件在 Broker 第一次启动时创建；`peers.json` 和 `directory.json` 在第一次有内容需要保存时出现。以 root 运行用户级 Broker 时（例如 root 在终端里用 `peers service run --user` 启动的那一个），运行时目录是 Linux 上的 `/run/peers`、macOS 上的 `/var/run/peers`，与系统级 Broker 相同。不经 `service install` 直接运行 Broker 时不会生成配置文件；文件不存在时，全部键都取默认值。

运行时目录通常是 `/tmp/peers-<uid>`。这个路径被别的账户占用时（是符号链接、不是目录，或是别的账户拥有的目录），Broker 改用后备目录：Linux 上是 `/run/user/<uid>/peers/`，macOS 上是 `getconf DARWIN_USER_TEMP_DIR` 给出的每用户临时目录下的 `peers/`。socket 与锁文件在其中沿用原来的文件名。Broker、会话与终端命令都会在这两处查找。后备目录的上级目录必须属于本账户，是目录而不是符号链接，并且组与其他人都没有任何权限；两处都不可用时，Broker 拒绝启动，错误写明占用者。root 的运行时目录不变。

Broker 每分钟把账户锁、运行时目录与本机 socket 的时间戳设为同一时刻，按路径设置时不跟随符号链接，所以按时间清理 `/tmp` 的规则不会删掉它们。

## 系统级 Broker

| 路径 | 内容 |
|---|---|
| `/etc/peers/config.yaml` | 配置文件，列出全部键，属主为 root，属组为默认的 `peers`，权限 `0660`。文件不存在时由 `peers service install --system` 生成。 |
| `/var/lib/peers/identity.key` | Broker 的私钥，权限 `0600`。 |
| `/var/lib/peers/identity.crt` | 与私钥配对的自签名证书，权限 `0600`。 |
| `/var/lib/peers/peers.json` | 已配对的对端，权限 `0600`。 |
| `/var/lib/peers/directory.json` | 本 Broker 已知的名片与公开 topic 声明。 |
| `/var/log/peers/audit.log` | 审计日志，权限 `0600`。 |
| `/run/peers/broker.sock` | 本机 socket，Linux。 |
| `/var/run/peers/broker.sock` | 本机 socket，macOS。 |
| `/run/peers/broker.lock` 或 `/var/run/peers/broker.lock` | 账户锁，与 socket 在同一目录。 |

运行时目录权限为 `0755`。socket 的属组与权限取自 `broker.local_socket_group` 和 `broker.local_socket_mode`；权限默认为 `0666`，本机任何账户都能连接，随后由配置文件的权限决定各账户能做什么。

## 服务定义

`peers service install` 写入一个服务定义：

| 平台 | 形态 | 路径 |
|---|---|---|
| Linux | 用户级 | `~/.config/systemd/user/peers.service`；`XDG_CONFIG_HOME` 为绝对路径时是 `$XDG_CONFIG_HOME/systemd/user/peers.service` |
| Linux | 系统级 | `/etc/systemd/system/peers.service` |
| macOS | 用户级 | `~/Library/LaunchAgents/io.github.wangzw.peers.plist` |
| macOS | 系统级 | `/Library/LaunchDaemons/io.github.wangzw.peers.plist` |

## 补全与 man 页

安装脚本写入 shell 补全，系统级安装还会写入 man 页：

| 级别 | 路径 | 内容 |
|---|---|---|
| 用户级 | `~/.local/share/bash-completion/completions/peers` | bash 补全。 |
| 用户级 | `~/.config/fish/completions/peers.fish` | fish 补全，主机装有 fish 时才写入。 |
| 系统级 | `/usr/local/share/bash-completion/completions/peers` | bash 补全。 |
| 系统级 | `/usr/local/share/zsh/site-functions/_peers` | zsh 补全。 |
| 系统级 | `/usr/local/share/fish/vendor_completions.d/peers.fish` | fish 补全，主机装有 fish 时才写入。 |
| 系统级 | `/usr/local/share/man/man1/peers.1` 与 `peers-*.1` | `peers` 的 man 页，帮助里列出的每个命令另有一页。 |

用户级安装不装 zsh 补全；`install.sh` 会打印手动启用它的命令。

## 日志

Broker 把运行日志写到标准错误，把审计日志写到上面的审计文件。

- Linux 服务把日志写入 systemd journal：用户级服务用 `journalctl --user -u peers.service`，系统级服务用 `journalctl -u peers.service`。
- macOS 用户级服务写入 `~/Library/Logs/peers/broker.log`；系统级守护进程写入 `/var/log/peers/broker.log`。
- 在终端里启动的 Broker，运行日志写进那个终端。

## 权限

用户级 Broker 以该账户身份运行。Broker 在数据目录不存在时以 `0700` 创建它，以 `0600` 创建文件，并以 `0600` 绑定本机 socket，所以只有该账户能访问它们。

系统级 Broker 以 root 运行。配置文件的权限决定其他账户能用管理工具和终端命令做哪些事：读权限允许读取配置、完整状态与审计日志；写权限允许修改配置和管理配对。系统级安装默认已经把新生成的配置文件以写权限交给 `peers` 组；已存在的文件用 `chgrp` 和 `chmod` 交给这个组，做法见[以配置文件权限管理 Broker]({{< relref "management-permissions.md" >}}#管理组)。文件不存在、由系统级 Broker 经 `set_config` 或 `peers config set` 新建时，文件以 root 为属主、权限 `0600`：Broker 不知道管理组，`0600` 也让其他账户读不到 `pairing.static_key`。这样新建的文件没有组写权限，管理员要用[管理组]({{< relref "management-permissions.md" >}}#管理组)一节里的命令把它交给组。`broker.listen` 使用小于 1024 的端口时，不要把文件交给这个组；规则与修复方法见[配置键]({{< relref "docs/reference/configuration" >}}#类型与约束)。Broker 检查调用方对实际文件的访问权限，所以经工具或终端命令能做的事不超出直接编辑文件能做的事。

socket 及其权限见[配置键]({{< relref "docs/reference/configuration" >}})。项目配置文件另见[项目配置文件]({{< relref "docs/reference/project-config" >}})，[使用指南]({{< relref "docs/guides" >}})介绍怎样安装服务。
