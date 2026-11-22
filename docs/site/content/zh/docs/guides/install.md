---
title: 安装
linkTitle: 安装
weight: 1
---

本指南介绍安装级别、安装指定版本、从发布包安装、安装脚本的选项、安装输出、安装失败、手动安装与从源码构建。一条命令的安装方式见[快速开始]({{< relref "/docs/getting-started" >}})；用 `peers update` 保持更新，见[更新 peers]({{< relref "update.md" >}})。

## 选择安装级别

一条命令安装与发布包里的 `sh ./install.sh` 两种级别都能装。以 root 运行时，脚本不经询问直接装系统级。其他账户在终端里运行、又没有给出指定级别的选项时，会看到询问 `Install for every account on this host? [y/N]`：回答 y 经 `sudo` 安装为系统级，其他回答装成本账户的用户级。没有终端时，非 root 账户装用户级。主机上已有系统级服务时，询问改为 `Upgrade the system installation? [Y/n]`：直接回车或回答 y 升级系统级安装，回答 n 时主机不做任何改动，以退出码 0 结束，并输出 `install.sh: the system installation was left unchanged.`。`--system` 与 `--user` 跳过询问；同时给出两个是用法错误，以 root 给出 `--user` 会被拒绝并提示去掉它。回答 y 之后 `sudo` 不可用时，脚本失败，并给出 `a system install needs sudo, which was not found; install sudo, or pass --user for a user install`。

## 用一条命令安装

安装指定版本时，先设好版本号，再传 `--version`：

```sh
VERSION=0.6.0
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh -s -- --version "$VERSION"
```

`--version` 必须写在所有其他参数之前。它指定一个已发布的版本；这个版本没有可安装的发布包时，脚本中止并报 `version <version> not found`。其余参数原样传给发布包里的 `install.sh`，所以下一节的选项在这里同样可用。

这条命令要求 macOS 或 Linux、amd64 或 arm64；用 `curl` 取脚本，脚本内部下载发布包时用 `curl` 或 `wget`；有 `tar`；有 `sha256sum` 或 `shasum`；能访问 github.com。系统级形式还需要 `sudo`。

除非用 `--version` 指定了版本，脚本会取最新正式版本的版本号，然后把本平台的发布包和同一版本的 `checksums.txt` 下载到临时目录，校验发布包，解压后执行其中的 `install.sh`。脚本的每次下载都只走 HTTPS；无论用 `curl` 还是 `wget`，遇到跳转到 http 地址都会拒绝。下载失败或校验和不符时，脚本中止，主机上不留任何改动；脚本退出或被中断时，临时目录会被删除。脚本自身没下载完整时什么都不会执行，因为它的工作在最后一行才开始。

把脚本通过管道交给 shell，信任程度与手动从 GitHub Releases 下载发布包相同。发布没有签名，`checksums.txt` 与发布包来自同一个发布，所以校验和相符只能说明下载没有损坏，证明不了这个发布本身可信。想先看过再执行，可以先把脚本下载下来，读一遍，再在它所在的目录执行：

```sh
curl -fsSLO https://github.com/wangzw/peers/releases/latest/download/install.sh
sh install.sh
```

脚本做的事情与发布包里的 `install.sh` 相同，见下一节。

## 从发布包安装

peers 以 `tar.gz` 发布包的形式放在 [GitHub Releases](https://github.com/wangzw/peers/releases/latest) 上。下载对应平台和架构的发布包，以及同一发布里的 `checksums.txt`：

```sh
VERSION=0.6.0
curl -LO https://github.com/wangzw/peers/releases/download/v${VERSION}/peers_${VERSION}_<os>_<arch>.tar.gz
curl -LO https://github.com/wangzw/peers/releases/download/v${VERSION}/checksums.txt
```

把 `<os>_<arch>` 换成 `darwin_amd64`、`darwin_arm64`、`linux_amd64` 或 `linux_arm64`。下面的校验与解压示例用 arm64 发布包；在 amd64 主机上，把 `arm64` 换成 `amd64`。

使用前先用 `checksums.txt` 校验发布包。Linux 上：

```sh
VERSION=0.6.0
grep peers_${VERSION}_linux_arm64.tar.gz checksums.txt | sha256sum -c -
```

macOS 上用 `shasum`：

```sh
VERSION=0.6.0
grep peers_${VERSION}_darwin_arm64.tar.gz checksums.txt | shasum -a 256 -c -
```

新建一个目录，把发布包解压进去：发布包没有外层目录，不新建目录时，`peers` 二进制、`LICENSE`、`README.md`、`install.sh`、`third_party/`、`THIRD_PARTY_LICENSES/` 与 `man/` 都会直接落在当前目录：

```sh
VERSION=0.6.0
mkdir peers-${VERSION}
tar -xzf peers_${VERSION}_darwin_arm64.tar.gz -C peers-${VERSION}
```

Linux 上把 `darwin` 换成 `linux`。

进入解压出的目录并运行脚本：

```sh
VERSION=0.6.0
cd peers-${VERSION}
sh ./install.sh
```

脚本会像在线命令一样询问级别；见[选择安装级别](#选择安装级别)。

用户级形式把二进制装到 `~/.local/bin`，安装并启动用户级 Broker 服务，并为你的账户注册 MCP 服务。系统级形式把二进制装到 `/usr/local/bin`，安装并启动系统级 Broker 服务，并为执行安装的账户注册 MCP 服务；它还会创建 `peers` 组，并询问是否把这个账户加入组内。管理组的含义、这个询问及其效果见[管理组]({{< relref "management-permissions.md" >}}#管理组)。

脚本接受这些选项：

| 选项 | 作用 |
|---|---|
| `--system` | 不经级别询问装为系统级；不是 root 时，脚本经 `sudo` 重新执行自己。 |
| `--user` | 不经级别询问装为本账户的用户级。不能与 `--system` 同时给出。 |
| `--prefix <dir>` | 把二进制装到 `<dir>`，代替默认目录：用户级安装为 `~/.local/bin`，系统级安装为 `/usr/local/bin`。 |
| `--no-service` | 不安装 Broker 服务，需要[自己启动 Broker](#启动-broker)。 |
| `--no-mcp` | 不注册 MCP 服务。 |
| `--admin-group <name>` | 改用组 `<name>` 管理系统级 Broker，代替默认的 `peers` 组；组名只能以小写字母或下划线开头，其后只能是小写字母、数字、下划线或连字符。只在与 `--system` 同用时有效。 |
| `--join-admin-group` | 不询问，直接把执行安装的账户加入管理组。只在与 `--system` 同用时有效。 |
| `--no-admin-group` | 不询问是否把执行安装的账户加入管理组。只在与 `--system` 同用时有效。 |

`--join-admin-group` 与 `--no-admin-group` 同时给出是用法错误。

脚本可以重复执行：替换二进制，用新二进制重启服务，不会重复注册 MCP 服务。如果已经注册为别的命令，脚本保留原有注册；要改过来，先运行 `claude mcp remove -s user peer`，再执行一次脚本。脚本不访问网络，安装用的所有文件都来自解压出的发布包。

传了 `--no-mcp` 时，脚本跳过注册，需要自己执行手动一节里的注册命令。找不到 `claude`，或者不经 `sudo` 直接以 root 运行且没有可注册的账户时，脚本会打印要执行的 `claude mcp add` 命令。

对应级别的配置文件不存在时，由 `service install` 生成它，列出全部配置项：用户级服务是 `~/.peers/config.yaml`，系统级服务是 `/etc/peers/config.yaml`。[配置键]({{< relref "/docs/reference/configuration" >}})介绍这个文件。

macOS 上，脚本自己清除二进制上的隔离属性，所以用浏览器下载也不需要额外步骤。用户级安装目录不在 `PATH` 上时，脚本会提示；把它加入 `PATH` 才能按名字运行 `peers`。macOS 上的用户级服务需要图形登录会话；没有图形会话的主机改用系统级安装或 `--no-service`。

系统级安装要求二进制所在目录及其每一级上级目录属主为 root，组与其他人都不可写。装了 Homebrew 的 Intel Mac 上，`/usr/local/bin` 属于你的账户，安装会被拒绝；先用 `sudo mkdir -p <dir>` 建一个 root 所有的目录，再用 `--prefix` 指定它。

## 补全与 man 页

安装脚本用刚装好的二进制为 `peers` 写入 shell 补全。系统级安装装好 bash、zsh 与 fish 的补全；用户级安装装好 bash 与 fish 的补全，并打印手动启用 zsh 补全的命令。主机上没有 fish 时，两个级别都不装 fish 补全。

也可以用 `peers completion <shell>` 自己生成 shell 补全脚本，写进你的 shell 读取补全的目录；支持 `bash`、`zsh`、`fish` 与 `powershell`。

系统级安装还会把 man 页写入 `/usr/local/share/man/man1`，可以用 `man peers` 查看。man 页位于发布包的 `man/` 目录。

系统级安装写入 shell 补全与 man 页之前，会从 `/usr/local` 起逐级检查目标目录。某一级是符号链接，属主不是 root 也不是执行 `sudo` 的账户，或组与其他账户可写时，安装跳过这个文件：打印一条警告，并给出手动安装该文件的命令，其余安装照常完成。跳过补全时的警告为

```
install.sh: warning: skipped the <shell> completion: <reason>; install it with: <prefix>/peers completion <shell> > <path>
```

跳过 man 页时的警告为

```
install.sh: warning: skipped the man page <page>: <reason>; install it with: install -m 0644 <src> <dir>/
```

用户级安装写入自己的目录，不做这项检查。

二进制所在的目录另行检查：从二进制所在目录一直到 `/`，每一级都必须属主为 root、组与其他人都不可写，并且其他账户可以进入；任何一级不合格，安装都会失败，以退出码 1 结束，例如输出 `install.sh: system install: <dir> is not owned by root; pass --prefix with a root-owned directory such as /usr/local/bin`。

## 安装输出

安装成功的汇总以 `peers <version> installed (<level> service, starts at <login|boot>)` 开头。脚本替换已安装的版本时，会在汇总之前先输出一行：新版本更晚时输出 `Upgraded from <old>.`，更早时输出 `Downgraded from <old>.`，相同时输出 `Reinstalled peers <version>.`，两个版本无法比较先后时输出 `Replaced peers <old>.`。带 `--no-service` 时，汇总以 `peers <version> installed (no service)` 开头。汇总给出级别、二进制路径、版本、配置文件路径、运行日志位置、审计日志路径，以及启动、停止、重启与卸载 Broker 的命令。系统级安装的汇总还有一行 `admin group:`，给出管理组和本次安装得到它的方式：`(created)`、`(existing, members: <a, b>)`、`(existing, no members listed)`，安装没有读成员名单时为 `(existing)`。系统级安装的启动、停止、重启命令带 `sudo`。[文件与路径]({{< relref "/docs/reference/files" >}})列出配置、运行日志、审计日志与运行时目录的路径。

安装失败时，输出写明原因：地址已被占用时给出地址、配置文件与 `broker.listen`；服务管理器报错时给出执行的命令与它的输出；权限不足时给出路径和缺少的权限；Broker 在启动阶段退出时给出运行日志里的原因。只有服务在运行、Broker 进入降级运行时，安装才保留服务、给出修复命令，并以退出码 3 结束；安装失败时写入或启用的服务，不会在开机或登录时自动启动。细节见 [peers service install]({{< relref "/docs/reference/cli" >}}#peers-service-install) 与 [FAQ 的端口一问]({{< relref "/docs/faq" >}}#端口被占用时会怎样)。

卸载安装见 [peers uninstall]({{< relref "/docs/reference/cli" >}}#peers-uninstall)；启动、停止与重启服务见 [service start, stop and restart]({{< relref "/docs/reference/cli" >}}#peers-service-start-stop-and-restart)。

## 手动安装

照上一节下载、校验并解压发布包，然后手动安装二进制。下面的命令在 `peers-${VERSION}` 目录的上一级执行；如果上一节已经进入该目录，先 `cd ..` 返回。

macOS 上，浏览器下载的文件带有隔离属性，而二进制没有签名，也没有经过公证，所以清除之前 macOS 会阻止运行二进制。如果发布包是浏览器下载的，先从解压出的二进制上清除：

```sh
VERSION=0.6.0
xattr -d com.apple.quarantine peers-${VERSION}/peers
```

用 `curl` 或 `wget` 从命令行下载不会带上这个属性。

如果要安装系统级服务，用[把 Broker 安装为服务]({{< relref "run-as-service.md" >}})里的 `sudo install` 代替下面的 `mv`。

把二进制移到 `PATH` 上的目录：

```sh
VERSION=0.6.0
mkdir -p ~/.local/bin
mv peers-${VERSION}/peers ~/.local/bin/
peers --version
```

最后一条命令打印的版本与下载的版本一致。

### 注册 MCP 服务

为账户注册一次 MCP 服务：

```sh
claude mcp add --scope user peer -- peers mcp
```

服务名必须是 `peer`。Claude Code 把工具显示为 `mcp__peer__<tool>`，权限规则也按这个名字写。如果 `peers` 不在 `PATH` 上，就换成它的绝对路径。

检查注册结果：

```sh
claude mcp get peer
```

输出里有 `Scope: User config`。此后启动的会话在 `/mcp` 里能看到 `peer`。

### 启动 Broker

[用一条命令安装]({{< relref "/docs/getting-started" >}})以及用发布包脚本安装，这两种方式都会替你启动服务。如果安装时用了 `--no-service`，或者手动安装，就要自己启动 Broker。

会话只连接 Broker，不会启动它。主机上已经有你能使用的系统级 Broker 时，跳过这一步，会话会连接到它。

否则，安装用户级服务，安装完成就会启动，此后随登录启动：

```sh
peers service install
```

用户级安装不能用 root 运行；在 macOS 上还需要图形登录会话，没有图形会话的主机改用系统级服务：以 root 或请管理员执行 `peers service install --system`，做法见[运行系统级 Broker]({{< relref "system-broker.md" >}})。另一种做法是在 Claude Code 之外的终端里运行 Broker，并保持运行：

```sh
peers service run
```

在会话里启动的 Broker 会以退出码 6 退出。启动 Broker 之后，会话不必重新连接 MCP 服务。系统级与用户级两种服务形态以及卸载方法见[把 Broker 安装为服务]({{< relref "run-as-service.md" >}})。

## 从源码构建

从源码构建需要 Go 1.27。克隆仓库并构建：

```sh
VERSION=0.6.0
git clone https://github.com/wangzw/peers
cd peers
go build -trimpath -ldflags "-X main.version=$VERSION" -o peers ./cmd/peers
```

不带 `-ldflags` 时，`peers --version` 输出 `peers dev`，这样的二进制不能用 `peers update` 更新。不支持用 `go install …@version` 安装，因为 `go.mod` 里的 `replace` 指令指向一份随仓库附带的依赖。

构建产物 `peers` 就在当前目录；把它放进 `PATH`、注册 MCP 服务并启动 Broker，做法见上面的手动安装一节。
