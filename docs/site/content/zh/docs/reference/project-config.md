---
title: 项目配置文件
weight: 5
description: "项目里的 .peers.yaml 文件声明在该项目中启动的会话自动加入的 topic，仓库的每个检出都会得到相同的默认值。"
---

项目里的 `.peers.yaml` 声明在该项目中启动的会话自动加入哪些 topic。把它提交到仓库，这样每个克隆都得到相同的默认 topic。

## 文件的查找位置

MCP 服务进程以会话的项目目录为工作目录启动。它先在该目录查找 `.peers.yaml`，再逐级查找上级目录，直到文件系统根目录，使用最先找到的那个。

- 该文件必须是普通文件。符号链接按目标判断，目标是普通文件时算数。
- 文件与所在目录必须属于调用方账户或 root。不满足时该文件被忽略，继续向上查找。
- 最先找到的文件无法读取或解析时，不加入任何默认 topic，不会退回到上级目录的文件。

## 键

| 键 | 类型 | 默认值 | 含义 |
|---|---|---|---|
| `topics` | 字符串列表 | 空 | 自动加入的 topic。空字符串与重复项会被去掉，保留第一次出现的位置。 |
| `auto_join` | 布尔 | `true` | 是否加入默认 topic。`false` 表示一个也不加入。 |

未知的键会被忽略。

```yaml
topics:
  - team-a
  - release-notes
auto_join: true
```

## 会话加入哪些默认 topic

会话登记完成后，按下面的顺序确定默认 topic，第一条命中的规则决定结果：

1. Broker 的 `topics.auto_join` 为 `false`：不加入任何 topic。
2. 找到项目配置文件且其 `auto_join` 为 `false`：不加入任何 topic。
3. 文件声明了非空的 `topics` 列表：按顺序加入其中每一个。
4. 否则，项目的 git 仓库有可规范化的 `origin` 地址时，加入以规范化结果命名的一个 topic。
5. 其余情况不加入任何 topic。

`topics: []` 视同未声明，会走到第 4 条。自动加入从不把 topic 声明为公开。会话之后可以用 `join_topic` 加入更多 topic。不带名字的 `join_topic` 或 `leave_topic` 即使自动加入已关闭，也作用于这些默认 topic，只按第 3 至第 5 条确定；见 [topic]({{< relref "/docs/concepts/topics" >}})。

Broker 侧的开关是[配置键]({{< relref "docs/reference/configuration" >}})里的 `topics.auto_join`。topic 与可见范围见[概念]({{< relref "docs/concepts" >}})。

## 仓库地址与 topic 名

走到第 4 条的会话从仓库的 `origin` 地址取得默认 topic。地址取自仓库的本地配置，用户在全局 git 配置里设置的 `remote.origin.url` 不会被使用。

地址规范化为 `host/path`：

- 带 `https`、`http`、`ssh` 或 `git` 的 URL，主机取 URL 的主机部分，去掉用户信息与端口；路径取 URL 的路径。
- 类 scp 写法 `user@host:path`，主机取最后一个 `@` 与第一个冒号之间的文字，冒号之后全部是路径。
- 本地路径或 `file:` URL 不产生 topic。

路径去掉首尾的 `/` 和结尾的 `.git`；主机转为小写；路径保持原大小写。例如下面三个地址都产生 topic `github.com/org/repo`：

- `git@github.com:org/repo.git`
- `ssh://git@github.com/org/repo.git`
- `https://github.com/org/repo`
