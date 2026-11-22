---
title: 使用指南
linkTitle: 使用指南
weight: 4
description: "使用指南按任务介绍安装 peers、主机配对、公开 Broker、topic、服务、系统级 Broker、更新、权限管理和卸载。"
---

这些指南按任务逐一说明 peers 的用法。第一篇介绍安装，其余各篇从已经装好的环境开始。

- [安装 peers]({{< relref "install.md" >}})：介绍各种安装方式，包括指定版本、从发布包安装、脚本的选项、手动安装与从源码构建。
- [两台主机配对]({{< relref "pair-hosts.md" >}})：在两个 Broker 之间建立链路。
- [在同一个 topic 里协作]({{< relref "collaborate-in-topic.md" >}})：让同一个项目里的会话互发消息。
- [发现并连接公开 Broker]({{< relref "public-brokers.md" >}})：通过网络发现一个 Broker 并与它配对。
- [把 Broker 安装为服务]({{< relref "run-as-service.md" >}})：让 Broker 随登录或开机启动。
- [运行系统级 Broker]({{< relref "system-broker.md" >}})：用一台主机上的 Broker 服务多个账户。
- [以配置文件权限管理 Broker]({{< relref "management-permissions.md" >}})：说明谁可以改什么。
- [更新 peers]({{< relref "update.md" >}})：用 `peers update` 安装新版本。
- [卸载 peers]({{< relref "uninstall.md" >}})：卸载安装，并选择是否保留数据。
