# 引用

| 标识 | 名称 | 说明 | 来源 | 版本 |
|---|---|---|---|---|
| REF-claude-code-cross-session-messaging | Cross-session messaging | Claude Code 官方文档，说明会话间消息的投递时机、inbox socket、入站控制与大小上限 | <https://code.claude.com/docs/en/cross-session-messaging> | 2026-11-18 读取 |
| REF-claude-code-socket-transport | claude-code-socket-transport | Go 库，实现向 Claude Code 会话 inbox socket 写入消息所需的帧格式、署名包装与会话注册表读取，其说明文档记有 inbox socket 的协议细节 | <https://github.com/PeterSR/claude-code-socket-transport> | 提交 480bd83c0bf1，登记见 third_party/UPSTREAM |
| REF-fsnotify | fsnotify | Go 库，提供跨平台的文件系统事件监视，Broker 降级运行时就绪后用它监视配置文件的变化 | <https://github.com/fsnotify/fsnotify> | v1.10.1，本地改动见 third_party/UPSTREAM |
| REF-mcp | Model Context Protocol | 模型与外部工具之间的开放协议，定义工具的声明、调用与只读标记 | <https://modelcontextprotocol.io> | — |
| REF-a2a | Agent2Agent Protocol | Agent 之间互操作的开放协议，定义 Message 等数据结构 | <https://a2a-protocol.org> | 1.0.0 |
| REF-epidemic-algorithms | Epidemic Algorithms for Replicated Database Maintenance | 论文，提出反熵对账、流言式传播与死亡证明（撤回记录） | <https://doi.org/10.1145/41840.41841> | 1987 |
| REF-crockford-base32 | Base32 Encoding | Douglas Crockford 提出的 Base32 编码，规定去掉易混字母之后的字母表 | <https://www.crockford.com/base32.html> | — |
| REF-github-rest-api | GitHub REST API | GitHub 的 REST API，规定查询最新正式发布版本的接口、返回内容与分页方式 | <https://docs.github.com/en/rest/releases/releases#get-the-latest-release> | 2022-11-28 |
| REF-cobra | cobra | Go 命令行库，生成命令树、帮助、用法与 shell 补全，另有 man 页生成 | <https://github.com/spf13/cobra> | v1.10.2 |
