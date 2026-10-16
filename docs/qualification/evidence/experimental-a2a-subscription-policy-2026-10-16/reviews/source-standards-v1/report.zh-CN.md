# A2A source-v3 Standards 独立审阅

本审阅者未编写这片生产代码或测试。固定来源为 source-v3：930 文件 tree `d994771b…`，538 个保守编译输入 `c04f7bf9…`；全清单逐项、摘要重算和前后字节均精确。相对 MCP Debug31a，仅改 `a2a_server.rs` 并新增两个测试文件，其余 927 项不变。

**新增非工具已报问题：0 个规范违规、0 个需修代码异味。** 这只限 source Standards；Spec、执行、文档采用和完整资格分别处理。

依据 `CONTRIBUTING.md`、RFC 流程、当前 Draft RFC25、冻结契约和 code-review 的 smell baseline：

- Interface 只增加现有 Options 的 consuming、`#[must_use]`、无参 enable 方法；文档明确默认关闭、幂等、原事实/current-policy 与不重新认证。无新公开 trait/type、依赖、配置限额或调用方状态协议。
- 私有 `AuthorizedSubscriptionStream` 一起拥有 backend stream 和 operation permit；绝对期限、授权请求和退役集中于它。Option unfold 在终结前同步 drop，没有 detached task、unsafe 或新增生产 panic。
- 原 `prepare/unary/stream/bounded_stream` 及 10 个非 Subscribe dispatch 方法逐个字节相等。订阅创建的相似代码属于保持旧路径的刻意局部隔离；不建议为消除重复而改动未启用路径。私有构造方法承担收集依赖与建立所有权，非无意义转发。
- 外部 TCP 测试穿过真实 Interface；私有 single-next oracle 验证 HTTP/SDK 顺序不能独自证明的 RAII 释放。v3 只对 SDK 动态 timestamp 做严格 UTC 解析后归一化，其余完整 error envelope 仍逐值比较；测试未以放宽 limits、断言或生产代码通关。

**已知门禁红保留：** v3 strict Clippy 实际 exit101，日志 `9152d15e…` 的 7 条诊断均在两个新测试文件；本报告不重复把工具检查作为人工发现，也不称 v3 strict/full delivery 通过。作者后续 v4 fixture lint 修正和重新执行须另审，不转授本次结论。

采用前仍须按 CONTRIBUTING 补双语 guide、Draft RFC25 状态和 Unreleased CHANGELOG。须明确宿主 callback 有界且非阻塞、原 principal/current host policy、仅 Subscribe opt-in、非 credential reauthentication、不追回已 yield 字节及无 durable/周期撤销保证。当前仅封源码审阅，未自行运行编译、网络或数据库。
