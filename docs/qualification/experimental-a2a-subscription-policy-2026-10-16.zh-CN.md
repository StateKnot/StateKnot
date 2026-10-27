# A2A 订阅策略实验验收 — 2026-10-16

固定 A2A 订阅候选的六项本机组件门禁全部通过。workspace 实际完成 **100 个 suite、2,108 passed、0 failed、6 个既有 ignored 夹具**；doctest 为 **10 组共 48 passed**。运行时已清除 StateKnot PostgreSQL 测试环境，条件数据库测试提前返回不能算 Native 数据库资格。参见[机器报告](experimental-a2a-subscription-policy-2026-10-16.json)、[原六门禁汇总](evidence/experimental-a2a-subscription-policy-2026-10-16/original/q5-network-enabled-retry-v1/six-gates-summary.json)和[执行范围](evidence/experimental-a2a-subscription-policy-2026-10-16/execution-profile.json)。

实验开关 `A2aServerHttpOptions::with_subscription_authorization_rechecks()` 默认关闭。开启后仅对 `SubscribeToTask` 的每次事件获取执行取数前、取数后的既有宿主 authorizer 检查，始终使用原 principal、tenant、规范化 scopes、operation、task ID 和空 config ID。同一个绝对 idle deadline 覆盖两次检查及 backend 等待，每次回调也受既有 request deadline 限制。原 `prepare`、unary、stream、bounded-stream 实现逐字不变，其他 operation、streaming capability 和数值限额保持原契约。

最终 workspace 实际执行 **8 项私有测试和 1 个真实 TCP 测试 family**。该 family 在 REST 与 JSON-RPC 两种绑定下覆盖[冻结的 17 个矩阵 ID](evidence/experimental-a2a-subscription-policy-2026-10-16/contract/acceptance-matrix.json)，**不是 34 个独立命名的 libtest**。范围包括 pre/post 拒绝与不可用、callback/idle/ready 竞争、三个 pending 阶段的 shutdown/断连、EOF/error/terminal/event cap、当前映射拒绝，以及保留原 response 时不同健康任务复用唯一 operation permit；另有 SendStreamingMessage/GetTask 回归对照。直接单次 next oracle 独立于 SDK 提前轮询，检查 terminal/error 返回前 backend 已 Drop、permit 已释放。真实 TCP 使用受控 policy/backend 测量夹具，不是新 IdP 或持久任务 backend。

当前 policy 由宿主 authorizer 负责；这里不重新认证 bearer token。初始 admission/backend 创建顺序保持，排队期间 policy 变化可能先创建 backend，再在事件交付前将其释放。已提交的 headers 或缓存 bytes 不能收回；headers 之后只能给既有安全 SSE 错误并结束，不能承诺 HTTP401。adapter 未被 poll 时没有周期授权或 idle 后台任务。同步阻塞回调不可被抢占，返回后仍用实际时钟/shutdown 拒绝继续。不承诺零间隙交付或不受背压影响的实时撤销时限。

最终六份收据均 exit0：Rust1.88 格式、严格 workspace/all-target/all-feature Clippy、workspace tests、独立 doctest、严格 Rustdoc，以及固定 `nightly-2026-10-07` workspace check。完整源码为 **930 文件**、tree `0e91c287e33ccf7f33c0f11e3ed0c204c2826f3373c4a6f3e701a56cd66c7bad`；**538 个按相对路径字符串排序的 supplied compilation inputs** 的 canonical SHA256 为 `43e9cb48b9fe45bb8631b99fe9548db84f8e652dba2d147c8198bd04dff3a6f4`。本机 dev/test opt0、debug0、incremental0、调试断言和溢出检查保持，warnings 拒绝，无 stack override。这不等于另一个 CI opt1 profile 或冷构建性能结果。原 argv/cwd/runner SHA/环境白名单及日志均保存，汇总跨两个原目录的 receipt_paths 未改写。

早期真实失败全部保留：

| 尝试 | 实际结果 | 失败边界 |
| --- | --- | --- |
| Q1 unit | exit101，7 passed/1 failed | 测试在 EOF 后重复 poll；Stream 没有 fused 保证。 |
| Q2 TCP | exit101；独立 unit8 通过 | 测试逐字比较 SDK 两次独立生成的 UTC timestamp；修正后先严格检查唯一 ErrorInfo 结构及 UTC 时间，再只规范化该字段。 |
| Q3 | TCP exit0；Clippy exit101 | 7 项仅测试夹具的 lint。 |
| Q4 fmt | exit1 | 夹具用 edition2021 格式化，与仓库 edition2024 检查不符。 |
| Q5 首次 workspace | exit101，首 harness29 passed/5 failed | 既有 loopback bind 被沙箱 PermissionDenied；相同源码随后在允许网络的续跑中通过剩余四门。 |

上述是保留的夹具、工具或环境失败，**不能当作产品回归证明**。生产文件从 Q1 到最终 Q5 逐字相同。[原记录索引](evidence/experimental-a2a-subscription-policy-2026-10-16/artifact-index.json)同时绑定压缩字节和原始哈希，没有重写任何失败收据。

[离线验证器](evidence/experimental-a2a-subscription-policy-2026-10-16/verify.py)重建已发布 Debug31a 源码链，再只采用本包三个新压缩源码 payload，检查最终 930 行完整清单，并通过原 patch 重建所有历史夹具版本。`--workspace` 仅检查所选工作树的精确 538 个输入，不宣称整个冻结快照等于 main/Git 树、新采用文档或 compiler read closure。既有引用仅复用源码字节，不转授此前执行或 Native 资格。日志和 patch 使用确定性 gzip，保留原 raw hash；[manifest](evidence/experimental-a2a-subscription-policy-2026-10-16/manifest.json)和[seal](evidence/experimental-a2a-subscription-policy-2026-10-16/seal.json)固定本包。

独立源码 Standards v1/v2 已按其原范围附入；最终独立 Spec 和发布包审阅未在本次 seal 中声称。组包和自验由测试作者完成。Native 数据库、新 IdP、当前 policy backend、持久任务 backend、全 CI、原 30 分钟 CI 限制解决、stdio、部署/外部试点及 R1 完成均仍**未授资格/开放**，RFC/里程碑采用和代码绑定需另行审阅。
