# 实验性帧 Return 进程故障资格记录 — 2026-10-16

PostgreSQL16.15、17.11 的四项保存二进制定向执行均通过：每版分别运行既有 Store 中层帧用例和新增 Runtime Return 控制器。本次仅补充 RFC-0022 实验实现的测试，不改变生产 Rust、SQL、API、迁移或限额。完整故障矩阵、全部 Native suite、Linux CI 和 R1 仍开放。

Runtime 控制器使用原 Driver 和完整 DIRECT provider，实际推进到 terminal 叶。PostgreSQL 协议代理分别截断 Return COMMIT 请求和成功响应；测试 owner 实际杀死并回收 writer，确认数据库 backend 消失，再于受信强制接管租约后启动冷 worker。十四张 fixture 表按完整 JSON 行比较：未转发事务没有任何变化；已提交事务原子保存 Return、event、result、caller completion 并弹出 stack，进程死亡后原记录不变。公开 owning reader 重新认证已提交事实。

接管后，旧 `renew_lease` 被拒且无写入。已提交分支先用原旧 fence Return 幂等恢复，控制器与冷 worker 的 rendezvous 证明此阶段没有新增事实，然后才运行冷 Driver。最终返回 Root、没有 quarantine，叶应用零次重派发、父应用调用一次；合法父级推进后，原 Return bundle 四行保持精确相等。这验证强制 supersede，不证明自然租约到期或新的 stale-fence Return 写入。两次提交截断是一个 controller 内的两个场景，不是两个独立库存测试。

Store 用例另外分别损坏并恢复悬停中层的三个组件，同时保留活动叶和紧凑身份：后继 checkpoint state、具有实际 checksum 的非规范 entry bytes、所属 journal projection digest。每次完整公开 active-frame reader 都拒绝损坏；恢复完整原物理行后读取成功、schema 核验通过。触发器修改具有事务性，仅限测试数据库的迁移 owner，并在 owning read 前重新启用。

| 实际执行 | PostgreSQL16 | PostgreSQL17 |
| --- | --- | --- |
| Store，库存264 | exit0；1通过/263filtered；3.152s | exit0；1通过/263filtered；0.988s |
| Runtime，库存191 | exit0；1通过/190filtered；37.467s | exit0；1通过/190filtered；22.671s |
| Return证据 | 两次截断、SIGKILL及冷恢复均核验 | 两次截断、SIGKILL及冷恢复均核验 |

实际执行平台为 macOS/aarch64，连接启用持久性设置的 Docker PostgreSQL；前后 fsync、synchronous_commit、full_page_writes 均为 on。fresh 默认 feature 二进制使用 Rust1.88、原 test opt1、默认栈、断言/溢出检查、debug0/incremental0。这不是 cold CI 或性能比较；数据库和原始日志均保留在编译缓存之外。

同一冻结编译输入的六类本机门禁也已通过：fmt、严格 workspace Clippy、workspace tests、独立 doctest、严格 Rustdoc 和固定 nightly check。实际成功回执由原两项加同源续跑四项组成，不能称原六门 driver 成功。workspace **100个汇总/2121通过/6个原ignored**；doctest **10组/48通过**。工作区执行中的条件数据库入口不授予 Native 资格。此本机门禁 profile 使用 opt0，并显式保留断言及溢出检查。

原失败保留：v1 completion 相等比较不能编译；v2 严格 Clippy 拒绝大 future；v3 首次 workspace 因默认沙箱无法绑定五个回环 listener 而失败，同源允许回环后通过。PG17 首次沙箱容器 inspect 失败时，既有 PostgreSQL server 已运行，但尚未创建本次新测试数据库或启动 testcase。PG16 Runtime 实际通过，但 v3 wrapper 由于两条合法证据将预期的连续 `test … ... ok` 文本分隔而 exit1；新 v4 parser 和独立只读补充核验认证原结果，没有改写 wrapper 失败或为此重跑 PG16。PG17 两项实际 v4 wrapper 均通过。

冻结选中源码932文件，SHA256为 `560a04129499fa5fb7a435e556a1ce3f19f2e95a75064af412338a000196e95a`；541项 supplied compilation inputs 摘要为 `4a59f6fbb2d89ae8a71a13e8806b819974ec985c07346c4e7cbe59ec1fd5821e`。相对 a544ad9 只有四个编译路径变化，均为测试；完整选中源码还含三份较旧的非编译 A2A 文档，采用时明确排除，主树保留新文档。此前 single-walk 生产优化不采用，其 wall-clock 实测没有建立稳定收益。

[机器范围](experimental-frame-return-process-faults-2026-10-16.json)、[原始 artifact 索引](evidence/experimental-frame-return-process-faults-2026-10-16/artifact-index.json)和[离线 verifier](evidence/experimental-frame-return-process-faults-2026-10-16/verify.py)保留 raw/stored 摘要、命令、源码清单、历史失败及独立审阅。verifier 核验归档字节和结果；`--workspace-root` 额外比较541项 supplied inputs，不执行 Rust/数据库/CI，不重构全部选中文档，也不证明 compiler read closure。本记录不授予一般进程树 owner、stdio、部署、容量或生产里程碑完成资格。
