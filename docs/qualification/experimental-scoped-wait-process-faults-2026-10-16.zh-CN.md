# 实验性 scoped Wait 进程故障资格记录 — 2026-10-16

PostgreSQL16.15、17.11 的新增 Runtime Wait 控制器定向执行均通过，每版一个库存测试、内部两次 COMMIT 截断。本增量仅增加实验性 RFC-0022 的三个测试路径；生产 Rust、SQL、API、迁移、业务预算和限额不变。完整 Native suites、Linux Runtime/完整 CI、完整故障矩阵及 G4/R1 仍未由本记录授予资格。按 CONTRIBUTING 的 RFC 流程，实验性实现不等于已支持的稳定公开契约。

控制器使用既有真实嵌套 Driver、完整 DIRECT provider 和原 3-event quantum，完成 Entry 与叶 Wait 应用 start/result 后停在尚未写入 Wait barrier 的位置。原 base/Root head、successor/event、fence/journal、非零 DIRECT、lifecycle revision 及两个带指定 principal 和 run.resolve scope 的原 Interrupt 条件保留。单连接协议代理在真实 scoped barrier INSERT 后，分别阻止 COMMIT 请求转发，或在真实 CommandComplete 和 idle ReadyForQuery 返回时扣留两帧。测试 owner 实际 SIGKILL/reap 自有 writer，关闭 proxy，等待确切 PostgreSQL backend 消失，再比较按精确 tenant/run 查询的22张 fixture 表完整 JSON 行。

Before 分支没有转发 COMMIT，cut 与 kill 后22表均无变化。只有此分支在仍 Active 时执行受信强制 lease supersede，取得更高 epoch，再用原 scenario 做新的旧 fence Wait 写入，明确返回 StaleFence 且无写入。冷 OS worker/新 Store/原默认 Driver 使用新 fence 恢复原 Pending 并形成新的 Waiting；这个新 Driver 请求没有被称作原请求 ACK。

After 分支原子保存 event1、checkpoint1、消费1、barrier1、原条件2和注册2，推进精确 scoped head；Root checkpoint 保持，Run Waiting/两条件/无 lease/不可调度。Waiting 不强制 supersede。冷 worker 在 Driver 继续前使用完全原样的 scenario 重放原 Wait 请求，必须 Idempotent、原 digest 且22表无新事实；最终 Return 后再次重放同一个原 ACK，也只读恢复原记录。原 Wait 的不可变 bundle 为五类表共六行：event、checkpoint、barrier、消费各一行及两条原条件。合法 resolution 会更新 registration 的 terminal 状态，不能错误要求最终全部22表等于 Waiting 快照。

错误 principal 和 insufficient scopes 是公开 InterruptResolutionIntent 构造器拒绝，未把它称为 Store SQL 授权或外部 IdP 资格。随后两次真实授权 resolution 各有精确重试；第一项后仍 Waiting/一条件、无 lease，claim 被拒，最后变成 Active/零条件。冷 worker 再做真实 fresh claim，epoch 高于原值，验证原叶 Wait 后完成原 scoped 后继一次、父应用一次、原 Wait 应用零次重派发、Return一次、无 quarantine。这个应用效果断言仅覆盖显式零IO fixture，不扩大为任意模型或外部工具效果。原完整 Root/owning reader、授权、fence、默认 Driver options、租约和业务时间界限均保留；只有 cut proxy pool1，冷 Driver pool8 与既有 fixture 相同。

| 实际执行 | PostgreSQL16.15 | PostgreSQL17.11 |
| --- | --- | --- |
| Runtime，实际库存193 | test/wrapper exit0；1通过/192filtered；17.1612s | test/wrapper exit0；1通过/192filtered；17.5670s |
| 原 COMMIT 故障证据 | 2次截断；每条19字段/type核验 | 2次截断；每条19字段/type核验 |
| 持久性设置 | 前后 fsync/synchronous_commit/full_page_writes 均 on | 前后 fsync/synchronous_commit/full_page_writes 均 on |

实际 harness 为 macOS/aarch64，连接原有受管 Linux Docker PostgreSQL。fresh 默认 feature 二进制使用 Rust1.88、原 Native test opt1、默认栈、debug0/incremental0，保留断言和溢出检查；四个实际 Cargo artifact 均 fresh=false/features=[]。542项输入 bytes 不变，保存的历史 mtime 均晚于前次编译结束。实际193库存包含条件 worker；两 cut 是单 controller 内的两个场景，不能称193项数据库场景、两个独立库存测试或性能比较。数据库和原始输出均保留，stdout/stderr 没有脱敏替换。

同一冻结输入的原六门 driver 实际成功：fmt、严格 workspace Clippy、workspace tests、独立 doctest、严格 Rustdoc 和 nightly-2026-10-07 check 均 exit0。workspace **100个汇总/2123通过/6个原ignored/0失败**，doctest **10组/48通过**。这组六门使用 opt0，并显式保留 debug assertions 和 overflow checks；工作区条件数据库入口不授予 Native 资格。首次 formatted source-v1 的 fmt0、Clippy101及“resolve_all 103/100行”原失败完整保留；source-v2 仅将构造器授权拒绝与22表无写入断言整体提取为私有 awaited helper，仍位于首次 resolution 前，无新增 allow、断言删改或顺序变化。

选中源码933文件，SHA256为 `b286803afacf84278ffa579e8f898610e9474378bd982897e90f1c3c0f0c718c`；542项 supplied compilation inputs 摘要为 `43877dc8d16584c5a957356a1d0310ac4faea1525b743c05c4a159178780c305`。相对 ff7fea7 只有 commit_proxy.rs、nested_driver.rs 和新增 frame_wait_process_faults.rs 三个测试路径变化；原 Return 用例不变。单列541输入的其他 CI head 不能把资格转授给本542输入。此场景也不代换真实 elapsed Timer、WaitReleased、Root ReadyReleased、自然租约到期、scoped child Join、unknown-effect failure close、stdio 或真实部署/容量/独立安全/发布验收。

[机器范围](experimental-scoped-wait-process-faults-2026-10-16.json)、[原181项索引](evidence/experimental-scoped-wait-process-faults-2026-10-16/artifact-index.json)、[追加后的v2索引](evidence/experimental-scoped-wait-process-faults-2026-10-16/artifact-index-v2.json)和[离线 verifier](evidence/experimental-scoped-wait-process-faults-2026-10-16/verify.py)保留原 bytes、SHA、命令、失败和审阅。v2索引184项仅追加独立执行审阅两文件及前次编译时钟的原回执一份，原181项、原索引和manifest均未修改。verifier 只用stdlib校验SHA/gzip、manifest与实际输出，不执行任何归档脚本、Rust、数据库或CI；`--workspace-root`另比较542项 supplied inputs。它不重构完整933文件、不证明 compiler read closure，也不重算未打包的约50MB二进制，只核原SHA/bytes记录的一致性和已归档独立执行复核。以上均不关闭完整 Native/CI、G4/R1或R2–R7。
