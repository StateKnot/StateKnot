# RFC-0022 / G4 帧事务进程故障矩阵与下一增量

基线：`ff7fea7db88a611e2f0db29ceedf6b0b4b48f5fa`。本页是源码核查和未执行候选设计；没有运行 Rust、Docker、数据库或 CI，不授予新增资格。当前远程 CI 属于另一个固定提交，不能代替本候选验收。

## 已有证据的精确边界

RFC-0022 的 Validation 第 4 项要求各持久事实写入边界的丢确认/进程死亡、更高 fence 恢复、零接受的 stale writes 与零重复已提交应用效果；第 5 项另要求真实嵌套叶等待、替换进程、授权解决、精确叶恢复及一次父返回。G4 继续要求完整矩阵，不能由一个事务或构造器检查关闭。

| 边界 | 当前源码 profile | 已有、可复用的断言 | 真正 COMMIT request/response cut + OS kill + 冷进程覆盖 |
| --- | --- | --- | --- |
| Entry | Store `frame_transactions::frame_transaction_commits_one_bundle_and_recovers_after_lease_expiry`；8 组件写入异常；Runtime `framework_entry_and_scoped_application_start_share_original_run` / shared quantum | 原 entry/start/claim/checkpoint/head/stack 与 Root 不变；真实过期后再次调用返回 Idempotent；默认栈七层/更高 fence Driver | 没有。现有“acknowledgment-loss reload”是已完成调用后的重试，不是 wire cut，也没有 child SIGKILL。 |
| Caller rebind | Store `frame_callers::framework_caller_epoch_race_recovers_once_without_reentering_child`、4 组件异常/晚租约；Runtime `higher_fence_rebinds_only_leaf_then_each_resumed_parent` | 24 路仅一个原始 binding；fresh Store 重读、旧/未来 fence 读取幂等；budget/物理历史不重置 | 没有。24 路竞争和对象重建没有替代真实进程死亡。 |
| 普通 Continue/Terminal scoped barrier | Store `frame_barriers::scoped_barrier_race_consumes_once_and_whole_retry_survives_takeover`、6 组件异常与晚 deferred guard；Runtime正常继续/terminal/quantum | 全部结果消费一次、successor/owning reader、Root 悬停、旧 ACK 在接管后完整重读 | 没有。没有为 scoped barrier 配置协议 proxy target 的旧用例。 |
| Scoped Wait（当前 V3） | Store `frame_waits::whole_leaf_wait_race_restarts_resolves_both_conditions_and_resumes_exact_scope`、组件异常；`frame_elapsed_waits` / `frame_elapsed_wait_faults` 原 Wait inventories；Runtime `scoped_wait_releases_once_resolves_all_conditions_and_resumes_actual_leaf`、`elapsed_graph_waits::scoped_absolute_wait_survives_real_delay_and_higher_fence_without_reexecution` | Waiting 原子释放 lease、所有条件/完整原结果、真实授权/时间、fresh Store/Driver、原 due/Ready 原 quantum、历史 ACK | 没有。Runtime fresh Driver 仍在同一 OS 进程；Store 原失败/重读不是 wire 丢确认。 |
| Return | 最新 `nested_driver::frame_return_process_faults::frame_return_commit_loss_survives_real_kill_and_cold_higher_fence_driver` | 两 cut、SIGKILL/reap/backend gone、14 表、原 ACK 幂等零新事实、cold Driver leaf0/parent1、原四行保持；原生产完整 owning proof | 已有 macOS/aarch64 + PG16.15/17.11 四次定向原生证据。Linux profile、完整 CI 和整个 G4 仍开放。 |
| Whole Closure | Store `frame_closures` 完整七层/逐组件 deferred 异常；Runtime `scoped_cancellation_committed_closure_ack_survives_guardian_fault_then_recovers_exactly` | 真实 CommitResponse proxy、guardian 故障时不丢 SQL、原 closure ACK / final cancellation 幂等 | 只有同进程持有/重连 ACK；没有 BeforeCommit 和 writer SIGKILL/冷进程完整组合。 |

另有 scoped ordinary Start 的 `scoped_start_lost_commit_ack_retries_exact_original_without_application_launch`：真实 CommitResponse 后同进程重连、原 start 保持 in-flight，应用调用 0；没有强制杀死 writer。`recovery_guardian.rs` 的 renewal/release/start/quarantine profile 也属于同进程 guardian/ACK 组合。`child_*_commit_loss.rs` 是真实进程 profile，但使用普通 child-Run fixture；不能自动授予 frame-scoped Join/settlement/cancellation 的 owning namespace 组合资格。SQL trigger rollback 证明事务回滚，不证明 SIGKILL。

## 下一边界：两个审批条件的 scoped Waiting COMMIT

优先于普通非终态 barrier：相同 successor/消费/帧 head 之外，还能一次覆盖完整 `graph_wait_conditions`、两个真实注册、Run Waiting 生命周期投影、lease 释放、未全部授权前不能重新调度，以及冷进程恢复原叶。这直接推进 RFC 第 4/5 项。所有条件为原 fixture 支持的审批，不把绝对 Timer 改成长 due 来绕过 event-clock 分支。Ready/elapsed Timer 的进程故障资格仍单独开放。

现有接口：`PostgresStore::commit_graph_frame_wait`（`src/graph_frames/barriers.rs`，当前持久化 V3）、`load_graph_frame_barrier`、`load_graph_frame_checkpoint`、`load_active_graph_frame`、`resolve_interrupt`、`claim_lease` / `supersede_lease`。原 Driver `src/driver/frame.rs::commit_scoped_barrier` 调用同一 public Store 写入口。当前精确 SQL target 为 `INSERT INTO stateknot.graph_frame_barriers (`（含表名后的空格）；只在单会话 Wait writer 上使用。

复用原 `nested_fixture_with_behavior(1, Wait(...))`、`admit_nested_fixture`、`nested_quantum(3)`、完整 `AdmissionDirectEvidence`、`TestProcess`、`CommitProxy`。新候选只增加一个私有 module、一个 test-only proxy target；不触碰 Return 用例，不新增通用操作路由、production hook、重试预算、SQL/迁移或依赖。

### 最小完整场景

1. 主控制器通过真实 Driver 的原 3-event quantum 完成 Entry、Wait 应用 start/result，明确应用1/后继0/父0、barrier0，然后重新 claim 并 rebind caller。保存 base、原 Root head、固定 successor/event、真实 journal/fence、完整非零 DIRECT、原 lifecycle revision 和两个 interrupt IDs。cut worker 只做 read/plan/public Wait transaction，避免 guardian/其他合法写混入 cut 快照。
2. 两个独立 Run 分别做 BeforeCommit 与 CommitResponse。前者 COMMIT 不转发，后者收到真实 CommandComplete + idle ReadyForQuery 后两帧不转发。独立连接读取完整事实，kill 自己拥有的 child，Unix 要求 signal9/reap，停止 proxy 并等待实际 backend PID 消失，再比较完整快照。
3. Before 分支：22 表全量 fixture 行在 cut 与 kill 后完全等于原件。仅在仍 Active 时强制 supersede，固定更高 epoch；在 fresh Driver 之前用原 plan/原 fence/仍准确 journal 做新的 Wait 写，必须明确 StaleFence，22 表零变化。冷 worker 用新 fence 和新 fixture/DIRECT 调用 Driver，rebind1、Wait1、应用0，停在 Waiting。
4. After 分支：一次原子 Wait 新增 event1/checkpoint1/消费1/barrier1/原条件2/注册2、推进精确 scoped head，Root pin 不变，Run Waiting/2 conditions/lease无/scheduler无。kill 后全部行保持。冷 worker 先用原 fence/plan 公共 Wait 调用，必须 Idempotent；父控制器在唯一 token rendezvous 处确认原 digest 与22表零新增，才允许继续。Waiting 时不伪造 `supersede_lease`。
5. 两分支冷 worker 均保留在 Waiting。wrong principal / insufficient scopes 是 `InterruptResolutionIntent::new` 的明确构造器拒绝，不伪称 Store SQL 授权拒绝；快照无写。随后两次真实授权 resolution 使用原 `nested-wait-resolved` 事件/data（含 interrupt_id），payload 保持原 DIRECT adapter 要求的 `test_payload`。首次仍 Waiting/1 condition/无lease、claim拒绝；最后 Active/0 condition。每次精确重试不新增 resolution。
6. 放行后冷 worker 自己获取真实 fresh claim，epoch 大于原值，用原默认 Driver options/DIRECT 完成原叶后继1、原父1、原 Wait 应用0，返回 Root、无 quarantine、Return1。原 pending 结果行及 Wait 的 event/checkpoint/barrier/消费/条件全部保持。最后两分支都用公开完整 owning reader核对原 Wait；仅 After 分支再用完全原样的 scenario（plan/event/fence/observed/DIRECT/revision 全原参数）做旧 ACK，必须原 digest Idempotent，22 表不变。Before 分支不把新 Driver 生成的请求说成原切断请求。

快照的22张表：原 Return14表，加 `graph_wait_conditions`、`run_wait_registrations`、`interrupt_resolutions`、`timer_firings`、`wait_abandonments`、`graph_frame_closures`、`graph_frame_closed_callers`、`run_quarantines`。所有查询按精确 tenant/run；不是整库、storage tuple 或一般进程树证明。合法 resolution 会修改 registration status；不能错误要求最终全部22表等于 Waiting 快照。不可变 Wait bundle 只绑定原 event/checkpoint/barrier/消费/条件，原 registration 由完整 owning readers 认证。

`verify_retry`（barriers.rs:1119）实际比较保存的 wait revision、完整 barrier 与 disposition；existing whole record的认证先于新 authority/DIRECT/observation。After 用全原参数重放不扩张该 API 契约；Before 不读取私有 SQL 来拼造不存在的原提交参数。

## 候选与后续验收

`candidate/` 已有三个 test-only 文件；均未格式化、编译或运行。Root 需先独立审阅并在新 source-only 副本中 fmt/严格编译，真实保留任何失败，再按仓库原六门及双 PG 默认 feature / 原 opt1 / assertions+overflow / 默认栈运行。预计 controller + conditional worker 使 Runtime 库存191→193，必须实际 --list；这不是已测结果。两 cut 是单 controller 内两个场景。

观测 runner 要绑定实际源码/二进制/profile/库存/精确 testcase/原预算，解析两个 `STATEKNOT_SCOPED_WAIT_COMMIT_LOSS_EVIDENCE`，区分 Before fresh-stale 写拒绝和 After ACK 恢复。不得复用连续 `test … ... ok` 假设；marker 会合法插入 libtest 输出。每版 PG 的 on/on/on、容器身份、测试原始 stderr/stdout、真实 process exit、source/binary 前后 hash 必须留存。只允许 proxy 连接 pool1，cold Driver pool8 保留原 fixture 设置。

本增量不涉及 STOPPED SQL37 cleanup、metadata-only Root、single-walk 优化、等待时强制取得执行权、自然 expiry、Timer Ready profile、scoped child Join、未知外部效果、全套 Linux进程矩阵或完整 G4/R1 关闭。

作者修正记录：v1 原件未执行并完整保留；v2 仅将最终原请求 ACK 断言限定在真正已提交的 After 分支，并明确 Before 为新 Driver 请求及公开 owning 重读。marker 已包含实际 SHOW server_version。
