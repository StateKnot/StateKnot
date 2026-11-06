<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 scoped Join owner-stop 限定验收记录

证据更新：2026-10-19。[English](experimental-scoped-join-owner-stop-2026-10-18.md) · [机器记录](experimental-scoped-join-owner-stop-2026-10-18.json) · [可移植证据与核验方法](evidence/experimental-scoped-join-owner-stop-2026-10-18/README.md)。

[RFC-0022 草案](../rfcs/0022-namespaced-graph-frames.md#registered-join-handoff-during-whole-frame-cancellation)中的选定实验性 public Store 契约已在下列精确候选上通过验收。[CI run 39185847363](https://github.com/StateKnot/StateKnot/actions/runs/39185847363) 的全部21个 job 均成功。严格离线分析真实 exit 0，`full_CI_success_verified=true`、`full_CI_qualification=true`，分析错误为空。Driver 集成、Root ConfirmCancellation、R1、R6、RFC 接受和 Supported 继续开放。

运行资格绑定不可变 commit `bbd9dcf833a3446b822e8e1398c53d19cf3b9a72`、tree `126d95b5a80c703b781e270a157c5b81a4a3ad39`，parent 为 `e372184fb1c80ffb9903ac0d9feb8380db51253e`。该量测 commit 中的三份本文档是历史 pending-V2 快照。本次证据/文档更新由独立提交承载，不能把后续文档 commit 称为本次 CI head。精确量测对象已真实采用到本地，38个暂存 payload 均精确核对，index/工作树干净且保留 DCO；该采用没有更新 remote PR 或 main，PR 仍为 Draft。

## 选中契约

实际当前叶中唯一最新的普通 owner 注册精确且未发布的 Join 后释放租约，原物理尝试仍为 Executing。只有整个 Root 绑定 lifetime 子账户的子级均已终态并结算，才允许取消关闭整栈。受信协调者须在保存的观察点提供完整已定价、覆盖整个 Run 的 DIRECT；结构下界和账户的 DIRECT 观察仍只是下界。已结算的 delegated 用量只累计一次。取消证明不创建普通完成/结果、Join 发布/消费、租约、launch grant 或新的 Worker 权限。

同一 owning 事务认证完整 Root/帧 lineage、原物理历史/fence、精确 request/registration 和全部归属键，按账户规范顺序锁住已结算终态子级，再重读 owning record 和选中 Join。deferred guard 与写后 owning 验证仍在事务内。Fresh quarantine、第二个未完成普通 owner、已被后续尝试取代的 owner、未完成工作、未知 Tool/Model 结果、未定价用量或未结算子级仍拒绝关闭。封存失败关闭保留原严格检查。重试先完整认证原已提交决定，再评估 fresh 候选字段；后来的运维 quarantine 不使既有原 ACK 失效。

## 源码、完整树与二进制绑定

| 输入 | 精确值 |
| --- | --- |
| Composed-V3 的 CI 覆盖前选定源码 | 936 files；`21c32c1446377a9691d63d835bb732b8d8cc6259e236c9cd0575dc13369f7a44` |
| 规范化编译输入 | 544 files；`30d6c10fd88118297131a28c0e3b78420f51dec17206a718948a348c7424de81` |
| Cargo.lock SHA-256 | `fc36c3966dc2abf990fae6a01b633459d95fedae0fd7f77c429b5f04447b2e94` |
| CI Linux Store binary / 库存 | `ed81c46b5b9a96b10a2aa4809e7ecca299ca85d9ad43d25e132c89a7a9c287a0` / 267 |
| CI Linux Runtime binary / 库存 | `3575b2e82d7a2319bcaed052e72665ac644b980432bed95c8ca55c3f9d265a1c` / 194 |
| 当前严格绑定 bundle manifest | `60fab0ff4c2cfee052da0353c0757e49acc780ca7b6d42c9ff0f6fa95c0dddf6` |

936文件清单只是选定源码域，不是全仓树。5个 CI 文件另行绑定：workflow 覆盖及4个新增 helper 组成940项选定字节绑定。官方完整树含4088个 blob / 4604个 entry。38个 publication payload 均与不可变 prepared 原字节核对 Git blob SHA-1 和 SHA-256；其他每个最终 blob 的 mode/type/SHA-1/size 必须与官方 parent 相同。内容 SHA-256 只覆盖实际读取过的选定源码、CI 覆盖及 payload 字节，不给其余官方树条目新增未计算的 SHA-256 主张。

Native CI 使用 Rust 1.88.0、Linux x86_64、默认 features、test opt-level 1，保留 debug assertions 和 overflow checks，关闭调试符号，使用默认线程栈。每个 major 保留原 Store/Runtime 两个真实 live owner，核实实际执行重叠及两个身份均完整退出。544项编译输入、原始测试库存、构建/binary hash、配置、数据库持久性设置和清理记录均通过核验。本机 macOS 二进制单独保留自身 hash 与真实结果。

## 真实执行与保留失败

| 源码 / 执行 | PostgreSQL16 | PostgreSQL17 |
| --- | --- | --- |
| V2 选中 Store 结算用例 | exit 0；1 passed / 266 filtered | exit 0；1 passed / 266 filtered |
| V2 public Store COMMIT-loss / 冷进程 | test exit 0；1 passed / 193 filtered | test exit 0；1 passed / 193 filtered |
| V2 完整 Store | exit 0；267 passed | exit 0；267 passed |
| V2 完整 Runtime | exit 101；193 passed / 1 failed | exit 101；193 passed / 1 failed |
| V3 本机完整 Runtime | exit 101；183 passed / 11 failed | exit 101；182 passed / 12 failed |
| V3 CI 完整 Store | exit 0；267 passed；505.98s | exit 0；267 passed；620.48s |
| V3 CI 完整 Runtime | exit 0；194 passed；803.05s | exit 0；194 passed；969.80s |

CI 的4套完整 Native 均为0 failed / ignored / filtered。V2 Runtime 唯一失败是角色测试仍预期 schema36，而实际已为37。V3 只将该测试 literal 改为37，并单独审查源码/编译输入 delta；V3 本机完整 Runtime 仍出现上表真实失败，所有 failed case 名称、stderr、summary 和 receipt 均保留。限定执行区间的电源日志包含30次 sleep 和1次 clamshell；这里只能说明相关，没有受控清醒重跑证明因果。Linux CI 成功是独立执行，不改写原本机失败。

选中的 COMMIT-loss 用例只走 public Store API，没有运行 Driver。代理扣住后端真实 COMMIT 响应，独立 reader 先证明 v2 关闭已提交，再真实 SIGKILL 并 reap 活跃 writer。另起冷进程读取相同关闭字节，以新 event ID、过期 revision 和零候选用量恢复 Existing，整个租户事实不变；原 owner 仍 Executing、Join 未消费。两份独立核验均通过，要求恰好22个强类型 marker 字段及真实 signal 9。PG16 原 wrapper 的 exit 1 继续保留：它的观察器错误地要求有效 nocapture marker 两侧的 test/`ok` 文本连续，而真实 test exit 0。原 false observations 和 Root 如实转录的外层结果出处均未改写。

V3 六项本机门禁全部真实 exit 0：fmt、strict Clippy、workspace tests、doctests、warning-denying Rustdoc 和固定 nightly check。Workspace 共100份 harness summary、2128 passed、6个既有 ignored，doctests 为48 passed。这些 all-features / opt-level 0 执行没有强制 PostgreSQL 环境。V3 本机已有 consumed-Join 的 logical backup 和 physical PITR 在双版各通过一次，共4次真实 exit 0，绑定 V3 源码及本机 Runtime binary。

## 完整 CI 证据

严格分析核对全部 job 和 step 状态、双 major 各原60个 PostgreSQL 源步骤、全部54个 artifact ZIP 的摘要及成员库存、1525个索引 raw 文件，以及99份成功的官方 GET 回执。Workflow 每个 major 有4个验收 shard，保留两个强制聚合 job，其他11个 job 不变；取消、必须步骤 skipped、失败或证据缺失均不能授资格。

双 major 的 frame Return 与 scoped Wait 均通过两个真实 cut marker（`commit_not_forwarded`、`commit_response_withheld`），每项库存194、exact 1 passed / 193 filtered。每个 major 的48个 workflow marker 契约及24份 raw harness 日志全部核对，覆盖 schema37 的真实 LOGIN/ACL、逻辑恢复、具备资源清理的 named physical PITR、强制15测试 MCP bound-state 执行、原协议/示例真实执行及固定真实 TLS identity/rotation/revocation/host marker。Reduced-host 原输出仍为 `release_qualified=false`。

首次严格分析真实 exit 1：通用观察器误拒合法的 ignored 子进程 fixture；其完整 bundle、原 raw 输入和失败 receipt 均保留。V5 只对3个精确源码绑定的 child entrypoint 核对原始计数、成功父测试及匹配 marker，其他 ignored/failed/empty harness 仍拒绝。Root 重新 prepare 新 bundle，重新收集完整官方证据后才获得实际 exit 0。没有改写产品代码、原始日志或测试结果。

## 迁移、历史与开放事项

迁移37替换完整关闭 guard，不新增表、backfill 或 ACL 权限，保留54表角色库存。v2 envelope/intent 只新增一个必填且恰好三字段的 `stopped_join`，v1 字节、domain 及原 getter 不变。读取两版历史时须注册两个精确离线 schema：

| Getter | 版本 | JCS 文档摘要 |
| --- | --- | --- |
| `graph_frame_closure_event_schema()` | 1.0.0 | `sha256:453045e205133eb10d3cd58f0242ee2ebbd26dd78051b0888cd6a95f8a53e520` |
| `graph_frame_join_stop_event_schema()` | 2.0.0 | `sha256:aec9b96de74a4185f288e1425a59a7fb8d8eb4363378bbd5522b2500a261ea5f` |

先排空旧 Worker、保留验证过的升级前备份，再正常执行 checksum-pinned SQLx 全量迁移至37，应用/审计匹配角色配置和精确 catalog，最后启动匹配二进制。裸函数替换和修改 ledger 不构成部署。测试 revert fixture 拒绝保留的 v2，不提供生产降级路径。已有 consumed-Join 恢复证据不能替代 owner-stop-v2 专属恢复或已发布版本兼容窗口。

较早 public consumer 的 schema/冷读及保留源码 v1 的 schema36→37 迁移继续单列：product tree 为 `bf329a3da4eacc9ccd9c34e681a1d53c8c9eab662e2c0a4b1831aac8f8266509`，consumer tree 为 `5d77326d5cce4988506b7acb549cedb37eba5be62c14b857502736eeffaa806d`，binary 为 `7c174940e83c9435e66ed7675b5faba4af3c8302c79732a648553b609350b30b`，4个 exact case 均通过。源码 fixture 不等于已发布 N−1/N−2 或滚动升级验收。

归档完整保留旧 assembly 的806个文件、baseline PostgreSQL test101/no-write、manifest/Docker preflight、ACK/composed Clippy、loopback 权限 workspace 失败及后续全部 Native 失败。历史 CI [39113329120](https://github.com/StateKnot/StateKnot/actions/runs/39113329120) 和 [39120579635](https://github.com/StateKnot/StateKnot/actions/runs/39120579635) 在原30分钟上限下都仍为11 success / 2 cancelled，包含完整 raw logs/ZIP 及独立审查；它们的 Native 库存264/193、两个/三个 owner 布局不向本候选转授资格。静态审查也与执行证据单列。

首个 Store profile 不覆盖已发布未消费 Join、spawn-before-registration 和后续物理 owner 恢复。更广反例、deferred-guard/故障矩阵、Root 终态确认、全部3处 Driver guard 移除、已发布版本/恢复窗口、必须 PR checks、容量、独立安全及部署/发布门禁仍需各自证据。后续 ignored F2 切片（含197/198库存）只是独立 future work，不授当前 commit 资格。PR 仍为 Draft，Driver、Root、R1、R6 继续开放。
