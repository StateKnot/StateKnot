<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 scoped Join owner-stop 限定验收记录

快照时间：2026-10-18 11:26 UTC。[English](experimental-scoped-join-owner-stop-2026-10-18.md) · [机器记录](experimental-scoped-join-owner-stop-2026-10-18.json)。

本文记录 [RFC-0022 草案](../rfcs/0022-namespaced-graph-frames.md#registered-join-handoff-during-whole-frame-cancellation)
中的实验性 public Store 取消边界，不启用 Driver 的 scoped 子 Join。
本快照已具备双版 PostgreSQL 的选中结算用例、提交响应丢失用例及六项本机门禁的真实通过记录；
最终组合的完整 Native 套件和可交付原始证据包仍待完成。完整 CI、Root ConfirmCancellation、
RFC 接受、Supported 与 R1 继续开放。

## 选中契约

实际当前叶中唯一最新的普通 owner 注册精确且未发布的 Join 后释放租约，原物理尝试仍为
Executing。只有整个 Root 绑定 lifetime 子账户的子级均已终态并结算，才允许取消关闭整栈。
受信协调者须在保存的观察点提供完整已定价、覆盖整个 Run 的 DIRECT；结构下界和账户的
DIRECT 观察仍只是下界。已结算的 delegated 用量只累计一次。取消证明不创建普通完成/结果、
Join 发布/消费、租约、launch grant 或新的 Worker 权限。

同一 owning 事务认证完整 Root/帧 lineage、原物理历史/fence、精确 request/registration
和全部归属键，按账户规范顺序锁住全部已结算终态子级，再重读 owning record 和选中的 Join。
deferred guard 与写后 owning 验证仍在事务内。Fresh quarantine、第二个未完成普通 owner、
已被后续尝试取代的 owner、未完成工作、未知 Tool/Model 结果、未定价用量或未结算子级仍拒绝关闭。
封存失败关闭保留原严格检查。重试先完整认证原已提交决定，再评估 fresh 候选字段；后来的运维
quarantine 不使既有原 ACK 失效。

## 源码与执行绑定

最终执行源码是从 `e372184fb1c80ffb9903ac0d9feb8380db51253e` 派生并冻结的 composed-v2：

| 输入 | 精确值 |
| --- | --- |
| 源码数量 / SHA-256 | 936 / `e8f3b8a1c93cbbd8ad00fa3622657be032a98fc775ea9580337c27c1d8ad77db` |
| 编译输入数量 / SHA-256 | 544 / `acc2f7706a60201874a2dec4de5c690fcee4bf7a04409822bf409f72ca7f1c16` |
| Cargo.lock SHA-256 | `fc36c3966dc2abf990fae6a01b633459d95fedae0fd7f77c429b5f04447b2e94` |
| Store Native binary SHA-256 / 库存 | `c7c5daf1e2c9606d57f2fdf5c4791b2eacffc65e37b7b50c50de60465a63d474` / 267 |
| Runtime Native binary SHA-256 / 库存 | `6f633c5883ccdba9308d7ba408c044653ec4c3a95c9a3c27469ae15e0dda4e7e` / 194 |
| 精确 graph-frame catalog SHA-256 | `cc9ddd58510822215dfa18cab1a956eacf2dd707860270da63f0b8d707ee0ad7` |

Native 使用 Rust1.88.0、默认 features、test opt-level1，关闭调试符号及增量缓存，保留
debug assertion 和 overflow check，并使用默认线程栈。Core、Store、Runtime 和选中
Runtime test artifact 均真实重编译为 `fresh=false`，不把它称为依赖缓存冷构建。
保存的 binary 直接执行，test-threads1。PostgreSQL16.15/17.11 分别绑定固定镜像的本机
loopback 容器及全新数据库；执行前后 fsync/synchronous_commit/full_page_writes 均为 on。
回执保留源码/binary 字节、数据库和容器绑定。

## 组合源码上已执行的用例

| 用例 | PostgreSQL16 | PostgreSQL17 |
| --- | --- | --- |
| Store 子级结算前提 | exit0；1 passed/266 filtered；2.6120s | exit0；1 passed/266 filtered；6.5325s |
| public Store 提交响应丢失/冷进程 | test exit0；1 passed/193 filtered；3.8833s | test exit0；1 passed/193 filtered；3.9361s |
| 独立 COMMIT-loss 证据核验器 | exit0 | exit0 |
| Store267 / Runtime194 完整 Native | 本快照待完成 | 本快照待完成 |

结算用例通过 public Store API 依次构造“子级仍活跃”和“已终态但未结算”，两次拒绝均比较
整个租户业务事实，确认没有写入；真实结算后才允许关闭，并保留原 owner、未发布 Join 和账目。

Runtime 包中的进程用例只执行 public Store API，没有运行 Driver。协议代理扣住后端真实
COMMIT CommandComplete 和 idle ReadyForQuery；独立 reader 先证明 v2 关闭已提交，再对
仍存活的本任务 writer 执行真实 SIGKILL 并 reap。另起冷进程读取相同关闭字节，以新 event ID、
过期 revision 和零候选用量恢复 Existing，整个租户事实保持不变。原 owner 仍 Executing、没有
普通完成，Join 没有消费。独立核验器要求恰好22个强类型 marker 字段、Unix signal9、冷进程成功、
精确 libtest 计数及完整源码/binary/持久性设置绑定。

PostgreSQL16 原 wrapper 实际 exit1：其连续 `test CASE ... ok` 观察器未允许合法的
nocapture marker 位于 start 与独立 `ok` 之间。真实 test exit0、原日志和 false observations
全部保留；外层结果明确标注为 Root 对真实工具返回的如实转录。PostgreSQL17 使用新的观察器，
直接保存外层工具实际 exit0。离线核验器独立对两份真实输入返回0，没有把 PG16 原 wrapper 改写成成功。

## 本机门禁与独立历史切面

六项门禁都绑定 composed-v2：gates-v2 的 fmt、strict workspace Clippy 实际 exit0；
gates-v3 的 workspace、doctests、将 warning 视为错误的 Rustdoc 及固定 nightly check
实际 exit0。Workspace 共100份 harness summary、2128 passed、6个既有 ignored；
doctests 共48 passed。这些 all-features、opt-level0 执行没有强制 PostgreSQL 环境，
不授予 Native/数据库资格；默认 features、opt-level1 的 Native 结果另行记录。

较早的 public consumer 在双版分别执行了“已持久化 v2 的离线 schema 验证及冷 owning 读取”
和“保留非空源码 v1 的 schema36→37 public SQLx Migrator 升级及冷读”，四次均真实
exit0/1 passed/1 filtered。其产品树为
`bf329a3da4eacc9ccd9c34e681a1d53c8c9eab662e2c0a4b1831aac8f8266509`，
consumer 树为 `5d77326d5cce4988506b7acb549cedb37eba5be62c14b857502736eeffaa806d`，
binary 为 `7c174940e83c9435e66ed7675b5faba4af3c8302c79732a648553b609350b30b`。
它们保留 v1 closure、Root 和业务字节及原 migration rows。源码 fixture 不等于已发布
N−1/N−2 兼容、滚动升级或完整恢复资格，也不自动认证后续源码。

## 迁移与 schema 采用

迁移37替换完整关闭 guard，不新增表、backfill 或 ACL 权限，保留 schema36 的54表角色库存。
v2 envelope/intent 只新增一个必填、恰好三字段的 `stopped_join`；v1 字节、domain 及原 getter
保持不变。消费两版历史时须将两个精确文档都注册到离线 schema registry：

| Getter | 版本 | JCS 文档摘要 |
| --- | --- | --- |
| `graph_frame_closure_event_schema()` | 1.0.0 | `sha256:453045e205133eb10d3cd58f0242ee2ebbd26dd78051b0888cd6a95f8a53e520` |
| `graph_frame_join_stop_event_schema()` | 2.0.0 | `sha256:aec9b96de74a4185f288e1425a59a7fb8d8eb4363378bbd5522b2500a261ea5f` |

先排空旧 Worker、保留验证过的升级前备份，再通过正常 checksum-pinned SQLx 完整迁移至37，
应用/审计匹配的角色配置和精确 catalog，最后启动匹配 reader/writer。禁止用裸函数替换或改写
migration ledger 部署。测试 revert fixture 拒绝保留的 v2，不构成生产降级路径。
当前真实 LOGIN/ACL 验收不能由“白名单未变”替代。

## 保留的失败与未关闭门禁

原 baseline tracer 在 PostgreSQL16/17 的真实 test101/`InvalidRunFailureClose` 和 no-write
证据继续保留；早期 manifest 排序、sandbox Docker preflight 失败与产品实际执行失败分开。
ACK preflight 的 Clippy101（局部 Version 声明位置）、composed-v1 Clippy101（Runtime controller
过长）以及 composed-v2 workspace101（5个 loopback bind 权限失败）均有原始记录。
后续源码机械提取及相同源码的 elevated workspace 执行分别解决对应检查，没有通过新增 lint allow、
增加业务预算、删除测试或线程栈 override 绕过。

历史 CI 量测 [39120579635](https://github.com/StateKnot/StateKnot/actions/runs/39120579635)
的 head 为 `dc13238b766eef98783f05cabb896e840a723be6`，终态11success/2cancelled，
保留 PostgreSQL 原30分钟限额。它使用未改动的 e372 产品输入及 CI 调度变更，不包含本 Join 候选；
60个 PostgreSQL 源步骤和未完成后续链仍作为证据，不能把该 run 或后续调度候选的资格转授这里。

采用前须交付完整索引的原始证据，并依据真实回执补齐待完成的全量 Native 结果。首个 profile
仍不覆盖已发布未消费 Join、spawn-before-registration、后续物理 owner 恢复等情况。
更广反例、late rollback/故障矩阵、已发布版本兼容、当前角色验收、Root 终态确认、全部3处
Driver guard 移除及最终源码完整 CI 均需独立证据。容量、独立安全、真实部署/发布门禁继续单列。
RFC-0022、R1 和 Supported 状态均未关闭。
