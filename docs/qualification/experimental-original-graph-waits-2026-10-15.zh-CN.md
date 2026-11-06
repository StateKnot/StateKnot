<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 实验性原始 Graph Wait：精确执行验收记录

日期：2026-10-15。**精确执行验收通过，资源清理仍有未解决异常。** 冻结的 schema36 与
scoped-attempt-proof 候选通过六项静态门禁、PostgreSQL 双版完整四套 Native，以及
独立强制 logical restore/PITR。PG16 PITR 遗留一个本测试 WAL 卷；持久性断言通过
不能替代清理成功或完整生产资格。

[机器回执](experimental-original-graph-waits-2026-10-15.json)、
[证据封签](evidence/experimental-original-graph-waits-2026-10-15/seal.json)及
[校验器](evidence/experimental-original-graph-waits-2026-10-15/verify.py)绑定以下结果。
[English](experimental-original-graph-waits-2026-10-15.md)。

## 精确源码与采用

实际候选共 922 文件，tree SHA-256 为
`88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb`，保留完整源码
归档与清单。采用资格绑定 **530 个 crate、配置和 fixture 输入**的保守超集，包括迁移、
测试和角色 SQL。实际主工作区基于 `a9b265d5b3a96e0e5bbed604b7558f4ae9edc136`
采用 47 个代码文件和另外审阅的八份文档，回执逐项确认编译输入字节等于该候选；保留
新 RFC-0024/RFC-0025 与既有历史。**不声明整个 main Git tree 等于旧 922 tree**。
`--workspace` 会重新核实际当前路径和文件，而非用隔离候选代替主工作区。

四份 Native 可执行文件均从该源码 fresh 编译并在缓存外重新核 hash，Cargo 记录为
`fresh: false`，保留调试断言和 overflow checks。使用 Rust 1.88、`-D warnings`、
关闭 incremental 与开发/测试 debug symbols 的用户要求。保留默认栈和各 fixture 原有
lease、事务限额、pool、quantum 与断言；这不代表所有 fixture 采用同一组资源值。
紧凑证据不包含大二进制或编译缓存。

## 实际门禁

| 项目 | 实际结果 |
| --- | --- |
| fmt、严格完整工作区 Clippy | 各 exit 0 |
| workspace all-targets/all-features/locked | 2,064 passed，原有 6 ignored，0 failed |
| 独立 workspace doctest | 45 passed，0 failed |
| 严格 Rustdoc | exit 0 |
| 固定 `nightly-2026-10-07` all-targets/all-features/locked | exit 0 |

静态运行清除了数据库环境，其 harness 数量不构成 Native 数据库资格。原 loopback
HTTP 测试使用所需本机权限执行，没有删减测试或放宽断言。

| Native 套件／独立 case | PG16 | PG17 |
| --- | --- | --- |
| Store | 264 / 0 / 0 | 264 / 0 / 0 |
| Runtime | 189 / 0 / 0 | 189 / 0 / 0 |
| integrations | 2 / 0 / 0 | 2 / 0 / 0 |
| 可恢复 Agent example | 1 / 0 / 0 | 1 / 0 / 0 |
| 显式强制 logical restore | exact 1 / 0 / 0，marker 1 | exact 1 / 0 / 0，marker 1 |
| 显式强制 PITR | exact 1 / 0 / 0，marker 1 | exact 1 / 0 / 0，marker 1 |

表中顺序为 passed / failed / ignored。四套合计 912 次 libtest 执行，独立 restore/PITR
再加四次，总计 **916 次执行**，不等于 916 个独立故障场景。每版 Runtime189 含 168
个普通数据库场景、15 个辅助子进程 worker、两个独立资源入口和四个 harness/protocol
自测。Worker 缺少控制器输入时可提前 return，普通 backup/PITR 入口缺少专用资源开关
时也可 return；上表独立 case 才提供强制开关并检查唯一完整 marker。

普通套件实际服务器为 PostgreSQL 16.15/17.11，结束时各有成功的 1–36 迁移记录和
54 张业务表。原 process-kill、commit-loss、受限 SQL role 和 maintenance marker
已保存并按精确 Runtime 清单核对。较早的 selected role fixture 保留其 919 源码身份；
最终 Runtime 也每版实际输出一次 role-profile marker。这些有界 fixture 不等于一般
权限、部署、容量或性能认证。

PITR 使用最终同一 Runtime binary，**测试进程为 macOS/aarch64，数据库为固定镜像
Linux/arm64 容器**，实际版本 16.14/17.11；不能称 Linux Host 运行或继承 CI x86_64
资格。Marker 确认 base backup、命名恢复点、排除后写入、完整持久快照、跨租户拒绝
和真实非初始 replay。Logical restore 另行确认 source/restore 隔离、schema、完整
快照、跨租户拒绝与非初始 replay，均保持原 profile 资源。

## 尚未解决的资源清理

PG16 PITR 后遗留 `stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1`。
原 before/after 清单证明它由本测试新增；只读 inspect 确认卷仍存在、没有引用容器，
同 UUID 容器均已退出。PG17 没有额外新增遗留卷。原 Drop 未检查删除错误，实际删除
命令失败的 stderr／具体原因没有捕获，因此根因仍开放。保留全部原证据和该卷；本
记录没有删除、stop 或 prune 资源。

这是真实资源生命周期异常，与已通过的持久性断言及下述 SQL 样式说明独立。
**不授予完整 PITR cleanup／资源生命周期资格**。资源审计另存真实最终 df 与保留的
非活跃任务缓存；不声称本次释放了多少缓存或已无任何资源遗留。

## 保留原红与 fixture 来源

原 919 文件组合首先运行完整套件：Store PG16 为 **261 passed / 3 failed**，PG17
为 **262 passed / 2 failed**；双版 Runtime189、integrations2、example1 均通过。
所有原始红日志与 exact follow-up 都保留，不能重标为 922 通过。

919→922 仅改两份 Store 测试模块，新增三份 fixture/provenance 文件；生产、配置与
1–36 迁移逐字不变。Timer oracle 区分原事件早于 due 的 `graph_registered` 和晚于
due 的 `graph_elapsed`，两分支均实际观察并保留时间戳。过时 source32 fixture 改为
实际 schema35 writer 配合既有 schema32 重建导出的原事实，不是假称历史 schema32
release。未加 exporter 的 frozen899 基线 **514 个编译输入**逐字等于 commit
`83559f…`；实际 producer 在该基线上另加一份精确的 exporter-only 测试文件 override，
不声明改过的测试输入也全部等于该 commit。真实 Git 归档
有 **980 文件**，此前冻结源码为 **899 文件**，不声明两棵完整树相等。保留 Git 归档、
exporter、原测试文件 override、全部 514 输入 hash 与实际 fixture 字节。

最初 fixture 编译／格式红、runner 顺序红、原 exact 红与之后绿均保留。单独 Return
测试原 **2 秒 lease、`pg_sleep(2.1)`** 没有改；原 full-run setup 失败与 exact 重跑仍
可复核，最终完整 Store 已通过该项。非空 current-source SOURCE35→36 回归独立保存，
也包含在最终 Store 清单中。

生成的 SQL fixture 保留原 pg_dump EOF 空行与精确 hash。Apply 实际报告这一项样式
warning。记录的 tracked-only `git diff --check` exit 0 当时未包含未跟踪 fixture，
不能宣称完整 staged 检查全绿；没有放宽 whitespace policy，也不豁免其他样式问题。

## 一个真实已发布 writer 场景

原 alpha.1 已封签 bundle 原样保留：唯一真实已发布版本的 registry Store/Core 原包和
原锁，schema26 writer，在双版各一个非初始 Root 场景上由实验性 **920 输入 reader**
升级读取。原 Store library/tests/lock 未改，五个原 published Core JSON 只补测试
include 所需 sibling 路径。全部 39 原行、完整原列值与 bytea 保留；迁移新增项及 claim
后六个实际 lease/fence 列变化逐项列明。旧 writer 拒绝新 schema 的 expected failure
仍是 negative，不能记 Native pass。

独立证明把 **160 个生产输入**及复用的父测试 helper 绑定于 922，不表示该 case 在
922 binary 上运行过、两套完整源码／binary 相同或资格自动转授。这只是一个具体
场景、一个实际 released writer，不存在第二个已发布 N-2 版本，也不是完整兼容矩阵。
本记录的 verifier 会再次执行嵌套原封包自带的纯离线 verifier。

## CI 与声明限制

此前 schema35 head `c7d8768…` 的 run `38717156638` 为 11 jobs success、两个普通
PostgreSQL jobs cancelled，均有原始 annotation：
“The job has exceeded the maximum execution time of 30m0s”。原快照与 annotations
保留，不转授本 schema36 候选。此次本地四套加 backup 链约 3,215／3,179 秒，没有
复现 CI 硬件或改动原 30 分钟预算。封签时未观察到此新候选 CI；完整 CI 不算通过。

RFC-0022 仍为 Draft。完整 R1、故障／兼容矩阵、生产容量／latency、schema37 Scoped
Join 和 stdio owner 仍开放；独立 A2A request-facts 候选未混入。此前 A-only SQL
计数诊断不能转授为此 binary 的 latency／性能资格。

在最终实际采用工作区运行下列命令；默认验证纯离线证据，`--workspace` 额外核实际
编译输入路径与字节，不能使用隔离候选冒充 main：

```console
python3 docs/qualification/evidence/experimental-original-graph-waits-2026-10-15/verify.py --require-execution-record --workspace
```

这里的 execution-record 仅指这份已封签精确执行记录，明确保留资源清理异常，不关闭剩余生产
里程碑。
