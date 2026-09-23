<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 子 Run Join 逻辑备份恢复验收

[English](child-join-backup-restore-qualification.md)

`child-join-logical-restore-v1` 在两个新建的隔离数据库之间执行真实的 PostgreSQL
Custom Format `pg_dump` 和 `pg_restore`。这是对一条持久化子 Run/Join 路径的恢复
完整性演练，不是生产备份策略，也不宣称完成时间点恢复（PITR）。

源数据库执行精确校验的迁移，并提交子 Run 准入、终态结算、Join 发布、带 Fence
的父节点物理尝试、Pending Node Result 和唯一 Join 消费。备份前关闭源 Store。
`pg_dump` 在固定 PostgreSQL 服务镜像内运行；备份流保留在内存中，证据记录其
SHA-256 摘要，再用 `pg_restore --single-transaction --exit-on-error` 导入另一个
空数据库。恢复后的 Runtime 不重新运行迁移，而是在连接时检查原始迁移校验和与
必需的 Schema 对象。

独立的恢复 Store 读取并比较完整源快照：父子 Run 投影、不可变归属/结算和预算账户、
Join Head 与消费记录、物理尝试、Pending Result、带校验和的当前 Checkpoint
以及两份完整 Journal。跨租户 Run 查询必须被拒绝。重建的 Graph Registry 随后将
恢复后的父 Run 推进至真实的非初始 Checkpoint：已消费 Join 节点不重复执行，后继
节点执行一次。成功后仅删除本测试创建的两个唯一命名数据库。

## 复现

使用可丢弃的本机 PostgreSQL 16 或 17 Docker 容器。将
`STATEKNOT_TEST_POSTGRES_CONTAINER` 设为该容器的精确 ID 或名称，
`STATEKNOT_TEST_DATABASE_URL` 设为其 Loopback 测试数据库 URL。测试拒绝
非 Loopback 数据库目标和格式不合法的容器标识。

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_REQUIRE_BACKUP_RESTORE_TESTS=1 \
STATEKNOT_TEST_POSTGRES_CONTAINER=<disposable-container> \
STATEKNOT_TEST_DATABASE_URL=<loopback-test-database-url> \
cargo test -p stateknot-runtime --test postgres --locked -- \
  --exact child_admission::ownership::join::backup_restore::consumed_child_join_survives_isolated_backup_restore \
  --nocapture --test-threads=1
```

两个强制 PostgreSQL CI Job 都要求恰好一条
`STATEKNOT_CHILD_JOIN_BACKUP_RESTORE_EVIDENCE` 记录，保留日志及源码/镜像清单
30 天；容器标识或备份进程不可用会直接失败。证据标记不包含备份内容、数据库 URL、
凭证或 Agent Payload。

## 尚未关闭的生产门禁

本测试在一台 PostgreSQL 服务器内恢复停写状态的单数据库逻辑备份。它不验证持续
WAL 归档、指定时间点 PITR、同步备库 Failover、RPO/RTO、大数据量、对象存储版本
与密钥对齐、全部持久化记录族，或负载下的全应用恢复。这些需要独立的参考拓扑演练，
通过之前不能宣称 StateKnot 具有生产灾备资格或整体生产就绪。
