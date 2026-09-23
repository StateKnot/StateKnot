<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 子 Run Join 的物理时间点恢复验证

[English](child-join-pitr-qualification.md)

`child-join-named-pitr-v1` 在 PostgreSQL 16/17 上，对一条已消费的持久化子
Run Join 执行真实的物理备份与命名恢复点演练。它只覆盖 v1 灾难恢复验收的一部分：
不能据此宣称已有生产备份服务、达到 RPO/RTO 目标，或主库故障切换零数据丢失。

测试启动一次性源集群，将 WAL 持续归档到私有 Docker 卷；执行 StateKnot
精确版本迁移，提交子准入、终态结算、Join 发布与消费、父节点物理尝试、待消费
结果、Journal 和 Checkpoint，并读取完整持久化快照。随后以流式 WAL 制作
`pg_basebackup`，用 `pg_verifybackup` 验证备份清单。基础备份结束后才创建
命名恢复点，提交一笔刻意晚于恢复点的写入，切换 WAL 并等待该写入所在段归档。

测试停止源进程，在另一 PostgreSQL 容器中以物理基础备份、已归档 WAL 和
`recovery.signal` 恢复并于命名点提升为主库。恢复后的连接不执行迁移，必须
通过原有迁移校验和检查；晚写入必须不存在。独立读取器对比父子 Run 投影、
归属、结算、预算、Join、Attempt、待消费结果、Checkpoint 和完整 Journal；
跨租户查询必须拒绝。重建执行注册表后，父图必须从非初始 Checkpoint 继续，
且不能重复执行已消费的 Join 节点。演练使用唯一命名的私有 Docker 容器与卷，
结束时清理。

## 复现

使用固定 digest 的 PostgreSQL 16 或 17 镜像，并确保 Docker 可用。测试只开放
临时本机回环端口，不修改现有数据库。

```sh
STATEKNOT_REQUIRE_PITR_TESTS=1 \
STATEKNOT_PITR_POSTGRES_IMAGE=postgres@sha256:<reviewed-image-digest> \
cargo test -p stateknot-runtime --test postgres --locked -- \
  --exact child_admission::ownership::join::pitr::consumed_child_join_survives_named_point_in_time_recovery \
  --nocapture --test-threads=1
```

PostgreSQL 16/17 的独立 CI 作业各要求恰好一条
`STATEKNOT_CHILD_JOIN_PITR_EVIDENCE` 记录，并将证据与提交、Git tree、锁文件、
工具链、镜像和主机信息保存 30 天。标记不包含备份字节、凭据或 Agent 载荷。

## 仍未完成的生产门槛

演练仅覆盖一台小型一次性集群与一条子 Join 路径，不验证按时间戳选点、持续
备份服务、WAL 缺失或损坏、备份与密钥/对象存储一致性、所有持久记录类型、
保留期与法律保全、参考规模数据、同步备库提升、已确认写入的 RPO 0、实测
RTO、多角色应用重启或长期稳态运行。完整的 [v1 场景](scenarios/002-long-running-approval.md)
与 [RFC-0003](rfcs/0003-postgresql-durability-recovery-and-migration.md) 仍未验收。
不得将本测试的一次性 Docker 卷当作生产备份方案。
