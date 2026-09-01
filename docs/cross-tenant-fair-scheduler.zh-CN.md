<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 跨租户耐久公平调度

`DurableFairScheduler` 在现有 Tenant-isolated Scheduler Worker 上增加 Replica-safe 加权 Tenant Selection。它仍是未发布的 pre-alpha。实现提供精确的“预约次数”饥饿上界；它不承诺墙钟延迟，也不保证空队列或竞争队列一定成功取得工作。

英文版见 [Cross-tenant durable fair scheduling](cross-tenant-fair-scheduler.md)。

## 为什么顺序必须耐久

Scheduler Replica 重启或横向扩容后，进程内 Round-robin Cursor 会分叉。StateKnot 改为编译一个确定性的 Smooth Weighted Round Robin Cycle，并在 PostgreSQL 中把 Canonical Byte 永久绑定到显式 `SchedulerShardId`。

任何 Replica 扫描 Tenant Queue 前，都先原子预约下一个全局 Slot。Reservation 包含：

- 稳定 UUIDv7 `SchedulerReservationId`；
- 不可变 Shard 与 Policy Digest；
- 单调递增的 Shard-global Sequence；
- 选中的 Cycle Slot；
- 权威 Database Reservation Time。

PostgreSQL Cursor Lock 只覆盖这个短 Reservation Transaction。Queue Discovery、Lease Claim、Graph Execution、Lifecycle Commit 与外部工作均在事务关闭后执行。

## 编译不可变加权 Policy

```rust,ignore
let tenant_a = TenantId::try_from("tenant-a")?;
let tenant_b = TenantId::try_from("tenant-b")?;

let policy = WeightedFairnessPolicy::new(
    SchedulerShardId::try_from("primary-v1")?,
    [
        TenantFairnessWeight::new(tenant_a.clone(), 3)?,
        TenantFairnessWeight::new(tenant_b.clone(), 1)?,
    ],
)?;

assert_eq!(policy.cycle_length(), 4);
let bound = policy
    .starvation_bound(&tenant_b)
    .expect("tenant belongs to the immutable policy");
assert!(bound.maximum_reservations_until_selection() <= 4);
```

构建过程按精确 Tenant Identifier 排序、拒绝重复项、生成一个完整确定性 Cycle、验证每个 Tenant 的出现次数严格等于配置 Weight，并计算每个 Tenant 两次 Selection 之间最大的环形 Slot Gap。

硬上界防止配置变成无界 Runtime Work：

- 每个 Shard 最多 1,024 个 Tenant Queue；
- 单个 Weight 范围为 1 到 1,024；
- 一个完整 Cycle 最多 4,096 个 Slot。

每个完整 Reservation Cycle 中的配置比例是精确的。它表示 Scheduling Opportunity Share，不表示 Token、Money、CPU Time 或 Completed Run Share。

## Claim Work 前注册

```rust,ignore
let scheduler = DurableFairScheduler::register(
    store,
    executable_registry,
    lifecycle_evidence_provider,
    DurableGraphDriverOptions::default(),
    DurableGraphLifecycleOptions::default(),
    DurableTenantSchedulerOptions::default(),
    policy,
    DurableFairSchedulerOptions::default(),
)
.await?;
```

Registration 构建现有 Tenant-scoped Worker，并 Idempotent 地持久化精确 Policy。同一个 Shard Identity 只能复用完全相同的 Policy Byte。修改 Weight、Tenant、Ordering 或 Algorithm 时必须发布新的 Shard Identity，例如 `primary-v2`；先有意 Drain 旧 Scheduler Deployment，再激活新版本。

Rollout 期间不要让两个不同 Shard Identity 的 Replica 同时处理同一个逻辑 Worker Pool：每个 Shard 有独立 Cursor，这样做会有意形成两套独立 Schedule。

## 执行一个全局 Scheduling Quantum

```rust,ignore
let tick = scheduler.tick(shutdown.clone()).await?;

record_selection(
    tick.reservation().sequence(),
    tick.tenant_id(),
    tick.starvation_bound(),
    tick.reservation_retries(),
    tick.tenant_tick(),
);
```

每次调用只分配一个 Reservation Identity；瞬时数据库错误只重试相同 Identity；Durable Slot 通过本地不可变 Cycle 映射到 Tenant；最后对该 Tenant 执行一个有界 `DurableTenantScheduler` Tick。

即使所选 Tenant Queue 为空，或 Candidate 在 Lease Contention 中失败，该 Reservation 也会被消费。这保持全局顺序，并防止 Busy Tenant 抢占其他 Tenant 的 Share。仍有 Capacity 时，调用方应继续 Tick，而不是在同一 Reservation 内扫描另一个 Tenant。

若要在下一次 Selection 前 Shutdown，就停止调用 `tick`。Reservation 一旦耐久，选中的 Tenant Tick 会返回正常关闭的 Cancelled/No-work/Work Outcome；Slot 不会回滚。

## 精确 Starvation Boundary

对于持续 Eligible 的 Tenant，`TenantStarvationBound` 返回该 Tenant 两次 Selection 之间最多经历的全局 Slot Reservation 数，包含命中它的 Slot。该值从真实环形 Cycle 计算，而不是从 Weight Ratio 估算。

只有部署同时约束以下因素时，它才能转换成墙钟 Service Objective：

- Scheduler `tick` 调用间隔；
- Database Reservation Latency 与 Retry Count；
- 所选 Tenant 的 Page Scan 与 Claim Duration；
- 每个 Tenant Scheduling Quantum；
- 可用 Replica/Worker Capacity。

Queue Contention 仍可能让已选 Tenant 无法 Claim Run。因此必须同时报告 Selection Lag 和 Successful-claim/Service Lag。

## Lost ACK 与 Retention

PostgreSQL Migration 14 保存不可变 Reservation Row。使用相同 Reservation ID 重试时，会返回原 Sequence/Slot，不会把 Shard Cursor 推进两次。Replica 并发只在 Cursor 处串行化；不同 Reservation 形成一条连续 Global Sequence。

旧 Reservation Evidence 可以用短事务协作式清理：

```rust,ignore
let policy = SchedulerFairnessRetentionPolicy::new(
    Duration::from_secs(24 * 60 * 60),
    1_000,
)?;
let report = store
    .prune_scheduler_fairness_reservations(policy)
    .await?;
```

Database Clock 决定 Exclusive Cutoff。Candidate 使用 Retention Index 与 `FOR UPDATE SKIP LOCKED`；并发 Maintenance Worker 不修改 Policy Row 或 Cursor Position。Retention Window 支持一小时到 366 天，每个事务最多删除 10,000 行。

部署绝不能在配置 Retention Window 之后重试 Reservation Identity。一旦 Evidence 被删除，相同 UUID 无法找回原 Slot，会破坏 Runtime 的 Lost-ACK Guarantee。Retention 必须长于所有 Scheduler Retry、Incident Replay 与 Audit Window，并监控最老保留 Row 和删除 Backlog。

## 运维指标

至少记录：

- 按 Shard 分类的 Reservation 与 Database Retry；
- 按 Global Sequence 排列的 Selected Slot/Tenant；
- 每个 Tenant 的配置 Weight、每个完整 Cycle 的实际 Selection 与精确 Starvation Bound；
- Tenant Tick Outcome、扫描 Page/Candidate、Contention Skip、Claim Retry 与 Execution Outcome；
- Reservation-to-tick 与 Selection-to-successful-claim Latency；
- Retention Cutoff、Deleted Row、Oldest Retained Reservation 与 Backlog。

Immutable Policy Conflict、Projection Mismatch、Sequence Exhaustion、持续 Eligible Tenant 超过 Reservation-count Bound，或 Retention 接近仍存活的 Retry/Audit Horizon 时必须告警。

## 验证证据与剩余 Blocker

Property Test 覆盖任意有界 Tenant 顺序和 Weight Set、精确 Cycle Count、Order Independence 与所有环形 Starvation Bound。真实 PostgreSQL Test 已证明 Immutable Registration、Lost-ACK Recovery、Same-ID/Unique-ID Concurrency、连续顺序、有界 Database-time Retention、Cursor Neutrality、Migration Verification，以及跨四次分布式 Scheduler Tick 的 3:1 双租户 Share。

这仍未完成生产调度。Role-separated Credential、Multi-replica Soak/Kill Test、Admission/Rate Policy、Capacity-aware Sharding、Telemetry、Failover/Restore Evidence、Stale-race Qualification 与 Operator Rollout Tooling 都是 v1 支持前的必需项。
