<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Agent 宿主容量与恢复验证

`stateknot-testkit` 是尚未发布的 StateKnot 宿主验证组件。它不依赖具体异步
Runtime，负责测量阶段、输入上界、封闭目标计算，并输出带 SHA-256 完整性摘要的
确定性 JSON。它不负责创建基础设施、执行故障注入、签署来源证明，也不会把 CI
冒烟测试包装成生产 SLO。

本指南实现仍为 Draft 的 [RFC-0017](rfcs/0017-host-qualification-harness.md)。
Crate 与报告 Schema 目前都属于 pre-alpha。

## 两种 Profile 不能混用

| Profile | 用途 | 发布结论 |
|---|---|---|
| `CiReduced` | 在有限 CI 资源上验证正确性、恢复接线与报告结构 | `release_qualified` 永远为 `false` |
| `ReleaseCandidate` | 完整参考拓扑、负载、测量窗口与故障矩阵 | 只有全部强制目标通过才可能成立 |

发布配置只有在以下条件同时成立时才允许启动：完整 v1 故障计划；启用同步
提交与同步备用库的 PostgreSQL 16/17；达到文档要求的拓扑和资源；应用到数据库
的中位 RTT 不超过 2 ms。它还强制至少 10 分钟预热、30 分钟测量与 5 分钟排空。
这些阶段由记录器使用单调时钟计算，驱动程序不能伪造已用时间。

## 记录一次有界验证

```rust,ignore
let plan = FaultPlan::ci_default();
let run = QualificationRun::start(
    QualificationProfile::CiReduced,
    QualificationScenario::AgentHostControlPlaneV1,
    QualificationWindow::ci_default(),
    environment,
    &plan,
)?;

run.begin_measurement()?;
let recorder = run.recorder();
let operation = recorder.begin_operation()?;
recorder.record_latency(
    LatencySignal::AdmissionCommit,
    admission_elapsed,
    Some(expected_offer_interval),
)?;
operation.finish(OperationOutcome::Completed)?;

recorder.fault_injected(FaultCaseId::HostRollingReplacement)?;
recorder.fault_invariant_checked(FaultCaseId::HostRollingReplacement)?;
recorder.fault_recovered(FaultCaseId::HostRollingReplacement)?;

run.begin_drain()?;
let report = run.finish()?;
let bytes = report.to_integrity_envelope()?.canonical_bytes()?;
```

操作守卫会增加进行中计数。测量阶段的守卫如果没有记录结果就被丢弃，会记为
非预期失败；还有守卫时不能结束排空。最多允许一百万个进行中操作，达到上限会
显式拒绝，不会在验证工具内形成隐藏队列。

## 报告能够证明什么

每次运行都会绑定 Git Commit/Tree、`Cargo.lock`、数据集与配置的 SHA-256，
并记录机器、数据库、拓扑和实测数据库 RTT。四类共享延迟使用 1 微秒到 1 小时、
三位有效数字的 HDR Histogram：准入提交、Runnable 到 Claim、已连接 SSE
投递和 SSE 重连。固定速率驱动程序应传入期望发送间隔，用于修正协调遗漏
（Coordinated Omission），同时保留原始样本数与 Histogram 样本数。

受检计数包括提交、接受、完成、拒绝、验证工具容量拒绝、预期注入失败、
非预期失败、已确认记录丢失、过期围栏写入被接受、跨租户
披露和外部效果重复。正确性/安全性计数、非预期失败和未完成的计划故障在两种
配置中都会失败。延迟、饱和度与噪声租户公平性阈值在 `CiReduced` 中只供
诊断，只有有效发布配置才强制执行。

缩减配置没有测量发布专用的饱和度或公平性时，会明确序列化为 `null`，并给出
`informational_missing`，不会用零值伪装通过；发布配置遇到同样缺失会失败。

## 故障矩阵

v1 使用固定 ID 表示数据库不可用、宿主依赖就绪检查失效、工作进程丢失、宿主滚动
替换、验证器不可用和运维策略过期。每个计划用例都记录注入次数、恢复次数和
有上界的必检不变量数量。自由文本用例名和计划外更新会被拒绝。

PostgreSQL 16/17 的缩减 CI 驱动程序只计划依赖失效和滚动替换。它走真实 HTTP
准入到 Graph 终态，使用精确 Journal 时间计算 Runnable-to-claim，验证实时与重放
SSE、脱敏运维读取、就绪拒绝/恢复、替换宿主先就绪再排空旧宿主，以及替换后的
继续执行。它必须只输出一条经过自身校验的
`STATEKNOT_HOST_QUALIFICATION_REPORT`。其中的时间只是该 CI Runner 的诊断值，
不能作为其他部署的容量指标。

## 完整性摘要不等于来源证明

`IntegrityEnvelope` 对严格报告做 Canonical JSON 编码，并在读取时重新计算
SHA-256。未知字段、不支持的版本、越界数据、派生目标不一致和字节篡改都会拒绝。
但任何人都能重新计算 SHA-256，因此它不能证明由谁运行。发布决策还必须通过可信
执行或 Attestation 系统，把报告绑定到经过审核的源码。

完整生产门禁仍包括参考负载、全部故障矩阵、真实身份/策略部署、24 小时浸泡测试、
数据库与 Object Store 故障切换、备份恢复、N-1/N-2 升级验证和安全审查。
