<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 耐久 Artifact Storage 与 A2A Task 完成

> 状态：已实现的 Pre-alpha 切片；Rust API 尚不稳定。<br>
> Metadata：PostgreSQL Migration 18。<br>
> Bytes：私有 S3-compatible Object Storage；测试使用 In-memory Backend。<br>
> 明确排除：公开下载 URL、通用 Retention/GC、签名 Artifact、跨 Region 复制验证，
> 以及稳定 Artifact API。

StateKnot 不再把已完成 A2A Task 的 Artifact Bytes 塞进 Tool Inline JSON。
`A2aRemoteAgent::bind_with_durable_artifacts` 只接受 Task Response，把每个 Terminal
Artifact Part 交给 `StateKnotArtifactStore` 持久化，最终只返回有界完成 Projection
和 Tenant-qualified `ArtifactRef`。

这是同一条证据链，而不是最终一致的便捷 API：

```text
耐久 Executing Event
  -> 一次 A2A Message Send
  -> 与 Endpoint 绑定的 Task Recovery Handle
  -> 直接 GetTask Poll（绝不通过重发业务 Message 轮询）
  -> 有界 Terminal Artifact Parts
  -> 唯一 Staging Object
  -> Conditional-create 确定性 Final Object
  -> 完整 Length + SHA-256 校验
  -> 锚定 Origin Event 的不可变 PostgreSQL Registration
  -> 授权查询 + Conditional Object Read + 完整重校验
```

## Storage 不变量

- Final Object Key 与 Registration Key 由精确 Tenant、Run、Logical Invocation、
  Physical Attempt、Origin Event、Tool、Remote Task/Artifact Identity 和 Part
  Position 确定性生成。完全相同的 Retry 会收敛；同一 Identity 下的不同 Bytes
  会触发完整性失败。
- Final Key 只允许 Destination-create。任何 Retry 都不能覆盖已有 Final Object。
  初始化会真实执行 put/copy-if-absent/read/repeated-copy/delete Probe；Backend
  无法证明该合约时直接拒绝启动。
- PostgreSQL 保存 Canonical `ArtifactRef` Bytes/Digest、Content Length/Digest、
  Cause Run/Event、同 Tenant Direct Parent，以及私有、Provider-neutral Object
  Locator。Locator 不会进入 `ArtifactRef`、公开 Error 或 `Debug`。
- Registration Transaction 内不执行 Object I/O。Object 先完整发布并验证；
  Migration 18 再原子注册不可变 Metadata 与 Lineage。
- Resolve 会在任何 Registry Lookup 前，先授权精确 Principal 与 Tenant-qualified
  Artifact Identity；随后使用已捕获的 Object Version/ETag（若存在），完整读取
  有界 Body，并在返回任何 Byte 前同时校验 Length 与 SHA-256。

## 配置生产边界

先由 Migration Role 应用 Migration 18，再构造 Runtime Store。Object Storage 应
使用 Workload Identity 或短期 Credential Provider；Wrapper 刻意不暴露 Access-key
参数。

```rust,no_run
use std::{net::IpAddr, sync::Arc};
use stateknot_artifact_store::{
    ArtifactStoreOptions, RemoteArtifactOrigin, S3CompatibleBackendBuilder,
    S3ConditionalCopy, StateKnotArtifactStore,
};

let objects = S3CompatibleBackendBuilder::from_env(
    "stateknot-private-artifacts",
    "ap-southeast-1",
)?
.with_https_endpoint("https://s3.example.internal")?
.with_conditional_copy(S3ConditionalCopy::AmazonMultipart)
.with_sha256_checksum()
.with_kms_key_id("alias/stateknot-artifacts")?
.build()?;

let origin = RemoteArtifactOrigin::https(
    "https://artifacts.partner.example",
    ["203.0.113.10".parse::<IpAddr>()?],
)?;
let options = ArtifactStoreOptions::default()
    .with_remote_origins([origin])?
    .with_limits(64 * 1024 * 1024, 64 * 1024 * 1024, 8 * 1024 * 1024, 2)?
    .with_concurrency_limit(8)?;

let artifacts = Arc::new(
    StateKnotArtifactStore::initialize(
        objects,
        Arc::new(postgres_store.clone()),
        Arc::new(artifact_read_authorizer),
        "production-artifacts-v1",
        options,
    )
    .await?,
);

let remote = A2aRemoteAgent::bind_with_durable_artifacts(
    descriptor,
    a2a_client,
    "answer",
    delivery,
    recovery,
    schemas,
    artifacts,
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`AmazonMultipart` 是 AWS S3 Strategy。兼容 Provider 可能需要其文档规定的
Conditional Destination Header 与失败 Status，应使用
`S3ConditionalCopy::header` 或 `header_with_status`。不要猜测：必须针对服务实际
使用的 Endpoint、Bucket Policy、Encryption Policy 和 Identity 运行 Startup Probe。

`from_env` 只导入 AWS Static/Session Credential、Web-identity 设置，以及经过校验的
ECS/EKS Container-credential 设置。环境中的 Endpoint、HTTP Enablement、Metadata
Override、Unsigned Request、Retry Behavior、Bucket/Region Selection 与 Encryption
设置不会被继承。非默认 Web-identity STS 服务必须通过经过校验的
`with_https_sts_endpoint` 显式配置。

Storage Namespace 用于标识一个稳定物理 Backend，但不暴露 Endpoint 或 Bucket。
若在同一 Namespace 下替换 Bucket，必须执行保持完整性的迁移；否则应分配新 Namespace。

## Remote URL Policy

目前支持 Text、Structured JSON、Inline Bytes 和 External URL Part。External URL
使用独立 Egress Boundary：

- 生产 Origin 必须是带显式 IP Pin 的精确 HTTPS Origin；
- 每一跳 Redirect 都重新解析并校验 Allowlist；
- 拒绝 URL Credential 与 Fragment；
- 禁用环境 Proxy、自动 Redirect、Retry、Cookie 与透明 Content Decoding；
- `Content-Encoding` 必须缺失或为 `identity`；
- Part 声明的 Media Type 必须与 Response Media Type 一致；
- Registration 前会同时依据 Request 与本地 Ceiling 校验声明和实测 Length。

Literal Loopback HTTP 仅用于测试或受控 Same-host Sidecar，不能作为跨 Host/Trust
Boundary 的生产 Transport。

## Bounds 与 Lifecycle 义务

| Limit | 默认值 | Hard Ceiling |
| --- | ---: | ---: |
| Object Operation Timeout | 60 s | 10 min |
| 完整 Remote Request Timeout | 120 s | 10 min |
| Multipart Part | 8 MiB | 5–64 MiB |
| Remote Object | 64 MiB | 1 GiB |
| Materialized Read | 64 MiB | 1 GiB |
| Redirect | 3 | 3 |
| Concurrent Operation | 8 | 256 |

一个 Process-local Permit 覆盖完整 Ingest 或 Materialized-read 路径，包括
Authorization、Registry、Remote Download 与 Object Operation。A2A Request 还会把这些
配置与 Tool Execution Limit 取交集：Artifact Count、Total
Artifact Bytes、Per-part Bytes 与 Inline Result Bytes。Direct Message、没有可恢复
Handle 的 Non-terminal Task、不支持的 Part、重复 Local Artifact Identity 或任意
超限输入都会 Fail Closed。

必须为 Abandoned Multipart Upload 和 `stateknot/staging/v1/` Prefix 配置 Provider
Lifecycle Rule。Runtime Cleanup 是 Best-effort；无法删除 Staging Data 时会增加
`staging_cleanup_failures()`，每次增长都应告警。

**不要**对 `stateknot/artifacts/v1/` 应用盲目的按年龄删除规则：Final Object 可能
已经完成耐久 Registration。若数据库在 Final Publish 后不可用，也可能留下未注册的
确定性 Object；完全相同的 Retry 会验证并接管它。在通用 Retention 与 Registry-aware
Orphan Collector 完成前，运维方必须盘点 Final Prefix，并在删除疑似 Orphan 前与
PostgreSQL 对账。

## 可执行证据与发布边界

```console
cargo test -p stateknot-artifact-store --locked

STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL='postgres://...' \
cargo test -p stateknot-artifact-store --test artifact_store --locked

STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL='postgres://...' \
cargo test -p stateknot-store-postgres --test postgres \
  artifact_registry_is_exact_tenant_scoped_and_lineage_safe --locked

cargo test -p stateknot-integrations --test a2a_client_contract \
  durable_task_handle --locked
```

测试覆盖 Exact Retry、Substitution 与 Object Tampering、Authorization-before-lookup、
URL Origin/Redirect/Media/Encoding/Size 拒绝、Multipart Ingestion、完整 Resolve、真实
PostgreSQL Registry、Migration 17→18，以及 HTTP+JSON/JSON-RPC 两种 Binding 下不重发
业务 Message 的 Terminal-task Recovery。

这些证据尚未验证 Live S3-compatible Service、Lifecycle Collector、Backup/Restore
Topology 或公开 Download Service；它们仍是明确的生产 Release Gate，而不是隐含声明。
