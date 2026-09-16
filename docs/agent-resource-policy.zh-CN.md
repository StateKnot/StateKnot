<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Agent 资源授权策略

[English](agent-resource-policy.md) · [RFC-0012](rfcs/0012-agent-resource-policy.md)

`stateknot::runtime::agent_policy::AgentResourcePolicy` 是可直接接入的离线、默认拒绝
资源授权实现，适用于明确配置的服务账号和资源访问清单。它独立于 OAuth 验证与租户映射：
登录成功不代表可以访问租户下所有运行。当前实现仍为 pre-alpha，不代表稳定 API 或完整生产版本。

## 配置与保留策略

可信控制面构造 `PolicyDocument`：`format_version: 1`、精确的 `policy: CapabilityIdentity`、
绝对到期时间 `valid_until: Timestamp`、`submissions: Vec<SubmissionRule>`、
`runs: Vec<RunRule>`。不能接受 HTTP 调用者提交的策略、预期摘要或身份绑定作为可信配置。

- `SubmissionRule` 包含 `tenant`、`principal`、`agent`、`input_schema`、
  `granted_scopes`、非空 `budget_limits`。签发方、主体、Agent 所有者及版本、Schema
  摘要均精确匹配。规则允许符合该 Schema 的输入，不解析业务字段实现 ABAC。
  策略、Agent 与请求预算仍须共同给出所有维度的有限上限；请求只能进一步收窄。
- `RunRule` 包含 `tenant`、`principal`、`operation: RunPermission` 与
  `target: RunAccessTarget`。Read 与 Cancel 独立配置。`Run(id)` 不自动允许按提交键查找；
  `Submission(key.digest_for(&tenant))` 只允许该键摘要的查找。取消必须使用 Run ID。
- `TenantRuns` 是**明确授予的租户运维权限**，覆盖该租户全部运行及键查找，不能冒充所有权判断。
  提交权限本身不附带读取或取消权限。

`PolicyArtifact::new(document)` 拒绝重复或重叠选择器、未知版本、空预算及超过 1024 条规则。
同时适用默认 JSON 上限：256 KiB、32 层、每容器 1024 项、16384 个节点、单字符串 64 KiB、
对象键 256 字节。空规则是合法的“拒绝所有人”；不可执行的按键取消规则直接拒绝。

激活前，将 `artifact.canonical_bytes()` 保存到私有、不可变的部署存储，通过独立认证的配置清单
分发 `artifact.digest()`，按运行和审计保留周期保留旧版本。摘要是规范化类型文档的 SHA-256，
不是原始文件排版的摘要，也不是数字签名。读取文件或下载时应先限制字节数，再用
`PolicyArtifact::from_json` 和可信预期摘要校验；不能先无限读取再期待解析器解决资源问题。

## 接入真实服务

下面加载器与源码中编译测试的示例使用同一组 API：

```rust
use std::{sync::Arc, time::Duration};
use stateknot::core::Digest;
use stateknot::runtime::{JsonSchemaRegistryBuilder, agent_policy::*};

fn load_policy(
    retained_bytes: &[u8], trusted_digest: Digest,
    schemas: &mut JsonSchemaRegistryBuilder,
) -> Result<Arc<AgentResourcePolicy>, PolicyError> {
    register_agent_policy_evidence_schema(schemas)?;
    let artifact = PolicyArtifact::from_json(retained_bytes, trusted_digest)?;
    Ok(Arc::new(AgentResourcePolicy::new(artifact, Duration::from_secs(300))?))
}
```

在冻结可执行注册表**之前**注册授权证据 Schema，同时保留原有 Graph、Admission 与服务控制
Schema。将同一共享策略传给 `AgentServiceV1::new(store, executable, deployments, policy.clone())`。
HTTP facade 已为该策略实现 `AgentHttpReadiness`。使用在线身份服务时，应在
`AgentHttpIntrospection::check().await` 前后检查资源策略就绪，不能只检查其中一个。
`AgentHttpServer` 还会检查实际数据库与可执行注册表。真实 Keycloak 测试已使用这套组合，
不再用默认放行的资源授权桩代替。

## 更新、撤销与恢复

通过 `generation()` 获取并发更新保护代数，再调用 `replace(expected, fresh_artifact, lease)`。
每次更新须重新验证可信来源，不能无条件续期旧缓存。租期必须大于零且不超过一小时。
策略绝对到期时间跨重启保留，安装时也限制单调时钟截止时间，之后系统时钟回拨不能延长该快照。
宿主仍需可靠授时；各副本分发与一致性不由本地代数保证。更新无效或代数过时，旧快照保持不变。

无匹配规则返回拒绝（HTTP 403）；策略过期或内部状态不可用返回 503，不降级为旧授权。
更新影响后续检查，不回滚已授权的提交。SSE 反复检查资源权限，撤销后关闭；已进入客户端网络
缓冲的数据无法收回。新鲜的空策略就绪正常但拒绝所有用户；“就绪”不等于“有访问权限”。

整份策略摘要标识部署版本，授权中的 `policy_digest` 则绑定**命中的规则及策略身份**。
请求证据绑定完整 Schema、输入与预算的域分离摘要。因此只刷新到期时间或其他运行的规则，
不会破坏响应丢失后的原键重试。修改实际命中的提交规则会改变授权，旧键重试可能返回 409；
应在当前独立授权下按键找回原结果，不能绕过策略，也不能自动换新键重复执行。

## 证据与运维边界

原子 Admission 记录保留主体授权、固定的封闭证据 Schema、请求与规则摘要、预算限制。
取消操作继续保存策略和决策摘要，并未新增完整决策账本。读取和拒绝决策不写 Journal；
取消摘要本身不能还原调用者。按需要保留受保护的策略文件和访问审计上下文，禁止记录 Bearer
凭据、原始提交键或请求体。策略文件也可能包含敏感身份及资源标识，不能放到公开官网。

这一配置方案不是自动运行所有权存储、通用策略语言、外部 PDP、策略签名验证器或托管服务。
精确 Run／键授权须由可信控制面显式配置；租户运维角色须主动授予。监控续期失败和到期，
协调多副本更新；有历史 Admission 引用时必须保留证据 Schema。回滚应用时同时选择相匹配的
受信配置，不能关闭授权恢复可用性。本阶段无数据库迁移、无依赖变更；Worker／调度器管理独立推进。

## 验证入口

`cargo test -p stateknot-runtime --lib agent_service::policy --locked` 验证选择器、边界、
歧义拒绝、确定性证据、Schema、过期、状态故障和并发更新。
真实 PostgreSQL 16/17 HTTP 测试要求 `STATEKNOT_REQUIRE_POSTGRES_TESTS=1` 与独立测试库
`STATEKNOT_TEST_DATABASE_URL`；覆盖提交已落库但响应丢失、无关规则更新后的同一运行恢复、
证据落库、幂等取消、撤销 SSE、过期拒绝，并证明没有在 HTTP 内执行节点。CI 要求唯一的
`STATEKNOT_RESOURCE_POLICY_EVIDENCE` 标记，避免过滤器没有匹配测试却误报通过。
独立 Keycloak TLS 资格测试也接入本策略；测试账号不会公开部署。
