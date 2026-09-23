// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

import type { Locale } from "./i18n";

// A single ordered status source keeps the English and Chinese claims aligned.
const statusEntries = [
  {
    status: "implemented",
    title: {
      en: "Bounded host qualification evidence",
      "zh-CN": "有界宿主验证证据",
    },
    body: {
      en: "The public-alpha stateknot-testkit owns monotonic phases, bounded HDR distributions, checked safety counters, a closed fault matrix and canonical integrity envelopes. A reduced PostgreSQL 16/17 profile exercises real host admission, SSE, operations, dependency recovery and rolling replacement, but is always non-release evidence; reference load, soak, failover and provenance remain open.",
      "zh-CN":
        "公共 Alpha stateknot-testkit 管理单调计时阶段、有界 HDR 延迟分布、受检安全计数、封闭故障矩阵与规范化完整性封装。PostgreSQL 16/17 缩减配置走真实宿主准入、SSE、运维读取、依赖恢复和滚动替换，但始终不是发布证据；参考负载、浸泡测试、故障切换和来源证明仍待完成。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Protected read-only host operations",
      "zh-CN": "受保护的只读宿主运维接口",
    },
    body: {
      en: "Independent owned loopback listener, explicit InspectHost scope/permission and a separate expiring exact-caller policy. Sanitized status/counters remain observable during business outages; authentication, requests, connections and shutdown are bounded. PostgreSQL 16/17 and dedicated real TLS identity qualification; no anonymous probes, administrative writes or capacity/SLO claim.",
      "zh-CN":
        "独立接管 loopback 监听器，强制 InspectHost scope/权限与独立的过期调用方策略。业务故障时仍可授权查看脱敏状态和计数，身份验证、请求、连接及停机均有界。PostgreSQL 16/17 与专属真实 TLS 身份验证；无匿名探针、管理写操作或容量/SLO 声明。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Co-located Agent host lifecycle",
      "zh-CN": "同进程 Agent 宿主生命周期",
    },
    body: {
      en: "Owns actual ingress, Worker and maintenance bindings with exclusive ingress claim, ordered startup, per-request sibling readiness gates and HTTP → Worker → maintenance joined drain. PostgreSQL 16/17 and real TLS identity composition qualification; cross-process rollout and measured capacity SLOs remain open.",
      "zh-CN":
        "统一接管实际接入、Worker 和维护绑定，提供独占认领、有序启动、逐请求兄弟角色门禁及 HTTP → Worker → 维护停机回收。PostgreSQL 16/17 与真实 TLS 身份组合验证；跨进程滚动上线和实测容量 SLO 仍待完成。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "HTTP-free typed Agent runner",
      "zh-CN": "无 HTTP 的强类型 Agent Runner",
    },
    body: {
      en: "One bounded typed run call over the real durable service, tenant Worker and maintenance roles. Caller-retained submission keys, PostgreSQL admission, authorization, timeout-without-cancellation, same-key recovery, terminal provenance/accounting validation, fail-stop sibling supervision and joined shutdown remain mandatory. AgentHost is the production network-service migration path.",
      "zh-CN":
        "通过一次有界强类型 Run 调用使用真实可恢复 Service、Tenant Worker 与 Maintenance。调用方持有 Submission Key，强制 PostgreSQL 准入、鉴权、超时不取消、同 Key 恢复、终态 Provenance/Accounting 校验、兄弟角色 Fail-stop 监督和 Join 关闭；生产网络服务继续迁移到 AgentHost。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Independent Agent maintenance lifecycle",
      "zh-CN": "独立 Agent 维护任务生命周期",
    },
    body: {
      en: "Explicit tenant rotation over deadline cancellation, child cancellation/settlement, Join publication and failure closure. Retained failed-page cursors, actual readiness, finite deadlines and joined shutdown; PostgreSQL 16/17 durable effects and SIGKILL recovery. No automatic retention or full multi-role capacity qualification.",
      "zh-CN":
        "按明确授权的租户轮询截止时间取消、子 Run 取消传播与结算、Join 发布及失败收敛。保留失败页游标，检查真实依赖，限制时限并回收停机任务；PostgreSQL 16/17 验证持久化效果及 SIGKILL 恢复。不自动清理历史数据，完整多角色容量验证仍待进行。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Independent durable scheduling Worker lifecycle",
      "zh-CN": "独立持久执行 Worker 生命周期",
    },
    body: {
      en: "Concrete tenant/fair schedulers with fixed slots, actual and host dependency readiness, finite pacing and deadlines, and joined drain including nested Graph nodes. PostgreSQL 16/17 qualifies fresh-process terminal replay and fair-slot continuity. Maintenance jobs and full multi-role production qualification remain separate.",
      "zh-CN":
        "管理具体租户或公平调度器，提供固定槽位、实际依赖与宿主就绪检查、有限退避和期限，以及包含 Graph 节点的停机回收。PostgreSQL 16/17 验证新进程终态重放与公平序号连续性；维护任务及完整多角色生产部署验证仍独立进行。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Declarative Agent resource authorization",
      "zh-CN": "声明式 Agent 资源授权",
    },
    body: {
      en: "Exact tenant/identity/Agent/schema/run selectors, explicit tenant operators, restrictive budgets, digest-pinned policy artifacts and deterministic admission evidence. Expiring CAS refresh preserves recovery across unrelated ACL changes. Real PostgreSQL 16/17 and Keycloak qualification; no inferred ownership or durable read-audit ledger.",
      "zh-CN":
        "精确租户、身份、Agent、Schema 与运行选择器，显式租户运维权限、预算限制、固定摘要的策略文件及确定性 Admission 证据。支持有时效的原子更新，无关权限变化不破坏提交恢复。真实 PostgreSQL 16/17 与 Keycloak 验证；不推断所有权，也不新增读取审计账本。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Real identity integration and expiring tenant policy",
      "zh-CN": "真实身份接入与过期租户策略",
    },
    body: {
      en: "Fixed-HTTPS OAuth introspection, exact claims and permission intersection, rotating secrets, atomic default-deny tenant mappings and expiry. Real Keycloak qualifies rotation, revocation, cross-tenant refusal, readiness recovery and SSE cleanup; resource policy stays mandatory. JWT/JWKS and full production deployment remain open.",
      "zh-CN":
        "固定 HTTPS OAuth 令牌内省、精确声明与权限交集、密钥轮换、默认拒绝的租户映射原子更新及过期检查。真实 Keycloak 验证轮换、撤销、跨租户拒绝、就绪恢复和 SSE 回收；资源授权仍强制，JWT/JWKS 与完整生产部署验收未完成。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Owned HTTP ingress lifecycle",
      "zh-CN": "HTTP 接入运行角色与优雅停机",
    },
    body: {
      en: "Loopback listener ownership, actual schema/executable and mandatory host readiness, fresh fail-closed admission, bounded connections and joined graceful/forced drain. Real PostgreSQL 16/17 fault tests; no implicit Worker, public health route or stable release claim.",
      "zh-CN":
        "接管本机监听器，校验实际 Schema、可执行注册表及强制宿主依赖就绪；证据失效时拒绝新业务，限制连接资源并回收优雅排空或强制关闭的任务。通过 PostgreSQL 16/17 故障测试，不自动启动 Worker、不暴露匿名健康入口、不宣称稳定版。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Resumable Agent activity SSE",
      "zh-CN": "Agent 活动 SSE 与断线续传",
    },
    body: {
      en: "Opt-in authenticated activity notifications with exact PostgreSQL cursors, fresh-process suffix recovery, independent snapshots, repeated authorization, finite queue/lifetime limits and quarantine-only updates. Not token streaming or historical snapshot reconstruction.",
      "zh-CN":
        "显式启用的认证活动通知，精确 PostgreSQL 游标、新进程后缀恢复、独立状态快照、持续授权检查、有界队列和连接时限，以及仅隔离状态变化通知。不等于 Token 流或完整历史状态重建。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Agent HTTP v1 JSON ingress",
      "zh-CN": "Agent HTTP v1 JSON 接入",
    },
    body: {
      en: "Mandatory credential verification and resource policy, durable submission/read/key lookup, two-phase cancellation, finite limits and shutdown, with real HTTP lost-response recovery on PostgreSQL 16/17. No inline dispatch or stable-release claim.",
      "zh-CN":
        "强制凭据验证与资源授权，持久化提交、读取、按键找回和两阶段取消，有限资源上限与停机控制，并在 PostgreSQL 16/17 上验证真实 HTTP 响应丢失恢复。不内联执行，也不宣称稳定版。",
    },
  },
  {
    status: "implemented",
    title: { en: "Core domain contracts", "zh-CN": "Core 领域合约" },
    body: {
      en: "Bounded canonical types, four MSRV-compiled public contract examples with a locked runtime-neutral dependency boundary, root-graph validation, and deterministic route/reducer/successor barrier planning. RFC-0001 remains Draft.",
      "zh-CN":
        "有界 Canonical 类型、四个通过 MSRV 编译并锁定 Runtime-neutral 依赖边界的公共合约示例，以及 Root Graph 编译校验和确定性 Route/Reducer/Successor Barrier Planning。RFC-0001 仍为 Draft。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "PostgreSQL durability slice",
      "zh-CN": "PostgreSQL 持久化切片",
    },
    body: {
      en: "PostgreSQL 16/17 migrations, journal, checkpoints, leases, ledgers, barriers, waits, outbox, quarantine, delayed wakeups, pinned graphs, and durable fairness reservations.",
      "zh-CN":
        "PostgreSQL 16/17 迁移、Journal、Checkpoint、Lease、Ledger、Barrier、Wait、Outbox、Quarantine、延迟唤醒、Pinned Graph 与持久化 Fairness Reservation。",
    },
  },
  {
    status: "implemented",
    title: { en: "Atomic Agent admission", "zh-CN": "原子 Agent Admission" },
    body: {
      en: "Immutable authenticated intent, deterministic finite budgets, database-clock admission, first event, initial checkpoint, scheduler visibility, exact retries, and fully verified loads.",
      "zh-CN":
        "不可变已认证 Intent、确定性有限 Budget、数据库时钟 Admission、首 Event、初始 Checkpoint、Scheduler Visibility、精确 Retry 与完整校验 Load。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Durable Agent runs and results",
      "zh-CN": "可恢复 Agent Run 与 Result",
    },
    body: {
      en: "Tenant-scoped durable ingress keys, fresh-candidate retry convergence, conflict detection, verified public lifecycle snapshots, and success/failure/cancellation outcomes.",
      "zh-CN":
        "Tenant-scoped 持久化 Ingress Key、Fresh-candidate Retry 收敛、Conflict Detection、经过验证的公开 Lifecycle Snapshot 与 Success/Failure/Cancellation Outcome。",
    },
  },
  {
    status: "implemented",
    title: { en: "Durable Graph Driver", "zh-CN": "可恢复 Graph Driver" },
    body: {
      en: "Offline executable schema/reducer/node closure, full checkpoint-state and bounded noninitial replay validation, durable-before-dispatch execution, canonical journal-isolated sibling batches, near-expiry lease refresh, monotonic renewal, durable cancellation polling, Continue barriers, and typed lifecycle handoffs.",
      "zh-CN":
        "离线可执行 Schema/Reducer/Node 闭包、完整 Checkpoint State 与有界非初始 Replay 验证、Durable-before-dispatch 执行、Canonical Journal-isolated Sibling Batch、Near-expiry Lease Refresh、单调 Renewal、持久化取消请求轮询、Continue Barrier 与类型化 Lifecycle Handoff。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Lifecycle and durable Agent Loop",
      "zh-CN": "Lifecycle 与可恢复 Agent Loop",
    },
    body: {
      en: "Atomic Wait/success/failure/cancellation handoff commits, trusted lifecycle evidence validation, exact lost-ack recovery, bounded exact-fence cleanup, and one tenant-scoped scheduler quantum.",
      "zh-CN":
        "原子 Wait/Success/Failure/Cancellation Handoff Commit、可信 Lifecycle Evidence 校验、精确 Lost-ACK Recovery、有界 Exact-fence Cleanup 与一次 Tenant-scoped Scheduler Quantum。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Durable model/tool execution",
      "zh-CN": "可恢复 Model/Tool 执行",
    },
    body: {
      en: "Exact immutable provider registries, trusted budget admission, durable-before-dispatch attempts, unary and durably-sunk streaming models, tool ambiguity, bounded provider reconciliation, and no-dispatch terminal recovery.",
      "zh-CN":
        "精确不可变 Provider Registry、可信 Budget Admission、Durable-before-dispatch Attempt、Unary 与 Durably-sunk Streaming Model、Tool Ambiguity、有界 Provider Reconciliation 和 No-dispatch Terminal Recovery。",
    },
  },
  {
    status: "implemented",
    title: { en: "Cross-tenant fair scheduling", "zh-CN": "跨租户公平调度" },
    body: {
      en: "Replica-global smooth weighted reservations, exact cycle shares, explicit reservation-count starvation bounds, concurrency-safe lost-ACK recovery, and bounded retention.",
      "zh-CN":
        "Replica-global Smooth Weighted Reservation、精确 Cycle Share、显式 Reservation-count Starvation Bound、Concurrency-safe Lost-ACK Recovery 与有界 Retention。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Typed Agent and first-party model adapters",
      "zh-CN": "强类型 Agent 与第一方 Model Adapter",
    },
    body: {
      en: "Generated canonical digest-pinned input/output schemas, offline provider-profile binding, bounded typed codecs, and OpenAI Responses plus Anthropic Messages unary/SSE adapters with explicit transport controls.",
      "zh-CN":
        "自动生成并 Canonical Digest-pinned 的 Input/Output Schema、离线 Provider-profile Binding、有界类型化 Codec，以及带显式 Transport Control 的 OpenAI Responses 与 Anthropic Messages Unary/SSE Adapter。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Provider-native Agent graph",
      "zh-CN": "Provider-native Agent Graph",
    },
    body: {
      en: "A digest-pinned prebuilt multi-turn model/tool graph with sequential or bounded parallel read-only Tools, serialized write barriers, provider-native transcript recovery, bounded durable structured-output repair, local policy evidence, exact accounting, automatic Unknown-to-Pending reconciliation without repeating the business call, and two-phase cancellation confirmation.",
      "zh-CN":
        "Digest-pinned 预置多轮 Model/Tool Graph，支持串行或有界 Parallel Read-only Tool、串行 Write Barrier、Provider-native Transcript Recovery、有界可恢复 Structured-output Repair、本地 Policy Evidence、精确 Accounting、不重复 Business Call 的 Unknown-to-Pending 自动 Reconciliation 与两阶段 Cancellation Confirmation。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "AgentService v1 embedding boundary",
      "zh-CN": "AgentService v1 嵌入式边界",
    },
    body: {
      en: "Exact-version deployment binding, authorization before deployment/run/key existence disclosure, tenant-scoped submission recovery, verified reads, and caller-retained two-phase cancellation identities.",
      "zh-CN":
        "精确版本 Deployment Binding、授权先于 Deployment/Run/Key 存在性披露、Tenant-scoped Submission Recovery、Verified Read 与由调用方保留 Identity 的两阶段取消。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Strict MCP Remote Tool profile",
      "zh-CN": "严格 MCP Remote Tool Profile",
    },
    body: {
      en: "One MCP 2026-07-28 client-side Tool binding with modern stateless discovery, complete JSON transport, exact local schema/server pins, attempt-scoped authorization, durable-before-dispatch PostgreSQL state, no-redispatch ambiguity, and authoritative reconciliation.",
      "zh-CN":
        "一个 MCP 2026-07-28 Client-side Tool Binding，支持 Modern Stateless Discovery、Complete JSON Transport、精确 Local Schema/Server Pin、Attempt-scoped Authorization、Durable-before-dispatch PostgreSQL State、No-redispatch Ambiguity 与权威 Reconciliation。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "General stateless MCP Tool client",
      "zh-CN": "通用 Stateless MCP Tool Client",
    },
    body: {
      en: "Bounded discovery/pagination, JSON and request-scoped SSE, standard and nested custom headers, invalid-Tool isolation, no network schema dereference, exact MRTR, and a frozen official client gate.",
      "zh-CN":
        "有界 Discovery/Pagination、JSON 与 Request-scoped SSE、标准与嵌套 Custom Header、无效 Tool 隔离、禁止网络 Schema Dereference、精确 MRTR 与冻结官方 Client 门禁。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "MCP OAuth client authorization",
      "zh-CN": "MCP OAuth Client Authorization",
    },
    body: {
      en: "Challenge-driven protected-resource and authorization-server discovery, pre-registration/CIMD/DCR, PKCE, exact issuer and callback validation, bounded scope/replay, durable store interfaces, and all 25 scored OAuth scenarios.",
      "zh-CN":
        "Challenge-driven Protected-resource/Authorization-server Discovery、Pre-registration/CIMD/DCR、PKCE、精确 Issuer/Callback 校验、有界 Scope/Replay、Durable Store Interface，以及全部 25 个计分 OAuth 场景。",
    },
  },
  {
    status: "implemented",
    title: { en: "MCP Server profile", "zh-CN": "MCP Server Profile" },
    body: {
      en: "Strict stateless HTTP plus StateKnot-owned immutable Tools, Resources, Resource Templates, Prompts, optional Completion, and MRTR with authorization-first policy; all 37 scored Server scenarios pass with zero failures or warnings.",
      "zh-CN":
        "严格 Stateless HTTP，加 StateKnot-owned Immutable Tools、Resources、Resource Templates、Prompts、Optional Completion 与 MRTR，并执行 Authorization-first Policy；全部 37 个计分 Server 场景以 0 Failure、0 Warning 通过。",
    },
  },
  {
    status: "implemented",
    title: { en: "MCP Skills profiles", "zh-CN": "MCP Skills Profile" },
    body: {
      en: "Final SEP-2640 static Server and Client/Host profiles: immutable exact-byte manifests, authorization-first disclosure and bounded verification. Schema 26 persists exact run-scoped activation approvals and database-clock acting windows with retry-stable IDs, restart resume, nested parent locking and immutable revocation. Exact-version Tool adapters serialize fresh payload-redacted receipt commits against revocation before provider I/O; later revocation does not recall an authorization already committed.",
      "zh-CN":
        "Final SEP-2640 静态 Server 与 Client/Host Profile：不可变精确字节 Manifest、Authorization-first Disclosure 与有界校验。Schema 26 持久化精确 Run-scoped Activation Approval 与数据库时钟 Acting Window，支持稳定重试 ID、重启恢复、Nested Parent Lock 与不可变撤销。精确版本 Tool Adapter 会在 Provider I/O 前让新的不含 Payload Receipt 与撤销串行化；后续撤销不会追回已经提交的授权。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "A2A 1.0 Client and durable remote agent",
      "zh-CN": "A2A 1.0 Client 与可恢复 Remote Agent",
    },
    body: {
      en: "All eleven HTTP+JSON and JSON-RPC operations, both bounded SSE surfaces, exact card/interface pins, bounded Bearer/OAuth2/OpenID security, local schemas, explicit delivery semantics, and operator-attested context/history or message-ID recovery for PostgreSQL-backed Unknown attempts.",
      "zh-CN":
        "全部 11 个 HTTP+JSON/JSON-RPC Operation、两个有界 SSE Surface、精确 Card/Interface Pin、有界 Bearer/OAuth2/OpenID Security、本地 Schema、显式 Delivery Semantics，以及面向 PostgreSQL-backed Unknown Attempt、由运维方背书的 Context/History 或 Message-ID Recovery。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Durable A2A artifact storage",
      "zh-CN": "A2A Artifact 持久化存储",
    },
    body: {
      en: "Direct no-resend polling of accepted tasks, migration 18's immutable PostgreSQL registry, private S3-compatible conditional publication, authorization-before-lookup, and complete length plus SHA-256 verification.",
      "zh-CN":
        "直接且 No-resend 地轮询已接纳 Task、Migration 18 不可变 PostgreSQL Registry、私有 S3-compatible Conditional Publication、Authorization-before-lookup，以及完整 Length/SHA-256 校验。",
    },
  },
  {
    status: "implemented",
    title: { en: "A2A 1.0 Server profile", "zh-CN": "A2A 1.0 Server Profile" },
    body: {
      en: "StateKnot-owned bounded Agent Card/message/task/artifact/push contracts behind strict HTTP+JSON and JSON-RPC/SSE policy; 177 official TCK cases pass, 88 declared cases skip, and none fail, error, or xfail.",
      "zh-CN":
        "StateKnot-owned 有界 Agent Card/Message/Task/Artifact/Push 合约位于严格 HTTP+JSON 与 JSON-RPC/SSE Policy 后；官方 TCK 177 Pass、88 声明 Skip、0 Failure/Error/Xfail。",
    },
  },
  {
    status: "implemented",
    title: {
      en: "Shared-state subgraphs and bounded loops",
      "zh-CN": "共享状态子图与有界循环",
    },
    body: {
      en: "Static nesting, exact schema/reducer pins, scoped ordered node/route identities, explicit return/exhaustion, pending-result and wait recovery, and pre-dispatch global step-limit supervision; no independent child-run lifecycle.",
      "zh-CN":
        "静态嵌套、精确 Schema/Reducer Pin、隔离且有序的节点/路线身份、显式返回与耗尽、Pending Result 与 Wait 恢复，以及派发前全图步数上限处理；不包含独立子 Run 生命周期。",
    },
  },
  {
    status: "active",
    title: {
      en: "Advanced execution and service boundary",
      "zh-CN": "高级执行与 Service Boundary",
    },
    body: {
      en: "Isolated child ownership/admission, cumulative reservations, Join suspension/publication/unique consumption, cancellation, deadlines and failure closure are implemented. PostgreSQL 16/17 CI force-kills fresh processes at eight successful Join boundaries and nine deadline cancel-and-join boundaries. A six-cell ambiguous-COMMIT matrix covers Join registration/publication and deadline cancellation; five separate two-cell matrices prove child admission, cancellation delivery with real Wait cleanup, child settlement, parent terminal finalization with exact direct-plus-child accounting, and atomic Join-result consumption with non-initial graph replay. Isolated logical pg_dump/pg_restore and physical base-backup/WAL named-PITR drills verify one consumed child-Join recovery path. Full disaster recovery, failover, provider effects, same-run nested namespaces, stable network transport and capacity qualification remain open.",
      "zh-CN":
        "已实现独立子归属/准入、累计预算预留、Join 挂起/发布/唯一消费、取消、Deadline 与失败关闭。PostgreSQL 16/17 CI 在成功 Join 的八个边界及 Deadline Cancel-and-join 的九个边界强制终止新进程；六格歧义 COMMIT 矩阵验证 Join 注册/发布和 Deadline 取消，五个独立两格矩阵分别验证子准入、含真实 Wait 清理的取消投递、子结算、按直接与子级用量精确记账的父 Run 终态收口，以及含非初始图重放的 Join 结果原子消费。隔离的 pg_dump/pg_restore 逻辑恢复与物理基础备份/WAL 命名恢复点演练验证一条已消费子 Join 路径。完整灾备、故障切换、Provider 效果、同 Run 嵌套命名空间、稳定网络传输及容量验证仍未完成。",
    },
  },
  {
    status: "planned",
    title: {
      en: "Stable network Agent API",
      "zh-CN": "稳定 Network Agent API",
    },
    body: {
      en: "The embedding facade and bounded authenticated JSON HTTP profile are available in the exact 0.1.0-alpha.1 public preview, but no stable HTTP/SSE API or production support claim exists yet. Preview compatibility follows the published versioning policy.",
      "zh-CN":
        "嵌入式服务与经过认证的有界 JSON HTTP 接入已随精确版本 0.1.0-alpha.1 公共预览发布，但稳定 HTTP/SSE API 与生产支持声明仍不存在；预览兼容性遵循已发布的版本策略。",
    },
  },
  {
    status: "planned",
    title: {
      en: "MCP client extensions and A2A qualification",
      "zh-CN": "MCP Client Extension 与 A2A Qualification",
    },
    body: {
      en: "Dynamic MCP Skills manifests, Skill-byte disk materialization and automatic discovery-to-Agent composition; MCP Tasks and other client extensions; stable SDK-tier evidence; A2A Client live-peer/conformance and recovery-attestation qualification; and A2A gRPC remain unshipped. Activation/window metadata and per-operation authorization receipts are durable.",
      "zh-CN":
        "Dynamic MCP Skills Manifest、Skill Byte 的 Disk Materialization 与自动 Discovery-to-Agent 组合，MCP Tasks 和其他 Client Extension，Stable SDK-tier 证据，A2A Client Live-peer/Conformance 与 Recovery-attestation Qualification，以及 A2A gRPC 尚未交付。Activation/Window Metadata 与逐次授权凭证均已持久化。",
    },
  },
  {
    status: "planned",
    title: {
      en: "Production server and operations",
      "zh-CN": "生产 Server 与运维",
    },
    body: {
      en: "Full HTTP/SSE role deployment, broader identity/resource-policy production qualification, observability, role isolation, general retention, failover, restore, OCI delivery, and release qualification remain ahead.",
      "zh-CN":
        "完整 HTTP/SSE 角色部署、更广身份与资源策略生产验收、可观测性、角色隔离、通用保留策略、故障切换、恢复、OCI 交付与发布验收尚未完成。",
    },
  },
] as const;

const labels = {
  implemented: { en: "Implemented", "zh-CN": "已实现" },
  active: { en: "Active work", "zh-CN": "开发中" },
  planned: { en: "Not shipped", "zh-CN": "尚未发布" },
} as const;

export const capabilitiesForLocale = (locale: Locale) =>
  statusEntries.map(({ status, title, body }) => ({
    state: labels[status][locale],
    className: `status-${status}`,
    title: title[locale],
    body: body[locale],
  }));
