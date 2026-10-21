# A2A durable authorization 与目的地策略：第二份执行前快照

本目录是后续源码增量，未覆盖 `a2a-auth-facts-network-tests/` 的第一份 raw evidence；已重新验证旧 seal 中全部文件仍匹配。当前独立 source 为 923 文件 / tree `755ffa939c9bf6650c8e55b9e6289cf68c6e1c26d8516581f9c68e6a339c7eb4`，完整 before/after manifests 已保存。相对上一快照只改三个测试文件，生产 `a2a_client.rs` 保持 root 的 `46022f375430c2387bb73e7914229a55de62144067417053327e0c06e3397216`，main/922 没有修改。

## PostgreSQL 用例的待执行断言

`mcp_durable.rs` 保留原匿名 A2A 测试名，把其原流程移入 `qualify_a2a_send(false, false)`。两个新认证用例使用同一默认 executor、相同 4 worker threads、原 lease、pool 1..8、acquire 30s、transaction 5s/20s 和原 paused lost-response loopback fixture：

- `a2a_authorized_send_reads_the_durable_start_and_recovery_does_not_send_again`：provider 回调内使用收到的真实 attempt tuple 调用公开 `PostgresStore::load_tool_invocation` 完整读取当前 Executing，再用 exact-revision owning reader 取得原 Start event。核对 original prepared 完整 intent/input、物理 attempt、准确前驱/下一 revision、预先保留的 Start EventId、journal head 和完整 worker fence。随后用独立 protocol literal 核对完整发送 message/config，data 必须是这份已完整认证的原 Input，而非仅凭 tuple/schema。固定 URL/method、scope、body、Debug 和无自动 token body 都有断言。允许后实际 wire 与 callback body 逐字相同，完整 URL 从真实 Host+target 比较；丢响应保留 Unknown/ReconcileFirst，原 handoff 重放为 Recovered，仍一 policy、一 business send、ledger head 不变。
- `a2a_authorized_denial_commits_once_without_business_send_or_policy_repeat`：同样在拒绝前完成上述真实 Store 读取和 payload 绑定。拒绝后必须已有 Start、Failed/NotStarted、PermissionDenied/Never，terminal event 和 executing 前驱准确；零 business send/零捕获请求。同 handoff 再执行必须 Recovered、ledger head 不变、policy 不重复。Runtime outcome 的 `Dispatched` 名称没有被当成 HTTP 已发送的证据。

原匿名的六个断言中五个归一空白后精确保留；Unknown 那个仅把同一完整 load 赋给 `unknown` 后做同一状态断言，以供新增认证 head 检查。`preservation-checks.json` 另证原 MCP server、shared read/clock、MCP wrappers/policy/qualification、schema/descriptor/Run-start/test_store 及原两个 MCP marker 行逐字不变。A2A card builder 只为新认证 profile 增加 security 元数据；匿名配置与原流程不变。

新证据 marker 为 `STATEKNOT_A2A_AUTHORIZATION_EVIDENCE`，独立的 `denial` / `lost_response` case。**这些 marker 目前只是源码中的输出语句，没有实际回执。** 编译后必须设置原 mandatory PostgreSQL 环境并在两个 major 的独立测试 DB 上实际运行、核对两 case 和结果，才具有该限定 profile 的证据；不能用旧 full integrations suite 或无数据库时的条件 entry 代替。

## 共享 provider 的真实目的地拒绝

网络模块现在有七个新 public 测试。新增 `a_shared_provider_refuses_a_destination_outside_its_preconfigured_url` 在 HTTP+JSON / JSON-RPC 中分别让同一 Arc provider 面对两台真实 fixture：allowed URL 来自 fixture constructor 已有配置，早于任何 callback；先允许 A，随后以相同 body 访问 B 必须 PermissionDenied、未 dispatch、B 零 business 请求。没有把第一次 callback echo 当 pin。harness 只新增保存已配置 interface URL 的字段和初始化（另有 import 格式化），原响应/矩阵/断言不变。

第一份快照已有的 payload、actual-poll deadline/drop、legacy/Anonymous 和 HTTP attested replay/revocation 测试继续保留。完整矩阵仍为两 binding × 十一实际 operation 的覆盖意图。durable 两个新增 case 和 attested replay fixture均明确只覆盖 HTTP+JSON，没有 JSON-RPC replay 资格声明。

## 执行状态

仅对两个父测试文件及子模块执行 pinned 1.88.0 rustfmt parser/format/check，exit 0；没有构建、Clippy、网络测试、数据库连接、mandatory PG16/PG17 或行为 red/green 执行。root 为固定 922 Native 原资源验收而暂停编译的要求仍生效；本 agent 没有使用构建 cache 或启动数据库任务。所有资格仍待最终冻结源码的真实严门禁。

`test-delta.patch` 和 `candidate/` 是最终本次测试输入；`mcp-durable-v1.patch` 仅保留较早静态草稿，不能代替最终 manifest。
