# MCP HTTP响应诊断实验限定记录 — 2026-10-16

本记录仅覆盖四类响应的 `Debug` 实现，绑定928文件source
`31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca`，
以及536项编译输入
`1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c`。
六项本机门禁通过；新增单个真实TCP测试family完整执行八个JSON/SSE分支。
另在PostgreSQL 16.15和17.11每版通过两个完整定向Native suite。
R1仍开放、RFC24/RFC25仍为Draft，不授予Supported或完整CI资格。

[机器记录](experimental-mcp-http-diagnostics-2026-10-16.json)包含完整哈希、实际命令、
binary profile、全部Native marker对象及范围声明。
[English](experimental-mcp-http-diagnostics-2026-10-16.md)。

## 响应契约

普通与pretty格式都只保留固定类型名、non-exhaustive标记和以下字段：

| 类型 | 保留字段 |
| --- | --- |
| `McpNotification` | `has_params` |
| `McpCompleteToolResult` | `content_count`、`has_structured_content`、`is_error` |
| `McpRemoteError` | 原i32 `code`、`has_data` |
| `McpToolCallResponse` | 固定 `outcome`（`complete`/`input_required`）、`notification_count` |

外层response不递归格式化result、continuation或notifications。远端method/message、
params/content、structured/raw value、data、state和Tool metadata不进入这四类摘要。
`StatelessMcpClientError::Remote`、`McpToolCall::Complete`通过其内层值继承限定摘要。
裸 `McpInputRequired` 与裸 `McpToolCall::InputRequired` 仍有原Tool metadata Debug行为，
不声明全部MCP值都payload-free。

显式raw getter仍返回不可信远端值。已有Clone、PartialEq、getter、显式resume、wire、
admission/资源上限和effect/retry行为保持。HTTP畸形complete write仍为 `Applied + Never`，
不采用stdio草案的 `Unknown` 映射；没有新增API、依赖、verifier或真实凭据认证。

## 本机实际执行

| 门禁 | 实际结果 |
| --- | --- |
| Rust1.88 format检查 | exit0 |
| 严格all-target/all-feature Clippy | exit0 |
| Workspace all-target/all-feature tests | 99suite；2099 passed、0 failed、6个原ignored、0 filtered |
| Workspace doc tests | 10suite；48 passed、0 failed/ignored/filtered |
| 严格Rustdoc | exit0 |
| 固定nightly-2026-10-07 check | exit0 |

保留原opt0、默认栈、debug assertions与overflow checks；每个门禁前后源码精确一致。
Workspace条件数据库项不提供Native证据。原六门禁receipt保留PID/start、command/
profile、log hash及source核对，但没有当时runner SHA或process environment allowlist；
本包补收完整runner源码不倒推为这些进程的环境attestation。Native原receipt则另含实际
runner hash与environment allowlist，两类证据强度分别说明。

新增family经真实HTTP JSON和分片SSE返回Complete的两个 `isError` 值、带通知的
InputRequired及显式resume、Remote error。普通/pretty两种格式都隐藏嵌套canary，包含
continuation Tool name；正控核对parsed raw Value及其序列化字节、已有Clone/PartialEq、
params/structured/data存在性、精确resume state/arguments/response keys、新ID与调用数。
外层response不泄漏InputRequired Tool metadata。

原Debug真实canary在第一个JSON Complete分支失败：exit101，0 passed/1 failed/
2 filtered，raw SHA `fde49495…`。最初修复候选Clippy亦实际exit101（测试fixture按值传
JSON ID），raw SHA `96e4fed5…`；两次原始失败与完整runner均保留在 `history/`。
最终v3只将fixture改为借用JSON ID，无新增lint allowance或生产变化，失败记录没有覆写。

## 独立定向Native执行

| PostgreSQL | 完整 `mcp_durable` suite | 完整 `mcp_reconciliation` suite | 完整JSON marker |
| --- | --- | --- | --- |
| 16.15 | 6 passed/0 failed/0 ignored/0 filtered | 10/0/0/0 | 7个精确对象 |
| 17.11 | 6 passed/0 failed/0 ignored/0 filtered | 10/0/0/0 | 7个精确对象 |

`mcp_durable`含A2A/MCP回归，reconciliation亦包含definition/wire非数据库项；双major
32次harness执行不是32个纯数据库用例。每个suite在本次精确source上以per-package
默认features（Cargo artifact为 `[]`）新鲜构建，opt0/debug0、assertions/overflow true，
随后串行、默认栈运行。完整suite list与实际Cargo artifact记录保留。两份binary哈希：
`2717d6db539ad1604ba541d7f2f2d397c758a0f99deaf148cc7fd2627c9048ff`、
`bc4f33032c7b0b207e1e5eac62e741e9445e52970981fb8cb818ca5a56f1c3c3`。

每版raw均有2个MCP authorization、2个A2A authorization、3个reconciliation对象；
verifier比较完整JSON对象，不只计数或前缀。旧 `schema:24` marker是profile常量，
不是当前数据库schema36。四次Native前后source均精确核对；两major新建数据库
`stateknot_r1_mcp_debug_20261016v3`均保留。完整实际runner SHA `234a01c8…`，
Native summary SHA `d0927746…`。此前bound-state在PostgreSQL 16/17上每版完整15项
串行suite结果仅属于其原source，不转授本tree，也不写成当前重跑；本次不授予完整
Store/Runtime Native或官方conformance资格。

## 完整性与边界

[证据manifest](evidence/experimental-mcp-http-diagnostics-2026-10-16/manifest.json)和
[seal](evidence/experimental-mcp-http-diagnostics-2026-10-16/seal.json)绑定全部raw日志、
完整runner、source引用/payload和离线verifier。复用18个已有source-only引用，仅存两份
新压缩源码payload；不复制旧完整archive、449文件包、binary或缓存。
源码重建还核对原357行测试byte prefix、error/network/parser/原unit suffix未改；
归一化证明用于范围核对，行为证据来自真实TCP执行。

```console
python3 docs/qualification/evidence/experimental-mcp-http-diagnostics-2026-10-16/verify.py --repository-root .
# 精确采用源码后，再核对workspace中536项输入：
python3 docs/qualification/evidence/experimental-mcp-http-diagnostics-2026-10-16/verify.py --repository-root . --workspace
```

不声明冻结928文件source等于当前main，也不将输入表声明为compiler read closure。
verifier只读日志/哈希，不执行Cargo、网络、Docker或数据库。Native结束时df可用
315708296 KiB，八个task cache逻辑大小10554584 KiB；当时proof-base的TAG/Git未跟踪、
lsof空、Cargo locks可取得已核对。逻辑大小不等于APFS可回收空间，不声明释放量；
凭据URL、可执行文件均不公开。

采用文档候选为docs-v3，三文档字节与v2完全相同，仅澄清receipt范围字段；v1/v2保留。
这是证据metadata修正，不是产品回归。原bound-state Added段、opt1跟踪和guide原示例保持。
作者不对自己撰写的源码/文档/证据提供独立审阅结论。

Parent另已定位base commit `8016b74…` CI的Windows checkout失败（Filename too long，
exit128，尚未执行Rust）；本记录不声明总体CI终态或Debug新tree远端CI资格。完整CI、
重跑官方MCP conformance、部署/容量、stdio owner、持久A2A backend、
真实凭据认证/长流撤销与完整R1验收继续分别推进，不转授旧源码执行结果。
