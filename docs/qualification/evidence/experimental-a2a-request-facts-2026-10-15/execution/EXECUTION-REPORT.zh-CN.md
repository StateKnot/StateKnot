# A2A request facts 执行结果

本轮已完成全部授权验收。固定候选为 923 文件、tree `250066800739a33390a316c7fbb40181335de67394cd9de6bb67ccb425226433`，生产 `a2a_client.rs` 始终为 `46022f375430c2387bb73e7914229a55de62144067417053327e0c06e3397216`。相对 schema36 最终 922 基线仅该生产文件和三个相关测试路径变化；Store、Runtime、Core、SQL 均逐字相同。完整四文件 patch 与对应 hashes 已存档。本任务没有修改 main 或提交 Git。

| 实际执行项 | 终态 |
| --- | --- |
| fmt、workspace all-targets/all-features 严格 Clippy | 通过 |
| workspace all-targets/all-features tests | 2073 passed，0 failed，6 个原有 ignored |
| workspace doctests | 45 passed，0 failed/ignored |
| workspace 严格 Rustdoc | 通过 |
| nightly-2026-10-07 workspace all-targets/all-features check | 通过 |
| 两个最终测试程序 fresh build | exact source path、fresh=false、复制前后 SHA、完整 --list 均核对 |
| 最终 a2a_client_contract 真实 loopback 全 suite | 26 passed，0 failed/ignored/filtered |
| mandatory PostgreSQL 16.15 mcp_durable 全 suite | 6 passed，0 failed/ignored/filtered；原 MCP 2 和新 A2A 2 markers 全核 |
| mandatory PostgreSQL 17.11 mcp_durable 全 suite | 6 passed，0 failed/ignored/filtered；同一程序 SHA，四份 markers 全核 |

普通 workspace 环境清除了数据库变量，189/264 等条件测试入口不代表执行 Native；真正的新持久化 profile 是 `native-v1` 中 mandatory 环境下的两个完整 6 case suite。workspace 的 6 个既有 ignored 没有被当作已执行或删除。

最终网络程序 SHA `792acfb788c19a8d387771f1a13c511d06ec6567d795375b9f928b0fcc57d8c2`；持久化程序 SHA `3b0b104b80456e70b1ebe3202cb608f49f648d764478e27a513525fb1d5be687`。实际 Cargo profile 为 opt-level 0、debuginfo 0，debug assertions 和 overflow checks 均启用。原严格 Rust 环境、worker/pool/lease/timeouts 保持，未增加栈或资源。

完整源码 tar SHA `5a882322c32b5ec165d1a37bf84f087f0c5afc8811b805c07cc4ac7a590e75e3`，所有 923 成员逐字复核。`source-manifest.json` 是精确供应源码快照；`cargo-input-manifest.json` 仅保守分类，不称完整编译器读依赖闭包。`compiler-observed-workspace-inputs.json` 与 raw dep-info 另记录最终两个程序及四个 workspace library 的 215 个实际编译器 source dependency inputs，全部绑定相同源码 hashes。依赖 registry、完整工具链文件或编译器所有文件读取不在该观察闭包声明内。采集器最初错误预期 Testkit 也为这两个程序的依赖，断言回执保留；按 Cargo 实际六个 artifacts 收敛，没有重编译、改源码或影响测试结果。

## 真实通过的限定行为

原 19 个网络 case 加新增 7 个 case 全部通过。两个 operation matrix 实际执行 REST/RPC 共 22 个 operation；callback facts 与真实 wire method、完整 URL/query、body 有无及完整字节相同。新增 case 验证 payload 允许后拒绝、同 provider 对预配置目的地允许而另一目的地拒绝零发送、actual-poll 后 deadline/drop、Anonymous/旧 metadata-only provider、HTTP attested replay 的原 attempt/URL/body 与当前撤销拒绝。Debug 未泄露新增 URL/body，credential 没有自动进入 body。

两个 Native 新 case 在实际 polled provider 中通过公开完整 PostgreSQL reader 读取 Executing、物理 attempt、原 Start journal/event 和完整原 Input，再逐字段核对实际 HTTP 释放 body。拒绝发生于 durable Start 已提交后，记录 PermissionDenied/NotStarted，业务发送 0、policy 1；同 handoff 恢复不重复 policy/send，也不增加 ledger revision。允许后丢响应为 Unknown，业务发送 1、policy 1；Recovered 不重发。原 MCP 与匿名用例、原断言及 markers 保留。

本轮不宣称 JSON-RPC replay 已实测，不将 HTTP replay 的整个 envelope 字节相等转授 RPC 的新 request id；没有运行官方 TCK、新外部 CI、跨平台 CI、真实远端认证基础设施、容量或并发验收。facts 提供 policy 输入，不能代替 host 配置 policy。既有 six-gate 中条件入口不能转授 mandatory Native 资格。

## 原红与最小修复链

1. 原 `755ffa…` 快照遗漏 DurationMillis import，是静态检查发现的未执行源码；保留原 seal，不伪造编译红或资格。
2. `preparation-v1` 的 `59fd…/gates-v1`：五个原有 loopback 测试 bind 返回 Os1 PermissionDenied，原 exit101/log 保留；同源码在允许 loopback 的环境重跑。
3. 同 `59fd…/gates-v2-loopback`：新 payload policy case 使用实际 `/a2a/remote-tenant/message:send`，原 matrix fixture 只认识无租户路径，server panic 后返回 502；25 个其余网络 case 通过。真实 red log SHA `7a714146a36a34cad074b63cd0995de7f7d76b02fb451fbff79a961e92f3c83d` 保留。
4. 仅给原 REST `message:send`、`message:stream` 两个分支增加本测试的精确 remote-tenant 路径；未知路径仍拒绝。没有移除 tenant、policy、零发送或其他断言。新 `250066…` 全部六门禁、fresh 程序、26 网络及双 PG 6 全绿。

## 文件、数据库和资源收尾

四个 stage 的 `summary.json`、全部 commands/env/exit/log hashes、数据库原不存在检查、实际 server/container identities、完整 markers 均在相应目录。两个独立 task DB 名为 `stateknot_r1_a2a_auth_facts_20261015v1`，分别位于原 PG16/17 container；原 DB 未改动，不删除任何数据库。

`terminal-resource-source-check.json` 记录所有 runner session 已 wait 到 exit0、923 文件前后 hashes 相同、本任务匹配进程为空、两个缓存 lsof 为空且构建锁可取得。最终可用空间 330,568,151,040 bytes（约 307.87 GiB）；`target/r1-oct15-proof-base` 3,007,868 KiB、nightly 427,292 KiB，均作为可复用的 inactive task cache 保留。目录 du 是逻辑用量，未作 APFS 释放空间声明；收尾不删除缓存。日志、程序副本、源码 tar、报告均位于缓存外。
