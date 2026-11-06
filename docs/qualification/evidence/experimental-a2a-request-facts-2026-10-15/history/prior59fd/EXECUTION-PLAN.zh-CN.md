# A2A request facts 严格验收计划

固定候选：`elapsed36-a2a-auth-facts-source`，923 文件，tree `59fd524f0e21460a9ffc8edf7de8cc6ad4d0f95d69384c6515200675364bbea6`。生产 `a2a_client.rs` SHA `46022f375430c2387bb73e7914229a55de62144067417053327e0c06e3397216`；仅修正新测试遗漏的 `DurationMillis` import。旧 `755ffa…` 未执行 snapshot 和所有原始回执保留。

`source-manifest.json` 为供应给 Cargo 的完整精确 923 文件快照；`cargo-input-manifest.json` 是保守输入分类，不能称编译器观测依赖闭包。源码 tar 的 923 成员已逐字节与 manifest 复核，存于缓存外。编译后另存 JSON artifact 的 source path、fresh=false、profile、可执行文件 SHA 及 dep-info 原文。

## 执行顺序与启动条件

Root 已明确确认 schema36 的长期 Native/backup/PITR 资源 gate 全部结束并授权启动。`root-start-authorization.json` 保存本次授权，runner 默认仅显示计划，须显式 `--start-authorized` 才执行。不注册自动任务或后台自启动。

1. 每次构建或测试前 `df -k` 确认至少 50 GiB 实际空间；任务缓存若超过 20 GiB 先评估。检查真实路径、Cargo CACHEDIR.TAG、Git 无跟踪、lsof 无打开文件及构建锁后，仅清理七个 workspace package；依赖缓存保留，不复制 target。
2. 原六门禁按以下次序串行：fmt、workspace 严格 Clippy、workspace all-targets/all-features tests、doctests、Rustdoc、固定 nightly check。全部 source 前后逐文件 hash；遇红保存回执并停止，任何必要修复另封新 tree，原回执不覆盖。
3. 六项全绿后，仅 clean `stateknot-integrations`，重新 `--no-run --message-format=json` 编译两个测试 binary。核对 exact source target path、fresh=false、profile 和可执行 SHA；保存 binary 到缓存外并保存完整 `--list`。两个 binary 必须来自同一冻结源码与原严格环境。
4. 使用保存的完整 `a2a_client_contract` binary 跑真实 loopback 网络全 suite，串行测试。旧测试、22-operation matrix 和新增 7 case 全部保留。完整 passed count 与 --list 对照，零失败、忽略、过滤；RPC replay 未新增测试，不授予该 profile 资格。
5. 静态与编译、网络全部完成后，使用同一 fresh `mcp_durable` binary，在两个原 PostgreSQL major 的全新独立 task DB 上逐次执行 mandatory full suite。先查询不存在再创建，不 drop 任何 DB、不复用原 fullsuite DB。每版要求 6 passed，0 failed/ignored/filtered，并核对原 MCP 2 marker 与新 A2A 2 marker 的 cases、次数和 durable/full-input 事实。先前无 DB 条件 entry 只表示函数入口，不计 Native 资格。

所有普通 Cargo 命令保留环境：`CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 RUSTFLAGS='-D warnings' RUSTDOCFLAGS='-D warnings'`。稳定缓存 `target/r1-oct15-proof-base`；nightly 独立 `target/r1-oct15-proof-base-nightly`。不修改 profile、pool、worker、lease、timeout 或 `RUST_MIN_STACK`。每个阶段单独目录，日志、失败回执、源码/可执行身份和最终交付均在 target 外。

## 原六门禁实际命令

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo test --workspace --doc --all-features --locked
cargo doc --workspace --all-features --no-deps --locked
cargo +nightly-2026-10-07 check --workspace --all-targets --all-features --locked
```

普通门禁清除 `STATEKNOT_TEST_DATABASE_URL` 和 `STATEKNOT_REQUIRE_POSTGRES_TESTS`。Native 单独设置这两个变量，后者必须为 `1`。最终 Native log 不记录带密码的 URL。

```sh
cargo test -p stateknot-integrations --test a2a_client_contract --test mcp_durable --all-features --locked --no-run --message-format=json
<saved-a2a_client_contract> --nocapture --test-threads=1
<saved-mcp_durable> --nocapture --test-threads=1
```

## 原 CI 中独立的协议、兼容和公开接口 gate

以下为实际 `.github/workflows/ci.yml` 与已固定脚本的门禁，完整本地六项不能替代外部 CI/不同平台结果；此计划不把未执行命令标绿。

- Core 公开类型和 wire 基线：`cargo test -p stateknot-core --test fixture_catalog --locked`、`--test public_type_inventory`、`--test public_enum_variants`、`--test canonical_execution_wires`；`cargo check -p stateknot-core --examples --locked`。这些是 Core inventory，不能写成新增 A2A getter 的独立 inventory。新 integration tests 的 public imports/getters 和 workspace facade 编译覆盖本次公开接口使用。
- 现有性质测试种子 `PROPTEST_RNG_SEED=20261008`，组为 canonical_values/value_properties/nested_json_properties/budget_composite_properties/graph_state_properties/graph_frame_contracts/graph_frame_calls/graph_frame_barriers。六项 workspace 测试包含这些 suite，但 CI 的固定种子结果独立记录。
- Fuzz 的 fmt/Clippy 用 `--manifest-path fuzz/Cargo.toml`；metadata `cargo metadata --format-version 1 --locked --no-deps`；Linux/macOS/Windows matrix 实际结果不能由本机 macOS 转授。
- Canonical time release overflow gate 是独立 profile：`RUSTFLAGS='-C overflow-checks=on' CARGO_TARGET_DIR=target/core-checked-release cargo test -p stateknot-core --test canonical_time --release --locked`。不擅自把它的 flags 混入普通构建或改变普通验收资源。
- 官方 A2A 服务协议：`bash conformance/a2a-server/run-1.0.sh`，然后 `python3 conformance/a2a-server/verify-results.py <run>/junitreport.xml <run>/summary.json`。脚本用 uv 0.11.25、固定 TCK commit `263b9cfaf16a554bdfb166a7ba5b67716e946349`、archive SHA `694c798e93fff30f650d44bdb3db0e1768b865a4f3ddbed64ec158db209bf5db`，先构建 `stateknot-integrations --example a2a_conformance_server --locked`。独立执行时 `STATEKNOT_A2A_CONFORMANCE_BIN` 必须明确指向本任务 target 的实际 example，results 指向缓存外，不能误用 root/target 的旧 binary。TCK memory backend、入站 server 和客户端 request facts/durable policies 各有不同证据范围。当前未启动该独立 TCK 下载/执行。
- 原 MCP conformance/profile 未改；不能将旧协议证据转授本次新 A2A outbound request-facts Native profile。

限定验收目标为完整 request snapshot 与实际 wire 一致、预配置目的地和 payload 拒绝零业务发送、实际 polled callback deadline/drop、既有 Anonymous/legacy provider、HTTP attested replay/current revoke、PostgreSQL durable Start-before-policy 和 Unknown/denial 恢复。facts 是 policy 输入，不表示 host 已配置策略，也不声称 CI、Supported、容量或并发资格。
