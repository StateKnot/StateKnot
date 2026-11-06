# PostgreSQL CI test profile 候选独立审查

固定基线 `686c42739e9f128db9d90fcb4e9d1aefd36942d8`。这是一个 reviewer 的 Standards/Spec 两轴审查；没有编译、网络或数据库执行，也没有采用候选。

## Standards

0 actionable findings。实际 patch 仅新增 PostgreSQL matrix job 的 9 行（两行注释、env 与六个变量）；逐字重建候选 SHA `6bead2659391392d43076624c56473b5cfae366d63efc5d985559009ed76336a`。符合 CONTRIBUTING 的小范围、可独立回退要求，没有新增模块、依赖或无关抽象。

## Spec

0 actionable findings。原 job 的 53 steps / 31 run、PG16/17 镜像、30 分钟 timeout、per-package 默认 feature 命令、所有 marker 与 fixture/stack/lease/deadline/quantum 保持原字节；其他 jobs 完全未改。该 job 及实际调用的 identity runner 使用 cargo test，未加入 --release/--profile/--all-features。根 Cargo.toml 与 pinned 1.88.0 未改。

本机 pinned Cargo 1.88.0 官方随附文档确认 test 继承 dev，环境变量可覆盖 profile；opt-level=1 是基本优化。显式 debug-assertions=true 保留 cfg(debug_assertions)/debug_assert!，overflow-checks=true 保留整数溢出 panic。debug=0 只控制调试信息，incremental=0 禁用增量编译；未指定 panic/LTO/codegen 等新配置，build scripts/proc macros 仍按原 build-override 规则，不能称全部 artifact 都是 opt1。DEV_DEBUG=0 的作用域限这个 job，未改变发布 Cargo 配置或其他 dev/release job。

资格边界：优化可能增加冷编译时间，debug=0 减少调试符号；旧缓存/二进制不能转授新 profile。当前本机实验为 workspace feature union，只能证明其自身配置，不能替代 CI per-package 默认 features、Ubuntu 或完整 30 分钟原 job。原 CI job 还有逻辑 restore、额外 facade/identity 等步骤；本轮四个原 Native suites 加单列 facts 并未执行完整 job。候选审查通过仍须原 CI 全链实际终态才能授予预算资格，不能增加 timeout 或分割原预算。

精确来源路径和 SHA、patch/proposal hash、重建与范围检查在同目录 review-receipt.json。当前双版实验尚有 PG17 Runtime 在执行，本审查不提前将其记绿。
