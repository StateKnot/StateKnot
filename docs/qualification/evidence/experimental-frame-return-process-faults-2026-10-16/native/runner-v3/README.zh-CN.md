# Return + middle corruption Native helpers v3（未执行）

这是独立新版本。原 singlewalk 042/d18 helpers、Return v1/v2 scripts、源码和红日志保持原件。v3 默认读取 `../graph-frame-return-process-fault-qualification-v3/scope.json`，核同一候选 932 源文件和 541 保守编译输入。Store 入口恢复原 middle-component case；Runtime 入口为新增 Return process-fault controller。构建和运行都必须对保存二进制实际 `--list`，分别核 Store264 / Runtime191 唯一库存与精确名称。这里的数量是守卫预期，当前尚无实测库存或 Native 资格。

构建复用原 `target/r1-oct15-native-opt1`，原 default features、六项 CI test-profile 设置、warnings、debug assertions、overflow checks、默认栈均保持；不加 DEV opt-level override。用最新同缓存已结束 compile receipt 作 freshness 截止线，只触新源码 mtime，核所有 541 输入 bytes 未变；实际 Cargo artifact 必须来自新源、`fresh:false`、features[]、opt1/debug0。Store 选 core/store lib+test 三份，Runtime 选 core/store/runtime lib+test 四份，保存二进制到缓存之外。原缓存检查与 df 检查保留，不复制或清理缓存。

下列命令在仓库根目录运行，`PREVIOUS_COMPILE_RECEIPT` 换为 root 的实际最新同 target 已结束回执绝对路径。若先构建 Store 再 Runtime，Runtime 的 previous receipt 用该次 Store build.receipt.json。out 已存在即拒绝。

```text
python3 -B .local-verification/2026-10-16/graph-frame-return-process-fault-native-runner-v3/build.py --role candidate --package store --source-manifest .local-verification/2026-10-16/graph-frame-return-process-fault-qualification-v3/frozen-source-manifest.json --compiled-inputs .local-verification/2026-10-16/graph-frame-return-process-fault-qualification-v3/compiled-inputs.json --previous-compile-receipt PREVIOUS_COMPILE_RECEIPT --out .local-verification/2026-10-16/graph-frame-return-process-fault-native-v3/build-store
python3 -B .local-verification/2026-10-16/graph-frame-return-process-fault-native-runner-v3/build.py --role candidate --package runtime --source-manifest .local-verification/2026-10-16/graph-frame-return-process-fault-qualification-v3/frozen-source-manifest.json --compiled-inputs .local-verification/2026-10-16/graph-frame-return-process-fault-qualification-v3/compiled-inputs.json --previous-compile-receipt .local-verification/2026-10-16/graph-frame-return-process-fault-native-v3/build-store/build.receipt.json --out .local-verification/2026-10-16/graph-frame-return-process-fault-native-v3/build-runtime
python3 -B .local-verification/2026-10-16/graph-frame-return-process-fault-native-runner-v3/run-case.py --build-dir .local-verification/2026-10-16/graph-frame-return-process-fault-native-v3/build-store --major 16 --leg B --case store_exact --out .local-verification/2026-10-16/graph-frame-return-process-fault-native-v3/pg16-store-exact
python3 -B .local-verification/2026-10-16/graph-frame-return-process-fault-native-runner-v3/run-case.py --build-dir .local-verification/2026-10-16/graph-frame-return-process-fault-native-v3/build-runtime --major 16 --leg B --case runtime_exact --out .local-verification/2026-10-16/graph-frame-return-process-fault-native-v3/pg16-runtime-exact
```

PG17 命令同样只换 major 与 out。每次只新建固定 `stateknot_r1_returnfault_20261016_pg{16|17}_{store|runtime}_b`，原容器 ID/image/task label/loopback port 与 server/DB durability before/after 检查保持；已有库即拒绝，不删除库、卷或改耐久设置。mandatory DB 环境真实设置。无过滤修改，原 exact argv 为 `--exact --test-threads=1 --nocapture`；Store 预期 1/263 filtered，Runtime 预期 1/190 filtered。600 s 仅继承的外部观察 watchdog，不改变 case 内业务 lease、timeout、quantum 或栈。

保存实际进程 receipt 后才做后置观察，保持公共 fixture 密码去敏及 raw/saved SHA 区分。wrapper 不增 marker 解析规则；Return controller 自身两 cut 断言必须完成，cut marker 供 root 单独审 raw。当前只完成脚本 AST 检查，不声明新源已执行、不转授旧 Native、完整 CI、性能收益或 R1 资格。
