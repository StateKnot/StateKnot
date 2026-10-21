<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# child 引入新币种时保留 failure-close 冻结 DIRECT

真实 Unknown Tool 完成授权对账、child 已结算后，generic failure close 仍可能拒绝正确的 parent terminal 总量：原冻结 DIRECT 没有币种项，而 child 有 USD11微单位成本；从 total 减 child 会保留显式 USD0，无法与原空 DIRECT 精确相等，finalizer 因此返回 IncompleteChildAccounting。修复按“原冻结 DIRECT + 实际 delegated usage”正向核对完整 total，再保留原 DIRECT 表示，而非用减法逆推它。

[English](experimental-failure-close-real-tool-unknown-2026-10-21.md)。下述本机定向 RED/GREEN 绑定准确源码；同一 GREEN 源码仍有两项测试风格 Clippy 错误，style/CI/docs 新组合须独立全验。R1、RFC-0022、stdio和发布不关闭。

## 两条私有生产路径与限界

- [failure_closes.rs](../../crates/stateknot-store-postgres/src/failure_closes.rs)从既有完整验证的 RunFailureClose 取原 DIRECT，要求 frozenDIRECT.checked_accumulate(actualdelegated) 与完整 terminal total 相等；错误总量仍拒绝，返回原 DIRECT 而不重建其币种表示。
- [child_runs.rs](../../crates/stateknot-store-postgres/src/child_runs.rs)在既有 frame 校验及 account.observe_direct 前使用该 resolver，DIRECT head推进到实际原terminal ACK；child entries/settlements/delegated不变。

Core明确保留 USD0 的减法语义、公开API、SQL、依赖和wire schema均未改。没有verified failure-close记录时保留原减法fallback。普通frame/noFailure及Root barrier的同类表示问题仍开放，不因generic RunFailureClose修复而获资格。

## 实际定向证据

RED与GREEN的整个 Runtime test文件完全相同，SHA256 `2730d90fccd8137f8583f57bac67f6cff3592cb6c9e1c23c0b2e178287d73374`；相对RED仅改上述两个Store生产路径。没有用SQL伪造Tool结果、终态或settlement。

| 绑定 | 真实RED | fixed minimal GREEN |
| --- | --- | --- |
| 源文件 | 972 / `6d9b7e6a37eb90181b2a69175b2123f9a86ff330a08049820d922ea4e17c7098` | 972 / `dcf0a54436606790093882744f3384fc78346ede72c4ab6d9eef318d0b745ed8` |
| 编译输入 | 563 / `87fc37c226b7ecc00a0911448b9775c738b0eb65893dca816b44c50e9938531e` | 563 / `88e00f7550cbe843c4e57145fd909be2b95688f5df41a7c0247b4569b0a6becd` |
| Root索引 | `420a34aa59e6ee3ba6df853ccdeb7ccf88b0a72b178379cfd2134147e7927ce0` | `523ca62e38dee1e35714d66e0df6935692dc90712ec34d21bd90df69108f4a0f` |
| PG16/17各exact | exit101，0passed/1failed/228filtered | exit0，1passed/0failed/228filtered |

GREEN prep `6dc564b312539e98714bd7bdf2d8f1343dd699e6736926274b00b6b115edad14`；build `86253ca349107c36d65ae2a5175123a8a03cec545f1640bd2080ffbaf7212abc`；Runtime binary `00caca25bb06cd770bcc29e26238d9883bbcf0328cbb0c95d5874b50233dbae5`。实际完整build库存为Store275/Runtime229；这两次capture只运行Runtime单exact，未运行任一完整套件。

| 本机GREEN | 耗时 | capture manifest |
| --- | --- | --- |
| PostgreSQL16 | 3.765131秒 | `bebd985f53c309d4d9bdc7e2b3daf55f643994dd7bbd5c7bbe509cd28782e0d0` |
| PostgreSQL17 | 3.707860秒 | `999f6c95c1a0ee87e153598cb54210152a2ff22808a6ed70f1a32d0e44b6dfb5` |

每包95索引/96物理文件、1个详细marker、58项checks全true及A/B两个profile，两版共4profiles/116true。source/binary/primary字节和mtime、container身份及durability不变。宿主为macOSaarch64配PG容器，保留defaultfeatures/stack、testopt1/debug0/assert+overflow及lease30/READY90/pair1800/Native30min，不称Linux宿主、性能/容量或OS-kill资格。

## Unknown、consent与原会计事实

**A：parent Unknown。** 先在真实admitted child执行known Tool、公开取消并settle，随后才发parent Tool。fixture backend保存原receipt后返回真实Unknown/ReconcileFirst；真实parent nodefailure已保存且Run保持Active。每个failure-close请求前重新取全tenant/Tool/account/ACK快照，精确CheckpointBlockedByToolInvocation且不写。该负例candidate usage不称COMPLETE DIRECT。

**B：child Unknown。** 实际child Tool进入Unknown；公开parent Loop注册原failure-close，原reconciler保存child RequestCancellation后才取各拒绝操作baseline。UnsettledChildRuns不改变pendingparent、Unknown记录或未settled账户。显式授权原receipt对账后，公开child Loop确认取消、reconciler settle原child一次。RED中A控制组已完成，两版B在child已settled、total正确时仍被finalizer拒绝；实际DB快照与原Coreunit共同证明币种表示原因，不从setup/超时猜因果。

无consent probe先拒且全tenant不写；当前显式consent绑定原invocation/attempt、descriptor/input/schema、operation、tenant/run/thread及原Executing origin，真实provider probe返回原test-backend receipt。每个原invocation跨Store/Runtime重建与对账仅一次业务send；A是两个各send1的invocation。本backend是同进程fixture，不证明任意外网exactly-once或外部backend持久恢复。trusted commit_tool_reconciliation只重放原完整reconciliation ACK，本身不调用provider。

完整reader据实际admissioninput、journal规范字节、checkpoint、failed-node usage、已对账Tool原history/input/output/artifacts与明确已知价格取DIRECT；parent DIRECT不含childsubtree。child完整terminalusage与拓扑峰值保留在原immutable settlement，delegated仅cumulative_only。两版A保持DIRECT USD11+child USD11=totalUSD22；B保持DIRECT costs[]+child USD11=totalUSD11，数值均为micro_units。child拓扑峰值不继承。终态account只合法推进DIRECT head/digest，完整expected为preaccount.observe_direct(actualterminalACKhead,frozenDIRECT)，原child entries/settlements/delegated精确保留。

原failure-close registration、child/parentterminal与reconciliation ACK完整保留。显式release、Store重开及Runtime对象重建后正常claim；再按原ACK重放，整个tenant和完整post-terminal account不写。这是同进程重建，非进程替换/自然expiry恢复。fixture claim清理、pool/Store关闭先于最终verdict和marker。

## 失败史与尚未完成的门禁

初始characterization设计早于已证生产缺陷。v2两个静态会计oracle及fmt-only源保留，不称功能RED；首轮build9项API类型错误未运行DB，也不称功能RED。后续API兼容源才产生上述真实双PG RED。Core原unit明确要求保留USD0，未被改成全局normalize。

定向GREEN同源严格Clippy真实101，仅新close_unknown_usage/close_unknown_finish为101/105行超过100；log SHA256 `5ebd35ca0f4ad6e1080b9599ee476e3559915b4ffc7d7612f9fa26122c9c4434`。另封source-only helper提取和六文件库存overlay将与本五docs组合；最终source/compiledhash及结果不得预测，必须另跑fmtcheck/严格Clippy/workspace/doc/nightly、双PG完整Store275/Runtime229及新发布/CI，不继承minimal GREEN。

详细 `STATEKNOT_FAILURE_CLOSE_REAL_TOOL_UNKNOWN_EVIDENCE=` 只由local exact --nocapture捕获。完整Native默认libtestcapture不自然输出成功marker；其证明是actual全库存、case恰一次及完整harness断言。无新增mandatoryprocessstep、分区、artifact、ignored、fixedsleep或扩预算。已发布6ec的workspace2171/doc48及完整Native1006仅历史；CI39464102714为21success/1WindowsNoRunner取消，56artifacts/3613索引已保存但collection_complete=false、15组件未判完、117controls未执行、fullCI=false。父53 CI39444455774与111offlinecontrols同样不转授。

更广scopedUnknown/effect/故障矩阵、普通frame/Rootbarrier fallback、独立Driver中断、R1/RFC-0022/R6/R7、MCPstdio、Supported及发布继续开放。


## 2026-10-21 完整 Native 负例追加

后续 D3组合的974源 `6baea426d90df70425c5f2879c5e26d4f1c1ae3d2983c58d082b856cac44c23e` /563输入 `872a34f78142caeec96fb2980e4a18f5a6a72a7f7dcf106f07e8506e9601b031` 未获完整 Native资格。Rootb13205d3…保留四份 watchdog完整原始捕获：PG16 Store/Runtime输出12/5个FAILED前缀，PG17为3/6；各binary到原1800秒时限后exit-15，均无完整harness summary。前缀数量不是完整失败汇总。前文定向GREEN只属于其原源码，不覆盖这些失败；不认定唯一环境或源码根因。

后续[D4 closed-frame记录](experimental-closed-frame-direct-costs-2026-10-21.zh-CN.md)属于不同的16863bd8…/48eccc16…源码：原D3 exact双版各1passed/228filtered、四A/B profiles/116true，只授回归范围，不能称重跑或接受上述旧四套完整Native。D4同源严格Clippy仍101，三项Store测试helper过长；最终组合门禁、完整Native、发布/新CI、R1/R6/R7、stdio及release仍待验/开放。前文全部历史段落保持原文。
