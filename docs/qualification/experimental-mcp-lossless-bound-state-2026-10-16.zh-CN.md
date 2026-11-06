<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 实验性 MCP 无损绑定状态：组件记录

2026-10-16。固定源码`69af3a0f…`自行通过六严格门禁及双版PostgreSQL完整15项串行组件用例。这仅是本机显式opt-in限定证据，不表示完整CI、生产身份backend、Supported或R1完成。[English](experimental-mcp-lossless-bound-state-2026-10-16.md)。

[机器记录](experimental-mcp-lossless-bound-state-2026-10-16.json)、[原件索引](evidence/experimental-mcp-lossless-bound-state-2026-10-16/artifact-index.json)与[离线校验器](evidence/experimental-mcp-lossless-bound-state-2026-10-16/verify.py)绑定精确原件和边界。

## 旧候选已被反例推翻

[旧14项记录](evidence/experimental-mcp-lossless-bound-state-2026-10-16/history/old-eedd-publication/docs/qualification/experimental-mcp-bound-state-2026-10-15.md)仅保留历史，不得采用。之后在生产字节完全相同的旧源码上，真实TCP反例持旧state把Tool参数从`2^53`改成`2^53+1`，得到HTTP200 Complete，业务counter2→3；预期400断言失败，exit101。该内存Host反例不证明数据库副作用。[原红](evidence/experimental-mcp-lossless-bound-state-2026-10-16/history/numeric-alias-red/test.log.gz)、源码重建和原224文件公开候选完整保留；旧14绿和静态0发现不能覆盖新红。

显式profile的operation binding改为克隆已准入的`serde_json::Value`，递归排序object键后用`serde_json::to_vec`编码，并升级私有AD domain至v3。array顺序和所表示的数值保持区分，包括宽整数相邻值、整数1/浮点1.0、浮点正负零。这绑定的是decoded Value，不承诺原始JSON空白、数字拼写、重复成员或任意十进制精度；该数字修复不再新增公开API或通用canonicalization契约。Private cursor仍v2、envelope仍version1、未配置legacy AD不变。旧strict v2 continuation无fallback地拒绝；切换应按应用原恢复契约排空/重启continuation，不能自动重发业务。

Prompt仍只准入已声明String参数。数字schema拒绝有正反控；私有共享helper数字单测不代表公开Prompt数字调用可达。Resource操作身份、原认证/策略/effect/限额保持。

## 源码与实际执行

[精确十二文件](evidence/experimental-mcp-lossless-bound-state-2026-10-16/source/final-reconstruction.json)复用已公开[A2A923源码](experimental-a2a-request-facts-2026-10-15.zh-CN.md)和[PITR helper](experimental-pitr-cleanup-confirmation-2026-10-15.zh-CN.md)，重建928文件`69af3a0f28d333f4e67d2b2cfcc2afc071938a6ce398527cc098d31ee8db60d8`：8生产/4测试文件变化，5文件新增。Core/Store/Runtime生产代码、SQL/migration、依赖/lock和原资源不变。只采用这12源码，不复制旧冻结docs/workflow/gitignore，不复制binary/cache/新完整源码归档。

536个保守供应crate/config/fixture输入，按relative path字符串排序的canonical摘要为`cfd0f2f8b48e062567fb591be2e5c427781cb7db616ee20b942cc2174a775b2d`；root原manifest-order摘要`ded1a866797889ed38df63f59f671c8a2c34a5b7373a688932cc714eae582794`按原顺序定义保留。两者是同一path/bytes/SHA map，不是compiler/registry读取闭包或整个Git tree资格。

| 实际本机执行 | 结果 |
| --- | --- |
| Rust1.88/固定nightly六严格门禁，原opt0/默认栈 | 全部exit0 |
| workspace all targets/features |99 suites；2098 passed；0 failed；6个原ignored|
| 独立doctest |10组；48 passed；0 failed|
| fresh默认feature Native binary |opt0/debug0；断言/overflow true；Cargo fresh:false|
| PostgreSQL16.15完整串行 |15 passed；0 failed/ignored/filtered；完整marker一次|
| PostgreSQL17.11完整串行 |15 passed；0 failed/ignored/filtered；完整marker一次|

binary SHA256为`0751e63128bf2fc1a28342f4e05bf62d7fd1416e9ae991032fa4136cf22bc8ed`，实际参数`--nocapture --test-threads=1`；命令、完整Cargo JSON、实际库存、task DB/server身份及源码核对均保留。workspace清空DB环境，其中条件返回不算Native。

库存是13 TCP、1构造配置、1真实PostgreSQL用例。新TCP family包含8组数字、同参completion、旧token变参400且完整业务facts字节/count/completions不变、新参数新state成功、真实wire对象键序重排成功；oracle来自handler实际decoded参数编码，正负零不靠Value相等比较。原14测试和原8私有unit body逐字保留，新增2私有unit函数。

durable用例仍核对3消费者/6次fixture commit/12次改绑拒绝及两个完整fixture `StoredRun`/分页全部`JournalEvent`账本，不是全部数据库/租户/表快照或通用持久server。旧完整Store/Runtime/A2A Native、历史916次或本机opt1-union资格不转授到536输入；没有性能/容量结论。

## 历史、角色与CI边界

更早lint、HTTP wrapper、非法event kind、serial marker红都在history-only旧包保留。新a87首次严格Clippy因新增测试闭包E0597而失败；69af只加显式引用类型。首次同69af workspace有29通过/5个原fixture在loopback bind遇OS1 PermissionDenied失败；获准网络能力后以完全相同源码/断言/资源重跑六门禁通过。收集器digest顺序与文件名假设错误另存为工具红，不算产品测试失败。

源码Standards与数字实现/测试Spec为独立固定范围；数字Spec审阅者编写了修订contract，因此不算该contract自身的独立认证。文档/CI与公开证据审阅另列；本收集者编写了外部测试，不计作独立源码/测试Spec。Markdown示例未单独编译；RFC-0025仍Draft。

CI15候选SHA`2c69f7e78b0b4c9b703e52864cdfd029b3cfc74d9ecceb31510768195b0e45ad`保留原53steps/31run、6个opt1环境、镜像、全部mandatory检查和30m预算；MCP新增2steps/1run后为55/32。相对旧MCP候选只改完整inventory/summary14→15；旧14日志拒绝、新双版15日志接受，其余谓词不放宽。这是离线checker重放，不是Ubuntu/test-opt1/完整job已运行；真实mapping/verifier、stdio、Supported与R1仍开放。

宿主仍负责当前可信mapping、持久且不复用的generation及排队后授权；Private catalog不重分类显式Public Resource内容。坏入站state不进入业务，出站state拒绝可能在业务执行后，不授予新重试权限；没有零时隙撤销、加密或单次兑换承诺。

最终资源观察仅覆盖3个已命名program/cache路径，lsof空、cache tag有效、锁在该时刻可得。df315550848KiB、任务逻辑cache11458960KiB分别记录，后者不是APFS可回收空间。cache和task DB保留，观察不执行清理/DB生命周期动作，不代表整机或全数据库检查。

先离线核验，精确采用12源码、已审7docs/CI和本包后，`--workspace`再核实际536路径/字节/hash及发布输入；后续改变应拒绝：

```console
python3 docs/qualification/evidence/experimental-mcp-lossless-bound-state-2026-10-16/verify.py
python3 docs/qualification/evidence/experimental-mcp-lossless-bound-state-2026-10-16/verify.py --workspace
```

校验器不构建、不联网、不连接DB/Docker。
