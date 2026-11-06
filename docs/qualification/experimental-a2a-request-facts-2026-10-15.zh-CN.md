<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 实验性 A2A 请求事实：精确执行记录

2026-10-15。固定候选已通过六项严格静态门禁、26 项真实 loopback 网络测试，以及
PostgreSQL 两版各完整6项 mandatory durable suite。这是出站客户端既有 Seam 的限定
执行证据；完整生产策略、协议服务和 CI 资格仍开放。[English](experimental-a2a-request-facts-2026-10-15.md)。

[机器记录](experimental-a2a-request-facts-2026-10-15.json)、
[封签](evidence/experimental-a2a-request-facts-2026-10-15/seal.json)与
[离线校验器](evidence/experimental-a2a-request-facts-2026-10-15/verify.py)
绑定源码、实际终态日志、测试库存和声明范围。

## 源码、审阅与采用

唯一完整源码归档含923文件，tree为
`250066800739a33390a316c7fbb40181335de67394cd9de6bb67ccb425226433`。
唯一生产文件 `a2a_client.rs` 的 SHA256 为
`46022f375430c2387bb73e7914229a55de62144067417053327e0c06e3397216`。
该固定文件的独立源码审阅为 Standards0/Spec0，不覆盖随后变化的测试；最终五份
文档的独立审阅也为0/0，保留并修正了此前一项 P2 表述问题。

[四文件精确差异](evidence/experimental-a2a-request-facts-2026-10-15/source/delta-from922.patch.gz)
相对采用提交 `0e8b4b5` 的 schema36/922源码，仅修改该生产文件、两个原 integration
测试模块并新增一个网络测试模块。Core/Store/Runtime、Cargo配置/lock、1–36迁移和
SQL 全部逐字不变；不包含 schema37、B、新 owner 或 PITR 清理确认 helper。实际采用
四份代码及五份已审文档；**531个**保守 crate/config/fixture输入与候选精确绑定，
不声明整个 main Git tree 等于923。两个 fresh 测试 binary 与四个 workspace library
另保留 compiler-emitted dep-info，覆盖215个 workspace源码条目；该观测集合不是
完整编译器、文件系统或 registry读取闭包。证据包不包含 binary 或构建缓存字节。

## 请求事实与策略边界

一个私有不可变 snapshot 保存实际 prepared method、规范化完整 URL/query 与已按
原限制完成序列化的 body。授权回调得到 clone，HTTP builder 消费同一 snapshot，
没有第二次序列化或应用层重写。REST read可无body，但URL/query仍携带资源/filter；
JSON-RPC read使用POST envelope，不能仅凭method判断写操作。访问器将实际请求数据
交给可信宿主策略，不会替宿主实现策略或 consent。

Anonymous不调用 token provider。旧 metadata-only/static provider仍可忽略新getter，
不会自动获得参数授权。Debug仅隐藏新增URL/body，原tenant/task/scope/attempt身份字段
仍可见。Clone共享不可变body并保留URL，可信provider可在调用结束后继续持有；原
body长度限制不是总保留量、序列化峰值内存或清零保证。事实也不认证全部header、DNS、
TLS peer或proxy。

拒绝发生在业务send之前。重复恢复**同一个已提交 durable Failed/Unknown attempt
receipt**既不再发送，也不重复策略求值。新的probe或operator-attested MessageIdReplay
是新的恢复操作，必须重新应用当前策略，并可能按原契约发送。HTTP replay真实覆盖
第二次获准发送仍保留原业务身份，以及撤销策略阻止第二次发送；不授予JSON-RPC replay
或通用重试资格。

## 实际执行

| 门禁 | 实际结果 |
| --- | --- |
| fmt / 全workspace all-target/all-feature严格Clippy | 各exit0 |
| workspace all-target/all-feature locked | 98个suite；2073 passed；0 failed；6个原ignored |
| 独立doctest | 45 passed；0 failed |
| 严格Rustdoc / 固定nightly2026-10-07 | 各exit0 |
| fresh保存的网络binary | 26 passed；0 failed/ignored/filtered |
| PostgreSQL16.15 mandatory durable binary | 6 passed；0 failed/ignored/filtered |
| PostgreSQL17.11 mandatory durable binary | 6 passed；0 failed/ignored/filtered |

静态阶段清除DB环境，条件入口不计Native数据库证据。两个实际新编译 executable均为
`fresh:false`，保留debug assertions和overflow checks，dev/test debug为0；严格flags、
默认栈与原fixture资源没有提高。保留保存binary身份与完整 `--list`。静态26与单列
fresh网络26属于同一库存的两次执行，不是52个不同场景。

网络26保留19个旧测试并新增7个。原两个matrix测试分别执行两种binding的各11操作，
逐一核对实际收到的method、URL/query和raw body与provider事实一致，含streaming send
与subscribe；这是两个函数中的22次operation调用。新增测试覆盖REST/RPC unary/SSE的
payload允许/拒绝、两种unary binding下同provider面对两个预先独立配置URL的目的地拒绝、
真实被poll的callback在deadline/abort后drop、Anonymous/legacy unary，以及**仅HTTP**的
attested replay/当前撤销。[覆盖表](evidence/experimental-a2a-request-facts-2026-10-15/coverage.json)
列明精确组合，不推导完整笛卡尔积覆盖。

Durable suite含4个旧测试与2个新测试，即MCP3项、A2A3项。A2A请求事实的数据库覆盖
**仅HTTP+JSON**。每版真实授权callback读取durable Start，核对完整input/attempt/fence后
再检查实际发送payload。拒绝为0业务发送/1策略调用；丢响应为1发送/1策略调用并保留
Unknown；同原回执重复恢复保持完整ledger且均不再执行。原MCP与新A2A的denial/
lost_response markers分别核对。实际DB身份是原16.15/17.11服务上的独立task库；十二次
libtest执行不等于十二个新增独立场景。

## 原失败与来源保留

`755ffa…`是缺少显式 `DurationMillis` import的source-only快照，没有伪造该快照已发生
编译或运行红。仅import修复产生 `59fd…`；其首次严格workspace因沙箱loopback bind失败，
获得原权限的loopback重跑随后出现真实fixture路由红：25 passed/1 failed，原因是遗漏
精确远端租户REST send路径。最终 `250066…`只补两个精确tenant send/stream fixture
路由；production46022、原资源与拒绝校验未变。原日志、退出码、清单及两个小patch全部
保留，校验器可从唯一final归档重建两份早期923快照。

Compiler dep-info collector首轮对Cargo target名称的辅助断言错误也保留，单列为工具
失败，没有改称编译/测试失败。离线证据collector自身的归档前缀、索引/路径规范化辅助断言
也另行保留，没有执行产品代码。最终全部阶段已wait至exit0，最终source hash逐字一致；
无匹配build/test/HTTP fixture活跃进程，原task缓存按lsof/lock和真实df回执保留。
没有drop DB，本collector未执行Docker/DB或清理操作。

## 历史与剩余资格

[schema36记录](experimental-original-graph-waits-2026-10-15.zh-CN.md)保留自己的922源码、
530输入采用提交 `0e8b4b5`与916次Native。旧offline证据继续有效；其whole-workspace530
检查只适用于历史采用状态，对新增module拒绝是正确边界，**不把原916次完整Native
转授新的531输入binary**。原PG16 PITR WAL清理异常仍开放。本包只引用原parent源码/
seal并保存四文件差异，不复制原750项证据。

五份采用guide/RFC与最新两份tracking快照均固定hash，表述区分已交付事实接口与仍开放
的宿主数据释放/当前consent策略、MCP principal改绑拒绝、verifier/delegation profile和
持久A2A服务backend。本轮未执行独立server TCK、CLI/release profile、多个独立官方对端、
531输入的完整Store/Runtime Native或完整CI；不授予时延/容量、安全证书、owner交付、
Supported或完整生产资格。RFC-0025和R1继续开放，PR保持Draft。

先运行默认offline校验；采用后加 `--workspace` 检查实际531输入路径/字节与五份已审文档：

```console
python3 docs/qualification/evidence/experimental-a2a-request-facts-2026-10-15/verify.py --require-execution-record --workspace
```

原patch及带原空白格式的文本证据以deterministic-gzip保存，raw hash/字节不变；不改源码或whitespace policy。
