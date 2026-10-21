<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 实验性 MCP bound state：组件执行记录

2026-10-15。最终固定候选已通过六项严格门禁，以及PostgreSQL16.15/17.11实际串行完整14项MCP组件suite。资格仅覆盖下述显式opt-in组件证据，不表示完整CI、生产身份backend、stdio、Supported或R1完成。[English](experimental-mcp-bound-state-2026-10-15.md)。

[机器记录](experimental-mcp-bound-state-2026-10-15.json)、[产物索引](evidence/experimental-mcp-bound-state-2026-10-15/artifact-index.json)和[离线校验器](evidence/experimental-mcp-bound-state-2026-10-15/verify.py)绑定源码、真实终态日志与边界。

## 精确组件与宿主边界

`McpServerHostBinding`携带可信认证issuer/subject、本地tenant、有效principal、非零映射generation与部署namespace。可选`McpServerBoundStateProfile`为private catalog cursor使用统一的身份/scopes fingerprint；Tool/Resource/Prompt三种MRTR消费者则另经配置codec使用严格state envelope和操作专属associated data。binding、generation/ABA、scope改变，或输入state畸形/旧版/篡改/过期，均在业务派发前拒绝；强profile不隐式回退legacy state。

测试宿主在decoded-policy排队后及catalog admission中核对当前映射。当前lookup、认证来源、generation持久化/不复用及部署策略仍归宿主负责，typed值和state fingerprint不生成权限。授权有线性化点；已授权在途操作或已释放字节没有零时隙撤销保证。

profile必须显式opt-in。legacy `McpServerPrincipal::new(subject, scopes)`构造器、未配置profile的host-bound服务，以及配置前保留的旧clone不会自动升级。direct/shared/composite配置顺序与clone保留有测试；private catalog元数据不改变独立显式Public Resource内容。缺少输入applicationState与显式null不同。输出aggregate超限的拒绝发生在业务callback之后，不证明业务回滚或授权重试。原TTL/大小限制、effect处理和完整Store认证保留。

## 源码、构建与实际执行

冻结928文件tree为`eeddc9f7e9c92023a0b0c1f9a7220cccc1b4c5d846ae4281d4a4aeff9b0f30f4`。复用已公开A2A923归档与PITR单文件得到3a84，再应用[精确十二个MCP文件](evidence/experimental-mcp-bound-state-2026-10-15/source/final-reconstruction.json)即可重建：8生产文件、4 facade测试文件，其中5文件新增。不复制新完整源码归档、binary或cache。原531个保守供应编译输入成为536，规范摘要为`31b3be83c5a1d38e8204d6d2fd9caf7989206b93d401aeac4045149e7f298807`。

该集合是crate/config/fixture供应输入superset，不是compiler-read闭包。旧冻结`.gitignore`、docs和workflow不等于实际main85f，也不属于采用候选；只能采用所列12源码文件，不能整拷928。Cargo依赖/lock、Core/Store/Runtime生产代码、迁移、SQL、原资源与默认栈均不变。

| 执行 | 实际结果 |
| --- | --- |
| Rust1.88.0及固定nightly2026-10-07六严格门禁 | 全部exit0 |
| 工作区all targets/features，原opt0 | 99 suites；2095 passed；0 failed；6个原ignored |
| 独立doctest | 48 passed；0 failed |
| fresh保存的per-package默认feature Native程序 | opt0/debug0；断言/overflow true；fresh:false |
| PG16.15，串行完整库存 | 14 passed；0 failed/ignored/filtered；marker独立行exact1；35.27s |
| PG17.11，串行完整库存 | 14 passed；0 failed/ignored/filtered；marker独立行exact1；35.15s |

Native binary SHA256为`3d3b32d05d1b442c2e72953e5bdee2f70c0ac3119724ef747b4d3d54810ba2f6`，实际参数为`--nocapture --test-threads=1`。完整Cargo JSON、库存、命令回执和两份日志均保留。工作区门禁清空DB环境，其中条件返回不算Native；Native使用mandatory新任务DB，库存为13个非DB用例（12个TCP加1个构造/配置）及1个durable用例，不是14个独立DB场景。

真实公开TCP路径覆盖三种消费者、private分页、改绑/scopes/ABA、严格envelope的presence/version/tamper/expiry和大小边界、direct/shared/composite、legacy及排队后current-policy核验。durable函数实际提交6次并拒绝12次改绑；它比较两个fixture专属的完整`StoredRun`表示与全部typed `JournalEvent`，按每页2行遍历，且明确fixture没有checkpoint或lease旁账，并断言另一tenant完整fixture账本不变。这不是全数据库表/所有tenant快照，也不导出journal payload dump。

48个Rustdoc不证明单独编写的Markdown guide示例已编译，7份采用文档另有独立于文档作者的冻结内容Standards0/Spec0审阅，不授予源码/测试实现资格。旧完整Store/Runtime Native、A2A事实或opt1-union资格不转授到536输入；没有加速、容量或CPU百分比结论。

## 保留失败与审阅

首轮严格Clippy以101退出，3项lint；下一轮101，17项lint。bounded wire解析移到private helper时保留原完整认证phase。下一轮工作区暴露fixture对HTTP400/SDK JSON wrapper解析的错误（8通过/6失败），exact1最小重现失败；修复保留生产检查并增加实际HTTP400断言。首次PG16 Native随后13通过/1失败，原因是fixture event kind使用非法点号，PG17未执行。仅将该测试字面量改为合法连字符后得到be326，其六门禁及双版默认并行14项真实通过。

旧binary的实际**串行exact1**重现产品exit0，但marker接在libtest pretty提示后，原anchored checker识别到0条marker，仍保留为checker/fixture红。最终只在原marker前增加字面backslash+n；其余927文件、8生产文件、断言和资源均不变。随后fresh构建eedd，串行完整14与六门禁才实际通过。原失败、manifest、精确fix和紧凑源码重建均保留，没有降低阈值、削弱checker、提高资源或绕过生产校验。

Standards0与Spec0覆盖固定be326；最终marker-only增量另有Standards0/Spec0，并用两份真实新serial日志关闭原CI marker P2。这些静态审阅本身不执行或授予CI资格；公开包与采用文档审阅属于另外范围。执行事实另有0剩余复核，独立于runner/collector；复核者是原测试作者，因此不计作独立源码/测试Standards或Spec。

## CI与资源边界

拟议[CI候选](evidence/experimental-mcp-bound-state-2026-10-15/CI-candidate/receipt.json.gz)SHA为`6568a90f4de078ef9f4bc7469e7891b72af7b00619301ee645dced64fea14fd9`。原53steps/31run命令保留，新增2steps/1run成为55/32，仍30分钟；原job env、镜像、全部旧检查和fixture资源不变。新增步骤要求完整14摘要、精确marker且无skip，保留Cargo失败并always上传证据。

CI job继承已采用的test opt1环境，而本机组件在opt0执行。Ubuntu、testopt1和原预算内完整job仍待实际CI运行；默认features相同或checker重放成功不能转授profile/平台/整job资格。

最终只读资源观察仅覆盖保存的Native程序目录与复用stable/nightly任务cache：lsof为空、cache锁可获取，实际df与逻辑cache大小分别记录；范围外缺TAG缓存保留不动。日志/程序位于cache外，没有清理或DB生命周期操作；这不表示整机没有任何进程或全数据库已清理。

先执行离线校验。采用12源码文件和审阅workflow后，`--workspace`再核完整实际536输入路径/字节/SHA；采用前或后续编译输入改变时必须拒绝：

```console
python3 docs/qualification/evidence/experimental-mcp-bound-state-2026-10-15/verify.py
python3 docs/qualification/evidence/experimental-mcp-bound-state-2026-10-15/verify.py --workspace
```

原文本使用deterministic gzip保存，原始hash精确可复核。校验器不执行构建、网络或数据库命令。
