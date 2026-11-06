Publication Spec：0 项可执行 finding。非作者审阅冻结的 178 路径候选 `739688ef10fe7f01579d1cb4ba0174ea3e257a0fc6782f82dc5a64f3cb90939c` / freeze `64288482…`，内部 evidence manifest `af5c515f…` / seal `0f5a3d13…` 保持原样。

实际独立运行纯离线检查：outer0、direct0、固定 source-v5 的 --workspace0；采用前 main 的 --workspace1 按预期拒绝旧 a2a_server.rs。四份真实 stdout/stderr/receipt 均另存，没有把预期拒绝抹除或改为产品失败。未执行任何 preserved gate helper、Rust、TCP、Docker、DB、CI 或缓存操作。

169 个索引原件逐项核压缩/raw hash，163 项与本机原件或最终 source payload 直接逐字对照；6 项派生元数据另核。完整 13 个实际 Python helper（6 个 run-gate 与7个 execute driver）均保留，未以只有 receipt 代替脚本。16 份原 gate receipt 保留 command/cwd/PID/source、当时 runner SHA、真实环境 allowlist、raw log hash 和原 exit；不是事后补写的进程环境证明。失败收据与 Q1–Q5 原件逐字相同。

重建验证实际检查全部930行与538项输入；本包只有3个新源码 payload，复用23个带 hash 的公开源码引用，没有复制 whole archive、binary、cache 或 Git。通过原 fixture patch 还原全部历史版本；生产文件从Q1至Q5相同。工作区检查明确只对 supplied538，不冒充完整 Git tree、采用文档或 compiler read closure；旧源码引用不转授旧执行/Native 资格。

中英报告与 JSON 的来源、数字、8private+1TCP family /17ID×2、六门 profile 与边界一致，16个相对链接有效。EOF重复poll、SDK自生timestamp、7项fixture lint、edition格式、5个sandbox bind拒绝分开保留，未称产品回归。最终100suite/2108passed/0failed/6原ignored及10组48doctest均对应原始成功日志。

原seal诚实保留当时尚无最终独立 Spec/publication 审核的状态；本报告以及后来 source/docs 独立审核应作为树外或独立 delivery 附件绑定，不能回写原seal。没有新 Native、IdP/current-policy backend、durable Task、完整CI/30m、stdio、部署pilot或R1资格声明。
