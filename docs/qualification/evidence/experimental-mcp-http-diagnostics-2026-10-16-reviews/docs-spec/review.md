# MCP HTTP Debug 文档/Native 增量独立 Spec 审阅

最终结论：**docs-v3 剩余 Spec findings 0**。先审的 docs-v2 有一项证据元数据矛盾；原件保存在 `finding.json`，没有覆盖。v3 的三份文档与 v2 逐字相同，receipt 仅把旧 `new_Native_or_fullCI_conformance_or_stdio_qualification:false` 改为明确的 `new_full_Native_or_fullCI_conformance_or_stdio_qualification:false`，与 `new_targeted_Native_qualification:true` 正确分开。最终固定 v3 manifest `aef6ecd8f947bf410d4e97f721ea3d0fb7c01a1a5b23dce1bf4d2d07b54a1044`，freeze `b78c134739263f99a62ef8c7e65e95dd8ab916b75c6031d89d42383354cff281`。

本次是他人编写的 EN/CN guide 与 CHANGELOG 增量 Spec 审阅。v1 独立审阅和文档原件保持不变。与 Git8016 的 baseline 逐字一致，文档仍只插入新段落；新英文明确是 PostgreSQL16/17 各完整15项旧 suite，而非 PostgreSQL15。新增本源码的两套定向 Native 事实与原有四类型/exclusions/raw/wire/effect 声明一致。

已独立读取 root 实际执行的 summary `d0927746…` 和 85 项过程证据绑定：四份原日志分别为 PG16.15/17.11 上完整 `mcp_durable`6/0/0/0 与 `mcp_reconciliation`10/0/0/0；每版 2 MCP + 2 A2A authorization 和三类 reconciliation 原 JSON 完整值匹配。`--list` 实际名称、两份保存在 cache 外的 binary SHA、Cargo artifact `fresh=false`、精确 source path、默认 feature 构建命令、opt0/debug0/assertions/overflow=true 均核实。32 是 harness 次数，含普通 definition/wire 测试，不是32个数据库场景；文档没有这样泛化。

每 run 的 wrapper SHA/inventory SHA、argv/cwd、日志/退出码、profile allowlist、无 stack/编译器 override 记录相符；mandatory PG switch 为1。source928 的前后记录与当前固定源码重哈希吻合。原 task container ID/image/label/port、真实版本、df≥50GiB/cache合计≤20GiB、TAG/Git/lsof/lock 记录一致；各 major 的数据库名称前后精确为既有集合加本次新库，全部保留。没有把这些记录变成全业务表、全 Store264/Runtime189、容量、部署或完整 CI 资格。

Native runner 由我准备、root 独占执行；本次明确是对他人文档和实际原始证据的事实复核，不自称 runner 的独立实现审阅或独立执行。此前六门禁/2099+6ignored/48doc/真实八分支限定结论按 v1 原记录延续，没有扩大或重跑。

v2/v3 package 只读 verifier 均 exit0。新 qualification EN/CN/JSON 三条保留路径须随发布包补齐，不能提前当作 main 已采用。v2 finding 已由全新 v3 元数据增量闭合；没有改写旧 sealed 包。审阅未运行 Cargo、网络、数据库、Docker 或 cache 操作，未修改 main、索引、源码或候选。
