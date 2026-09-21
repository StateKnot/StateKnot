<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Skill 组合与生命周期

[English](skill-composition.md)

StateKnot 不会再引入一套名为“Skill”的执行引擎。Skill 是由应用维护的能力包，由已有的
版本化 Instruction、Tool、Graph、Policy 和配置组成。这样，持久状态、授权、预算和
恢复仍然沿用同一组经过审计的路径。

建议使用三层模型：

1. **能力包**：Instruction 加上一组本地、MCP 或 A2A Tool Descriptor。大多数 Skill
   到这一层就足够。
2. **共享状态子图**：能力需要在当前 Run 中执行确定、可恢复的多步控制流时使用。
3. **持久子 Run**：工作需要独立生命周期、状态、截止时间、预算、取消或所有权边界时使用。

MCP Skills 负责远端静态内容的分发与受保护激活。它不替代上述执行选择，也不会让 Skill
文本或 `allowed-tools` 自动获得权限。

## 由应用维护 Module 声明

应用可以使用下面的组合模型，不必向框架添加可变注册表：

```rust,ignore
struct SkillModule {
    identity: SkillIdentity,                 // 精确的应用版本
    dependencies: Vec<SkillRequirement>,    // 精确版本
    instructions: Vec<Instruction>,         // 顺序稳定的 Slot
    tools: Vec<ToolDescriptor>,              // 精确可执行版本
    graph: Option<CompiledGraph>,             // 仅用于真实控制流
    policy_profile: SkillPolicyProfile,
}
```

这是应用层组合模式，不是已经承诺的 StateKnot 公共 API。部署启动时，把选中的 Module
编译成一个不可变 `AgentDescriptor`、可执行 Graph Registry 和 Provider Registry。
持久化数据只记录 Runtime 已理解的普通精确身份，因此在途 Run 不依赖另一套“Skill
Checkpoint”或可变插件查询，仍可确定性重放。

## 在准入前解析依赖

生产环境应要求精确 Skill 版本。启动过程为：

1. 读取部署明确指定的 Root Module；
2. 解析全部精确依赖；
3. 拒绝缺失版本、依赖环和重复 Skill 身份；
4. 拒绝冲突的 Instruction 身份、Tool 身份、模型可见 Tool 名和 Graph 身份；
5. 使用有文档说明的稳定 Tie-breaker 进行拓扑排序；
6. 构建并验证一个不可变部署快照。

Run 恢复时不能解析 `latest`、SemVer 范围或 Marketplace 优先级。升级会产生新的
Agent/Deployment 版本；旧版本的 Run 全部排空前，必须继续安装旧快照。

远端嵌套 MCP Skill 不是隐式依赖。它在同一 Run Scope 中需要独立的 Activation、审批和
Acting Window。不能通过从在线 Registry 删除 Tool 来模拟撤销 Activation。

## 用保留来源的强类型 Slot 组合 Prompt

Prompt 顺序由应用控制。每条 Instruction 都应拥有唯一 `InstructionIdentity`、Origin、
Trust Label、Security Label 和不可变内容。建议顺序为：

1. 平台与应用策略；
2. Agent 角色和输出契约；
3. 按拓扑顺序排列的本地 Skill Instruction；
4. 标记为不可信的、已经验证的远端 Skill 内容；
5. Run 输入和检索证据。

重复身份或未声明的顺序冲突应在启动时失败。不能把远端 `SKILL.md` 字节拼接到高权限
System Instruction。每个进入模型上下文的远端文件都应保留 `McpSkillFileIdentity`，
使策略和审计逻辑可以追踪 Origin、Manifest Digest 与 Content Digest。

Instruction 可以建议调用 Tool，但不能授予权限。权限链仍由 Agent 的精确
`AgentTools`、资源策略、Tool 策略、Caller Grant，以及必要时的 MCP Skill Acting
Window Permit 组成。

## 主动选择状态边界

| 需求 | 组合原语 | 状态与恢复语义 |
| --- | --- | --- |
| Prompt + 相互独立的 Tool | 能力包 | 没有 Skill 私有运行状态；Tool Attempt 使用当前 Run |
| 共享 Agent 状态的固定多步流程 | `SharedStateSubgraph` | 编译进父 Graph；每个 Node 独立提交 Barrier |
| 带明确耗尽出口的有限重复 | `GraphSubgraphCall::bounded_loop` | 静态展开；仍受全局 Step 预算约束 |
| 隔离的长时间工作 | 持久子 Run | 独立 Run、状态、预算、截止时间与取消传播 |
| 远端静态 Skill 包 | MCP Skill Host Activation | 不可信但已验证的文件，加持久审批/Acting Window；执行仍使用普通 Tool/Graph |

只有控制流本身需要确定性持久执行时才使用子图。一个 Tool 集合不需要 Graph。反过来，
也不能为了表面上的组合简单而把多步业务事务隐藏在一次 Tool Call 内，否则会丢失逐步
Attempt 证据、取消点和恢复边界。

共享状态子图会让父 Graph 看到每次提交后的 Barrier，并共享 Schema 与取消；它不是一次
原子的子调用。若子任务需要跨 Worker 存活、独立预算、所有权或异步完成，应使用持久子
Run。参见[共享状态子图与有限循环](graph-composition.zh-CN.md)和
[持久子 Run](durable-child-runs.zh-CN.md)。

## 在已有边界执行预算

不要创建无法执行的 `skill_budget` 字段。Module 限制应编译到已有控制面：

- Agent `BudgetLimits`：模型轮次、Tool Call、字节、成本、Fan-out 和截止时间；
- 每个 Tool 的 `ToolExecutionLimits` 与资源策略；
- Graph 最大 Superstep、并行度和有限循环展开；
- 隔离任务的 Child Run Budget；
- MCP Catalog、文件、Acting Window 和逐操作授权限制。

多个 Module 同时贡献限制时，部署编译器应取兼容限制中的最严格值，或拒绝有歧义的冲突，
绝不能静默放宽上限。

## 通过替换不可变部署完成激活

本地 Skill 的启用属于部署配置。构建新的精确 Agent 版本与 Registry 快照，验证后再把新
准入流量路由给它。不能在 Worker 运行期间添加或删除 Binding。Feature Flag 可以在准入前
选择已验证的精确部署，但选择结果必须体现在被准入的 Agent 身份中。

远端 MCP Skill 使用独立的 Host 生命周期：发现、明确策略审批、按需验证读取、持久
Acting Window、受保护 Tool 注册和不可变撤销。Catalog Discovery 本身不会修改 Agent。
动态 Manifest、磁盘安装和自动从发现结果组合 Agent 仍不属于已实现 Profile。

## JiaClaw 组合示例

假设 JiaClaw 提供文件管理、Web 调研和代码辅助：

- **文件管理**是本地 Rust 读写 Tool 组成的能力包。写 Tool 声明非幂等副作用，并要求
  路径与资源策略。
- **Web 调研**是一组带严格 Allowlist、响应字节上限和来源证据的 MCP Tool。如果每次
  Search 相互独立，它不需要子图。
- **代码辅助**贡献应用控制的 Instruction 和有限 Review 子图。耗时 Test/Build 应成为
  持久子 Run，而不是长时间阻塞一次 Tool Attempt。
- 远端安装的 Review Checklist 是一次 MCP Skill Activation。其文件仍是不可信上下文；
  它请求的 Tool 名会提交给策略判断，不会自动启用。

JiaClaw 在启动时解析精确 Module 版本、拒绝名称冲突、构造唯一的 Instruction 顺序、冻结
所有 Provider、编译 Review Graph，并发布新的 Agent 版本。现有会话继续使用旧快照，
新 Run 使用新版本。

## 权衡与非目标

该方案更重视确定性恢复和可审查权限，而不是进程内动态修改插件。它会产生更多部署版本，
并要求在途 Run 排空前保留旧 Artifact；换来的好处是每条持久记录都能解析到精确的代码、
Schema、Prompt 与策略证据。

StateKnot 当前不提供 Skill Marketplace、Native Code 热加载、SemVer 依赖求解器、跨 Skill
隐藏 Memory、自动 Prompt 冲突合并或自动 Tool Grant。应用可以在本契约之上构建 Catalog
和部署工具，但必须保留不可变准入与 Default-deny 执行边界。
