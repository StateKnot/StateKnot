<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 耐久共享状态子图与有界循环

[English](graph-composition.md)

`SharedStateSubgraph`、`GraphSubgraphCall` 和 `GraphComposition` 实现静态共享状态组合。
编译结果是普通 `CompiledGraph`，通过 PostgreSQL-backed `DurableGraphDriver` /
`DurableAgentLoop` 执行。这是已实现但尚未发布的 pre-alpha 契约，不代表独立子工作流
或整个框架的生产资格验证已经完成。

## 运行仓库示例

```console
cargo run -p stateknot-runtime --example durable_graph_composition --locked
```

[完整示例](../crates/stateknot-runtime/examples/durable_graph_composition.rs) 构建 Schema、
Reducer、两个节点的审核子图、最多三次循环、明确的成功/耗尽出口，以及封闭的可执行注册表。
结果包含八个可执行节点，不调用数据库或模型服务。接纳运行时使用初始状态 `{"count":0}`，
示例的决策节点在计数达到二时接受。实际执行时，将注册表和可信的累计用量证据提供者接入
[耐久 Agent Loop](durable-agent-loop.zh-CN.md)。
[PostgreSQL 测试](../crates/stateknot-runtime/tests/postgres/graph_composition.rs)
验证真实执行与恢复，没有内存执行后端作为替代。

## 定义返回契约

模板是有限无环的已编译图。其中纯 Terminal 节点是**符号返回端口**，不会执行代码。
返回端口不能同时声明 Continue、Route 或 Wait，也不能出现在入口集合中。
其他节点才是可执行节点，不能返回 Terminal Output。子图通过显式 State Update
传递结果，不会丢弃某个子节点的输出再假装返回成功。

父图的调用位置只声明 Route。默认情况下，每个子图返回端口对应一个同名父图 Route：
例如 `done` 对应 `done -> finish`，`again` 对应 `again -> exhausted`。

```rust,ignore
let subgraph = SharedStateSubgraph::new(body)?;
let call = GraphSubgraphCall::bounded_loop(
    NodeId::new("review")?,
    subgraph,
    NodeId::new("again")?,
    3,
)?;
let composition = GraphComposition::compile(parent, [call])?;
```

此片段依赖完整示例中的 `body`、`parent` 定义，不是独立程序。
`GraphSubgraphCall::once` 只实例化一次。多个调用位置复用相同模板时，父图的 Route ID
仍须全图唯一；使用 `with_return_routes` 显式映射，如 `done -> left.done`、
`again -> left.again`。映射必须完整且一一对应；重复、缺失、多余的端口或路线在接纳前拒绝。

## 循环与状态语义

- 循环采用先执行后判断的语义。每次进入该调用位置，最多执行声明的轮数。
  `again` 进入下一轮，其他返回端口立即退出。
- 最后一轮选择 `again` 时，执行对应的父图出口。这里必须放置真实的失败节点或明确的
  备用处理，耗尽不会隐式变成成功。外层循环可以重新进入调用位置，因此全图步数预算仍然必需。
- 子图的每个工作节点都有自己的耐久 Start、Result 和 Barrier。状态在每个 Barrier 后
  可见，不是等整个子图结束后才原子提交。父子图必须使用完全相同的 Input/State/Update/Output
  Schema Reference 和 Reducer Revision；没有私有子图状态，也没有隐式类型转换。
- 并行仍采用原有 BSP 语义：稳定排序的归约、去重的下一 Ready Set，不增加异步跨步 Join
  累加器。同一调用位置、同一轮内部保持模板的节点排序；跨作用域顺序由展开后节点 ID 决定。
- 子图 Wait 暂停整个所在 Run，并保留映射后的后继集合。耐久 Wait 被解析后从该集合继续，
  不重复已完成子节点。取消作用于整个所在 Run。
- 已展开的无环图可以再次作为模板，实现静态嵌套；启动时展平，不引入递归运行时调用栈。

## 正确绑定执行身份

只注册 `composition.graph()` 作为可部署图，不注册用于编写结构的父图或符号返回节点。
对生成节点调用 `composition.node_source(node_id)`，可以得到精确模板引用、原始节点 ID、
调用位置和从零开始的轮次。构造普通 `GraphNodeExecutor` 时，绑定**组合后的图引用和节点 ID**。

Route ID 同样隔离。通过 `source.route_id(&local_route_id)` 取得生成后的 Route ID，
再返回 `NodeControl::Route`；直接返回模板中的原始 Route ID 会按未声明路线拒绝。
Reducer 收到的也是组合后节点 ID；归约逻辑应与角色名称无关，或者显式绑定不可变 Source Map
来查询源角色。

向耐久调用代码原样传递真实 `GraphNodeContext`。不要伪造子 Checkpoint、将 Activation
替换成模板身份、或重置根 Superstep。Invocation Binding 始终指向实际 Tenant/Run/
Checkpoint/Activation，接管租约时也不改变逻辑身份。

## 资源、恢复与升级保证

完整展开最多包含 1,024 个可执行节点，每节点最多 256 条路线，规范化定义最多二 MiB。
分配前计算所有循环副本；超限直接拒绝，不退回不可持久化执行。大量且数据相关的迭代应使用
已有的普通循环图，并设置有限的全图步数预算。模板本身拒绝环，最长工作路径必须符合模板自己的
步数上限。父图并行上限不能高于任何子图模板的上限，其他同时就绪的父图节点也计入；这是无需
第二套调度器即可执行的保守限制。

生成节点 ID 格式为 `skc-<完整 SHA-256 作用域摘要>-<四位序号>`。作用域摘要基于 RFC 8785
JSON，包含域标识 `stateknot.graph-composition.v1`、完整父图及模板引用、调用位置、轮次、
最大轮数、重复端口和返回映射。序号来自模板规范节点排序，包括符号端口。
生成路线为 `skr-<完整 SHA-256>`，通过域标识 `stateknot.graph-composition.route.v1`
绑定生成节点和原始路线。

因此组合图摘要固定源版本、调用位置、循环上限及返回绑定。为在途运行保留编译器版本、所有源定义、
生成的 Source Map 和精确可执行产物。修改模板或组合方式需要新的图版本；旧版运行排空前不能移除
旧注册表条目。

本实现不修改 Checkpoint Wire，不需要新的 PostgreSQL Migration。空组合保留原始图字节。
恢复过程仍重放同一 Checkpoint Lineage，消费精确 Pending Result；只有 Start 而没有完成记录的
节点遵守原来的更高 Fence 接管规则。

Driver 在**派发新节点之前**检查 `maximum_supersteps`，重启后正好位于上限的情况也适用。
`GraphDriveBlockers::superstep_limit_reached` 进入生命周期失败处理。可信 `failure_evidence`
提供者必须恢复精确累计用量，即便当前 Checkpoint 没有失败节点记录。证据不可用就保持未完成，
不能猜测零用量。Lifecycle 记录 `runtime.graph.superstep_limit_reached` 并禁止重试；
原有事件 Schema 中的节点阻塞计数仍然只表示节点计数。保留的失败 Handoff 在响应丢失后可以
重复提交，不重新读取证据，也不替换原始终态失败。

## 验证与未覆盖边界

Core 测试覆盖确定性身份、冻结摘要、源节点排序、静态嵌套、返回转发、端口/路线/Schema/Reducer
拒绝及资源上限。真实 PostgreSQL 测试覆盖重建执行注册表后消费 Pending Result、提前退出、
显式耗尽、孤立 Attempt 接管、组合版本漂移时禁止派发、Wait/Resume，以及全图超限前停止派发，
包含证据不可用和响应丢失重试。CI 同时运行 PostgreSQL 16 与 17。

独立子状态、独立调度的子 Run、动态递归、独立子取消和异步跨步 Join 不在本 API 内。
这些能力需要单独的命名空间 Checkpoint/Lifecycle 契约，不能用静态组合冒充。

范围划分参考了 [LangGraph 子图通信](https://docs.langchain.com/oss/python/langgraph/use-subgraphs)
和 [Temporal 子工作流生命周期](https://docs.temporal.io/child-workflows)；上述编译与持久化语义
属于 StateKnot 自身的契约。
