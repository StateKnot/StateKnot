<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Agent 活动事件与断线续传

[English](agent-events.md) · [HTTP 接入](agent-http.md) ·
[RFC-0009](rfcs/0009-agent-sse-replay.md)

这是显式启用的 pre-alpha 能力：提供可重放的活动通知，不是模型 Token 流、
内部日志导出或完整历史状态重建。接入层不会执行 Agent。stknot.com 只部署文档，
没有部署使用测试凭据的公共 Agent API。

## 启用与连接

```rust
use stateknot::agent_http::{AgentHttpOptions, AgentHttpSseOptions};

let options = AgentHttpOptions::new(["agents.example.com".to_owned()])?
    .with_sse(AgentHttpSseOptions::default());
// 交给 AgentHttpService::new，并配置真实身份验证与资源授权策略。
```

不调用 `with_sse` 就不开启事件流。请求形式：

```http
GET /v1/agent-runs/{run_id}/events HTTP/1.1
Host: agents.example.com
Authorization: Bearer <经过验证的凭据>
Accept: text/event-stream
Last-Event-ID: <最后完整处理并保存的活动事件ID>
```

首次连接不带 Last-Event-ID；续传时只能提供一个非空值。Accept 必须精确为
`text/event-stream`，请求体为空。JSON 操作不接受 Last-Event-ID。
必须同时拥有可信凭据的 `Read` 权限和目标 Run 的资源访问权限。
不增加匿名模式、Cookie 认证、URL 凭据、查询参数或 CORS。
浏览器原生 EventSource 不能自定义 Authorization 头，应使用支持认证头的流式
HTTP 客户端，不能为适配客户端而放宽认证。

## 分清活动与快照

`activity` 对每一条持久化日志记录输出一次通知。JSON 只有 `sequence`（十进制
字符串，非生命周期版本号）和 `recorded_at`（数据库记录时间）。SSE 的 `id`
是完整游标。不会返回日志种类、私有输入、对话、Worker 标识、Schema 或提供商内容。

`snapshot` 返回现有的 `AgentHttpRunResponse { request_id, snapshot }`。
每次新连接都会发送，之后完整内容变化时才发送，包括“生命周期版本未变，但 Run
被隔离”的情况。快照不带 `id`，也不推进续传进度。快照先读取，日志页随后读取，
两次读取不是同一份数据库快照；快照可能早于同时提交的活动。不能把它解释为
某个事件位置上的历史状态。中间快照可能合并，活动通知则按日志顺序逐条重放。

无变化时发送 `: keep-alive` 注释。`error` 帧同样没有 id。SSE 解析器可能把上一个
事件 ID 沿用到后续事件，因此只能在处理 `activity` 时保存游标，不能对快照、
错误或心跳保存解析器继承的 ID。

## 正确恢复

1. 读取完整的 UTF-8 SSE 帧；断连时丢弃不完整帧。
2. 幂等处理活动后，再持久保存完整 ID。如果处理过程产生业务副作用，需要把
   业务处理进度与游标一起提交。传输层不承诺 exactly-once。
3. 携带当前有效凭据和已保存的 Last-Event-ID 重连。即使换进程或副本，也从
   该精确事件之后继续。没有游标则从序号 1 开始。
4. EOF、429、503 使用有界指数退避和随机抖动。连接关闭不保证能收到错误帧。
5. 401 重新获取并验证凭据；403 停止并处理权限；`409 agent_http.invalid_cursor`
   需要核对保留历史或恢复情况，不能悄悄清除游标。非规范/畸形编码为 400，
   授权后的 Agent 不存在为 404，完整性错误为 500。

游标是带版本的规范 base64url 编码，包含租户、Run、事件、序号、时间和校验和；
它不是加密秘密或访问凭据，客户端应整体保存而不自行拼接。数据库核对每个字段与
保留事件完全一致，再校验连续日志后缀。跨租户、跨 Run、被篡改、超前及历史缺失
的游标都不会自动重置。普通校验和用于发现损坏，不能对抗重写整段历史的数据库管理员。

需要按重连窗口保留日志；恢复旧备份可能让已确认的游标失效。
RPO/RTO 仍取决于数据库与宿主部署。EOF 不是执行成功，只有经过验证的终态结果
才能表明完成；`CancellationRequested` 仍是非终态。终态后连接仍可继续观察隔离状态，
直到达到连接时限；应用不再需要观察时应主动关闭。

## 上限与撤权边界

默认 16 个活动生产任务、60 秒连接时限、1 秒空闲轮询/心跳、5 秒队列等待。
`AgentHttpSseOptions::new` 允许 1–128 个任务、1–600 秒连接时限，以及
50 毫秒–15 秒轮询/队列等待；后两者不能超过连接时限。每条连接累计输出上限
64 MiB，每批帧（包括 SSE 格式开销）受现有 `max_response_bytes` 限制。

生产任务先取得单槽队列空间，再重新验证凭据与资源权限。一批最多一个快照和
一个活动；数据库每页为校验后续是否存在，最多读取两条私有事件，只输出一个公开
事件头。有积压时继续读取，否则等待轮询间隔。这优先确保内存和权限有界，
不代表已经获得通用高吞吐性能指标，生产环境应以真实日志与验证器进行容量测试。

每轮（包括心跳）都重新验证凭据和精确资源策略；同一凭据被映射成其他租户或主体时
关闭连接。每轮受 JSON 超时限制，整个生产任务另外受绝对时限限制。异步超时不能
抢占阻塞的宿主验证器。撤权不是瞬间撤回所有字节：本批已授权数据及已交给 HTTP
栈的数据无法收回，因此还要约束策略、认证和网络层的延迟。

事件流使用独立的并发额度，不长期占用 JSON 请求槽位；建立连接仍经过短请求额度。
丢弃响应会终止生产任务；即使调用方保留响应却完全不读取，满队列也会超时释放额度。
停机取消待处理任务；错误帧只做不等待的尽力发送，无法发送时以 EOF 结束。

## 生产部署清单

- 配置 TLS、真实凭据和策略、租户/跨副本配额、数据库池容量，以及有限的请求头、
  Socket 写入、连接数和连接寿命。生产任务结束并不控制宿主所有 Socket 缓冲区。
- 在该路由关闭代理缓存、缓冲、内容转换和压缩；验证实际代理，而不是只依赖
  `private, no-store, no-transform` 和 `X-Accel-Buffering: no` 响应头。
- 代理空闲超时应大于轮询周期加认证/数据库有界延迟，并通过真实代理测试慢客户端。
- 监控连接数、重放滞后、错误、数据库池压力与排空。日志不记录凭据、私有快照和
  原始路径，不使用 Run 作为高基数指标标签。
- 发布时停止接入，调用 `shutdown()`，限制宿主排空时间，替换兼容版本并凭原游标
  重连。保留回滚版本和验证过的数据库备份。本阶段没有新增迁移。

## 验证

运行 `cargo test -p stateknot --test agent_http --locked -- --nocapture --test-threads=1`，
设置 `STATEKNOT_TEST_DATABASE_URL`；设置 `STATEKNOT_REQUIRE_POSTGRES_TESTS=1`
让缺少数据库配置直接失败而非跳过。CI 在 PostgreSQL 16/17 上保留源提交/树与
HTTP、SSE 两组证据标记。覆盖新操作系统进程精确续传、在线取消、游标篡改、撤权、
仅隔离状态变化、连接时限、不消费响应、停机及不内联执行。完整服务角色与发布验收
仍是后续门槛，不等于整个框架已达到稳定生产版本。
