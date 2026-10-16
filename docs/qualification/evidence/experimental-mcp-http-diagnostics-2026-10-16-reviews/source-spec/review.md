# MCP HTTP Debug 独立 Spec 审阅

结论：**0 个可执行 Spec findings**。最终固定基点为 source-v3 `31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca`（928 文件），相对 lossless `69af`；536 个编译输入的路径字符串排序 canonical SHA-256 为 `1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c`。契约为作者修改前封存的 `mcp-http-debug-authoring-v1/contract-v1`。本审阅未参与这次四类型实现或新增测试的编写；仅承担 Spec 轴，Standards 由其他审阅者负责。

四个手工 formatter 的字段和顺序精确符合契约：Notification 只有 `has_params`；Complete 只有 `content_count` / `has_structured_content` / `is_error`；Remote 只有有界 i32 `code` / `has_data`；Response 只有固定 `outcome` / `notification_count`。均使用 non-exhaustive 标记。Response 仅匹配枚举种类，不递归格式化 Complete、InputRequired 或 notifications；普通和 pretty 格式共用这一实现。未遍历、复制或格式化 payload。

新增单一 TCP family 的 8 个静态分支确实经过公开 McpClient：JSON 与分片 SSE，各含 Complete(false/true)、InputRequired 后显式 resume、Remote。SSE 通知有/无 params；嵌套 canary 进入真实解析后的四类型及受保护外壳。测试检查普通/pretty Debug 无 canary，同时以 raw/getter/序列化 Value 字节及 Clone/PartialEq 作正控。InputRequired 通过原消费式 resume 捕获原 state、arguments、精确 response keys、请求顺序与唯一 ID；没有新增 state getter 或用私有构造代替协议路径。fixture 的 JoinHandle 在超时/取消时仍归 owner，Drop 可 abort。

所有 928 项固定输入和 536 编译输入已逐项重算。只有两个授权路径改变；生产差异仅四个 Debug 与说明，撤销该差异、忽略文档和空白后与 69af 逐行一致。错误枚举起至文件末尾的网络/解析/原 unit tests 字节完全相等；其余文件（包括 durable `mcp.rs` 的既有 HTTP Applied/Never 路径）均匹配基线。原测试 357 行、13,917 字节前缀完整。v2→v3 仅 fixture `id: &Value` 及调用借用，生产、断言和协议数据不变。

已独立读取旧生产红例 `fde49495…`：首个 JSON Complete 业务返回后，普通 Debug 实际泄漏 canary，0 pass / 1 fail / 2 filtered；它只覆盖该首分支。v2 fixture Clippy 101 原件保留。**本次没有运行 Cargo、TCP、网络、DB、Docker 或 cache 操作，也不授予 v3 门禁或 8 分支执行资格。** root 的后续实际结果须独立绑定。

范围仍排除 bare `McpInputRequired` / `McpToolCall::InputRequired` 的既有 Tool metadata Debug；也未把整个 MCP 表面说成 payload-free。`StatelessMcpClientError` 的相关真实外壳是 `Remote`，没有虚构 ToolCall variant。raw 访问器及 Clone 仍可读取或保留敏感数据，不构成 zeroization。未发现需求缺失、错误实现或越界行为。
