# HTTP MCP 响应 Debug 脱敏切片

本契约在实现前冻结，限定四个现有公共类型的Debug/prettyDebug诊断，不增加API、协议、依赖、认证或stdio能力。原始访问器仍是明确的数据读取；脱敏不代表zeroization，也不声明全部MCP类型均payload-free。

| 类型 | 允许Debug字段（按顺序） | 不格式化的数据 |
| --- | --- | --- |
| McpNotification | 固定type、has_params布尔、non-exhaustive标记 | method/params/raw全部内容及key |
| McpCompleteToolResult | 固定type、content_count、has_structured_content、is_error、non-exhaustive标记 | content/structured_content/raw及嵌套key/value |
| McpRemoteError | 固定type、code i32、has_data布尔、non-exhaustive标记 | message/data及嵌套key/value |
| McpToolCallResponse | 固定type、outcome固定字符串complete或input_required、notification_count、non-exhaustive标记 | outcome/notifications不递归Debug，特别是不遍历InputRequired的client/tool/arguments/requestState/inputRequests |

标准与pretty格式使用同一字段策略；字段值不包含远端原文、state或tool metadata。计数只取已有Vec长度，布尔只表示Option存在/既有is_error，不遍历或复制payload。错误code保留原有有界i32。不是token脱敏regex或“敏感词猜测”。

原McpNotification/CompleteToolResult/RemoteError的Clone/PartialEq、所有getter、原始Value、MRTR消费式resume、HTTP JSON/SSE解析、发送/重试/外部effect保持。McpToolCallResponse原本没有Clone/PartialEq，不新增。McpInputRequired自身Debug明确不在本范围，McpToolCall enum InputRequired直接格式化也不取得新的范围承诺；外层ToolCallResponse须独立阻止它递归泄漏。McpToolCall::Complete及StatelessMcpClientError::Remote通过受保护内部类型取得对应脱敏。若根任务扩大这条边界，须先新版本契约，不能回写本冻结。

## 必须由root执行的真实TCP oracle

使用已有mcp_client_contract.rs的TCP读写/请求捕获模式，新增单一family遍历HTTP JSON和fragmented SSE，以及Complete(isError false/true)、InputRequired、JSON-RPC Remote error。所有canary来自真实server响应，不能只构造私有类型或mock formatter。

Complete的text、structuredContent、raw扩展key/value带不同canary；input-required的tool name/title/description、输入request key/params、opaque state和原arguments带canary；SSE notification的method/params/raw扩展各有canary；Remote error的message/data/key有canary。普通与prettyDebug对受保护类型、外层response及Remote error枚举均不得含任何canary。InputRequired自身不格式化作新承诺，但外层response必须隐藏其中tool metadata。

正控：raw/content/structured/notification/error getter与server预期Value及serde_json序列化字节完全一致；Clone/PartialEq保持；isError/通知count/code未变。InputRequired只能经现有has_state/input_requests访问器及真实resume验证：回发原opaque state、原arguments、精确response keys，返回正常Complete；不得为测试新增state getter。捕获实际wire请求并核无隐式重发、只有明确resume多一轮。

原测试文件全部原字节保留，新增family仅追加；mcp.rs及durable effect实现不改。root之后统一fmt、freeze、独立review、实际网络+工作区门禁；本authoring阶段无编译/测试结论，不使用Docker/DB/cache。
