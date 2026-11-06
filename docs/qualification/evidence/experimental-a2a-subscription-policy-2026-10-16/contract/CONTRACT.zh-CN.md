# A2A只读订阅的当前策略复核：Interface候选v1

这是下一切片的**待审设计冻结**，不是实现、可编译例子、执行结果或独立审阅结论。仅新增Options无参显式enable方法；不修改main、现有source-v3、RFC或旧证据。

## 范围与配置

拟议 `A2aServerHttpOptions::with_subscription_authorization_rechecks(self) -> Self` 无参数、无新错误，设置私有bool。`new()/Default`默认false；重复enable幂等，Clone及原有with_*方法保留它，不重设timeouts/limits。该开关不广告/启用streaming能力；card/service不支持时，仍由原prepare顺序拒绝。默认false及所有非 `SubscribeToTask` 路径保持旧实现，尤其 `SendStreamingMessage` 的写入、授权/准入次数、effect/retry不变。

启用时仍先执行原来的HTTP认证/解码、capability/extension检查、一次authorizer、admission、operation permit获取及backend订阅创建，顺序和已有creation request_timeout不变。新guard只包住已建立的订阅event stream，不能把拒绝误称“backend从未创建”，也不取消已经持久存在的Task。

固定原 `A2aServerAuthorizationRequest`：最初认证的完整 `A2aServerPrincipal`（精确tenant、issuer/subject和canonical scopes）、operation恒为 `SubscribeToTask`、原解码task ID、config_id恒None。每次调用原 `A2aServerAuthorizer`；不从event、header、routing hint或新映射改写这些事实。宿主authorizer负责据当前权威记录拒绝旧映射/权限，Allow实现或仅打开开关不能证明这一部署条件。

没有新credential、verified lease或principal字段。库不会保留bearer来重新认证，也不把Agent HTTP verifier/固定三scope/复核周期移植为A2A事实。这个切片只保证**当前decoded-policy callback在规定阶段被强制执行**；真实token expiry/revocation、零时隙撤销、全部长流寿命、durable backend仍开放。

## 单个next-step与绝对期限

HTTP栈开始poll下一步时只建立一次 `idle_deadline = now + 原stream_idle_timeout`，覆盖pre-check、backend next等待及post-event check，三阶段之间绝不重新计时。每次auth callback还受原 `request_timeout`约束：其deadline是idle_deadline与该阶段开始+request_timeout的较早值；没有新上限、复核interval或等待预算。

1. 先检查terminal/count及shutdown；耗尽原event cap直接退役，不能为取更多数据重复授权。
2. pre-check通过后才poll backend.next。失败/不可用/超时不poll这一轮backend。
3. backend给出一个正常event后，再以同一原事实post-check；通过且实际clock/shutdown仍允许才把event交给SDK。等待backend期间撤销使已准备event也不可交付。
4. 每次成功await后再核实际monotonic clock小于相关deadline且shutdown未取消；不能只依赖timeout future和ready先后竞争。host同步阻塞callback不能被Rust timeout抢占，宿主仍须提供有界非阻塞实现。
5. 原idle等待语义仍受消费者poll/背压控制；不加后台周期任务。未poll、已交付到SDK/代理/网络缓冲的内容不在新撤销保证中。

最后成功post-check是这一event的策略线性化点，随后变更不承诺与TCP发送原子化。此设计不追回已交付字节、不回滚外部效果、不自动重试、不授予新资源访问。

## 退役与资源释放

启用路径须有独立owned state或等效所有权结构，stream/operation permit可被同步take/drop。不能仅设置terminal_seen并把live stream/permit携带到返回Err后的状态，等待客户端再poll才释放。

pre/post拒绝、不可用、check timeout、原idle timeout、shutdown、backend error、event cap耗尽在**返回终结Err前**drop backend stream及operation permit；backend EOF在返回None前drop；合法终态event必须先post-check，再于交付该event前drop。Rust owner Drop各一次；terminal状态不保留live句柄。挂起的check/next future均随步骤或HTTP stream被丢弃，不创建detached检查task。

真实客户端断连时，只承诺HTTP栈实际观察关闭并drop订阅body/stream后的所有权释放；不把调用方本地drop瞬间当成服务器已reap。backend Stream的Rust Drop不代表未知的durable Task/异步后台系统已取消或清理。现有operation permit从backend创建前到退役始终连续持有，不能release/reacquire制造泄漏窗口。

错误沿用现有safe映射：Forbidden调用 `map_authorization_error(Forbidden)`（存在性隐藏）；Unavailable及auth-phase request timeout调用原Unavailable映射。整体idle到期仍用原 `A2A stream idle timeout`，shutdown/eventlimit/backend error沿用原对应构造。HTTP头提交后不能改成HTTP403，交由当前SDK在各binding内生成一次终结stream error，再结束；不反射token、task输入或策略原文。精确wire投影由真实REST/JSON-RPC oracle固定。

## 验收方法及现有证据边界

`acceptance-matrix.json`是逐项可执行的必需矩阵，全部待实现/执行。新的fixture必须用实际公开 `A2aServer::router()` 起真实TcpListener，经HTTP+JSON与JSON-RPC两条公共路径读取SSE。现有a2a_client_contract真实TCP使用手写wire server，现有a2a_server模块测试走router.call；两者都不能冒充已覆盖这个新生产guard。

政策fixture可用可控latch记录每次原tuple；owned backend stream记录poll/yield/drop。它只测library强制点，不是生产IdP、committed backend、外部consumer或pilot。重点是：第一event后，第二步pre-check或backend等待处改当前策略；post-case使已准备第二event零交付；拒绝后无后续poll。

资源oracle同时需要：(a)直接poll strict next-step，观察Err/终态返回前drop与permit已经释放；(b)真实TCP保留第一response不关闭，以operation cap1发起另一仍获准taskB订阅成功，并观察backend Drop一次。仅看到TCP错误/EOF不足以证明adapter在yield前退役，因为SDK可能提前drop上游；仅helper单测也不算公共路径资格。

超时矩阵必须证实pre/backend/post累计跨过同一个原idle期限即拒绝、callback还受原request_timeout约束，不能让每一阶段重置idle。测试仅使用现有可配置合法下界（request>=10ms、idle>=1s）及有限harness等待；不提高生产默认/硬上限或栈。保留原边界、backpressure、终态、shutdown/drop、默认关闭与非订阅回归。

## 实施面与未接受项

预计仅改 `crates/stateknot-integrations/src/a2a_server.rs`（Options/私有严格订阅guard与原Subscribe路径）；API只是现有Options方法，无需新lib reexport。新增真实server TCP测试可放 `crates/stateknot-integrations/tests/a2a_server_subscription.rs`，保留原client/route tests。后续双语server guide、RFC25限定状态、CHANGELOG随实际实现证据更新，不在本设计操作它们。

当前没有实现、编译、TCP/DB执行或独立Standards/Spec结论。需另人审阅此设计及实施，再按最终源码执行真实矩阵/完整门禁。这个切片不证明动态映射后端、凭据再认证、跨副本即时撤销、durable A2A后端、stdio或R1完成。
