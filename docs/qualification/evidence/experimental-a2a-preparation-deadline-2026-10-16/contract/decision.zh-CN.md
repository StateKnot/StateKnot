选定下一项实际生产修复：认证后的真实 JSON 输入和初始策略共用一个原 request_timeout 绝对期限。当前 a5997b… 中，canonicalize_request_payload 的网络 body collect 以及 prepare 的 authorizer/admission 均可无限 pending；REST push 还有 SDK Json<Value> 网络 body 路径。锁定 SDK router 没有隐藏 timeout。该结论是源码事实，尚未执行产品 red。

v2 的 afterdecode-only 契约保留为历史，不称完整 finite ingress。v3 由 root 确认 body 408/plain、policy generic operation-timeout wire error；认证和 backend 原独立预算保留。一个私有 task-local D 足够，不用 outer next.run timeout、phase-notify、重置期限或新增 API。

锁定版本实为 Axum 0.8.9、axum-core 0.5.6、http-body-util 0.1.5、a2a-server-lf 0.4.3。最小兼容 push 方案是原 configured DefaultBodyLimit 外移至 boundary 外层，再复用原 Bytes extractor/JsonRejection 类型；只插 extension 不提前授权或读取 body。push 原字节与 headers 交 SDK，错误也保持 SDK buffer/syntax/ProtoJSON 各自分工；允许 Origin 的原拒绝响应头不丢。>2MiB 正控必须证明没有误用 Axum 默认 limit。

现有真实 TCP subscription fixture 可复用，不需要外部 IdP/DB/OS profile，也不碰被拒绝的 SQL37/Root metadata 变更。下一事实问题是：在原完整错误/正常正控不变的同时，慢输入与累计两阶段策略等待能否确实按单 D 终结、释放请求容量且从未占用 backend permit。acceptance.json 将此问题拆成可执行 oracle。生产 authoring 与实际 red→green 由 root/测试作者接续；本审计不写 main、不编译、不自行授予独立 review 或新产品资格。
