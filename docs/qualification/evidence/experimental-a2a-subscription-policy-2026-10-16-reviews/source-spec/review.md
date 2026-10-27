Spec：0 项可执行 finding。已审最终 source-v5（930 项 `0e91c287…` / 编译 538 项 `43e9cb48…`），以冻结的订阅契约及 17 项矩阵为准；审阅者写过该契约，未写生产实现或回归测试。本结论只审实现符合性，不自称对契约设计的独立审核。

无参显式开关默认关闭、重复启用幂等，Clone/其他 builder 保留原限额。新增 guard 只接 SubscribeToTask；完整逆向归一恢复4e726原文件，原 prepare、unary、stream、bounded_stream、SendStreamingMessage 字节均未变。创建仍经过原授权/准入并持续持有 permit，不能声称队列期间撤销使 backend 从未创建。

每次被 poll 的 next 只建立一个 idle 绝对期限，涵盖 pre-check、backend 等待、post-check；每次 callback 又取 request 期限较小值。原 principal/tenant/identity/scopes/task/op/config 固定。每个成功 await 后核实际时钟及 shutdown；同步阻塞 callback 仍不可抢占，宿主必须有界且非阻塞。最后成功 post-check 后到网络发送的竞态、既交付字节、未 poll 的背压状态都不在即时撤销承诺内。

错误、EOF、合法终态、event cap 与 shutdown 使用 owned state 退役；直接单次 next 的资源断言证明返回前 drop，实际 REST/JSON-RPC TCP 保留旧响应并以 cap1 访问健康 taskB 验证可复用。17 个 ID 在两 binding 内逐项调用，集中于 1 个实际 TCP test family；另有 8 个 private test。没有把它们称为 34 个 Rust test、生产 IdP、durable backend 或外部 pilot。

EOF 修正只避免再次 poll 已结束的 unfold，保留首次 None 前释放断言。错误 oracle 仅在完整封闭 shape/类型/值及 RFC3339 UTC 校验后归一 SDK 自生 timestamp；其余字段仍全等。后续 lint/edition 修正仅改两份 fixture，v1→v5 生产 bytes 相同，无新增 allow、删断言或调大期限。旧失败原件保留。

独立核原六门 receipt、raw 与 runner SHA 全一致 exit0：100 个 workspace suite，2108 passed / 0 failed / 6 原 ignored；10 组 doctest、48 passed。原 sandbox bind 拒绝单列保留。这里只读取实际终态，没有重跑；不授新 Native、完整 CI、容量、stdio、durable A2A 或完整 R1 资格。
