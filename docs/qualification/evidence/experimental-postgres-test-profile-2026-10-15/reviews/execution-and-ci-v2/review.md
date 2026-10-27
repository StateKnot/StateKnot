# opt1 执行与 CI 候选独立事实审查

结论：**0 个严重问题、0 个剩余可执行发现**。v1 CI 注释的低优先范围问题已由 root 保留 v1、另建 v2 单行注释修正，逐字复核闭合。公开包尚未产出/封签，本结论未审公开发布件。

实际核对 execution-seal 的273文件+8外部绑定全部 hash/bytes，原6严格门禁 receipt/log 全0、workspace98suite2073/0/6及10组doctest45/0/0；五个 saved program 与原 Cargo JSON fresh=false/profile opt1/debug0/assertions+overflow true 精确一致。双 PG16.15/17.11 的10个 suite 真实全0，每版264+189+2+1+6=462 reported passes，全部0fail/ignore/filter；Runtime15类 marker 与 factsMCP2+A2A2 从原日志独立重读，与原qualification精确一致。Runtime内 backup/PITR函数的条件返回不构成实际drill资格。

原 runner **exit1** 必须原样发布：全部产品命令与最终输入检查之后，final guard读取缺失 CACHEDIR.TAG 触发FileNotFoundError；原summary.json不存在、native-summary保持passed-so-far。后续只读audit显示原PID/保存程序/缓存无活跃、锁free，两个缺TAG缓存保留；不得称补跑runner成功、授清理资格或抹除原红。后续audit的aggregate工具exit0仅有原工具回执（执行owner确认chunk4fe9fb），无单独保存aggregate command receipt；包内真实逐项command/stdout/stderr和锁核对足以支持“审计完成”，不应发明第二个验收runner。

CI v2实际SHA f6a2e9b648b35bf25607555a22ae115fcfbb932f8d1ff427df620f1f5624b9d7；删除新增9行精确还原原workflow。新增6环境变量，postgres原53steps/31run/30m、全部commands、fixtures/resources及其他job字节不变。v1→v2仅第二注释改为Build/test profile范围。原six-gates-summary短语dev/release unchanged只可按原Cargo配置/opt-level未改解释，不能遮蔽command-local dev-debug0/incremental0；semantic/report已有精确边界。

可采用的是**本机 workspace all-features union 的限定产品执行事实**及待实际CI运行的9行候选；不是默认特性依赖图、Ubuntu、完整30分钟job、backup/PITR、容量/CPU或R1资格。package.features=[]不证明依赖图同CI。源923实际map精确tree3a84c68e…，actualmain531完整供应输入集合精确ddaf1aba…；保守供应集合不是compiler-read闭包，旧opt0 whole2182不转授whole923。

本审查无编译、程序执行、网络、DB、Docker、TAG/缓存/runner/main/index写入。自有只读辅助脚本首次因Path组件序与字符串序列表比较而assert失败；原错误与修正map核对保存为独立诊断，不是产品失败或源变化。
