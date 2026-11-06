MATRIX.zh-CN.md 的 Caller rebind 行“4 组件异常”应为“5 组件异常”：
frame_callers.rs:384–390 实际依次覆盖 run_events、graph_frame_caller_bindings、
run_attempt_claims、node_attempts 的 INSERT，以及 runs 的 UPDATE。
这是报告计数纠正；已冻结候选源码、manifest 和作者 receipt 未改，无新增执行。
