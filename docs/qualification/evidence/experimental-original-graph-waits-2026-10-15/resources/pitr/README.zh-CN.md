# 独立强制PITR最终证据

最终922/tree88dd9d36；原四suite+mandatory backup双链终态后，使用同fresh922 Runtime binary、清DB env、只设REQUIRE_PITR与固定image串行执行，未改源码、默认栈或原profile资源/断言。

| Major | 实际server | Exact /marker /exit | seconds |
|---|---|---|---|
| 16 | 16.14 (Debian 16.14-1.pgdg13+1) | 1pass/unique1/0 | 10.082 |
| 17 | 17.11 (Debian 17.11-1.pgdg13+2) | 1pass/unique1/0 | 10.212 |

固定CI镜像实际Linux/arm64，harness在Mac宿主。完整marker认证真实base backup+pg_verifybackup、manifestSHA、WAL归档、named restore point、排除目标之后写入、完整durable snapshot、跨租户拒绝和noninitial replay。上述durable invariants双绿，原exact和marker证据保留。

资源生命周期未合格：原Drop后PG16留1个自有wal卷 `stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1`，实际CreatedAt 2026-10-15T11:58:13Z。当前只读inspect确认无引用容器、同UUID source/target容器均不在；PG17无额外新增卷。全部先前container身份与volumes名字保留。该卷已按root指示保留，未执行额外stop/rm/prune或DB操作。状态为exact_execution_passed_resource_cleanup_open，不把PITR数据正确性绿称作cleanup成功。实际rm失败stderr未捕获；异步--rm移除竞态只作待证假设。

独立证据不修改原四suite/backup报告范围；不授予CI30min预算、额外性能或全部R1/生产里程碑资格。
