# 当前源码非空 SOURCE35 → 36：原单例双版本实测

仅执行现有 `root_graph_barriers::trusted_upgrade::populated_source35_upgrade_preserves_registration_bytes_and_legacy_identity`，每版恰好一次。`binary-list.log` 保存实际 `--list`，`test-selection.json` 固定精确名字和已保存严格Store binary SHA256 `fc0f64ecc2dcd761d6ac0423e56ff88cce0147fb4c95e941a1adf7188e70dfc9`。919文件源码 tree `bca1889badb0d3318fcca66490d153a2fec08ab28ab205a474db44f9b6510d45` 前后完整清单相等，没有改源码、fixture、断言或资源。

| 实际服务器 | 原测试结果 | libtest耗时 | wrapper总耗时 |
| --- | --- | --- | --- |
| PostgreSQL16.15 / 160015 | 1passed / 0failed / 0ignored | 2.61s | 2.78s |
| PostgreSQL17.11 / 170011 | 1passed / 0failed / 0ignored | 2.52s | 2.70s |

容器标签均为本任务 `stateknot.task=r1-oct15`；真实container ID、image、127.0.0.1端口55416/55417和服务器版本已记录在各 `pg16/pg17` 目录。每版新建原本不存在的独立base DB `stateknot_r1_a36_upgrade_base_oct15`，开始业务表数0，不使用Runtime数据库。原fixture另创建自己的随机 `stateknot_v35_<RunId>` 数据库；成功后由原cleanup删除，其前后数据库清单一致。base DB保留，结束时ledger恰好1..36全部成功、54业务表。

原测试从当前schema36写入真实legacy V1 Continue、Terminal、future Interrupt Wait；调用原测试还原脚本恢复完整SOURCE35 guard/table/catalog而非只删ledger；确认旧35 checksum/success和非空原注册，再拒绝新版直连35并实际SQLx迁移36。全部原断言通过：旧业务表每行摘要及数量不变，1..35 ledger checksum/success相同，legacy identity backfill严格对应原注册且不发明graph owner，原完整Root读与Unavailable回调下lost-ACK保持，新增V3后反向去除原condition source被拒绝并完整rollback。默认栈、原lease/deadline和全部原参数保持。

`run_exact.py`、每版runner、完整native.log、manifest、df、环境和receipt以及汇总 `summary.json` 都保存在target之外。会话68788/99324已exit0。只运行此单例各版，没有新增并发Runtime或role测试。

这是**当前源码SOURCE35重建→36**回归；没有启动真实历史N−1/N−2发行二进制，没有独立验证受限runtime/retention角色，不能替代完整Native、历史升级/恢复、性能或R1资格。其他既有红保持原状态。
