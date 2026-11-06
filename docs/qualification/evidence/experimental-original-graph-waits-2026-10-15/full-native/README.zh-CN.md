# 最终922 Schema36+A 验收

冻结922文件/tree `88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb`，从头重新执行六静态门禁及双major完整Native四suite，另单列强制logical backup-restore。生产/原1..36、default stack、leases/quantum/pool/断言保持；2s lease/2.1s Return delay测试逐字未改。所有编译先串行结束后才启动Native，两major并行，每major内部串行。

| PostgreSQL | Store264 | Runtime189 | integrations2 | example1 | 强制backup restore |
|---|---|---|---|---|---|
| 16 | 264passed/0failed，exit0，1500.99s | 189passed/0failed，exit0，1703.39s | 2passed/0failed，exit0，2.02s | 1passed/0failed，exit0，2.81s | 1passed/0failed，marker 1，qualified=True |
| 17 | 264passed/0failed，exit0，1470.31s | 189passed/0failed，exit0，1690.53s | 2passed/0failed，exit0，3.83s | 1passed/0failed，exit0，4.65s | 1passed/0failed，marker 1，qualified=True |

六门禁全绿=True，完整四套双版全绿=True，强制backup双版全绿=True。合计libtest执行914（四套912+单列backup2），914passed/0failed/0ignored。Runtime full suite中的backup/PITR conditional早返回不能当作drill；单列backup仅凭required env、真实container健康/pg_isready、exact1case、唯一完整marker和原全snapshot/跨租户拒绝/真实noninitial replay断言均过授予。PITR无资格。

所有实际source/binary SHA、test list/count、container/server/独立DB身份、命令、原日志、exit与耗时均在summary.json、pg16/pg17/<suite>/receipt.json；最终两主库实际ledger1..36全success、schema36、54业务表。原919完整红和原exact红、921最小绿均保留，没有覆盖。

资格只对应本922批次。没有继承旧919/921的Native资格；没有变更或授予CI原30分钟预算。没有从本批次声称额外role/performance、PITR、所有R1或全部生产里程碑已达标。fixture来源与重现步骤随源码SQL/JSON旁README交付；旧83559生产257文件精确核对及README实际patch复现证明另见上级目录。
