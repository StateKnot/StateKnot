# Schema36+A 原919完整双版Native四套验收

原919源码（tree `bca1889badb0d3318fcca66490d153a2fec08ab28ab205a474db44f9b6510d45`）保持逐字冻结。Store与Runtime使用先前strict原source保存binary；integrations与example从同一919 source新构建fresh artifact，四个binary的SHA和实际--list数量均核对。每版新独立ownDB，实际PG16.15/17.11、task r1-oct15容器标签/端口/镜像已确认。原default stack、lease/quantum/pool/CI资源和断言不变，每major按Store→Runtime→integrations→example串行，两major并行。

| Suite | PG16 | PG17 |
|---|---|---|
| Store264 | 261 passed/3 failed，1591.26s，exit101 | 262 passed/2 failed，1585.56s，exit101 |
| Runtime189 | 189 passed/0 failed，1725.65s，exit0 | 189 passed/0 failed，1723.82s，exit0 |
| integrations2 | 2 passed/0 failed，3.055s，exit0 | 2 passed/0 failed，2.700s，exit0 |
| example1 | 1 passed/0 failed，2.533s，exit0 | 1 passed/0 failed，3.086s，exit0 |

合计libtest912项，907passed/5failed/0ignored。两条wrapper均exit1以保留Store红；Runtime、integrations、example全部继续至终态。实际Default7 Runtime七层公开case双版绿。所有suite日志/exit/count/SHA都在pg16/pg17/<suite>/receipt.json与native.log；四binary身份见four-binary-identities.json。最终两库账本1..36全success、schema36、54业务表。

Store红全部保留：双版Wait replay原500ms时间假设、双版以新V3数据回退构造32被正确guard拒绝；PG16另有deferredReturn case在setup LeaseExpired，PG17同原suite通过。前两项独立最小测试-only修复见wait-oracle-v2与source32-real-old-producer-v1，不能替换本轮原红。第三项原exact双版各1/1绿，真实target触发Timeout/PgSleep，原2s lease/2.1s delay未改，不把单次setup失败称作生产断言缺陷。

资格边界：本报告仅对应原919，不能泛化为后续921/922全源码资格。Runtime中的backup/restore和PITR两个conditional case可早返回，本轮无drill marker/required env，libtest绿不授予这两项独立drill资格。原CI postgres job预算30min；本机原配置链分别3323.06s/3315.83s，约55.3min，未取得该CI预算资格，也不推断不同CI硬件耗时。
