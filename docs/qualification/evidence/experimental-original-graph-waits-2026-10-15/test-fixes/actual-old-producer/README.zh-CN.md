# 实际schema35 writer 导出source32 fixture

冻结父源 proof-base-erased-source（899文件），仅新增独立测试exporter，Store/Core生产源及全部迁移逐字未改。

原测试业务前缀创建真实Wait和Return，执行原revert_graph_frame_closures并assert32后结束。两个Native导出均1/1绿，账本32/1..32/all-success、50表、scoped barrier V1×1/V2×1，没有V3。fixture DB和原dump均保留。

| PostgreSQL | Producer Native | 原dump bytes | 原dump SHA256 |
|---|---|---|---|
| 16 | 1 passed /0 failed，3.627s | 361875 | `2921a7cd08e9458a7bcbed9bf6225a7e2d0f48d5230d6ada35a28258b2dbc990` |
| 17 | 1 passed /0 failed，3.048s | 361871 | `79802f7b54680e8b8718ece12f3764520467bdd7d45398f696bd382b07d24a2b` |

各export目录含原metadata.json、data.original.sql、actual-source32-identity.log、original-bytea.pack和original-bytea-manifest.json。bytea清单记录每个原SQL literal位置以及pack offset/length/SHA；348项/151642bytes，重复原值保留，不经过JSON decode/re-encode。

新36回归统一加载PG16的同一真实固定snapshot，双PG新reader验收见../source32-real-old-producer-v1。原SQL只有两条psql客户端restrict行被剥用于raw SQL执行；其他字节包括全部INSERT及ledger原值不变。

资格边界：实际schema35 writer + source32 schema重建；不能视为实际历史schema32 writer二进制或released N−1/N−2资格。未修改生产guard、未做V3→V2重标或转换。
