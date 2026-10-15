# 实际旧 writer source32 升级回归

第二红来自测试构造错误：当前36 writer已写入V3原条件事实，原回退guard正确拒绝把这些事实重建成32。原exact测试在PG16/17均以Pg22012失败，完整日志保留；生产guard未改。

修复只改隔离副本的该测试及Wait oracle测试，增加真实旧fixture两个固定文件。独立源921文件/tree `343eaa8ae7299a520fa2858fa2983025116b8b3ea55926c1ad8faef55df6508d`，相对原919的生产Rust/Core/Store、全部1..36 migrations、配置、其余测试逐字相同。严格Store构建0/fresh，binary `de221bb45fcb9bd2efe50ebd9355bca18b302bbea6113604c06b09ef4f9c2bc7`，原配置/default stack。

旧producer来自899文件schema35+A冻结源，仅追加测试exporter；实际Wait及Return业务调用、原revert至32、原assert32保留。PG16/17各1/1通过，实际账本1..32全success、50业务表、scoped barrier V1×1/V2×1、无V3。两个原dump与metadata均保留。本回归双PG统一使用PG16实际snapshot，非分别生成新期望。原dump361875字节，SHA `2921a7cd08e9458a7bcbed9bf6225a7e2d0f48d5230d6ada35a28258b2dbc990`。仅剥两条psql客户端restrict行，所有其他字节不变；原bytea逐项定长打包348项/151642字节并有offset/length/SHA清单。未解析再编码任何canonical row，也未做V3→V2转换。

新测试先在独立空36库按原guards重建32，在单事务用原pg_dump SQL恢复真实旧数据及原32账本，逐一核对所有trigger状态与空32一致，关闭dump连接清其session设置；再使用真实MIGRATOR升36。原checksum/双Run14表facts/Wait行/Return reader/active CP/取消与wholeClosure/拒绝回退与rollback后closure等断言保留，另新增完整wholeWait reader和前置V3 guard假值认证。逐项表见assert-ledger.json。

| PostgreSQL | 原919 exact | 新921 exact |
|---|---|---|
| 16.15 | 0 passed /1 failed，5.626s，Pg22012 | 1 passed /0 failed，6.329s |
| 17.11 | 0 passed /1 failed，3.190s，Pg22012 | 1 passed /0 failed，6.569s |

以上是实际schema35 writer生成的数据加source32 schema重建，不能声称是实际历史schema32二进制或released N−1/N−2资格。当前只授予严格Store构建、fmt和这一个exact回归的双PG资格；完整Store/Runtime/静态六项均需重新验收，不继承旧绿。原919完整双链仍运行，源码、binary和红均未覆盖。
