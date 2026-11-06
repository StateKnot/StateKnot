# Wait oracle v2：生产源码不变的独立测试修复

父为冻结schema36+A、919文件/tree `bca1889…`。v2仍919文件，仅 `crates/stateknot-store-postgres/tests/postgres/frame_application_replay.rs` 的这一条test函数改变；其余Rust/SQL/配置/其他test逐字相同。新tree `7c95defc78ea3d9829d30139baea143bb49ba10d2eafdec9586b8396b70de5b7` 的算法显式记录于manifest。所有v1全链/双红/孤立绿及原始fixture保留。

根因证据是原输入due在prepared全入栈/认证之前设为now+500ms，而新合法V3按实际提交event clock分类。v1全suite PG16/17的event分别晚于due54.659/9.317ms，实际graph_elapsed/noTimer；原assert remaining1错误地预设必定注册。独立原例也可绿：event比due早3.703ms，真实graph_registered。它是时间条件决定的fixture假设错误，不能称稳定生产代码缺陷。

v2 oracle仅取原due与实际saved.event.recorded_at，不读取profile/effect来选期望值。future保留原remaining1与完整真实fire流程；elapsed逐项独立核canonical原spec/intent、实际physical缺失和真实Interrupt resolve，随后两者共用全部原fence/replay/transition/reducer断言。原500ms、所有lease/quantum/caps/配置不变，没有诊断println残留。

| 原assert或要求 | 保留/新增 |
|---|---|
| 原now+500000µs、Waits IDs/kinds、prepared/suspend | 逐字保留；只clone原输入供独立核验 |
| Wait期新claim拒绝 | 原assert保留，两分支都仍有真实Interrupt阻塞 |
| Interrupt请求与授权resolve调用 | 原调用完整保留；新增actual interrupt record resolution journal==真实resolved event |
| future remaining==1 | 由原due>实际saved.event.recorded_at选择后原assert原样保留 |
| future真实load Timer、等待原due、fire_timer | 原body忽略缩进精确相同；未改due、租约、延迟或target |
| 原successor fence与首次完整graph replay | 共用原尾段精确相同：Wait1/Barrier1/FrameCP2/leaf/superstep1 |
| 原实际terminal transition与第二次完整replay | 共用原尾段精确相同：Wait1/Barrier2/FrameCP3/leaf/reducer.calls1 |
| 原schema/disposition/result | 新增完整原NodeWaits与实际Pending result control equality；原Wait disposition及Waiting receipt独立核验 |
| elapsed input证明 | 新增canonical condition bytes/checksum、完整原Timer intent与spec digest、实际owner/source/successor绑定 |
| elapsed物理事实 | 新增graph_elapsed、typed Timer lookup缺失、registration count0、firing count0；resolve后Active0/真实head/无lease/无quarantine，后半共用全部原assert |

严格Store新binary `019e6d61f94872aa04150a2b710aa1bfa3dabcbdcae982292b635fdb7ff23029`，Cargo fresh:false，package_id为独立v2源码路径。第一次compile101的源tar/manifest/patch及3个test-local诊断错误保存在initial-compile-red*与strict-store-build-v1；修正限定类型名和EventId值访问后strict-store-build-v2为0，原记录没有覆盖。fmt检查0；其他static/full Native资格不继承。

| 精确Native原名测试 | PG16 | PG17 |
|---|---:|---:|
| 1 passed / 0 failed / 0 ignored |5.631s|5.221s|
| 实际oracle branch |future / graph_registered|elapsed / graph_elapsed|
| event与原due差 |早39.905ms|晚421.648ms|
| 真实Timer firing |J7|无；仅真实Interrupt J6|

两个分支的全部原common tail均通过；完整原行与canonical bytes保存于readonly-candidate-capture-v1，精确source/binary/command/exit见各native-pg*/receipt.json。完整v1 Store仍是真红：PG16 261/264、PG17 262/264，其余链继续，v2没有覆盖这些结论。

升级source32的V3回退拒绝红、PG16 Return-delay setup LeaseExpired另行处理，不混入本patch。全Store重跑依父任务决定，当前只授予此精确双版Native和严格构建/fmt范围。
