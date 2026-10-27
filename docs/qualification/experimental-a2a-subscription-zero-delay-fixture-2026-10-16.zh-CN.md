# A2A 订阅零延迟夹具限定资格记录 — 2026-10-16

本项仅修改 `crates/stateknot-integrations/tests/a2a_server_subscription.rs`。测试 Policy 的零延迟 Allow 现在在首次 poll 返回 Ready；非零 delay、同步 block、pending 与 gate 保持原语义。新增控制从真实 HTTP 的 HEALTHY/TASK 授权记录 Clone 请求，分别只 poll 一次 fresh Policy future。原 17-ID / 两 binding 矩阵、81 个原断言、10 ms / 40 ms 等原 timeout、生产代码和资源预算均保留。失败响应补充 binding、case/variant、关键 Setup 与既有 phase counters，并有界输出脱敏 HTTP body 或已解析 SSE；正常响应不额外读取。

最初 source-v1 因 unused `Future` import 在严格编译时 exit101，未执行测试，不能当作 red。source-v2 删除该未用 import 并格式化后，真实控制测试 exit101，原日志明确为 `task-b Pending` / `task-a Pending`。随后仅将夹具的 `else` 改为 `else if !delay.is_zero()`；同一控制实际 exit0，完整 subscription target 实际 10 passed / 0 failed / 0 ignored / 0 filtered。两项失败原件与成功原件分别保留。

最终冻结源码为 933 文件，tree `577fa6e6dac49dad92d37eb0683bfaf43a673e86f99d3ad47111b51b26a9104a`；542 个编译输入的生产者原行序摘要为 `550a257ada181f8686648fb4fb4fbbac324250208d22841d14220eb54abda956`。相对原 Wait 冻结树，唯一变化为上述测试文件。六项严格本机门禁实际全部 exit0，总耗时 329.68 秒：工作区 100 个 summary / 2,124 passed / 0 failed / 6 项原有 ignored / 0 filtered，独立文档测试 10 组 / 48 passed；格式、Clippy、Rustdoc 和固定 nightly 检查均通过。普通门禁为 opt0、保留调试断言和溢出检查，未扩大默认线程栈。

**最终组合 Native 的独立追加范围。** 相同最终源码新编译 Runtime：Core、Store、Runtime library 与 postgres harness 四项均 `fresh=false`、features 空、opt1，并保留断言与溢出检查。记录的二进制 SHA 为 `8b24dc6926fecc84b9d92acdc85c5c2ff106d33486ea66872ef11b9c27fa5f22`。193 个库存入口不是 193 个通过案例；本轮仅 PostgreSQL 16.15 / 17.11 各实际执行一个原 Wait exact case，均 1 passed / 192 filtered，分别 16.28 / 17.46 秒。各有两个完整 19 字段 COMMIT-cut marker：真实 SIGKILL/reap、22 表对照、授权恢复后 fresh Driver、Wait 应用零复派发、successor/parent 各一次；原请求 Return 后 ACK 仅在 committed 分支验证。宿主为 macOS aarch64，数据库在 Linux 容器中，不能称 Linux Runtime profile 通过。前后持久性设置 `fsync/synchronous_commit/full_page_writes` 都为 on，原容器身份一致。没有继承旧 Wait 的二进制或执行资格。

Native 首次预检因输入行序断言 exit1，尚未启动 Cargo；原输出目录保留。后续只按 path 排序同一 542 项 path/bytes/hash，得到 Native ordered 摘要 `d88e7d73b5b08eeda7bf8d98480be8ad2e68e3b9b4513a002b4742c820f02b16`，不表示源码或编译输入集合变化，六门原件未改。首次预检失败也不称实际编译错误。

独立 reviewer `/root/a2a_fixture_review_oct16c` 对原源码与记录的 red/green 完成 Standards/Spec 审查，剩余 finding 均为零；其收据 SHA `3337f4f77fca400137eef6f8c02ab8d572eccb184f55c5d6e33f8bb9a40ceb44` 保留原件。该审查由同一独立 reviewer 完成两轴，不冒充两位 reviewer 或新的测试执行。

原 CI 的三个失败信号仍独立保留：run38875878233 的 macOS `ready_race` HTTP500、同 run Windows `healthy` HTTP500，以及 run38877511506 / head99878cf 的 Windows `healthy` HTTP200 后首 SSE task.id 为 Null。原件缺少 error body/event，故本次 fixture red→green 只证明零延迟语义，不证明这些 CI 失败的根因已确认或已修。跨平台实际 CI、完整 Native、容量、完整 A2A backend/IdP、部署与 R1 仍未由本项关闭。

[机器范围](experimental-a2a-subscription-zero-delay-fixture-2026-10-16.json)绑定[原件索引](evidence/experimental-a2a-subscription-zero-delay-fixture-2026-10-16/artifact-index.json)。`python3 -B evidence/experimental-a2a-subscription-zero-delay-fixture-2026-10-16/verify.py` 仅用标准库离线核验所有 gzip 还原后的完整原件、失败分类、源码/输入摘要、六门日志、Native 编译/库存/marker/持久性与文档绑定；不启动 Cargo、数据库或远程。可选 `--workspace-root /absolute/workspace` 精确核验全部 542 个最终编译输入，供采用后执行。证据包保存日志和 manifests，未打包二进制、target 或完整 workspace 副本。
