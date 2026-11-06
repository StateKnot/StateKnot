# PITR 清理确认：精确候选资格，2026-10-15

独立的单测试候选通过了 Runtime 定向严格 Clippy、全新 Runtime `postgres` 构建，以及固定 PostgreSQL 16.14 / 17.11 镜像各一次强制命名 PITR。每次均为 **1 通过 / 0 失败 / 0 忽略**，仅输出一个完整耐久证据 marker，且 `resource_cleanup_verified=true`。每次所属三个命名卷和两个命名容器实际均已不存在。这只确认两个新 UUID 的成功路径清理，不保证全部失败路径或整个发布。

候选共 922 文件，tree 为 `82fc93f1e66fd13703bd9ae8bff268460829324cebe19ee94eaca497d87aed7d`。相对于最终922的 `88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb`，只改变 `crates/stateknot-runtime/tests/postgres/child_join_pitr.rs`；Runtime/Core/Store 生产源码与 SQL 逐字相同。包内保存源归档、两份清单、精确 patch 和独立定向审阅的原始字节及哈希。原 preparation 文档保留创建时尚未编译的历史状态，后续执行回执才是本次运行结果。

新增显式清理在成功 marker 前确认精确命名容器不存在、卷删除且不存在。原耐久断言、源容器停止30秒及各清理10秒预算、默认栈和 best-effort Drop 均保持。Drop 仍忽略删除失败，也可能自身失败；本次没有认证 panic、命令超时、Docker daemon 丢失、主机故障等所有失败路径的清理。此前最终922 PG16 遗留 WAL 的历史开放结果不改写。

新二进制 SHA-256 为 `994be9d00fe567660a847627c82b96a5e51c45e9ded56b0ba54e6760b75f0a32`，Cargo 对 Runtime 库和测试产物记录 `fresh:false`。使用 Rust 1.88.0、`--locked`；定向 Clippy 保持全部 targets/features、`-D warnings`、调试断言和溢出检查。`--list` 的189项与最终922精确相同，但本次只实际执行指定 PITR 一项。为得到新产物，仅在检查为空闲的缓存内清除 Runtime 包产物，复用依赖，未复制 target。

本轮等待独立 A2A 整条资源链真实终态回执及无活跃进程证据后才开始编译。PITR 只设置 `STATEKNOT_REQUIRE_PITR_TESTS=1` 和固定镜像 digest，未继承既有数据库测试环境。宿主为 macOS/aarch64，数据库容器为 Linux/arm64；这不是 Linux 宿主认证。

PG16 UUID 为 `01a13f89b1647070894c494a3a7f6099`，PG17 为 `01a13f89d42073c089c09c09ad3b7c4e`。只读 Docker 前后清单显示：每个 UUID 的三个卷、两个命名容器运行前后均不存在；没有新增容器 ID 或卷名，既有 ID/卷名全部保留。历史卷 `stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1` 仍在，inspect 原始输出前后逐字相同。runner 未执行额外人工 stop/rm/prune 或历史清理。终态限定审计确认本轮记录的五个 PID 不存在、缓存无打开文件、Cargo 锁可取得。缓存为后续组成验证保留，实测大小与最终 `df` 存于 JSON，不把目录大小声称为已释放空间。

首次终态只读 `ps` 被 sandbox 拒绝，限制记录保留；随后限定范围的升权只读审计成功。没有为该审计重跑编译或 PITR。

运行 `python3 verify.py` 可完全离线验证压缩及解压后哈希、922个归档源码输入、相对父候选仅一文件变化、回执与原日志绑定、Cargo 新构建、精确189清单、两次终态摘要/唯一完整 marker 和全部资源差异。可选 `--source /绝对/冻结源码 --binary /绝对/已保存二进制` 进一步比对实际冻结目录和二进制。校验器不联网、不启动子进程、Docker 或编译。原日志/源归档确定性 gzip 保存，原回执 raw-log 哈希不变；大二进制及编译缓存不进入紧凑目录。

这份证据不等于执行189项整套 Runtime、四套完整 Native、完整 workspace 六门禁、组成 main 验证、全 CI、容量/性能认证或关闭 R1。验证没有修改 main 源码、公开 API、依赖或资源政策；后续组成候选仍需自己的输入绑定和资格记录。
