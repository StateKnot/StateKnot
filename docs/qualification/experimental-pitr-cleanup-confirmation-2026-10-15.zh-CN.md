<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# 实验性 PITR 清理确认

2026-10-15。独立的test-only候选通过Runtime定向严格Clippy、fresh构建，以及固定
PostgreSQL16.14/17.11镜像各一次强制named-PITR。实际各为1 passed/0 failed/0 ignored、
唯一完整marker且 `resource_cleanup_verified=true`；每个新UUID的两个命名容器和三个卷
均实际不存在。[English](experimental-pitr-cleanup-confirmation-2026-10-15.md)。

实际测试源码含922文件，tree为
`82fc93f1e66fd13703bd9ae8bff268460829324cebe19ee94eaca497d87aed7d`。
相对[schema36源码](experimental-original-graph-waits-2026-10-15.zh-CN.md)，仅改变
`child_join_pitr.rs`；Runtime/Core/Store生产源码及SQL逐字相同。fresh保存的Runtime
binary为 `994be9d00fe567660a847627c82b96a5e51c45e9ded56b0ba54e6760b75f0a32`。
189项库存与父源码一致，但本轮每版只实际执行精确PITR一项，没有运行完整189套。

新增显式清理在成功marker前，以原30秒/10秒预算确认精确命名资源不存在。原耐久断言、
默认栈/资源及best-effort Drop保留。本次只证明两个成功清理路径；panic、命令超时、
Docker/主机失败及其他全部失败路径不据此获得资格。旧PG16 WAL卷
`stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1`仍保留，旧schema36清理开放记录
不重写。macOS/aarch64进程配合Linux/arm64数据库容器不构成Linux宿主资格。

[原中英文报告及校验器](evidence/experimental-pitr-cleanup-confirmation-2026-10-15/README.zh-CN.md)
与[机器记录](experimental-pitr-cleanup-confirmation-2026-10-15.json)保留121个原证据文件
及精确hash，原封签不变。`verify.py`完全离线，不编译、不运行资源操作。本交付没有
新建归档，不添加binary或构建缓存。

[A2A记录](experimental-a2a-request-facts-2026-10-15.zh-CN.md)保留独立531输入绑定。
两项组成后的main仍需自己的**原opt0完整六门禁，本记录中的结果为待执行**；不把任一
旧记录转授为组成候选的完整Runtime189/四套Native、完整CI、全部失败路径清理、容量/
性能或R1接受。RFC-0022仍为Draft。
