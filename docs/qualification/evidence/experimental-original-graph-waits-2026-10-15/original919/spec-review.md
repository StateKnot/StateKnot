# Spec review: schema36 Ready + A-erased

只读审阅固定 `1fccf1816a2549d8ca7296b635434b52b30140e9`、Ready36 `c9e6ed1a81da511246e36d16fb277d958e146f747b59109854a4644e18d5875c` 与组合919文件 `bca1889badb0d3318fcca66490d153a2fec08ab28ab205a474db44f9b6510d45`。逐文件清单、固定点差异与引用 SHA 在 `spec-review-fixed-basis.json`、`spec-resource-preservation.json`、`spec-review.json`。本次没有编译、数据库执行或源码修改，Spec 新可执行源码发现 **0**。

| 规格要求 | 当前源码检查 |
| --- | --- |
| RFC0022:846–899：原绝对 Timer、真实提交时钟、expired Interrupt不可批准 | `graph_waits.rs:111` 从完整result/control重建；`due_at<=event.recorded_at` 保留独立elapsed事实，future才注册；Interrupt到期报错，未改due/发明firing。 |
| 原完整Root来源 | `root_graph_barriers.rs:1281,897` 与 `trusted_graph_root.rs:91` 保留完整checkpoint、真实admission或FIRST/Start/INITIAL认证；V3形状要求正向且唯一来源。 |
| 全inventory身份 | `graph_waits/database.rs:292` 精确比较完整物理列与canonical condition bytes；ordinal、每owner index、owner/spec/source/successor均重建；0036的tenant+WaitID唯一仲裁覆盖旧注册与新事实。 |
| cached Return必须含真实组件 | `returns.rs:728,429` 命中accounting prefix后仍核对whole anchor并实际重读caller completion、Pending result与bindings。A没有触及该路径。 |
| Ready创建与后续普通lease操作 | `0036:611` 仅journal advance时限制lease/scheduler等创建期块；same-journal生命周期、revision、Wait-index仍冻结；原open-frame/Waiting/discharge守卫仍运行。acquire/renew/release/authorize_worker四方法与1fcc逐字节相同。 |
| fresh postguard | `root_graph_barriers.rs:1759` flush后重载完整实际parent/successor/results/whole edge和Run，再检查原lease/clock/deadline。`barriers.rs:1781` 保留完整实际edge/Wait-set、原lease等值、最终原fence/deadline/budget；不以preflight副本替代实际组件。 |
| Runtime的Ready不授权执行 | `driver/barrier.rs:73` 使用实际Committed/Idempotent receipt分类；`driver/frame.rs:509` 更新durable观察，继续原quantum和claimed plan。`lifecycle.rs:1153` 用原fence正常release，superseded时返回原receipt且release=None。 |
| 组合及资源边界 | A只在公开只读repeatable-read load内借用刚完成的完整普通scoped base证明，完整Start/Completion/result/bindings仍检查；generic writer/postguard仍传None。Ready专属文件与父清单精确一致，1..36原样、无B/37、Cargo配置/锁文件不变，没有lease、栈或caps提高。 |

已读取原Ready36回执 `../evidence/elapsed36-ready-v2-handoff/receipt.json`：28例中25通过、3失败（PG16 scoped absolute Wait、PG16/17真实角色完整工作）；原红全部保留。它们属于918文件父源码及其原二进制，既不能被本次源码审阅消除，也不能转移为919组合的Native资格。性能、默认栈、完整Native、upgrade/restore和R1仍须实际验证。

独立源码快照起于较早Ready父本，因此未带入1fcc新增的11个Driver资格报告/证据文件；这不是本次三文件组合的删除补丁。实际接入时保留已提交证据，只移植经过审核的变更，不整目录覆盖当前分支。
