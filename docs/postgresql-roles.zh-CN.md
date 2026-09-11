<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# PostgreSQL 受信任服务端角色隔离

[English](postgresql-roles.md)

`trusted-server-roles-v1` 是适用于 PostgreSQL 16/17、Schema 24 的可执行最小权限
部署配置，分离迁移、服务端运行时和公平调度预留记录清理账号。它**不构成不可信
Worker 或租户的 SQL 安全边界**：运行时仍能跨租户读取并更新控制面投影。不得向
用户、Tool、插件或远程 Worker 分发数据库凭证。RFC-0003 的 Worker 专用过程/服务
边界和 RFC-0004 的完整子任务验收仍未完成。

## 三类账号的权限

| 账号 | 允许 | 禁止 |
|---|---|---|
| 迁移所有者 | 拥有专用数据库、Schema、表和 invoker 函数；显式迁移及权限配置/审计 | 在本配置中使用超级用户、角色管理、复制或绕过 RLS；凭证进入运行时进程 |
| 运行时 | CONNECT、Schema USAGE、读取迁移元数据；精确 40 张框架表的 SELECT/INSERT；仅指定投影列的 UPDATE；UUID 校验函数执行 | DDL、临时对象、DELETE/TRUNCATE、修改不可变证据或身份列、修改迁移元数据、转授权、切换所有者、禁用触发器 |
| 清理 | 读取迁移元数据；预留记录表的 SELECT/DELETE；仅该表 `reservation_id` 列的 UPDATE | 运行时写入、读取或修改日志/Checkpoint、修改调度策略或游标、DDL、迁移 |

清理账号是**受信任的破坏性维护账号**。PostgreSQL 的 `FOR UPDATE SKIP LOCKED`
要求至少一列 UPDATE 权限，该列权限也允许真正更新 ID；此账号还能直接删除预留
记录。保留时间下限一小时、数据库时钟和批次数量限制由现有
`prune_scheduler_fairness_reservations` API 执行，并非 SQL 账号自身强制。因此清理
进程不得接受租户 SQL，也不能宣称数据库权限强制保留窗口。其他不可变账本没有
这样的 UPDATE/DELETE 授权。

本阶段同时修复了最小权限下无法运行的问题：节点尝试读取原先对不可变表加行锁，
从而意外要求 UPDATE。调用方已经持有 Run 行锁来串行化开始、完成和 Join 注册，
因此移除两个多余的不可变记录锁；提交键读取同样保留原有事务级键 advisory lock
和 Run 锁，不再锁不可变映射记录。Fence、日志 CAS、唯一约束和锁顺序不变，并有
真实受限连接的 24 路提交/节点完成竞争验证。

## 部署步骤

必须使用**专用数据库和迁移所有者**，不能直接套用到其他应用共享的数据库。
脚本会撤销该数据库 PUBLIC 的 CREATE/TEMPORARY，以及 `public`/`stateknot` 和
框架表、函数的 PUBLIC 权限，并调整当前数据库中该所有者的默认权限。它不修改
`pg_hba.conf`、网络规则、角色密码、全局角色属性或对象所有权。

1. 由 DBA 创建三个独立 LOGIN 身份，均使用 NOSUPERUSER、NOCREATEDB、NOCREATEROLE、
   NOREPLICATION、NOBYPASSRLS。数据库归迁移角色所有，框架对象也必须由它迁移创建。
   运行时与清理角色双向都不能有角色成员关系，包括 SET-only/管理成员关系；
   NOINHERIT 不足以隔离。角色组配置不属于此验收范围。若托管数据库强制附加成员
   关系，需要单独审核配置，本脚本会明确拒绝。
2. 通过密钥管理系统或交互式 `psql \password` 配置认证，不在版本库 SQL、命令行
   URL 或日志中写密码。由固定版本的 `PostgresStore::migrate_database` 执行迁移；
   它验证迁移校验和并关闭临时 DDL 连接池。
3. 配置 libpq service `stateknot_migration`，明确数据库、迁移用户、CA 与
   `sslmode=verify-full`；密码使用受限 passfile 或密钥托管。审核
   [权限白名单 SQL](../crates/stateknot-store-postgres/ops/trusted-role-profile.sql)，从仓库根目录执行：

```sh
PGSERVICE=stateknot_migration psql -X --no-password \
  --set=runtime_role=sk_runtime --set=retention_role=sk_retention --set=apply=true \
  --file=crates/stateknot-store-postgres/ops/trusted-role-profile.psql
```

替换为实际创建的角色名。包装脚本对变量使用 SQL 字面量引用，动态标识符使用
`%I`；必须以**非超级用户迁移所有者**执行。角色缺失、所有权不符、额外表、版本
不符、角色提权或成员关系都会失败关闭。旧库需要调整所有者时应另行审核迁移，
脚本不会执行全局 REASSIGN OWNED 或擅自修复集群权限。

应用模式会显式撤销遗留的表级和**列级**授权，再授予白名单，最后审计有效权限。
审计失败则整个事务回滚；没有新增 SECURITY DEFINER 或运行时提权路径。
事务配置 5 秒锁超时、30 秒语句超时，超时后应调查争用，再重试完整步骤。

4. 将同一命令改为 `--set=apply=false` 执行不改变 ACL 的审计。它检查有效的
   表/列/函数/Schema/数据库权限、转授权、危险复制参数、成员关系，以及全局和
   Schema 局部默认权限。非零退出必须阻止发布。
5. 应用只使用运行时凭证调用 `PostgresStore::connect`，保留默认 VerifyFull TLS。
   清理任务使用独立进程和连接池。注意：`connect` 验证精确 Schema，**不会自动执行
   此权限审计**；部署前及每次管理员改动权限后都应执行审计。

## 升级、轮换与故障处理

- 新表、函数不会自动得到运行时授权；已有表新增列会继承表级 SELECT/INSERT，但
  不会自动得到 UPDATE。新迁移必须配套审核版本化白名单，再依次
  迁移、应用、审计、功能冒烟，最后启动应用。本脚本检查 Schema 24，迁移校验和
  由固定版本的 provider 验证。
- 重复应用幂等，并在已有耐久历史的数据上验证。只读审计只报告漂移，修复必须显式
  应用；其他 Schema 的异常创建权限会让操作回滚，不会被广泛撤销。
- 密码/证书按身份独立轮换，排空并重建相应池。蓝绿身份需要单独审核授权及旧身份
  下线；脚本不会自动发现或撤销历史账号。
- DBA/所有者可以绕过或修改权限与数据。ACL 不是抵御被攻陷管理员的防篡改证明。
  仍需审计特权会话、限制网络、加密备份、验证恢复/故障切换并保存源码绑定证据。
  不应通过给运行时 GRANT ALL 来绕开审计。

## 验证与边界

强制 PostgreSQL 16/17 CI 创建唯一可丢弃数据库及三个真实 LOGIN 身份，使用非超级
用户迁移，通过各自凭证连接运行时/清理账号，验证 current_user = session_user。
测试遍历 40 张表及列的有效权限，并断言禁止操作准确返回 SQLSTATE 42501；覆盖
PUBLIC/列/转授权/默认权限漂移、SET-only 成员关系拒绝、配置整体回滚及未来对象
默认不可访问。业务验证包含原始失败保留、子级取消/结算、Join 发布/消费/非初始
Checkpoint 恢复、Agent Service 提交、模型/Tool 账本恢复及独立有界清理。
Provider 响应来自确定性测试装置，不是在线 Provider 认证。

给隔离 loopback 管理员测试库设置 `STATEKNOT_TEST_DATABASE_URL` 后复现：

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::failure_close::role_profile::trusted_sql_role_profile_enforces_privileges_and_runs_durable_work \
  --nocapture --test-threads=1
```

测试拒绝远程目标，不修改传入数据库的 Schema/ACL，成功后仅移除自身生成的库和
角色；失败后应丢弃隔离测试容器，禁止在生产集群执行。CI 产物
`trusted-role-profile-postgres-<版本>-<运行 ID>` 保留 30 天，必须包含一条通过的
机器可读记录、成功退出以及源码/tree/锁文件/固定镜像/工具链/环境信息。
空测试筛选不能通过。原有 COMMIT/SIGKILL 验收仍独立执行，本阶段不宣称所有故障
矩阵已在这些角色下完成，也不宣称实现 RLS、Worker 专属 SQL 能力、故障切换/恢复
或容量/SLO 验收。

PostgreSQL 官方依据：[GRANT 与列级权限叠加](https://www.postgresql.org/docs/17/sql-grant.html)、
[默认权限](https://www.postgresql.org/docs/17/sql-alterdefaultprivileges.html)、
[有效权限检查](https://www.postgresql.org/docs/17/functions-info.html)。
