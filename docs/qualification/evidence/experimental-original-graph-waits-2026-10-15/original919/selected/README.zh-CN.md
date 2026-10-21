# schema36+A 精确组合 Native 反馈：30/30 通过

本轮用冻结919文件/tree `bca1889badb0d3318fcca66490d153a2fec08ab28ab205a474db44f9b6510d45` 和缓存外保存的原严格二进制，重新执行原Ready36-v2全部28项，再加A成功scoped attempt owning proof各版1项。PG16/PG17各15/15、总30/30，全部真实1 passed/0 failed/0 ignored；不是旧25/28结果的转授。

| 项目 | PG16 | PG17 |
|---|---:|---:|
| frame_waits::elapsed::expired_original_interrupt_blocks_elapsed_ready_without_any_partial_commit | 通过，1.50s | 通过，1.37s |
| frame_waits::elapsed::maximum_original_wait_inventory_retains_all_sixty_four_real_owners | 通过，2.23s | 通过，1.63s |
| frame_waits::elapsed::mixed_original_conditions_release_only_the_real_remaining_registrations | 通过，30.70s | 通过，30.52s |
| frame_waits::elapsed::original_condition_corruption_blocks_complete_read_and_ack_without_writes | 通过，1.93s | 通过，1.95s |
| root_graph_barriers::elapsed::elapsed_root_missing_inventory_or_changed_positive_source_blocks_read_and_ack | 通过，0.51s | 通过，0.52s |
| root_graph_barriers::elapsed::expired_root_interrupt_cannot_turn_mixed_wait_into_ready_or_partially_commit | 通过，0.99s | 通过，0.95s |
| root_graph_barriers::elapsed::root_mixed_originals_register_only_remaining_conditions_and_keep_waiting_ack | 通过，0.59s | 通过，0.59s |
| root_graph_barriers::elapsed::root_ready_checks_original_expiry_after_the_actual_deferred_guard | 通过，2.56s | 通过，2.66s |
| root_graph_barriers::elapsed::root_ready_reloads_actual_successor_bytes_before_returning_the_receipt | 通过，0.34s | 通过，0.46s |
| frame_waits::elapsed_faults::substituted_canonical_scoped_wait_wire_rolls_back_the_complete_commit | 通过，1.31s | 通过，1.74s |
| frame_waits::elapsed_faults::substituted_ready_lease_cannot_extend_the_original_commit_authority | 通过，3.58s | 通过，3.77s |
| nested_driver::elapsed_graph_waits::root_absolute_wait_survives_real_delay_and_higher_fence_without_reexecution | 通过，3.31s | 通过，3.41s |
| nested_driver::elapsed_graph_waits::scoped_absolute_wait_survives_real_delay_and_higher_fence_without_reexecution | 通过，21.54s | 通过，24.03s |
| child_admission::ownership::failure_close::role_profile::trusted_sql_role_profile_enforces_privileges_and_runs_durable_work | 通过，36.26s | 通过，36.97s |
| proof_base_reuse::successful_scoped_attempt_read_keeps_complete_owning_proof | 通过，2.80s | 通过，2.71s |

Store binary SHA `fc0f64ecc2dcd761d6ac0423e56ff88cce0147fb4c95e941a1adf7188e70dfc9`；Runtime binary SHA `1d50b80ec096bcd328fec101b5d2198d427e8e48ffea97c76ffb79d663b45c30`。运行前用各自 `--list` 核对每个完整测试名唯一存在。两版任务容器标签 `stateknot.task=r1-oct15`、真实server版本160015/170011、实际端口55416/55417、独立新库 `stateknot_r1_a36_feedback_oct15v1` 均已核验，最终exact1..36成功ledger、54业务表。前后完整919manifest一致。

原配置、完整测试body/断言、默认stack、原lease/clock/timeout、24路concurrency与Store pool8均未修改。runner只设置原DB URL和require PostgreSQL；无编译、无资源/环境扩容、无任何SQL37/B/main/Git变更。另一个原角色管理辅助pool原为4，保持原值。

环境receipt里 `original_node_timeout_seconds:30` 是runner标签写错，不是运行参数；实际30s为原Store lease和pool acquire，Runtime原默认node timeout为900s，elapsed Native仍直接使用defaults。原记录保留，精确更正与源码SHA在 `resource-proof-and-metadata-correction.json`；测试没有设置这个metadata值。

原Ready36-v2三项红（PG16 Scoped Wait、双版完整role profile）在本轮组合上均独立通过；旧失败日志完整保留。本轮机器负载与旧执行不同，不能仅凭通过/耗时把原因归于A。SOURCE35→36原非空升级双版2项已在 `../source35-upgrade-native/` 独立通过，本轮未重复；也不冒充历史N−1/N−2二进制运行。

这些是选定Native反馈，不是完整Store/Runtime suite、完整Default7或R1资格。完整原日志/运行时PID/每项receipt与最终汇总见 `pg16/`、`pg17/` 和 `summary.json`。数据库与所有证据保留；已存构建缓存未动。
