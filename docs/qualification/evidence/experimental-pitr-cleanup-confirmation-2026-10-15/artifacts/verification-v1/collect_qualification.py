#!/usr/bin/env python3
"""Seal the completed single-test evidence. No build, DB, Docker or main writes."""
from pathlib import Path
import gzip
import hashlib
import json
import shutil

OUT = Path(__file__).resolve().parent
FROZEN = OUT.parent
BASE = FROZEN.parent
ROOT = BASE.parent.parent
DEST = OUT / "qualification"

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def write(path,value):
    path.write_text(json.dumps(value,indent=2,ensure_ascii=False)+"\n")

def main():
    assert not DEST.exists(), "Never overwrite a sealed collection"
    build = json.loads((OUT/"strict-runtime-build-v1/receipt.json").read_text())
    native = json.loads((OUT/"mandatory-pitr-v1/summary.json").read_text())
    terminal = json.loads((OUT/"terminal-resources.json").read_text())
    assert build["targeted_Runtime_Clippy_exit"] == build["fresh_Runtime_build_exit"] == 0
    assert native["both_exact_cases_and_cleanup_confirmations_passed"]
    assert terminal["recorded_task_PIDs_absent"]
    inputs = []
    for path in sorted(FROZEN.rglob("*")):
        if not path.is_file() or path == OUT/"strict-runtime-build-v1/stateknot-runtime-postgres" or "__pycache__" in path.parts:
            continue
        inputs.append((path.relative_to(FROZEN).as_posix(),path))
    inputs += [("context/final922-source-manifest.json",BASE/"elapsed36-attempt-proof-integration/final-test-fixes-v1/source-manifest.json"),
               ("context/final922-four-binary-identities.json",BASE/"elapsed36-attempt-proof-integration/final-test-fixes-v1/four-binary-identities.json")]
    gate = json.loads((OUT/"resource-gate.json").read_text())
    for index,row in enumerate(gate["terminal_receipts"]):
        source=ROOT/row["path"]
        assert sha(source.read_bytes()) == row["sha256"]
        inputs.append(("context/counterpart-"+str(index)+".json",source))
    DEST.mkdir()
    profile = {
        "status":"two exact mandatory PITR executions and success-path owned-resource cleanup confirmations passed for this candidate",
        "source_tree_sha256":native["source_tree_sha256"],
        "source_files":922,
        "parent_source_tree_sha256":"88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb",
        "only_changed_path":"crates/stateknot-runtime/tests/postgres/child_join_pitr.rs",
        "production_Runtime_Store_Core_SQL_unchanged_from_parent":True,
        "targeted_Runtime_Clippy_passed":True,
        "fresh_Runtime_postgres_build_passed":True,
        "binary_sha256":native["binary_sha256"],
        "runtime_inventory_count":189,
        "full_Runtime189_executed":False,
        "two_exact_PITR_execution_and_owned_cleanup_passed":True,
        "tests_executed":2,"passed":2,"failed":0,"ignored":0,
        "actual_postgres_versions":[row["markers"][0]["postgres"] for row in native["rows"]],
        "mandatory_env":["STATEKNOT_REQUIRE_PITR_TESTS=1","STATEKNOT_PITR_POSTGRES_IMAGE=<pinned digest>"],
        "host":"macos/aarch64", "database_containers":"linux/arm64",
        "resource_identities":[row["resource_identity"] for row in native["rows"]],
        "each_new_UUID_owned_resources_absent_after_execution":{"volumes":3,"named_containers":2},
        "all_prior_container_IDs_and_volume_names_preserved":True,
        "historical_WAL_volume":"stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1",
        "historical_WAL_volume_removed":False,
        "historical_WAL_before_after_inspect_byte_equal":True,
        "historical_final922_cleanup_open_qualification_rewritten":False,
        "all_failure_path_cleanup_qualified":False,
        "original_best_effort_Drop_unchanged":True,
        "original_business_assertions_and_30s_10s_budgets_unchanged":True,
        "full_workspace_six_gates_qualified":False,
        "full_four_Native_suites_rerun":False,
        "composed_main_qualification_transferred":False,
        "full_CI_or_capacity_or_performance_qualified":False,
        "R1_closed":False,
        "main_or_source_written_by_verifier":False,
        "cache_copied":False,"task_cache_deleted":False,
        "retained_cache":terminal["retained_task_cache_realpath"],
        "retained_cache_KiB":terminal["retained_task_cache_KiB"],
        "final_free_KiB":terminal["free_KiB"],
        "process_audit_scope":"Known five recorded task PIDs absent; not a system-wide process guarantee",
        "sandbox_audit_restriction":"Post-execution ps was denied in sandbox; scoped read-only elevated audit passed. No build/Native rerun.",
        "binary_storage":"Actual 70,951,264-byte binary retained outside Cargo cache; SHA bound in build receipts, deliberately excluded from this compact bundle",
        "freeze_snapshot_semantics":"Original freeze/preparation/review retain their original uncompiled status at creation; later receipts are the execution record"
    }
    (DEST/"qualification.json").write_text(json.dumps(profile,indent=2,ensure_ascii=False)+"\n")
    en = """# PITR cleanup confirmation: exact candidate qualification, 2026-10-15

The isolated single-test candidate passed targeted Runtime strict Clippy, a fresh Runtime `postgres` build, and one mandatory named PITR execution on each pinned PostgreSQL 16.14 and 17.11 image. Each execution returned **1 passed / 0 failed / 0 ignored**, emitted exactly one complete durable-evidence marker with `resource_cleanup_verified=true`, and left none of its three named volumes or two named containers. This is a successful-path cleanup result for two fresh UUIDs, not a guarantee for every failure path or the whole release.

Candidate source is 922 files, tree `82fc93f1e66fd13703bd9ae8bff268460829324cebe19ee94eaca497d87aed7d`. Against final922 tree `88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb`, only `crates/stateknot-runtime/tests/postgres/child_join_pitr.rs` changed. Production Runtime, Core, Store and SQL bytes remain identical. The source archive, both manifests, exact patch and independent focused review are embedded with their original bytes and hashes. The original preparation documents describe their historical pre-build state; later execution receipts establish the result.

The new explicit cleanup path waits for exact named-container absence and confirms volume removal before emitting the success marker. Original durable assertions, 30-second source-stop and 10-second cleanup budgets, default stack and best-effort Drop remain unchanged. Drop still ignores unsuccessful removal and can itself fail; this run does not qualify panic, command-timeout, Docker-daemon loss, host failure or all other failure-path cleanup. The prior final922 PG16 WAL leak remains a historical open result and was not rewritten.

Fresh binary SHA-256 is `994be9d00fe567660a847627c82b96a5e51c45e9ded56b0ba54e6760b75f0a32`. Cargo recorded `fresh:false` for Runtime library and test artifacts. Rust 1.88.0, `--locked`, all targets/features for targeted Clippy, `-D warnings`, debug assertions and overflow checks were retained. The 189-entry `--list` exactly matches final922; only the named PITR entry was executed here. Runtime-only artifacts were cleaned inside the checked idle cache to force the fresh build; dependencies were reused and no target directory was copied.

Execution waited until the independent A2A chain's actual terminal receipts and inactive-process evidence released the resource window. The only PITR switches were `STATEKNOT_REQUIRE_PITR_TESTS=1` and the fixed image digest. No pre-existing database environment was inherited. Host was macOS/aarch64; database images were Linux/arm64. This is not Linux-host qualification.

PG16 UUID is `01a13f89b1647070894c494a3a7f6099`; PG17 UUID is `01a13f89d42073c089c09c09ad3b7c4e`. Read-only Docker inventories show each UUID's three volumes and two named containers absent before and after execution, no additional container IDs or volume names, and all prior IDs/names retained. The historical volume `stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1` remains, with byte-identical inspect output. The runner issued no manual stop/rm/prune or historical cleanup. Scoped post-execution audit found all five recorded PIDs absent, no open cache files, and the Cargo lock available. Cache is retained for composed follow-up. Its measured size and actual final `df` are recorded in JSON; no released-space claim is made.

The original sandbox denied the final read-only `ps`; the restriction is retained, and a scoped elevated read-only audit succeeded. No build or PITR was repeated for that audit.

Use `python3 verify.py` for a fully offline check of stored/decompressed hashes, the 922 archived source inputs, parent-only-one-file delta, receipt/log bindings, Cargo freshness, exact 189 inventory, both terminal summaries/unique complete markers and all resource deltas. Optional `--source /absolute/frozen/source --binary /absolute/saved/binary` additionally compares the live frozen source and saved binary. No network, subprocess, Docker or compilation occurs in the verifier. Raw logs and source archive use deterministic gzip; original raw-log receipt hashes remain unchanged. The large binary and compilation cache are excluded from this compact folder.

This evidence does not run the 189-test suite, the four complete Native suites, the six complete workspace gates, composed-main checks, full CI, capacity/performance tests, or close R1. It changes no main source, public API, dependency or resource policy. A later composed candidate requires its own input binding and qualification.
"""
    cn = """# PITR 清理确认：精确候选资格，2026-10-15

独立的单测试候选通过了 Runtime 定向严格 Clippy、全新 Runtime `postgres` 构建，以及固定 PostgreSQL 16.14 / 17.11 镜像各一次强制命名 PITR。每次均为 **1 通过 / 0 失败 / 0 忽略**，仅输出一个完整耐久证据 marker，且 `resource_cleanup_verified=true`。每次所属三个命名卷和两个命名容器实际均已不存在。这只确认两个新 UUID 的成功路径清理，不保证全部失败路径或整个发布。

候选共 922 文件，tree 为 `82fc93f1e66fd13703bd9ae8bff268460829324cebe19ee94eaca497d87aed7d`。相对于最终922的 `88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb`，只改变 `crates/stateknot-runtime/tests/postgres/child_join_pitr.rs`；Runtime/Core/Store 生产源码与 SQL 逐字相同。包内保存源归档、两份清单、精确 patch 和独立定向审阅的原始字节及哈希。原 preparation 文档保留创建时尚未编译的历史状态，后续执行回执才是本次运行结果。

新增显式清理在成功 marker 前确认精确命名容器不存在、卷删除且不存在。原耐久断言、源容器停止30秒及各清理10秒预算、默认栈和 best-effort Drop 均保持。Drop 仍忽略删除失败，也可能自身失败；本次没有认证 panic、命令超时、Docker daemon 丢失、主机故障等所有失败路径的清理。此前最终922 PG16 遗留 WAL 的历史开放结果不改写。

新二进制 SHA-256 为 `994be9d00fe567660a847627c82b96a5e51c45e9ded56b0ba54e6760b75f0a32`，Cargo 对 Runtime 库和测试产物记录 `fresh:false`。使用 Rust 1.88.0、`--locked`；定向 Clippy 保持全部 targets/features、`-D warnings`、调试断言和溢出检查。`--list` 的189项与最终922精确相同，但本次只实际执行指定 PITR 一项。为得到新产物，仅在检查为空闲的缓存内清除 Runtime 包产物，复用依赖，未复制 target。

本轮等待独立 A2A 整条资源链真实终态回执及无活跃进程证据后才开始编译。PITR 只设置 `STATEKNOT_REQUIRE_PITR_TESTS=1` 和固定镜像 digest，未继承既有数据库测试环境。宿主为 macOS/aarch64，数据库容器为 Linux/arm64；这不是 Linux 宿主认证。

PG16 UUID 为 `01a13f89b1647070894c494a3a7f6099`，PG17 为 `01a13f89d42073c089c09c09ad3b7c4e`。只读 Docker 前后清单显示：每个 UUID 的三个卷、两个命名容器运行前后均不存在；没有新增容器 ID 或卷名，既有 ID/卷名全部保留。历史卷 `stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1` 仍在，inspect 原始输出前后逐字相同。runner 未执行额外人工 stop/rm/prune 或历史清理。终态限定审计确认本轮记录的五个 PID 不存在、缓存无打开文件、Cargo 锁可取得。缓存为后续组成验证保留，实测大小与最终 `df` 存于 JSON，不把目录大小声称为已释放空间。

首次终态只读 `ps` 被 sandbox 拒绝，限制记录保留；随后限定范围的升权只读审计成功。没有为该审计重跑编译或 PITR。

运行 `python3 verify.py` 可完全离线验证压缩及解压后哈希、922个归档源码输入、相对父候选仅一文件变化、回执与原日志绑定、Cargo 新构建、精确189清单、两次终态摘要/唯一完整 marker 和全部资源差异。可选 `--source /绝对/冻结源码 --binary /绝对/已保存二进制` 进一步比对实际冻结目录和二进制。校验器不联网、不启动子进程、Docker 或编译。原日志/源归档确定性 gzip 保存，原回执 raw-log 哈希不变；大二进制及编译缓存不进入紧凑目录。

这份证据不等于执行189项整套 Runtime、四套完整 Native、完整 workspace 六门禁、组成 main 验证、全 CI、容量/性能认证或关闭 R1。验证没有修改 main 源码、公开 API、依赖或资源政策；后续组成候选仍需自己的输入绑定和资格记录。
"""
    (DEST/"README.md").write_text(en)
    (DEST/"README.zh-CN.md").write_text(cn)
    shutil.copyfile(OUT/"verify_qualification.py",DEST/"verify.py")
    for name in ["qualification.json","README.md","README.zh-CN.md","verify.py"]:
        inputs.append((name,DEST/name))
    artifacts=[]
    raw_suffixes={".stdout",".stderr",".txt",".log",".df",".patch",".rs",".tar"}
    for key,path in inputs:
        raw=path.read_bytes()
        compressed=path.suffix in raw_suffixes
        stored=gzip.compress(raw,compresslevel=9,mtime=0) if compressed else raw
        own = path.parent == DEST
        relative=key if own else "artifacts/"+key+(".gz" if compressed else "")
        target=DEST/relative
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(stored)
        artifacts.append({"key":key,"path":relative,"encoding":"gzip" if compressed else "identity","stored_bytes":len(stored),"stored_sha256":sha(stored),"raw_bytes":len(raw),"raw_sha256":sha(raw)})
    write(DEST/"artifact-manifest.json",{"schema":"pitr-cleanup-confirmation-evidence-v1","scope":"exact candidate, targeted Runtime Clippy/build, two mandatory PITR success-path cleanup executions; no full workspace/Native/CI qualification","artifacts":artifacts})
    write(DEST/"seal.json",{"artifact_manifest_sha256":sha((DEST/"artifact-manifest.json").read_bytes()),"profile_sha256":sha((DEST/"qualification.json").read_bytes()),"source_tree_sha256":native["source_tree_sha256"],"binary_sha256":native["binary_sha256"],"history_overwritten":False,"full_workspace_or_R1_qualification":False})
    print(json.dumps({"folder":str(DEST),"artifacts":len(artifacts),"manifest_sha256":sha((DEST/"artifact-manifest.json").read_bytes()),"seal_sha256":sha((DEST/"seal.json").read_bytes())},indent=2))

if __name__ == "__main__":
    main()
