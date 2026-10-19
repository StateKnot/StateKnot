<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Portable owner-stop qualification evidence / 可移植验收证据

[English qualification](../../experimental-scoped-join-owner-stop-2026-10-18.md) · [中文验收记录](../../experimental-scoped-join-owner-stop-2026-10-18.zh-CN.md) · [Machine record / 机器记录](../../experimental-scoped-join-owner-stop-2026-10-18.json).

The selected experimental Store contract and complete candidate CI bind immutable code commit [`bbd9dcf833a3446b822e8e1398c53d19cf3b9a72`](https://github.com/StateKnot/StateKnot/tree/bbd9dcf833a3446b822e8e1398c53d19cf3b9a72), tree `126d95b5a80c703b781e270a157c5b81a4a3ad39`, and [run 39185847363](https://github.com/StateKnot/StateKnot/actions/runs/39185847363), attempt 1. Retrieve source from that exact commit. This archive deliberately omits executable bodies, build caches and complete source checkouts; their recorded hashes remain. The later documentation/evidence commit is separate from the measured runtime commit. Driver, Root, R1 and R6 remain open.

选定实验性 Store 契约及完整候选 CI 绑定上述不可变代码 commit/tree/run；代码应从该精确 commit 取得。归档不复制可执行二进制、构建缓存或整个源码 checkout，但保留原 hash。后续文档/证据提交与运行量测 commit 分开。Driver、Root、R1、R6 继续开放。

## Files and integrity / 文件与完整性

| File | Meaning / 内容 |
| --- | --- |
| [review-originals.tar.gz](review-originals.tar.gz) | 7,837 original regular-file members / 原文件成员 |
| [artifact-index.json](artifact-index.json) | Complete relative member paths, lengths, SHA-256 and provenance / 全量相对路径、长度、摘要和出处 |
| [verify.py](verify.py) | Standard-library offline verification; optional safe extraction / 标准库离线核验及可选解包 |

Archive: 40,108,591 bytes; SHA-256 `c34956fbea17a1e9abe408f555c425ec62033d9dbb07cd24d5e400097a80537c`. Indexed original file bytes: 139,439,640. Index SHA-256: `825ccd9d31f6a9792c7c626fd3439ea9002867a3333f9c7041cc1fb9ed1c2c8f`. Verifier SHA-256: `6895edd2dc38571a6049b8867f4ab7593a0bacd0789af762f30b318e05b65cbe`.

The archive has sorted USTAR regular files, mode 0644, uid/gid/mtime zero and a gzip mtime of zero. These container metadata do not change any original file content. Every member is indexed, and verification refuses extras, missing/duplicate paths, non-regular members or changed bytes. Original absolute paths inside receipts are preserved; `original_path` is provenance, never a requirement to recreate the author's filesystem.

归档只含按路径排序的 USTAR 常规文件，统一 mode0644、uid/gid/mtime0 和 gzip mtime0；封装元数据不改变原文件内容。每个成员均有索引，核验拒绝 extra、缺失、重复路径、非普通文件及字节变化。原 receipt 中的绝对路径保持原样；`original_path` 是出处信息，无需在其他机器重建作者目录。

From this directory / 在本文所在目录执行：

```sh
python3 verify.py
python3 verify.py --extract unpacked
```

Extraction requires a new destination and never overwrites an existing directory. Verification checks package integrity; it does not execute tests, access a database/network, or award new runtime qualification. After extraction, use the index `path` directly under `unpacked/` to find a receipt and its unchanged neighboring logs. All saved scripts remain data until explicitly run.

解包目标必须是全新目录，不覆盖已有目录。核验只验证证据包完整性，不执行测试、访问数据库/网络或新增运行资格。解包后按索引 `path` 在 `unpacked/` 中定位 receipt 及同目录原始日志。保存的脚本只有被明确执行时才会运行。

## Current binding and history / 当前绑定与历史

Current authoritative inputs are `artifacts/ci-postgres-shards-run39185847363-bundle-v4/`, `artifacts/ci-postgres-shards-run39185847363-terminal-v2/`, and `artifacts/ci-postgres-shards-run39185847363-analysis-v2/receipt.json`. The strict V5 analysis actually returned 0, both full-CI fields are true and errors are empty. It verified all 21 successful jobs, all original 60 PG steps per major, 54 ZIPs, 1,525 indexed raw files and 99 official GET receipts. Source domains stay distinct: selected936, compiled544, five CI files, exact38 payloads and complete4088-blob official tree.

当前有效输入为上述 bundle-v4 / terminal-v2 / analysis-v2。V5 严格分析真实返回0，两个 full-CI 字段均为true、错误为空；验证21个成功 job、双 major 各原60个 PG steps、54 ZIP、1525个索引 raw 文件及99份官方 GET。selected936、compiled544、5CI文件、38payload 和官方4088-blob完整树分别绑定。

The archive also keeps all 806 original assembled files; the composed-V2 selected/full outcomes; composed-V3 local Runtime failures, power-log correlation, all six local gates and four recovery passes; role37 delta/source manifests; public catalog/consumer evidence; both older cancelled CI snapshots with complete logs/ZIPs; the first strict analysis failure and its original bundle/raw; publication/dispatch and local adoption receipts. Both cancelled runs remain 11-success/2-cancelled. V3 local failures remain 183-pass/11-fail and 182-pass/12-fail; sleep evidence is correlation only. The first strict-analysis failure remains an actual failure caused by the generic observer, with the exact V5 fix provenance retained. No source or binary qualification is transferred between these histories.

归档同时保留旧806件 assembly、V2选中/全量结果、V3本机 Runtime 失败和电源相关日志、V3六门禁及4次恢复通过、role37 delta/源码清单、public catalog/consumer、两次完整 cancelled CI raw logs/ZIP、首次严格分析失败及原 bundle/raw、publication/dispatch 和本地采用 receipt。两次旧 CI 仍各为11success/2cancelled，V3本机仍为183pass/11fail、182pass/12fail，sleep 只说明相关；首次严格分析的真实失败与 V5 精确观察器修复出处单独保留，不跨源码/二进制转授资格。

`history/qualification-v2-snapshot/` preserves the measured commit's historical pending-V2 documents. `history/portable-package-v2/` preserves the earlier portable archive/index/verifier exactly; this v3 package additionally includes the four independently recorded COMMIT-loss validator receipt/input files missing from that earlier package. The historical 96-reference planning index is unmodified: 95 current-scope references are included; its one later F2 RED receipt is explicitly excluded from this qualification. Later F2 slices remain independent future work.

`history/qualification-v2-snapshot/` 保存量测 commit 的历史 pending-V2 文档；`history/portable-package-v2/` 原样保存上一版归档/索引/verifier，本 v3 补齐前版遗漏的4个独立 COMMIT-loss 核验 receipt/input 文件。历史96引用规划索引未改写：95项当前范围证据完整纳入，另1项后续 F2 RED receipt 明确排除，不归入本次资格。

For independent offline CI analysis after verification/extraction, use the saved analyzer with a new output path / 如需独立重做离线 CI 分析，先核验解包，再使用新输出目录：

```sh
python3 unpacked/artifacts/ci-postgres-shards-run39185847363-bundle-v4/analyze.py \
  --bundle unpacked/artifacts/ci-postgres-shards-run39185847363-bundle-v4 \
  --raw unpacked/artifacts/ci-postgres-shards-run39185847363-terminal-v2 \
  --out analysis-new
```

This reads the complete saved official evidence. It does not rerun CI or turn historical failed/cancelled executions into success. Required PR review/checks and deployment/release gates remain separate.

该操作读取完整已保存官方证据，不重跑 CI，也不将历史失败/取消执行改为成功。必须 PR review/checks 与部署/发布门禁继续单列。
