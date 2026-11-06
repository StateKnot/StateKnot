# A2A subscription review delivery appendix

This is an author-prepared lossless delivery appendix, not a new independent review or execution result. It supplements the unchanged178-path experimental-a2a-subscription-policy-2026-10-16 package (candidate manifest739688ef…, evidence manifestaf5c515f…, source0e91c287…/930 and supplied538 canonical43e9cb48…). The full identifiers and all original hashes are in delivery.json. None of the original package, review manifests, receipts, pending fields, guides, source, Git index or main was rewritten.

Five newly delivered groups contain48 original files:

| Group | Original files | Scope/relationship retained from its receipt |
| --- | ---: | --- |
| source-spec | 7 | Reviewer did not author implementation/tests; authored the interface contract and does not independently review that own design. |
| docs-spec | 6 | Non-author review of the five adoption documents. |
| docs-standards | 8 | Reviewer did not author the five documents; authored the tests/publication package, so this is not an independent test/source/package review. |
| publication-spec | 20 | Reviewer did not author the package, production or tests; interface-design authorship remains disclosed. |
| publication-standards | 7 | Root did not author the publication package; production-author role is distinct, so this is delivery Standards rather than an independent review of root's implementation. |

Two earlier groups are reused in place from the base package's reviews/source-standards-v1 and reviews/source-standards-v2 (7 original files each). They are not copied again. Thus there are five new groups plus two base groups, not seven newly independent reviewers. The base v1 review's initial source/lint scope and v2's exact fixture-only increment remain distinguishable.

Every original file is bound by original path/bytes/SHA and stored path/bytes/SHA. Raw .patch/.diff inputs would be deterministically gzipped (mtime0, no filename) without changing the original manifest/receipt. Here both patch originals were already gzip, so their exact containers are retained and their uncompressed body hashes are additionally recorded. There are no new raw-patch conversions. Original manifests that intentionally exclude their subsequently written receipt remain exact; the receipt is separately bound by delivery.json.

Run `python3 verify.py` after adoption, or pass `--base-candidate-root /absolute/path/to/the/original178/candidate` before adoption. The verifier only reads hashes, decompresses lossless data and checks original manifest entries in memory. It also hashes the exact original178 publication paths and reads the two base groups in place. It never executes retained review scripts, gate runners, Cargo, product code, Docker, DB, network or CI. It creates no new independent review or product qualification.

The original base profile and reports continue to say that final independent reviews were not included in that earlier seal. These later records supplement that historical package; they do not retroactively change its fields. The separate adoption/ records retain root's actual186-path working-tree copy and exact538 workspace tool result (chunk0532db). These2 records are not reviews. The tool receipt is explicitly post-hoc retention of the actual tool result, not a newly run gate. They bind the working-tree adoption and unchanged original CI637 configuration, not a whole Git/read closure, commit, push or fullCI. New Native database, IdP/backend, durable Task, fullCI/cold30m, stdio, deployment and R1 qualifications remain outside this appendix.

The appendix therefore contains48 original review files plus2 non-review adoption records, four delivery metadata/tool files, and references14 earlier Standards files in place. Original178-package pending fields record its earlier seal time; the five later review records and two earlier Standards groups are delivered separately without retroactive edits.
