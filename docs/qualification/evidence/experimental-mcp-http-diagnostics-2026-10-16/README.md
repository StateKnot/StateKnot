# Bounded MCP HTTP response diagnostics evidence

This package records source `31a5ff55…` (928 frozen source files) and the supplied
536 compilation input map `1ec97f72…`. It adds exactly two compressed final source
payloads over the already published bound-state source closure; the 18 existing
source references are reused by hash. It does not duplicate the prior archive,
449-file package, target directories or binaries. The frozen 928-file snapshot
is not asserted to equal the current main tree; the 536-file map is not a claim
about every file a compiler could read.

Four response Debug implementations expose only the fixed summaries in the
contract. Bare `McpInputRequired` and `McpToolCall::InputRequired` retain their
existing Tool-metadata behavior and are outside that guarantee. Explicit raw
getters still return untrusted remote data. The normalized source proof and real
TCP family preserve getters/Clone/PartialEq/resume/wire/effect/retry behavior.

`execution/` contains all six raw gate logs, actual receipts and the **complete**
`run-gate.py` and `execute-six.py` sources. `history/` retains the real original
canary failure and the first fixed candidate's Clippy fixture warning, their
full runners and exact source derivations. The final fixture fix only borrows the
JSON ID. No new production behavior or lint allowance was added in that repair.

`native/` contains two fresh default-feature build logs/profile records, full
suite lists, the full actual runner and immutable planning inventory, all four
serial execution logs/receipts and complete marker objects, and initial/final
resource probes. The planning inventory's `executed:false` is preserved as its
original authoring fact; execution `summary.json` independently records passed.
Each PostgreSQL major runs two complete targeted suites (6+10), not 16 database-
only cases. The legacy marker `schema:24` denotes the old reconciliation profile
constant, not current database schema 36. Binary hashes/profile are recorded;
executable bytes, caches, credential URLs and unrelated database inventories
are excluded. Runtime credentials were read inside the actual runner and never
written to its allowlist receipts. Existing source-only references do not grant
any prior execution result to this changed tree.

Run the read-only verifier from the repository (no Cargo/Docker/network):

```console
python3 docs/qualification/evidence/experimental-mcp-http-diagnostics-2026-10-16/verify.py --repository-root .
# After adopting the exact two source payloads, additionally check 536 workspace bytes:
python3 docs/qualification/evidence/experimental-mcp-http-diagnostics-2026-10-16/verify.py --repository-root . --workspace
```

The verifier reads logs and hashes; it never executes recorded runners. It
cannot prove remote CI, capacity, deployment, fresh official conformance, stdio
ownership, full Store/Runtime Native or R1 completion. Source/test/document author
authored this evidence and does not supply its independent review conclusion.
