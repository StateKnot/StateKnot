# HTTP MCP Debug — independent Standards review

Fixed source: `31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca` / 928 files. Conservative inputs: 536, relative-path-string-sorted digest `1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c`. Comparison is against frozen `69af3a0f…`; only `crates/stateknot-integrations/src/mcp_client.rs` and `crates/stateknot-integrations/tests/mcp_client_contract.rs` differ.

**Standards: 0 documented breaches; 0 actionable heuristic smells.** This reviewer authored neither file's Debug slice or its test family. Spec is assigned separately; this report does not merge or substitute that axis.

`CONTRIBUTING.md` calls for focused, independently reviewable changes and tests for changed behavior. Four local `fmt::Debug` implementations use ordinary `debug_struct`/`finish_non_exhaustive`, fixed type/field labels, existing bounded code, booleans and vector lengths. They avoid payload traversal, copying or generic redaction machinery. The small implementations have different metadata contracts; introducing a shared abstraction would add complexity without removing substantive duplication. The outcome match is local and exhaustive.

The appended integration family reuses the existing real TCP request/JSON helpers and a local owned server task. The outcome enum, payload builders and assertions give the eight JSON/SSE cases a concrete purpose; `Drop` aborts the owned task and the finish wait is bounded. It verifies raw getters, explicit resume wire facts and request counts alongside Debug canaries, rather than merely reproducing formatter bodies. No new lint allowance, global fixture framework or production resource change is introduced.

The original test prefix remains byte-identical: 357 lines / 13917 bytes, SHA `b093e27d…`. Reversing only the four manual Debug implementations, associated doc additions and derive changes reconstructs the entire old production file byte-for-byte. The original Display/error/network/parser/unit-test suffix is unchanged. Existing Clone/PartialEq behavior remains; no Clone/PartialEq is added to `McpToolCallResponse`.

The type docs state the narrowed diagnostic behavior and explicitly exclude bare `McpInputRequired`/`McpToolCall::InputRequired`. `StatelessMcpClientError::Remote` is the actual outer error path; no invented ToolCall error variant or whole-MCP privacy guarantee is asserted. Broader adoption docs/CHANGELOG remain the publication owner's requirement under `CONTRIBUTING.md`; this source review does not waive them.

This was read-only source review. No Cargo, tests, network, DB, Docker, cache operation or saved runner was executed. Existing execution/red receipts retain their own scope; this review grants no new runtime, CI, RFC, production-support or R1 qualification.
