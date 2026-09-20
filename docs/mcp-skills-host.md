<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP Skills client and Host profile

> Status: implemented pre-alpha **static-manifest client and Host** profile;
> public API is not stable.<br>
> Extension: Final SEP-2640, `io.modelcontextprotocol/skills`.<br>
> Base protocol: MCP `2026-07-28`.<br>
> Explicit boundary: dynamic manifests, remote directory reads, persisted
> activation approval/acting windows, disk materialization, signatures, and
> automatic discovery-to-Agent composition are not implemented or claimed.

StateKnot can discover and activate a remote static Agent Skill without turning
its files or frontmatter into authority. The client validates the Final
SEP-2640 wire contract. The Host then assigns a local origin, asks application
policy for fresh approval, fetches only requested manifest entries from the
same client binding, verifies exact size and SHA-256, and retains the approved
entry for the complete acting window.

The normative sources are the
[MCP Skills extension](https://modelcontextprotocol.io/extensions/skills/overview),
the [stable extension specification](https://github.com/modelcontextprotocol/ext-skills/blob/main/specification/stable/skills.mdx),
and the [Agent Skills format](https://agentskills.io/specification).

## Implemented lifecycle

1. Connect with `McpClientOptions::for_skills()`. Only this explicit profile
   advertises the Skills client extension. It also raises bounded JSON/SSE
   ceilings enough to carry a worst-case escaped 16 MiB text resource while
   limiting concurrency to two requests.
2. Require both the base Resources capability and the Skills extension from
   `server/discover` before calling `skills/list`, `skills/get`, or
   `resources/read`.
3. Accept only `resultType: "complete"` static manifests. Every entry must
   contain `SKILL.md`, use canonical in-root paths and lower-case `sha256:`
   digests, and remain within 512 files, 16 MiB, and bounded frontmatter,
   pagination, and catalog ceilings. `"dynamic"` fails closed.
4. Identify a Skill by the pair `(host-assigned origin, exact Skill URI)`.
   Name and self-reported server metadata are never identities.
5. Present the exact entry, complete manifest binding digest, origin, source,
   description, and untrusted `allowed-tools` request to
   `McpSkillHostPolicy::approve_activation`. No Skill file is fetched before
   that fresh decision succeeds.
6. Fetch `SKILL.md` lazily from that same MCP client, verify URI, byte size, and
   digest, parse strict duplicate-free frontmatter, and require field-for-field
   equality with the approved entry.
7. Keep `McpActivatedSkill` alive for the acting window. Reads are limited to
   its retained complete manifest. Directory views are derived locally, so a
   live server cannot add a file after approval.
8. Treat a nested `SKILL.md` as a new activation. It must be listed by the
   parent and receives a separate, fresh policy decision.
9. Bind an activated Skill to an exact owner/name/version Tool and the canonical
   digest of its complete descriptor with `McpSkillBoundTool`. The Host must
   explicitly declare whether the Tool can execute code in its environment.
10. Ask policy immediately before every execution and reconciliation provider
    call. The request distinguishes both operations and carries the exact Tool
    identity, descriptor digest, Host-code exposure, origin, Manifest digest,
    activation ID, tenant/run/invocation/attempt correlation, committed origin
    event when present, and bounded schema-bound input. `allowed-tools` remains
    untrusted comparison data.
11. Require policy to return an exact owner/name/version policy identity,
    immutable policy-artifact digest, and decision-evidence digest. Before any
    provider I/O, write a payload-redacted `ToolAuthorizationReceipt` through
    the mandatory durable sink. Schema 25 binds that immutable receipt to the
    exact tenant/run/thread/invocation/attempt/origin event, Tool descriptor,
    input digest, operation, policy, and database commit time. An unavailable
    sink fails before dispatch with a safe delayed retry; rejected or crossed
    evidence fails closed and is never retried.
12. Register the guarded adapter—not the raw provider—in the ordinary immutable
    `ToolProviderRegistryBuilder`. Durable attempt-start, terminal evidence,
    retry, and reconciliation semantics therefore remain unchanged.

## Host construction

```rust,ignore
let client = McpClient::connect(
    endpoint,
    McpClientIdentity::new("orders-agent", env!("CARGO_PKG_VERSION"))?,
    authorization,
    McpClientOptions::for_skills(),
)
.await?;

let host = McpSkillHost::new(
    client,
    McpSkillOrigin::new("production/orders-mcp")?,
    Arc::new(application_skill_policy),
    McpSkillHostOptions::default(),
)?;

let catalog = host.list_skills().await?; // metadata only; no file prefetch
let selected = catalog
    .find_uri("skill://incident-review/SKILL.md")
    .ok_or(AppError::SkillUnavailable)?;
let active = Arc::new(host.activate(selected).await?); // approve, then verify

// Preserve `file.identity()` beside its bytes in every model-visible message.
let file = active.read_file("references/checklist.md").await?;
model_context.push_untrusted_skill_file(file.identity(), file.bytes())?;

// Freeze the exact provider under this activation before registry construction.
let guarded = Arc::new(McpSkillBoundTool::new(
    Arc::clone(&active),
    ticket_create_tool, // Arc<dyn ErasedTool>
    Arc::new(postgres_store.clone()), // mandatory durable receipt sink
    McpSkillHostCodeExecution::NotPossible,
)?);
tool_registry.register(guarded)?;

// The ordinary durable executor now authorizes every call and recovery probe.
let tools = tool_registry.build();
```

The application policy is the user/policy interaction boundary. A production
implementation should display the host-assigned origin, exact URI, manifest
digest, file count/bytes, description, activation source, requested Tool, exact
registered Tool identity and descriptor digest, operation, and Host-code
exposure, durable correlation, and exact arguments; bind the decision to those
facts; log only public-safe evidence; return a version-pinned
`McpSkillToolAuthorizationGrant`; and deny when its authority is unavailable.
Never approve solely by Skill name or the `allowed-tools` string,
and never log `McpSkillToolInvocation::input()` without application-level
redaction.

## Cache and restart behavior

Verified files may enter a private immutable process-memory cache keyed by the
exact client binding, host-assigned origin, resource URI, and digest. Cache
entry and byte ceilings are mandatory. Cache fullness skips insertion instead
of evicting or weakening verification. Cached `Arc<[u8]>` values cannot be
mutated, so a cache hit retains the original verification result.

StateKnot does not write remote Skill bytes into filesystem Skill-discovery
paths and does not persist activation approval or the acting window. It does
persist each successful bound Tool authorization as an immutable,
payload-redacted receipt before provider I/O. On restart the client binding,
acting windows, permits, activation approvals, and memory cache all disappear. A later activation
must discover and approve the current complete manifest again. This is a safe
restart contract, not durable approval. A worker may rebuild the same guarded
Tool descriptor only after that fresh activation; already-started durable Tool
attempts retain their normal recovery ledger, but reconciliation performs a
new exact Skill policy check before provider I/O.

## Security boundary

- Digest verification proves consistency with the advertised manifest, not
  authorship, safety, or trust.
- `McpVerifiedSkillFile::identity()` must remain visible to the model and audit
  trail. The library returns the origin-tagged object but cannot force a model
  adapter to preserve it.
- The low-level `McpClient::read_skill_resource` result is untrusted. Only bytes
  returned through an activated Host have been checked against a retained
  approved manifest.
- `McpSkillBoundTool` consumes the non-cloneable permit inside the ordinary
  `ErasedTool` boundary. Authorization denial produces `NotStarted` evidence
  for writes (or `NotApplicable` for reads) and never invokes the provider.
- A durable receipt proves the exact authorization decision only. It does not
  prove provider dispatch or an external effect; terminal Tool evidence and
  reconciliation remain authoritative for outcome.
- The adapter must replace the raw provider in the executable registry. Keeping
  both bindings or dispatching the provider directly is a Host configuration
  error outside the Skill authorization boundary.
- Cross-server reuse fails because an entry is bound to one client instance.
  The application must assign a stable, unique origin label to that binding;
  self-reported server metadata never supplies it.
- Verification failure does not automatically refetch, substitute, or execute.
  The caller must begin a new discovery and approval lifecycle.

## Verification

```console
cargo test -p stateknot-integrations mcp_skill_host --locked
cargo test -p stateknot-integrations --test mcp_skills_host --locked
cargo test -p stateknot-store-postgres --test postgres --locked \
  tool_authorization_receipts_are_exact_immutable_and_page_verifiable
```

The loopback contract suite proves lazy listing, capability advertisement,
bounded pagination, host-origin preservation, approval-before-read, exact
digest/size/frontmatter reconciliation, immutable cache hits, manifest-only
reads, local directory views, fresh nested consent, per-call Tool authorization,
exact descriptor/identity disclosure, separate execution/reconciliation
approval, immutable-registry compatibility, denial before provider dispatch,
durable-receipt-before-provider ordering, unavailable-sink retry evidence,
receipt idempotency/immutability/pagination, and failure closure for content drift.

## Not claimed

- dynamic manifests or `resources/directory/read`;
- persisted activation approvals, durable acting windows, disk caches, or
  filesystem Skill installation (per-operation Tool authorization receipts are
  durable and are a narrower contract);
- signature verification, provenance, marketplace trust, malware/content
  safety, or sandboxing;
- automatic Tool discovery, `allowed-tools` pattern interpretation, or dynamic
  discovery-to-Agent composition;
- stable Rust API, crates.io release, or official Skills-extension conformance.
