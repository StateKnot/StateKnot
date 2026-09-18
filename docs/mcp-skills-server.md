<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP Skills server profile

> Status: implemented pre-alpha **server** profile; public API is not stable.<br>
> Extension: Final SEP-2640, `io.modelcontextprotocol/skills`.<br>
> Base protocol: MCP `2026-07-28`.<br>
> Companion: the separate [static client and Host profile](mcp-skills-host.md)
> verifies and activates Skills without widening this server claim.

StateKnot can publish static Agent Skills through the Final MCP Skills
extension. This is not a generic file-server shortcut. The server freezes the
complete Skill at startup, derives the manifest from the exact bytes it will
serve, and uses that same immutable snapshot for `skills/list`, `skills/get`,
and `resources/read`.

The normative sources are the
[MCP Skills extension](https://modelcontextprotocol.io/extensions/skills/overview),
the [stable extension specification](https://github.com/modelcontextprotocol/ext-skills/blob/main/specification/stable/skills.mdx),
and the [Agent Skills format](https://agentskills.io/specification).

## Implemented wire contract

When a Skill catalog is configured, `server/discover` advertises both the base
`resources` capability and an empty `io.modelcontextprotocol/skills` extension
object. An empty object intentionally means that optional directory reads are
not supported.

The server implements:

- bounded, cacheable and paginated `skills/list`;
- direct `skills/get`, independently of list visibility;
- complete static manifests containing every file exactly once;
- raw-byte `sha256:` digests and exact byte sizes;
- text or Base64 resource delivery through `resources/read`;
- deterministic Skill and file ordering;
- `resultType: "complete"`, `ttlMs`, and `cacheScope` on extension results;
- JSON-RPC `-32602` for unknown, scope-hidden, policy-denied, or malformed
  Skill lookups; policy-backend outages remain internal errors.

`resources/directory/read`, `directoryRead: true`, dynamic manifests, catalog
mutation, and list-changed notifications are intentionally absent from this
profile.

## Startup validation

`McpServerSkillDefinition` rejects traffic before startup if any invariant is
broken:

- `SKILL.md` is missing, binary, non-UTF-8, or has malformed YAML frontmatter;
- a YAML mapping contains duplicate keys at any depth, or aliases expand beyond
  the bounded materialized-value budget;
- `name` or another defined Agent Skills field violates the format;
- the URI parent and frontmatter `name` differ;
- a path is absolute, empty, contains traversal, backslashes, control bytes, or
  a URI-unsafe segment;
- paths, resource URIs, or Skill URIs collide;
- a Skill exceeds 512 files or 16 MiB;
- the aggregate registry exceeds its configured Skill, file, or byte ceiling.

Frontmatter fields unknown to the current Agent Skills revision are preserved
as JSON-compatible values instead of being discarded. This keeps extension
metadata forward-compatible without treating it as authority.

## Authorization and cache isolation

Authentication still occurs at `McpServerHttpService`. After that,
`McpServerSkillAuthorization` receives the authenticated principal, exact
operation, and untrusted URI. Direct `skills/get` and `resources/read` requests
run policy before lookup discloses existence. Discovery filters both exact
required scopes and dynamic policy decisions.

Unknown, scope-hidden, and policy-denied Skills collapse to the same public
error class. A scope-filtered catalog or principal-sensitive/dynamic policy
cannot be configured with public cache metadata. Authorization policies default
to private-cache-only and must explicitly attest that decisions remain identical
for every principal throughout the advertised TTL before public caching is
accepted. Private cursors bind the frozen catalog digest, principal subject,
canonical scope set, surface, and offset.

Digests establish consistency with the server's own manifest; they do not
establish authorship or trust. A consuming host must still treat the Skill as
untrusted input. StateKnot's separate [client and Host profile](mcp-skills-host.md)
implements that verification and approval boundary for complete static
manifests.

## Construction outline

```rust,ignore
let skill = McpServerSkillDefinition::new(
    "skill://code-review/SKILL.md",
    [
        McpServerSkillFile::text(
            "SKILL.md",
            "text/markdown",
            include_str!("skills/code-review/SKILL.md"),
        )?,
        McpServerSkillFile::text(
            "references/checklist.md",
            "text/markdown",
            include_str!("skills/code-review/references/checklist.md"),
        )?,
    ],
)?;

let mut skills = McpServerSkillCatalogBuilder::default();
skills.register(skill)?;

let app = McpServerApplicationBuilder::new(options)
    .with_skills(skills.build()?, skill_authorization)?
    .build()?;
```

Keep Skill bytes in a reviewed build input or another deployment-controlled
source. Do not rebuild the catalog per request.

## Verification

```console
cargo test -p stateknot-integrations mcp_server_skill --locked
cargo test -p stateknot-integrations --test mcp_skills_server --locked
```

The HTTP contract suite covers extension negotiation, pagination, direct
lookup, exact text and binary digest/size reconciliation, scope hiding,
authorization ordering, malformed requests, traversal, duplicate YAML keys,
bounded alias expansion, and public-cache refusal.

## Not claimed

- dynamic manifests or directory reads on this server surface;
- any claim that server delivery alone makes content trusted or executable;
- signatures, provenance, marketplace trust, or content safety;
- stable Rust API, crates.io release, or complete-framework conformance.
