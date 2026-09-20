// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Verification-first client and durable Host authority for static MCP Skills.
//!
//! This profile deliberately declines dynamic manifests. It never materializes
//! remote files into filesystem Skill discovery paths, fetches only on demand,
//! and obtains content-bound approval before activation. Approval/window
//! metadata is mandatory durable authority; remote file bytes remain verified
//! on demand in a bounded private process cache.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde_json::{Map, Value};
use stateknot_core::{
    BoxFuture, CapabilityIdentity, Digest, SkillActingWindow, SkillActingWindowDuration,
    SkillActingWindowId, SkillActingWindowOpenRequest, SkillActingWindowRevocation,
    SkillActingWindowRevocationReason, SkillActivationApproval, SkillActivationApprovalId,
    SkillActivationScope, SkillActivationSource, SkillActivationStore, SkillActivationStoreFailure,
    SkillAuthorizationSubject,
};
use thiserror::Error;

use crate::{
    MCP_SKILLS_EXTENSION_ID, McpCachePolicy, McpClient, StatelessMcpClientError,
    mcp_server_skill::{
        parse_frontmatter, validate_frontmatter, validate_relative_path, validate_skill_uri,
    },
    mcp_skill_tool::{McpSkillToolBinding, McpSkillToolInvocation, McpSkillToolOperation},
};

const MEBIBYTE: usize = 1024 * 1024;
const MAX_SKILL_URI_BYTES: usize = 4096;
const MAX_CURSOR_BYTES: usize = 4096;
const MAX_MIME_TYPE_BYTES: usize = 255;
const MAX_ORIGIN_LABEL_BYTES: usize = 128;
const MAX_TOOL_NAME_BYTES: usize = 1024;
const SKILL_DOCUMENT: &str = "SKILL.md";
const SKILLS_LIST_METHOD: &str = "skills/list";
const SKILLS_GET_METHOD: &str = "skills/get";
const RESOURCE_READ_METHOD: &str = "resources/read";

/// Raw-byte support floor required by Final SEP-2640.
pub const MCP_SKILL_MAXIMUM_BYTES: usize = 16 * MEBIBYTE;
/// File-count support floor required by Final SEP-2640.
pub const MCP_SKILL_MAXIMUM_FILES: usize = 512;
/// `StateKnot`'s bounded JSON representation ceiling for advertised frontmatter.
pub const MCP_SKILL_MAXIMUM_FRONTMATTER_BYTES: usize = 64 * 1024;
/// Bounded JSON/SSE envelope needed for worst-case escaping of a 16 MiB text file.
pub const MCP_SKILL_WIRE_RESPONSE_BYTES: usize = 104 * MEBIBYTE;

/// Parsed optional capability settings for the MCP Skills extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpSkillCapabilities {
    directory_read: bool,
}

impl McpSkillCapabilities {
    /// Returns whether the server advertised `resources/directory/read`.
    #[must_use]
    pub const fn directory_read(self) -> bool {
        self.directory_read
    }
}

/// One exact static file descriptor in a Skill manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpSkillResource {
    uri: Arc<str>,
    digest: Arc<str>,
    size: usize,
    relative_path: Arc<str>,
}

impl McpSkillResource {
    /// Returns the originating server's exact resource URI.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the exact lower-case `sha256:` digest.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns the raw-byte size covered by the digest.
    #[must_use]
    pub const fn size(&self) -> usize {
        self.size
    }

    /// Returns the canonical path relative to the Skill root.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }
}

/// One strictly validated static Skill entry from an exact client binding.
#[derive(Clone)]
pub struct McpSkillEntry {
    binding_id: u64,
    uri: Arc<str>,
    name: Arc<str>,
    description: Arc<str>,
    frontmatter: Arc<Map<String, Value>>,
    resources: Arc<BTreeMap<String, McpSkillResource>>,
    manifest_digest: Arc<str>,
    total_bytes: usize,
}

impl McpSkillEntry {
    /// Returns the exact URI of this Skill's `SKILL.md`.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the untrusted Agent Skills name label.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the untrusted Agent Skills description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns every advertised frontmatter field unchanged as JSON data.
    #[must_use]
    pub fn frontmatter(&self) -> &Map<String, Value> {
        &self.frontmatter
    }

    /// Returns the complete manifest in canonical URI order.
    pub fn resources(&self) -> impl ExactSizeIterator<Item = &McpSkillResource> {
        self.resources.values()
    }

    /// Returns the aggregate raw-byte size declared by the manifest.
    #[must_use]
    pub const fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Returns `StateKnot`'s deterministic binding digest for approval records.
    ///
    /// This is not a signature or a trust anchor.
    #[must_use]
    pub fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }

    fn skill_document(&self) -> &McpSkillResource {
        self.resources
            .get(self.uri())
            .expect("validated Skill manifests contain SKILL.md")
    }

    fn resource_by_path(&self, relative_path: &str) -> Option<&McpSkillResource> {
        self.resources
            .values()
            .find(|resource| resource.relative_path() == relative_path)
    }
}

impl fmt::Debug for McpSkillEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpSkillEntry")
            .field("uri", &self.uri)
            .field("name", &self.name)
            .field("resources", &self.resources.len())
            .field("total_bytes", &self.total_bytes)
            .field("manifest_digest", &self.manifest_digest)
            .finish_non_exhaustive()
    }
}

/// One bounded page returned by `skills/list`.
#[derive(Clone, Debug)]
pub struct McpSkillPage {
    skills: Vec<McpSkillEntry>,
    next_cursor: Option<Box<str>>,
    cache: McpCachePolicy,
}

impl McpSkillPage {
    /// Returns validated static entries in wire order.
    #[must_use]
    pub fn skills(&self) -> &[McpSkillEntry] {
        &self.skills
    }

    /// Returns the next bounded cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    /// Returns the required list cache metadata.
    #[must_use]
    pub const fn cache(&self) -> &McpCachePolicy {
        &self.cache
    }
}

/// Complete bounded static Skill catalog from one server binding.
#[derive(Clone, Debug)]
pub struct McpSkillCatalog {
    skills: Vec<McpSkillEntry>,
}

impl McpSkillCatalog {
    /// Returns every validated entry in server pagination order.
    #[must_use]
    pub fn skills(&self) -> &[McpSkillEntry] {
        &self.skills
    }

    /// Finds one entry by exact URI. Names are deliberately not identifiers.
    #[must_use]
    pub fn find_uri(&self, uri: &str) -> Option<&McpSkillEntry> {
        self.skills.iter().find(|entry| entry.uri() == uri)
    }
}

/// Exact raw bytes returned by one base `resources/read` request.
#[derive(Clone, Debug)]
pub struct McpSkillResourceContent {
    uri: Arc<str>,
    mime_type: Option<Arc<str>>,
    bytes: Arc<[u8]>,
}

impl McpSkillResourceContent {
    /// Returns the exact response resource URI.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the optional validated MIME type.
    #[must_use]
    pub fn mime_type(&self) -> Option<&str> {
        self.mime_type.as_deref()
    }

    /// Returns decoded raw content bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Strict MCP Skills client failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum McpSkillClientError {
    /// The underlying bounded stateless transport failed.
    #[error(transparent)]
    Transport(#[from] StatelessMcpClientError),
    /// This client did not opt into the Skills transport and capability profile.
    #[error("MCP client was not configured for Skills")]
    ClientSkillsDisabled,
    /// Discovery did not advertise the base Resources capability.
    #[error("MCP server did not advertise the Resources capability required by Skills")]
    ResourcesNotAdvertised,
    /// Discovery did not advertise the Skills extension.
    #[error("MCP server did not advertise the Skills extension")]
    SkillsNotAdvertised,
    /// The Skills extension capability object was malformed.
    #[error("MCP Skills capability is invalid")]
    InvalidCapability,
    /// This safe profile declines unverifiable dynamic manifests.
    #[error("dynamic MCP Skills are unsupported by the static Host profile")]
    DynamicSkillUnsupported,
    /// A `skills/list` result violated the Final extension contract.
    #[error("MCP Skills catalog response is invalid")]
    InvalidCatalog,
    /// A `skills/get` result violated the Final extension contract.
    #[error("MCP Skills lookup response is invalid")]
    InvalidLookup,
    /// A Skill entry or complete static manifest was malformed.
    #[error("MCP Skill entry is invalid")]
    InvalidEntry,
    /// A cursor was empty, repeated, or oversized.
    #[error("MCP Skills pagination cursor is invalid")]
    InvalidPagination,
    /// Catalog traversal exceeded the configured page ceiling.
    #[error("MCP Skills catalog exceeds its page limit")]
    CatalogPageLimit,
    /// Catalog traversal exceeded the configured entry ceiling.
    #[error("MCP Skills catalog exceeds its entry limit")]
    CatalogTooLarge,
    /// One URI appeared more than once across catalog pages.
    #[error("MCP Skills catalog contains a duplicate URI")]
    DuplicateSkillUri,
    /// A base Resource result could not be represented as one bounded file.
    #[error("MCP Skill resource response is invalid")]
    InvalidResource,
}

impl McpClient {
    /// Parses the negotiated Final Skills extension settings.
    pub fn skill_capabilities(&self) -> Result<McpSkillCapabilities, McpSkillClientError> {
        if !self.options().skills_enabled() {
            return Err(McpSkillClientError::ClientSkillsDisabled);
        }
        if !self.server().supports_resources() {
            return Err(McpSkillClientError::ResourcesNotAdvertised);
        }
        let extensions = self
            .server()
            .capabilities()
            .get("extensions")
            .and_then(Value::as_object)
            .ok_or(McpSkillClientError::SkillsNotAdvertised)?;
        let value = extensions
            .get(MCP_SKILLS_EXTENSION_ID)
            .ok_or(McpSkillClientError::SkillsNotAdvertised)?;
        let object = value
            .as_object()
            .ok_or(McpSkillClientError::InvalidCapability)?;
        let directory_read = match object.get("directoryRead") {
            Some(value) => value
                .as_bool()
                .ok_or(McpSkillClientError::InvalidCapability)?,
            None => false,
        };
        Ok(McpSkillCapabilities { directory_read })
    }

    /// Requests one bounded, strictly validated page of static Skills.
    pub async fn list_skills_page(
        &self,
        cursor: Option<&str>,
    ) -> Result<McpSkillPage, McpSkillClientError> {
        self.skill_capabilities()?;
        if cursor.is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES) {
            return Err(McpSkillClientError::InvalidPagination);
        }
        let mut params = Map::new();
        if let Some(cursor) = cursor {
            params.insert("cursor".to_owned(), Value::String(cursor.to_owned()));
        }
        let result = self
            .send_extension_rpc(SKILLS_LIST_METHOD, None, params)
            .await?;
        parse_skill_page(
            &result,
            self.binding_id(),
            self.options().maximum_skill_catalog_entries(),
        )
    }

    /// Traverses a complete bounded static Skill catalog.
    pub async fn list_skills(&self) -> Result<McpSkillCatalog, McpSkillClientError> {
        let mut cursor: Option<Box<str>> = None;
        let mut seen_cursors = HashSet::new();
        let mut seen_uris = HashSet::new();
        let mut skills = Vec::new();
        for _ in 0..self.options().maximum_catalog_pages() {
            let page = self.list_skills_page(cursor.as_deref()).await?;
            let next_len = skills
                .len()
                .checked_add(page.skills.len())
                .ok_or(McpSkillClientError::CatalogTooLarge)?;
            if next_len > self.options().maximum_skill_catalog_entries() {
                return Err(McpSkillClientError::CatalogTooLarge);
            }
            for skill in page.skills {
                if !seen_uris.insert(skill.uri().to_owned()) {
                    return Err(McpSkillClientError::DuplicateSkillUri);
                }
                skills.push(skill);
            }
            let Some(next) = page.next_cursor else {
                return Ok(McpSkillCatalog { skills });
            };
            if !seen_cursors.insert(next.to_string()) {
                return Err(McpSkillClientError::InvalidPagination);
            }
            cursor = Some(next);
        }
        Err(McpSkillClientError::CatalogPageLimit)
    }

    /// Resolves one static Skill by exact URI, independently of listing.
    pub async fn get_skill(&self, uri: &str) -> Result<McpSkillEntry, McpSkillClientError> {
        self.skill_capabilities()?;
        if uri.is_empty() || uri.len() > MAX_SKILL_URI_BYTES {
            return Err(McpSkillClientError::InvalidEntry);
        }
        let result = self
            .send_extension_rpc(
                SKILLS_GET_METHOD,
                None,
                Map::from_iter([("uri".to_owned(), Value::String(uri.to_owned()))]),
            )
            .await?;
        let object = result
            .as_object()
            .ok_or(McpSkillClientError::InvalidLookup)?;
        validate_complete(object, McpSkillClientError::InvalidLookup)?;
        parse_required_cache(object).map_err(|()| McpSkillClientError::InvalidLookup)?;
        let entry = parse_skill_entry(
            object
                .get("skill")
                .ok_or(McpSkillClientError::InvalidLookup)?,
            self.binding_id(),
        )?;
        if entry.uri() != uri {
            return Err(McpSkillClientError::InvalidLookup);
        }
        Ok(entry)
    }

    /// Reads one resource after Skills capability negotiation.
    ///
    /// Host callers must still verify these bytes against a retained entry.
    pub async fn read_skill_resource(
        &self,
        uri: &str,
    ) -> Result<McpSkillResourceContent, McpSkillClientError> {
        self.skill_capabilities()?;
        if uri.is_empty() || uri.len() > MAX_SKILL_URI_BYTES {
            return Err(McpSkillClientError::InvalidResource);
        }
        let result = self
            .send_extension_rpc(
                RESOURCE_READ_METHOD,
                Some(uri),
                Map::from_iter([("uri".to_owned(), Value::String(uri.to_owned()))]),
            )
            .await?;
        parse_resource_content(&result, uri)
    }
}

fn parse_skill_page(
    value: &Value,
    binding_id: u64,
    maximum_entries: usize,
) -> Result<McpSkillPage, McpSkillClientError> {
    let object = value
        .as_object()
        .ok_or(McpSkillClientError::InvalidCatalog)?;
    validate_complete(object, McpSkillClientError::InvalidCatalog)?;
    let cache = parse_required_cache(object).map_err(|()| McpSkillClientError::InvalidCatalog)?;
    let entries = object
        .get("skills")
        .and_then(Value::as_array)
        .filter(|entries| entries.len() <= maximum_entries)
        .ok_or(McpSkillClientError::InvalidCatalog)?;
    let mut skills = Vec::with_capacity(entries.len());
    let mut seen_uris = HashSet::new();
    for value in entries {
        let entry = parse_skill_entry(value, binding_id)?;
        if !seen_uris.insert(entry.uri().to_owned()) {
            return Err(McpSkillClientError::DuplicateSkillUri);
        }
        skills.push(entry);
    }
    let next_cursor = match object.get("nextCursor") {
        Some(value) => Some(
            value
                .as_str()
                .filter(|cursor| !cursor.is_empty() && cursor.len() <= MAX_CURSOR_BYTES)
                .ok_or(McpSkillClientError::InvalidPagination)?
                .to_owned()
                .into_boxed_str(),
        ),
        None => None,
    };
    Ok(McpSkillPage {
        skills,
        next_cursor,
        cache,
    })
}

fn parse_skill_entry(value: &Value, binding_id: u64) -> Result<McpSkillEntry, McpSkillClientError> {
    let object = value.as_object().ok_or(McpSkillClientError::InvalidEntry)?;
    let uri = object
        .get("uri")
        .and_then(Value::as_str)
        .filter(|uri| !uri.is_empty() && uri.len() <= MAX_SKILL_URI_BYTES)
        .ok_or(McpSkillClientError::InvalidEntry)?;
    let uri_parent = validate_skill_uri(uri).map_err(|_| McpSkillClientError::InvalidEntry)?;
    let frontmatter = object
        .get("frontmatter")
        .and_then(Value::as_object)
        .cloned()
        .ok_or(McpSkillClientError::InvalidEntry)?;
    if serde_json::to_vec(&frontmatter)
        .map_err(|_| McpSkillClientError::InvalidEntry)?
        .len()
        > MCP_SKILL_MAXIMUM_FRONTMATTER_BYTES
    {
        return Err(McpSkillClientError::InvalidEntry);
    }
    validate_frontmatter(&frontmatter).map_err(|_| McpSkillClientError::InvalidEntry)?;
    let name = frontmatter
        .get("name")
        .and_then(Value::as_str)
        .ok_or(McpSkillClientError::InvalidEntry)?;
    if name != uri_parent {
        return Err(McpSkillClientError::InvalidEntry);
    }
    let description = frontmatter
        .get("description")
        .and_then(Value::as_str)
        .ok_or(McpSkillClientError::InvalidEntry)?;
    let resources = match object.get("resources") {
        Some(Value::String(value)) if value == "dynamic" => {
            return Err(McpSkillClientError::DynamicSkillUnsupported);
        }
        Some(Value::Array(resources)) => resources,
        _ => return Err(McpSkillClientError::InvalidEntry),
    };
    if resources.is_empty() || resources.len() > MCP_SKILL_MAXIMUM_FILES {
        return Err(McpSkillClientError::InvalidEntry);
    }
    let root = uri
        .strip_suffix(SKILL_DOCUMENT)
        .ok_or(McpSkillClientError::InvalidEntry)?;
    let mut manifest = BTreeMap::new();
    let mut total_bytes = 0usize;
    for resource in resources {
        let resource = parse_skill_resource(resource, uri, root)?;
        total_bytes = total_bytes
            .checked_add(resource.size())
            .ok_or(McpSkillClientError::InvalidEntry)?;
        if total_bytes > MCP_SKILL_MAXIMUM_BYTES
            || manifest
                .insert(resource.uri().to_owned(), resource)
                .is_some()
        {
            return Err(McpSkillClientError::InvalidEntry);
        }
    }
    if !manifest.contains_key(uri) {
        return Err(McpSkillClientError::InvalidEntry);
    }
    let manifest_digest = manifest_binding_digest(uri, manifest.values());
    Ok(McpSkillEntry {
        binding_id,
        uri: Arc::from(uri),
        name: Arc::from(name),
        description: Arc::from(description),
        frontmatter: Arc::new(frontmatter),
        resources: Arc::new(manifest),
        manifest_digest: Arc::from(manifest_digest),
        total_bytes,
    })
}

fn parse_skill_resource(
    value: &Value,
    skill_uri: &str,
    root: &str,
) -> Result<McpSkillResource, McpSkillClientError> {
    let object = value.as_object().ok_or(McpSkillClientError::InvalidEntry)?;
    let uri = object
        .get("uri")
        .and_then(Value::as_str)
        .filter(|uri| !uri.is_empty() && uri.len() <= MAX_SKILL_URI_BYTES)
        .ok_or(McpSkillClientError::InvalidEntry)?;
    let relative_path = if uri == skill_uri {
        SKILL_DOCUMENT
    } else {
        uri.strip_prefix(root)
            .ok_or(McpSkillClientError::InvalidEntry)?
    };
    validate_relative_path(relative_path).map_err(|_| McpSkillClientError::InvalidEntry)?;
    if format!("{root}{relative_path}") != uri {
        return Err(McpSkillClientError::InvalidEntry);
    }
    let digest = object
        .get("digest")
        .and_then(Value::as_str)
        .filter(|digest| valid_sha256(digest))
        .ok_or(McpSkillClientError::InvalidEntry)?;
    let size = object
        .get("size")
        .and_then(Value::as_u64)
        .and_then(|size| usize::try_from(size).ok())
        .filter(|size| *size <= MCP_SKILL_MAXIMUM_BYTES)
        .ok_or(McpSkillClientError::InvalidEntry)?;
    Ok(McpSkillResource {
        uri: Arc::from(uri),
        digest: Arc::from(digest),
        size,
        relative_path: Arc::from(relative_path),
    })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn manifest_binding_digest<'a>(
    skill_uri: &str,
    resources: impl Iterator<Item = &'a McpSkillResource>,
) -> String {
    let mut material = b"stateknot/mcp-skills/approval/v1".to_vec();
    append_digest_part(&mut material, skill_uri.as_bytes());
    for resource in resources {
        append_digest_part(&mut material, resource.uri().as_bytes());
        append_digest_part(&mut material, resource.digest().as_bytes());
        append_digest_part(&mut material, &(resource.size() as u64).to_be_bytes());
    }
    Digest::sha256(material).to_string()
}

fn append_digest_part(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

fn parse_resource_content(
    value: &Value,
    expected_uri: &str,
) -> Result<McpSkillResourceContent, McpSkillClientError> {
    let object = value
        .as_object()
        .ok_or(McpSkillClientError::InvalidResource)?;
    validate_complete(object, McpSkillClientError::InvalidResource)?;
    let contents = object
        .get("contents")
        .and_then(Value::as_array)
        .filter(|contents| contents.len() == 1)
        .ok_or(McpSkillClientError::InvalidResource)?;
    let content = contents[0]
        .as_object()
        .ok_or(McpSkillClientError::InvalidResource)?;
    let uri = content
        .get("uri")
        .and_then(Value::as_str)
        .filter(|uri| *uri == expected_uri)
        .ok_or(McpSkillClientError::InvalidResource)?;
    let mime_type = match content.get("mimeType") {
        Some(value) => {
            let mime = value
                .as_str()
                .filter(|mime| {
                    !mime.is_empty()
                        && mime.len() <= MAX_MIME_TYPE_BYTES
                        && mime.trim() == *mime
                        && mime.parse::<mime::Mime>().is_ok()
                })
                .ok_or(McpSkillClientError::InvalidResource)?;
            Some(Arc::from(mime))
        }
        None => None,
    };
    let bytes = match (content.get("text"), content.get("blob")) {
        (Some(Value::String(text)), None) => text.as_bytes().to_vec(),
        (None, Some(Value::String(blob))) => BASE64_STANDARD
            .decode(blob)
            .map_err(|_| McpSkillClientError::InvalidResource)?,
        _ => return Err(McpSkillClientError::InvalidResource),
    };
    if bytes.len() > MCP_SKILL_MAXIMUM_BYTES {
        return Err(McpSkillClientError::InvalidResource);
    }
    Ok(McpSkillResourceContent {
        uri: Arc::from(uri),
        mime_type,
        bytes: Arc::from(bytes),
    })
}

fn validate_complete(
    object: &Map<String, Value>,
    error: McpSkillClientError,
) -> Result<(), McpSkillClientError> {
    if object.get("resultType").and_then(Value::as_str) == Some("complete") {
        Ok(())
    } else {
        Err(error)
    }
}

fn parse_required_cache(object: &Map<String, Value>) -> Result<McpCachePolicy, ()> {
    let ttl_ms = object.get("ttlMs").and_then(Value::as_u64).ok_or(())?;
    let scope = object
        .get("cacheScope")
        .and_then(Value::as_str)
        .filter(|scope| matches!(*scope, "private" | "public"))
        .ok_or(())?;
    Ok(McpCachePolicy::required(ttl_ms, scope.into()))
}

/// Host-assigned server label used in every Skill identity and model-visible file.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct McpSkillOrigin(Arc<str>);

impl McpSkillOrigin {
    /// Constructs a bounded printable label. Never use self-reported server info.
    pub fn new(value: impl Into<String>) -> Result<Self, McpSkillOriginError> {
        let value = value.into();
        if value.is_empty() {
            return Err(McpSkillOriginError::Empty);
        }
        if value.len() > MAX_ORIGIN_LABEL_BYTES {
            return Err(McpSkillOriginError::TooLong);
        }
        if value.trim() != value {
            return Err(McpSkillOriginError::BoundaryWhitespace);
        }
        if value.chars().any(char::is_control) {
            return Err(McpSkillOriginError::ControlCharacter);
        }
        Ok(Self(Arc::from(value)))
    }

    /// Returns the exact host-assigned label.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for McpSkillOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("McpSkillOrigin")
            .field(&self.0)
            .finish()
    }
}

/// Invalid host-assigned MCP server label.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpSkillOriginError {
    /// The label was empty.
    #[error("MCP Skill origin label is empty")]
    Empty,
    /// The label exceeded 128 bytes.
    #[error("MCP Skill origin label is too long")]
    TooLong,
    /// Trimming would change the label.
    #[error("MCP Skill origin label has boundary whitespace")]
    BoundaryWhitespace,
    /// The label contained a control character.
    #[error("MCP Skill origin label contains a control character")]
    ControlCharacter,
}

/// Collision-safe identity: host-assigned origin plus exact server-scoped URI.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct McpSkillIdentity {
    origin: McpSkillOrigin,
    uri: Arc<str>,
}

impl McpSkillIdentity {
    /// Returns the host-assigned server origin.
    #[must_use]
    pub fn origin(&self) -> &McpSkillOrigin {
        &self.origin
    }

    /// Returns the exact URI within that origin.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

/// Why a fresh Skill activation was requested.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpSkillActivationSource {
    /// User or host selected a top-level Skill directly.
    Direct,
    /// An active Skill requested one of its manifest-listed nested Skills.
    Nested {
        /// Collision-safe identity of the parent Skill.
        identity: McpSkillIdentity,
        /// Durable authority window of the parent Skill.
        parent_window_id: SkillActingWindowId,
    },
}

/// Caller-retained idempotency identities for one activation attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpSkillActivationAttempt {
    approval_id: SkillActivationApprovalId,
    window_id: SkillActingWindowId,
}

impl McpSkillActivationAttempt {
    /// Generates fresh `UUIDv7` identities which callers retain across retries.
    #[must_use]
    pub fn generate() -> Self {
        Self {
            approval_id: SkillActivationApprovalId::generate(),
            window_id: SkillActingWindowId::generate(),
        }
    }

    /// Constructs an explicit retry-stable activation attempt.
    #[must_use]
    pub const fn new(
        approval_id: SkillActivationApprovalId,
        window_id: SkillActingWindowId,
    ) -> Self {
        Self {
            approval_id,
            window_id,
        }
    }

    /// Returns the immutable approval identity.
    #[must_use]
    pub const fn approval_id(self) -> SkillActivationApprovalId {
        self.approval_id
    }

    /// Returns the durable acting-window identity.
    #[must_use]
    pub const fn window_id(self) -> SkillActingWindowId {
        self.window_id
    }
}

/// Owned, content-bound facts presented before a Skill is fetched or activated.
#[derive(Clone, Debug)]
pub struct McpSkillActivationRequest {
    identity: McpSkillIdentity,
    source: McpSkillActivationSource,
    entry: McpSkillEntry,
    scope: SkillActivationScope,
    attempt: McpSkillActivationAttempt,
}

impl McpSkillActivationRequest {
    /// Returns the collision-safe identity being approved.
    #[must_use]
    pub const fn identity(&self) -> &McpSkillIdentity {
        &self.identity
    }

    /// Returns whether this is direct or fresh nested activation.
    #[must_use]
    pub const fn source(&self) -> &McpSkillActivationSource {
        &self.source
    }

    /// Returns the exact entry whose complete Manifest binds the decision.
    #[must_use]
    pub const fn entry(&self) -> &McpSkillEntry {
        &self.entry
    }

    /// Returns the exact durable run scope receiving authority.
    #[must_use]
    pub fn scope(&self) -> &SkillActivationScope {
        &self.scope
    }

    /// Returns the caller-retained idempotency identities.
    #[must_use]
    pub const fn attempt(&self) -> McpSkillActivationAttempt {
        self.attempt
    }

    /// Returns the untrusted `allowed-tools` request, without granting it.
    #[must_use]
    pub fn requested_allowed_tools(&self) -> Option<&str> {
        self.entry
            .frontmatter()
            .get("allowed-tools")
            .and_then(Value::as_str)
    }
}

/// Owned facts for one Tool operation proposed during an active Skill window.
#[derive(Clone, Debug)]
pub struct McpSkillToolAuthorizationRequest {
    identity: McpSkillIdentity,
    manifest_digest: Arc<str>,
    activation_id: u64,
    acting_window_id: SkillActingWindowId,
    tool_name: Arc<str>,
    tool_identity: Option<CapabilityIdentity>,
    tool_descriptor_digest: Option<Digest>,
    operation: McpSkillToolOperation,
    invocation: Option<McpSkillToolInvocation>,
    host_code_execution: bool,
    requested_allowed_tools: Option<Arc<str>>,
}

impl McpSkillToolAuthorizationRequest {
    /// Returns the collision-safe active Skill identity.
    #[must_use]
    pub const fn identity(&self) -> &McpSkillIdentity {
        &self.identity
    }

    /// Returns the complete Manifest binding digest.
    #[must_use]
    pub fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }

    /// Returns this process-local activation identifier.
    #[must_use]
    pub const fn activation_id(&self) -> u64 {
        self.activation_id
    }

    /// Returns the durable authority window for this operation.
    #[must_use]
    pub const fn acting_window_id(&self) -> SkillActingWindowId {
        self.acting_window_id
    }

    /// Returns the exact proposed host Tool name.
    #[must_use]
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    /// Returns the exact owner-qualified Tool version for a runtime-bound call.
    ///
    /// Name-only custom execution paths return `None`; policies may reject
    /// those paths when an exact registry binding is required.
    #[must_use]
    pub const fn tool_identity(&self) -> Option<&CapabilityIdentity> {
        self.tool_identity.as_ref()
    }

    /// Returns the canonical descriptor digest for a runtime-bound call.
    #[must_use]
    pub const fn tool_descriptor_digest(&self) -> Option<Digest> {
        self.tool_descriptor_digest
    }

    /// Returns whether execution or recovery reconciliation was requested.
    #[must_use]
    pub const fn operation(&self) -> McpSkillToolOperation {
        self.operation
    }

    /// Returns exact bounded invocation facts for a runtime-bound operation.
    ///
    /// Name-only custom execution paths return `None`.
    #[must_use]
    pub const fn invocation(&self) -> Option<&McpSkillToolInvocation> {
        self.invocation.as_ref()
    }

    /// Returns whether the Tool can execute code on the Host.
    #[must_use]
    pub const fn host_code_execution(&self) -> bool {
        self.host_code_execution
    }

    /// Returns the untrusted frontmatter request for UI comparison only.
    #[must_use]
    pub fn requested_allowed_tools(&self) -> Option<&str> {
        self.requested_allowed_tools.as_deref()
    }
}

/// Explicit application policy for activation and every acting-window Tool call.
pub trait McpSkillHostPolicy: Send + Sync + 'static {
    /// Obtains fresh user/policy consent before any Skill file is fetched.
    fn approve_activation(
        &self,
        request: McpSkillActivationRequest,
    ) -> BoxFuture<'_, Result<McpSkillActivationGrant, McpSkillHostPolicyError>>;

    /// Authorizes one exact Tool operation without trusting `allowed-tools` as a grant.
    fn authorize_tool_call(
        &self,
        request: McpSkillToolAuthorizationRequest,
    ) -> BoxFuture<'_, Result<McpSkillToolAuthorizationGrant, McpSkillHostPolicyError>>;
}

/// Exact policy evidence and bounded authority lifetime for an activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpSkillActivationGrant {
    policy: CapabilityIdentity,
    policy_digest: Digest,
    decision_digest: Digest,
    duration: SkillActingWindowDuration,
}

impl McpSkillActivationGrant {
    /// Constructs a version-pinned activation grant.
    #[must_use]
    pub const fn new(
        policy: CapabilityIdentity,
        policy_digest: Digest,
        decision_digest: Digest,
        duration: SkillActingWindowDuration,
    ) -> Self {
        Self {
            policy,
            policy_digest,
            decision_digest,
            duration,
        }
    }

    /// Returns the policy implementation identity.
    #[must_use]
    pub const fn policy(&self) -> &CapabilityIdentity {
        &self.policy
    }

    /// Returns the immutable policy artifact binding.
    #[must_use]
    pub const fn policy_digest(&self) -> Digest {
        self.policy_digest
    }

    /// Returns the private decision-evidence binding.
    #[must_use]
    pub const fn decision_digest(&self) -> Digest {
        self.decision_digest
    }

    /// Returns the approved bounded lifetime.
    #[must_use]
    pub const fn duration(&self) -> SkillActingWindowDuration {
        self.duration
    }
}

/// Exact policy evidence retained by a successful Skill Tool authorization.
///
/// The decision digest binds private policy inputs without placing them in the
/// permit or durable authorization receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpSkillToolAuthorizationGrant {
    policy: CapabilityIdentity,
    policy_digest: Digest,
    decision_digest: Digest,
}

impl McpSkillToolAuthorizationGrant {
    /// Constructs exact immutable policy evidence for one decision.
    #[must_use]
    pub const fn new(
        policy: CapabilityIdentity,
        policy_digest: Digest,
        decision_digest: Digest,
    ) -> Self {
        Self {
            policy,
            policy_digest,
            decision_digest,
        }
    }

    /// Returns the owner-qualified, version-pinned policy implementation.
    #[must_use]
    pub const fn policy(&self) -> &CapabilityIdentity {
        &self.policy
    }

    /// Returns the immutable policy artifact binding.
    #[must_use]
    pub const fn policy_digest(&self) -> Digest {
        self.policy_digest
    }

    /// Returns the policy-supplied exact decision evidence binding.
    #[must_use]
    pub const fn decision_digest(&self) -> Digest {
        self.decision_digest
    }
}

/// Safe default denying all remote Skill activation and execution.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenyMcpSkillHostPolicy;

impl McpSkillHostPolicy for DenyMcpSkillHostPolicy {
    fn approve_activation(
        &self,
        _request: McpSkillActivationRequest,
    ) -> BoxFuture<'_, Result<McpSkillActivationGrant, McpSkillHostPolicyError>> {
        Box::pin(async { Err(McpSkillHostPolicyError::Denied) })
    }

    fn authorize_tool_call(
        &self,
        _request: McpSkillToolAuthorizationRequest,
    ) -> BoxFuture<'_, Result<McpSkillToolAuthorizationGrant, McpSkillHostPolicyError>> {
        Box::pin(async { Err(McpSkillHostPolicyError::Denied) })
    }
}

/// Public-safe Host policy failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpSkillHostPolicyError {
    /// User or policy denied this exact operation.
    #[error("MCP Skill Host policy denied the operation")]
    Denied,
    /// The policy authority is temporarily unavailable.
    #[error("MCP Skill Host policy is unavailable")]
    Unavailable,
}

/// Bounded private memory-cache policy for verified files.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpSkillHostOptions {
    maximum_cache_entries: usize,
    maximum_cache_bytes: usize,
}

impl McpSkillHostOptions {
    /// Hard process-local cache entry ceiling.
    pub const HARD_MAXIMUM_CACHE_ENTRIES: usize = 65_536;
    /// Hard process-local cache byte ceiling.
    pub const HARD_MAXIMUM_CACHE_BYTES: usize = 1024 * MEBIBYTE;

    /// Constructs positive cache limits within implementation ceilings.
    pub const fn new(
        maximum_cache_entries: usize,
        maximum_cache_bytes: usize,
    ) -> Result<Self, McpSkillHostOptionsError> {
        if maximum_cache_entries == 0 || maximum_cache_bytes == 0 {
            return Err(McpSkillHostOptionsError::ZeroLimit);
        }
        if maximum_cache_entries > Self::HARD_MAXIMUM_CACHE_ENTRIES
            || maximum_cache_bytes > Self::HARD_MAXIMUM_CACHE_BYTES
        {
            return Err(McpSkillHostOptionsError::AboveHardMaximum);
        }
        Ok(Self {
            maximum_cache_entries,
            maximum_cache_bytes,
        })
    }

    /// Returns the process-local verified-file entry ceiling.
    #[must_use]
    pub const fn maximum_cache_entries(self) -> usize {
        self.maximum_cache_entries
    }

    /// Returns the process-local verified-file byte ceiling.
    #[must_use]
    pub const fn maximum_cache_bytes(self) -> usize {
        self.maximum_cache_bytes
    }
}

impl Default for McpSkillHostOptions {
    fn default() -> Self {
        Self {
            maximum_cache_entries: 4096,
            maximum_cache_bytes: 64 * MEBIBYTE,
        }
    }
}

/// Invalid MCP Skill Host cache policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpSkillHostOptionsError {
    /// Cache limits must be positive.
    #[error("MCP Skill Host cache limits must be positive")]
    ZeroLimit,
    /// A cache limit exceeded its hard ceiling.
    #[error("MCP Skill Host cache limit exceeds the hard maximum")]
    AboveHardMaximum,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    binding_id: u64,
    origin: McpSkillOrigin,
    uri: Arc<str>,
    digest: Arc<str>,
    size: usize,
}

#[derive(Default)]
struct MemoryCache {
    entries: HashMap<CacheKey, Arc<McpSkillResourceContent>>,
    bytes: usize,
}

struct McpSkillHostInner {
    client: McpClient,
    origin: McpSkillOrigin,
    scope: SkillActivationScope,
    policy: Arc<dyn McpSkillHostPolicy>,
    activation_store: Arc<dyn SkillActivationStore>,
    options: McpSkillHostOptions,
    cache: Mutex<MemoryCache>,
    next_activation_id: AtomicU64,
}

/// Secure static-manifest Host bound to one exact MCP client and assigned origin.
#[derive(Clone)]
pub struct McpSkillHost {
    inner: Arc<McpSkillHostInner>,
}

impl McpSkillHost {
    /// Builds a fail-closed Host. Use [`crate::McpClientOptions::for_skills`]
    /// when connecting the underlying client.
    pub fn new(
        client: McpClient,
        origin: McpSkillOrigin,
        scope: SkillActivationScope,
        policy: Arc<dyn McpSkillHostPolicy>,
        activation_store: Arc<dyn SkillActivationStore>,
        options: McpSkillHostOptions,
    ) -> Result<Self, McpSkillHostError> {
        let transport = client.options().transport();
        if transport.maximum_response_bytes() < MCP_SKILL_WIRE_RESPONSE_BYTES
            || transport.maximum_sse_line_bytes() < MCP_SKILL_WIRE_RESPONSE_BYTES
            || transport.maximum_sse_event_bytes() < MCP_SKILL_WIRE_RESPONSE_BYTES
            || transport.maximum_sse_total_bytes() < MCP_SKILL_WIRE_RESPONSE_BYTES
        {
            return Err(McpSkillHostError::TransportProfileTooSmall);
        }
        client.skill_capabilities()?;
        Ok(Self {
            inner: Arc::new(McpSkillHostInner {
                client,
                origin,
                scope,
                policy,
                activation_store,
                options,
                cache: Mutex::new(MemoryCache::default()),
                next_activation_id: AtomicU64::new(1),
            }),
        })
    }

    /// Returns the host-assigned origin, never self-reported server metadata.
    #[must_use]
    pub fn origin(&self) -> &McpSkillOrigin {
        &self.inner.origin
    }

    /// Returns the exact durable run scope receiving Skill authority.
    #[must_use]
    pub fn scope(&self) -> &SkillActivationScope {
        &self.inner.scope
    }

    /// Lists entries without fetching any Skill files.
    pub async fn list_skills(&self) -> Result<McpSkillCatalog, McpSkillHostError> {
        Ok(self.inner.client.list_skills().await?)
    }

    /// Resolves one entry without fetching its Skill files.
    pub async fn get_skill(&self, uri: &str) -> Result<McpSkillEntry, McpSkillHostError> {
        Ok(self.inner.client.get_skill(uri).await?)
    }

    /// Assigns the collision-safe identity for one entry from this binding.
    pub fn identity(&self, entry: &McpSkillEntry) -> Result<McpSkillIdentity, McpSkillHostError> {
        self.require_local_entry(entry)?;
        Ok(McpSkillIdentity {
            origin: self.inner.origin.clone(),
            uri: entry.uri.clone(),
        })
    }

    /// Resolves, approves, verifies, and activates one exact URI.
    pub async fn activate_uri(
        &self,
        uri: &str,
        attempt: McpSkillActivationAttempt,
    ) -> Result<McpActivatedSkill, McpSkillHostError> {
        let entry = self.get_skill(uri).await?;
        self.activate_entry(entry, McpSkillActivationSource::Direct, attempt)
            .await
    }

    /// Freshly approves and activates one listed entry.
    pub async fn activate(
        &self,
        entry: &McpSkillEntry,
        attempt: McpSkillActivationAttempt,
    ) -> Result<McpActivatedSkill, McpSkillHostError> {
        self.activate_entry(entry.clone(), McpSkillActivationSource::Direct, attempt)
            .await
    }

    async fn activate_entry(
        &self,
        entry: McpSkillEntry,
        source: McpSkillActivationSource,
        attempt: McpSkillActivationAttempt,
    ) -> Result<McpActivatedSkill, McpSkillHostError> {
        let identity = self.identity(&entry)?;
        let grant = self
            .inner
            .policy
            .approve_activation(McpSkillActivationRequest {
                identity: identity.clone(),
                source: source.clone(),
                entry: entry.clone(),
                scope: self.inner.scope.clone(),
                attempt,
            })
            .await
            .map_err(map_activation_policy_error)?;
        let instructions = self.verify_instructions(&identity, &entry).await?;
        let subject = Self::authorization_subject(&identity, &entry)?;
        let durable_source = match source {
            McpSkillActivationSource::Direct => SkillActivationSource::Direct,
            McpSkillActivationSource::Nested {
                parent_window_id, ..
            } => SkillActivationSource::Nested { parent_window_id },
        };
        let approval = SkillActivationApproval::new(
            attempt.approval_id(),
            self.inner.scope.clone(),
            subject,
            durable_source,
            grant.policy().clone(),
            grant.policy_digest(),
            grant.decision_digest(),
            grant.duration(),
        )
        .map_err(|_| McpSkillHostError::ActivationEvidenceInvalid)?;
        let window = self
            .inner
            .activation_store
            .open(SkillActingWindowOpenRequest::new(
                attempt.window_id(),
                approval.clone(),
            ))
            .await
            .map_err(map_activation_store_error)?;
        if window.window_id() != attempt.window_id() || window.approval() != &approval {
            return Err(McpSkillHostError::ActivationStoreRejected);
        }
        self.inner
            .activation_store
            .assert_active(window.clone())
            .await
            .map_err(map_activation_store_error)?;
        self.finish_activation(identity, entry, instructions, window)
    }

    /// Restores one exact, unexpired, unrevoked acting window after restart.
    pub async fn resume(
        &self,
        entry: &McpSkillEntry,
        window_id: SkillActingWindowId,
    ) -> Result<McpActivatedSkill, McpSkillHostError> {
        self.require_local_entry(entry)?;
        let window = self
            .inner
            .activation_store
            .load_active(self.inner.scope.tenant_id().clone(), window_id)
            .await
            .map_err(map_activation_store_error)?;
        let identity = self.identity(entry)?;
        let subject = Self::authorization_subject(&identity, entry)?;
        if window.approval().scope() != &self.inner.scope || window.approval().subject() != &subject
        {
            return Err(McpSkillHostError::ActivationStoreRejected);
        }
        let instructions = self.verify_instructions(&identity, entry).await?;
        self.inner
            .activation_store
            .assert_active(window.clone())
            .await
            .map_err(map_activation_store_error)?;
        self.finish_activation(identity, entry.clone(), instructions, window)
    }

    fn finish_activation(
        &self,
        identity: McpSkillIdentity,
        entry: McpSkillEntry,
        instructions: McpVerifiedSkillFile,
        window: SkillActingWindow,
    ) -> Result<McpActivatedSkill, McpSkillHostError> {
        let activation_id = allocate_monotonic_id(&self.inner.next_activation_id)
            .ok_or(McpSkillHostError::ActivationIdExhausted)?;
        Ok(McpActivatedSkill {
            host: self.clone(),
            identity,
            entry,
            instructions,
            activation_id,
            window,
        })
    }

    async fn verify_instructions(
        &self,
        identity: &McpSkillIdentity,
        entry: &McpSkillEntry,
    ) -> Result<McpVerifiedSkillFile, McpSkillHostError> {
        let instructions = self
            .verified_file(identity, entry, entry.skill_document())
            .await?;
        let text = std::str::from_utf8(instructions.bytes())
            .map_err(|_| McpSkillHostError::SkillDocumentNotUtf8)?;
        let frontmatter = parse_frontmatter(text)
            .map_err(|_| McpSkillHostError::FrontmatterVerificationFailed)?;
        if &frontmatter != entry.frontmatter() {
            return Err(McpSkillHostError::FrontmatterVerificationFailed);
        }
        Ok(instructions)
    }

    fn authorization_subject(
        identity: &McpSkillIdentity,
        entry: &McpSkillEntry,
    ) -> Result<SkillAuthorizationSubject, McpSkillHostError> {
        let manifest_digest = entry
            .manifest_digest()
            .parse()
            .map_err(|_| McpSkillHostError::ActivationEvidenceInvalid)?;
        SkillAuthorizationSubject::new(
            "mcp",
            identity.origin().as_str(),
            identity.uri(),
            manifest_digest,
        )
        .map_err(|_| McpSkillHostError::ActivationEvidenceInvalid)
    }

    async fn verified_file(
        &self,
        identity: &McpSkillIdentity,
        entry: &McpSkillEntry,
        resource: &McpSkillResource,
    ) -> Result<McpVerifiedSkillFile, McpSkillHostError> {
        self.require_local_entry(entry)?;
        let key = CacheKey {
            binding_id: entry.binding_id,
            origin: identity.origin.clone(),
            uri: resource.uri.clone(),
            digest: resource.digest.clone(),
            size: resource.size,
        };
        if let Some(content) = self
            .inner
            .cache
            .lock()
            .map_err(|_| McpSkillHostError::CacheUnavailable)?
            .entries
            .get(&key)
            .cloned()
        {
            return Ok(McpVerifiedSkillFile::new(
                identity.clone(),
                resource.relative_path.clone(),
                content,
            ));
        }
        let content = Arc::new(
            self.inner
                .client
                .read_skill_resource(resource.uri())
                .await?,
        );
        verify_resource(resource, &content)?;
        let mut cache = self
            .inner
            .cache
            .lock()
            .map_err(|_| McpSkillHostError::CacheUnavailable)?;
        if let Some(existing) = cache.entries.get(&key).cloned() {
            return Ok(McpVerifiedSkillFile::new(
                identity.clone(),
                resource.relative_path.clone(),
                existing,
            ));
        }
        let next_bytes = cache.bytes.checked_add(content.bytes().len());
        if cache.entries.len() < self.inner.options.maximum_cache_entries
            && next_bytes.is_some_and(|bytes| bytes <= self.inner.options.maximum_cache_bytes)
        {
            cache.bytes = next_bytes.expect("checked cache size exists");
            cache.entries.insert(key, Arc::clone(&content));
        }
        Ok(McpVerifiedSkillFile::new(
            identity.clone(),
            resource.relative_path.clone(),
            content,
        ))
    }

    fn require_local_entry(&self, entry: &McpSkillEntry) -> Result<(), McpSkillHostError> {
        if entry.binding_id == self.inner.client.binding_id() {
            Ok(())
        } else {
            Err(McpSkillHostError::ForeignClientBinding)
        }
    }
}

impl fmt::Debug for McpSkillHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpSkillHost")
            .field("origin", &self.inner.origin)
            .field("scope", &self.inner.scope)
            .field("client", &self.inner.client)
            .field("options", &self.inner.options)
            .field("policy", &"[POLICY]")
            .field("activation_store", &"[DURABLE STORE]")
            .finish_non_exhaustive()
    }
}

/// One verified, origin-tagged file safe to expose only as untrusted MCP input.
#[derive(Clone)]
pub struct McpVerifiedSkillFile {
    identity: McpSkillIdentity,
    relative_path: Arc<str>,
    content: Arc<McpSkillResourceContent>,
}

impl McpVerifiedSkillFile {
    fn new(
        identity: McpSkillIdentity,
        relative_path: Arc<str>,
        content: Arc<McpSkillResourceContent>,
    ) -> Self {
        Self {
            identity,
            relative_path,
            content,
        }
    }

    /// Returns the collision-safe MCP origin tag that must remain model-visible.
    #[must_use]
    pub const fn identity(&self) -> &McpSkillIdentity {
        &self.identity
    }

    /// Returns the canonical path relative to the active Skill root.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns the exact originating Resource URI.
    #[must_use]
    pub fn uri(&self) -> &str {
        self.content.uri()
    }

    /// Returns the server-declared MIME type after syntax validation.
    #[must_use]
    pub fn mime_type(&self) -> Option<&str> {
        self.content.mime_type()
    }

    /// Returns digest- and size-verified raw bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        self.content.bytes()
    }

    /// Returns verified UTF-8 text when representable.
    #[must_use]
    pub fn text(&self) -> Option<&str> {
        std::str::from_utf8(self.bytes()).ok()
    }
}

impl fmt::Debug for McpVerifiedSkillFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpVerifiedSkillFile")
            .field("identity", &self.identity)
            .field("relative_path", &self.relative_path)
            .field("uri", &self.content.uri)
            .field("size", &self.content.bytes.len())
            .finish_non_exhaustive()
    }
}

/// Direct child derived locally from the retained complete Manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpSkillDirectoryEntry {
    relative_path: Arc<str>,
    uri: Arc<str>,
    directory: bool,
}

impl McpSkillDirectoryEntry {
    /// Returns the path relative to the active Skill root.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns the exact or derived URI at the originating server.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns whether this direct child represents a directory.
    #[must_use]
    pub const fn is_directory(&self) -> bool {
        self.directory
    }
}

/// Acting-window handle retaining exact durable authority and verified content.
pub struct McpActivatedSkill {
    host: McpSkillHost,
    identity: McpSkillIdentity,
    entry: McpSkillEntry,
    instructions: McpVerifiedSkillFile,
    activation_id: u64,
    window: SkillActingWindow,
}

impl McpActivatedSkill {
    /// Returns the origin-scoped Skill identity.
    #[must_use]
    pub const fn identity(&self) -> &McpSkillIdentity {
        &self.identity
    }

    /// Returns the exact held entry for the full acting window.
    #[must_use]
    pub const fn entry(&self) -> &McpSkillEntry {
        &self.entry
    }

    /// Returns the verified, origin-tagged `SKILL.md` content.
    #[must_use]
    pub const fn instructions(&self) -> &McpVerifiedSkillFile {
        &self.instructions
    }

    /// Returns the exact durable approval and authority window.
    #[must_use]
    pub const fn acting_window(&self) -> &SkillActingWindow {
        &self.window
    }

    /// Lazily reads one manifest-listed file from the same originating server.
    pub async fn read_file(
        &self,
        relative_path: &str,
    ) -> Result<McpVerifiedSkillFile, McpSkillHostError> {
        self.assert_active().await?;
        validate_relative_path(relative_path)
            .map_err(|_| McpSkillHostError::InvalidRelativePath)?;
        let resource = self
            .entry
            .resource_by_path(relative_path)
            .ok_or(McpSkillHostError::ResourceOutsideHeldManifest)?;
        let file = self
            .host
            .verified_file(&self.identity, &self.entry, resource)
            .await?;
        self.assert_active().await?;
        Ok(file)
    }

    /// Lists direct children from the held Manifest without a live directory read.
    pub fn list_directory(
        &self,
        relative_directory: &str,
    ) -> Result<Vec<McpSkillDirectoryEntry>, McpSkillHostError> {
        if !relative_directory.is_empty() {
            validate_relative_path(relative_directory)
                .map_err(|_| McpSkillHostError::InvalidRelativePath)?;
        }
        let prefix = if relative_directory.is_empty() {
            String::new()
        } else {
            format!("{relative_directory}/")
        };
        let root = self
            .entry
            .uri()
            .strip_suffix(SKILL_DOCUMENT)
            .expect("validated Skill URI ends in SKILL.md");
        let mut children = BTreeMap::<String, bool>::new();
        for resource in self.entry.resources() {
            let Some(remainder) = resource.relative_path().strip_prefix(&prefix) else {
                continue;
            };
            if remainder.is_empty() {
                continue;
            }
            let (name, directory) = remainder
                .split_once('/')
                .map_or((remainder, false), |(name, _)| (name, true));
            let child = format!("{prefix}{name}");
            children
                .entry(child)
                .and_modify(|current| *current |= directory)
                .or_insert(directory);
        }
        Ok(children
            .into_iter()
            .map(|(relative_path, directory)| McpSkillDirectoryEntry {
                uri: Arc::from(format!("{root}{relative_path}")),
                relative_path: Arc::from(relative_path),
                directory,
            })
            .collect())
    }

    /// Activates one nested `SKILL.md` only after a separate fresh approval.
    pub async fn activate_nested(
        &self,
        relative_skill_document: &str,
        attempt: McpSkillActivationAttempt,
    ) -> Result<McpActivatedSkill, McpSkillHostError> {
        self.assert_active().await?;
        validate_relative_path(relative_skill_document)
            .map_err(|_| McpSkillHostError::InvalidRelativePath)?;
        if relative_skill_document == SKILL_DOCUMENT
            || !relative_skill_document.ends_with("/SKILL.md")
        {
            return Err(McpSkillHostError::InvalidNestedSkill);
        }
        let resource = self
            .entry
            .resource_by_path(relative_skill_document)
            .ok_or(McpSkillHostError::ResourceOutsideHeldManifest)?;
        let nested = self.host.get_skill(resource.uri()).await?;
        self.host
            .activate_entry(
                nested,
                McpSkillActivationSource::Nested {
                    identity: self.identity.clone(),
                    parent_window_id: self.window.window_id(),
                },
                attempt,
            )
            .await
    }

    /// Permanently revokes this authority window with immutable evidence.
    pub async fn revoke(
        &self,
        reason: SkillActingWindowRevocationReason,
    ) -> Result<SkillActingWindowRevocation, McpSkillHostError> {
        self.host
            .inner
            .activation_store
            .revoke(self.window.clone(), reason)
            .await
            .map_err(map_activation_store_error)
    }

    async fn assert_active(&self) -> Result<(), McpSkillHostError> {
        self.host
            .inner
            .activation_store
            .assert_active(self.window.clone())
            .await
            .map_err(map_activation_store_error)
    }

    /// Obtains an explicit permit for one exact Tool call during this window.
    pub async fn authorize_tool_call<'activation>(
        &'activation self,
        tool_name: &str,
        host_code_execution: bool,
    ) -> Result<McpSkillExecutionPermit<'activation>, McpSkillHostError> {
        self.assert_active().await?;
        if tool_name.is_empty()
            || tool_name.len() > MAX_TOOL_NAME_BYTES
            || tool_name.trim() != tool_name
            || tool_name.chars().any(char::is_control)
        {
            return Err(McpSkillHostError::InvalidToolName);
        }
        let requested_allowed_tools = self
            .entry
            .frontmatter()
            .get("allowed-tools")
            .and_then(Value::as_str)
            .map(Arc::from);
        let grant = self
            .host
            .inner
            .policy
            .authorize_tool_call(McpSkillToolAuthorizationRequest {
                identity: self.identity.clone(),
                manifest_digest: self.entry.manifest_digest.clone(),
                activation_id: self.activation_id,
                acting_window_id: self.window.window_id(),
                tool_name: Arc::from(tool_name),
                tool_identity: None,
                tool_descriptor_digest: None,
                operation: McpSkillToolOperation::Execute,
                invocation: None,
                host_code_execution,
                requested_allowed_tools,
            })
            .await
            .map_err(map_tool_policy_error)?;
        self.assert_active().await?;
        Ok(McpSkillExecutionPermit {
            identity: self.identity.clone(),
            manifest_digest: self.entry.manifest_digest.clone(),
            activation_id: self.activation_id,
            acting_window: self.window.clone(),
            tool_name: Arc::from(tool_name),
            tool_identity: None,
            tool_descriptor_digest: None,
            operation: McpSkillToolOperation::Execute,
            invocation: None,
            host_code_execution,
            grant,
            activation: PhantomData,
        })
    }

    pub(crate) async fn authorize_bound_tool_operation<'activation>(
        &'activation self,
        binding: &McpSkillToolBinding,
        operation: McpSkillToolOperation,
        invocation: &McpSkillToolInvocation,
    ) -> Result<McpSkillExecutionPermit<'activation>, McpSkillHostError> {
        self.assert_active().await?;
        let requested_allowed_tools = self
            .entry
            .frontmatter()
            .get("allowed-tools")
            .and_then(Value::as_str)
            .map(Arc::from);
        let tool_name = Arc::from(binding.tool_name());
        let grant = self
            .host
            .inner
            .policy
            .authorize_tool_call(McpSkillToolAuthorizationRequest {
                identity: self.identity.clone(),
                manifest_digest: self.entry.manifest_digest.clone(),
                activation_id: self.activation_id,
                acting_window_id: self.window.window_id(),
                tool_name: Arc::clone(&tool_name),
                tool_identity: Some(binding.identity().clone()),
                tool_descriptor_digest: Some(binding.descriptor_digest()),
                operation,
                invocation: Some(invocation.clone()),
                host_code_execution: binding.host_code_execution(),
                requested_allowed_tools,
            })
            .await
            .map_err(map_tool_policy_error)?;
        Ok(McpSkillExecutionPermit {
            identity: self.identity.clone(),
            manifest_digest: self.entry.manifest_digest.clone(),
            activation_id: self.activation_id,
            acting_window: self.window.clone(),
            tool_name,
            tool_identity: Some(binding.identity().clone()),
            tool_descriptor_digest: Some(binding.descriptor_digest()),
            operation,
            invocation: Some(invocation.clone()),
            host_code_execution: binding.host_code_execution(),
            grant,
            activation: PhantomData,
        })
    }
}

impl fmt::Debug for McpActivatedSkill {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpActivatedSkill")
            .field("identity", &self.identity)
            .field("manifest_digest", &self.entry.manifest_digest)
            .field("activation_id", &self.activation_id)
            .field("acting_window", &self.window)
            .finish_non_exhaustive()
    }
}

/// Non-constructible proof of explicit policy approval for one exact Tool operation.
#[derive(Debug)]
#[must_use = "the permit must be consumed by the exact approved Tool execution"]
pub struct McpSkillExecutionPermit<'activation> {
    identity: McpSkillIdentity,
    manifest_digest: Arc<str>,
    activation_id: u64,
    acting_window: SkillActingWindow,
    tool_name: Arc<str>,
    tool_identity: Option<CapabilityIdentity>,
    tool_descriptor_digest: Option<Digest>,
    operation: McpSkillToolOperation,
    invocation: Option<McpSkillToolInvocation>,
    host_code_execution: bool,
    grant: McpSkillToolAuthorizationGrant,
    activation: PhantomData<&'activation McpActivatedSkill>,
}

impl McpSkillExecutionPermit<'_> {
    /// Returns the approved Skill identity.
    #[must_use]
    pub const fn identity(&self) -> &McpSkillIdentity {
        &self.identity
    }

    /// Returns the content binding approved for this call.
    #[must_use]
    pub fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }

    /// Returns the process-local activation identifier.
    #[must_use]
    pub const fn activation_id(&self) -> u64 {
        self.activation_id
    }

    /// Returns the durable window which must remain active at receipt commit.
    #[must_use]
    pub const fn acting_window(&self) -> &SkillActingWindow {
        &self.acting_window
    }

    /// Returns the exact approved Tool name.
    #[must_use]
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    /// Returns the exact runtime Tool identity when this permit is registry-bound.
    #[must_use]
    pub const fn tool_identity(&self) -> Option<&CapabilityIdentity> {
        self.tool_identity.as_ref()
    }

    /// Returns the exact runtime descriptor digest when registry-bound.
    #[must_use]
    pub const fn tool_descriptor_digest(&self) -> Option<Digest> {
        self.tool_descriptor_digest
    }

    /// Returns the approved execution or reconciliation operation.
    #[must_use]
    pub const fn operation(&self) -> McpSkillToolOperation {
        self.operation
    }

    /// Returns exact bounded invocation facts when this permit is registry-bound.
    #[must_use]
    pub const fn invocation(&self) -> Option<&McpSkillToolInvocation> {
        self.invocation.as_ref()
    }

    /// Returns whether Host code execution was disclosed and approved.
    #[must_use]
    pub const fn host_code_execution(&self) -> bool {
        self.host_code_execution
    }

    /// Returns the exact policy evidence authorizing this operation.
    #[must_use]
    pub const fn grant(&self) -> &McpSkillToolAuthorizationGrant {
        &self.grant
    }
}

/// Closed Host lifecycle failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum McpSkillHostError {
    /// The strict Skills client boundary rejected the exchange.
    #[error(transparent)]
    Client(#[from] McpSkillClientError),
    /// The connected client transport cannot carry the SEP-2640 support floor.
    #[error("MCP client transport limits are too small for static Skills")]
    TransportProfileTooSmall,
    /// An entry originated from another client binding.
    #[error("MCP Skill entry belongs to another client binding")]
    ForeignClientBinding,
    /// Fresh activation approval was denied.
    #[error("MCP Skill activation was denied")]
    ActivationDenied,
    /// Tool execution approval was denied.
    #[error("MCP Skill Tool call was denied")]
    ToolCallDenied,
    /// The policy authority was unavailable.
    #[error("MCP Skill Host policy is unavailable")]
    PolicyUnavailable,
    /// Fetched bytes did not match the retained Manifest.
    #[error("MCP Skill resource verification failed")]
    ResourceVerificationFailed,
    /// The fetched `SKILL.md` was not UTF-8.
    #[error("MCP Skill SKILL.md is not UTF-8")]
    SkillDocumentNotUtf8,
    /// Parsed `SKILL.md` frontmatter differed from the approved entry.
    #[error("MCP Skill frontmatter verification failed")]
    FrontmatterVerificationFailed,
    /// A file read was outside the held complete Manifest.
    #[error("MCP Skill resource is outside the held Manifest")]
    ResourceOutsideHeldManifest,
    /// A relative path was not canonical and safe.
    #[error("invalid MCP Skill relative path")]
    InvalidRelativePath,
    /// A nested activation did not identify a descendant `SKILL.md`.
    #[error("invalid nested MCP Skill activation")]
    InvalidNestedSkill,
    /// A proposed Tool name was empty, oversized, or ambiguous.
    #[error("invalid MCP Skill Tool name")]
    InvalidToolName,
    /// Private cache synchronization was unavailable.
    #[error("MCP Skill private cache is unavailable")]
    CacheUnavailable,
    /// A unique process-local activation ID could not be allocated.
    #[error("MCP Skill activation identifier space is exhausted")]
    ActivationIdExhausted,
    /// Activation evidence could not be constructed from validated inputs.
    #[error("MCP Skill activation evidence is invalid")]
    ActivationEvidenceInvalid,
    /// Durable activation storage was temporarily unavailable.
    #[error("MCP Skill activation store is unavailable")]
    ActivationStoreUnavailable,
    /// The durable acting window did not exist.
    #[error("MCP Skill acting window was not found")]
    ActingWindowNotFound,
    /// The durable acting window expired or was revoked.
    #[error("MCP Skill acting window is inactive")]
    ActingWindowInactive,
    /// Durable activation evidence conflicted with the requested activation.
    #[error("MCP Skill activation store rejected the evidence")]
    ActivationStoreRejected,
}

fn verify_resource(
    expected: &McpSkillResource,
    actual: &McpSkillResourceContent,
) -> Result<(), McpSkillHostError> {
    if actual.uri() != expected.uri()
        || actual.bytes().len() != expected.size()
        || Digest::sha256(actual.bytes()).to_string() != expected.digest()
    {
        return Err(McpSkillHostError::ResourceVerificationFailed);
    }
    Ok(())
}

fn map_activation_policy_error(error: McpSkillHostPolicyError) -> McpSkillHostError {
    match error {
        McpSkillHostPolicyError::Denied => McpSkillHostError::ActivationDenied,
        McpSkillHostPolicyError::Unavailable => McpSkillHostError::PolicyUnavailable,
    }
}

fn map_tool_policy_error(error: McpSkillHostPolicyError) -> McpSkillHostError {
    match error {
        McpSkillHostPolicyError::Denied => McpSkillHostError::ToolCallDenied,
        McpSkillHostPolicyError::Unavailable => McpSkillHostError::PolicyUnavailable,
    }
}

fn map_activation_store_error(
    error: stateknot_core::SkillActivationStoreError,
) -> McpSkillHostError {
    let failure = error.failure();
    drop(error);
    match failure {
        SkillActivationStoreFailure::Unavailable => McpSkillHostError::ActivationStoreUnavailable,
        SkillActivationStoreFailure::NotFound => McpSkillHostError::ActingWindowNotFound,
        SkillActivationStoreFailure::Inactive => McpSkillHostError::ActingWindowInactive,
        _ => McpSkillHostError::ActivationStoreRejected,
    }
}

fn allocate_monotonic_id(counter: &AtomicU64) -> Option<u64> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current != u64::MAX).then_some(current + 1)
        })
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn static_entry_rejects_dynamic_and_manifest_escape() {
        let dynamic = json!({
            "uri": "skill://review/SKILL.md",
            "frontmatter": {"name": "review", "description": "Review code."},
            "resources": "dynamic"
        });
        assert!(matches!(
            parse_skill_entry(&dynamic, 1),
            Err(McpSkillClientError::DynamicSkillUnsupported)
        ));

        let escaped = json!({
            "uri": "skill://review/SKILL.md",
            "frontmatter": {"name": "review", "description": "Review code."},
            "resources": [{
                "uri": "skill://other/secret.txt",
                "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "size": 1
            }]
        });
        assert!(matches!(
            parse_skill_entry(&escaped, 1),
            Err(McpSkillClientError::InvalidEntry)
        ));
    }

    #[test]
    fn origin_and_digest_are_stable_and_collision_safe() {
        assert!(McpSkillOrigin::new(" docs ").is_err());
        let first = McpSkillIdentity {
            origin: McpSkillOrigin::new("docs-a").unwrap(),
            uri: Arc::from("skill://review/SKILL.md"),
        };
        let second = McpSkillIdentity {
            origin: McpSkillOrigin::new("docs-b").unwrap(),
            uri: Arc::from("skill://review/SKILL.md"),
        };
        assert_ne!(first, second);
    }

    #[test]
    fn static_entry_requires_a_complete_bounded_canonical_manifest() {
        let digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let missing_document = json!({
            "uri": "skill://review/SKILL.md",
            "frontmatter": {"name": "review", "description": "Review code."},
            "resources": [{
                "uri": "skill://review/reference.md",
                "digest": digest,
                "size": 1
            }]
        });
        assert!(matches!(
            parse_skill_entry(&missing_document, 1),
            Err(McpSkillClientError::InvalidEntry)
        ));

        let upper_case_digest = json!({
            "uri": "skill://review/SKILL.md",
            "frontmatter": {"name": "review", "description": "Review code."},
            "resources": [{
                "uri": "skill://review/SKILL.md",
                "digest": "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "size": 1
            }]
        });
        assert!(matches!(
            parse_skill_entry(&upper_case_digest, 1),
            Err(McpSkillClientError::InvalidEntry)
        ));

        let oversized_frontmatter = json!({
            "uri": "skill://review/SKILL.md",
            "frontmatter": {
                "name": "review",
                "description": "Review code.",
                "untrusted-extension": "x".repeat(MCP_SKILL_MAXIMUM_FRONTMATTER_BYTES)
            },
            "resources": [{
                "uri": "skill://review/SKILL.md",
                "digest": digest,
                "size": 1
            }]
        });
        assert!(matches!(
            parse_skill_entry(&oversized_frontmatter, 1),
            Err(McpSkillClientError::InvalidEntry)
        ));
    }

    #[test]
    fn catalog_page_enforces_the_callers_entry_ceiling() {
        let entry = json!({
            "uri": "skill://review/SKILL.md",
            "frontmatter": {"name": "review", "description": "Review code."},
            "resources": [{
                "uri": "skill://review/SKILL.md",
                "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "size": 1
            }]
        });
        let page = json!({
            "resultType": "complete",
            "skills": [entry.clone(), entry],
            "ttlMs": 0,
            "cacheScope": "private"
        });
        assert!(matches!(
            parse_skill_page(&page, 1, 1),
            Err(McpSkillClientError::InvalidCatalog)
        ));
    }
}
