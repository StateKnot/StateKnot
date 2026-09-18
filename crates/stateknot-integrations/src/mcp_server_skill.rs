// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Immutable, authorization-first MCP Skills server profile.
//!
//! This module implements the server half of the Final SEP-2640
//! `io.modelcontextprotocol/skills` extension. Skill bytes are frozen at
//! startup. The same immutable snapshot drives the advertised manifest and
//! `resources/read`, preventing manifest/content drift by construction.

use std::{
    borrow::Cow,
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    fmt,
    rc::Rc,
    sync::Arc,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        CustomRequest, CustomResult, DiscoverResult, ExtensionCapabilities, Implementation,
        ListResourceTemplatesResult, ListResourcesResult, PaginatedRequestParams, ProtocolVersion,
        ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult, Resource,
        ResourceContents, ResourcesCapability, ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
};
use serde::{
    Deserialize, Deserializer,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value, json};
use stateknot_core::{BoxFuture, Digest};
use thiserror::Error;

use crate::{
    McpServerApplicationOptions, McpServerCacheScope, McpServerPrincipal, mcp_server_principal,
    mcp_server_tool::append_digest_part,
};

/// Stable MCP extension identifier assigned by SEP-2640.
pub const MCP_SKILLS_EXTENSION_ID: &str = "io.modelcontextprotocol/skills";

const SKILL_LIST_METHOD: &str = "skills/list";
const SKILL_GET_METHOD: &str = "skills/get";
const SKILL_DOCUMENT: &str = "SKILL.md";
const MEBIBYTE: usize = 1024 * 1024;
const MAX_URI_BYTES: usize = 4096;
const MAX_RELATIVE_PATH_BYTES: usize = 4096;
const MAX_FRONTMATTER_BYTES: usize = 64 * 1024;
const MAX_SCOPE_COUNT: usize = 128;
const MAX_SCOPE_BYTES: usize = 256;

/// One immutable file within an MCP-served Agent Skill.
#[derive(Clone)]
pub struct McpServerSkillFile {
    relative_path: Arc<str>,
    mime_type: Arc<str>,
    bytes: Arc<[u8]>,
    representation: McpServerSkillFileRepresentation,
    digest: Arc<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum McpServerSkillFileRepresentation {
    Text,
    Binary,
}

impl McpServerSkillFile {
    /// Creates a UTF-8 text file with an explicit MIME type.
    pub fn text(
        relative_path: impl Into<String>,
        mime_type: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<Self, McpServerSkillFileError> {
        let text = text.into();
        Self::new(
            relative_path.into(),
            mime_type.into(),
            Arc::<[u8]>::from(text.into_bytes()),
            McpServerSkillFileRepresentation::Text,
        )
    }

    /// Creates an opaque binary file with an explicit MIME type.
    pub fn binary(
        relative_path: impl Into<String>,
        mime_type: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) -> Result<Self, McpServerSkillFileError> {
        Self::new(
            relative_path.into(),
            mime_type.into(),
            Arc::<[u8]>::from(bytes.into()),
            McpServerSkillFileRepresentation::Binary,
        )
    }

    fn new(
        relative_path: String,
        mime_type: String,
        bytes: Arc<[u8]>,
        representation: McpServerSkillFileRepresentation,
    ) -> Result<Self, McpServerSkillFileError> {
        validate_relative_path(&relative_path)?;
        if mime_type.is_empty()
            || mime_type.len() > 255
            || mime_type.trim() != mime_type
            || mime_type.parse::<mime::Mime>().is_err()
        {
            return Err(McpServerSkillFileError::InvalidMimeType);
        }
        if bytes.len() > McpServerSkillDefinition::HARD_MAXIMUM_BYTES {
            return Err(McpServerSkillFileError::TooLarge);
        }
        if matches!(representation, McpServerSkillFileRepresentation::Text)
            && std::str::from_utf8(&bytes).is_err()
        {
            return Err(McpServerSkillFileError::InvalidUtf8);
        }
        let digest = Digest::sha256(&bytes).to_string();
        Ok(Self {
            relative_path: Arc::from(relative_path),
            mime_type: Arc::from(mime_type),
            bytes,
            representation,
            digest: Arc::from(digest),
        })
    }

    /// Returns the canonical path relative to the Skill root.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns the declared MIME type.
    #[must_use]
    pub fn mime_type(&self) -> &str {
        &self.mime_type
    }

    /// Returns the exact raw byte size represented in the manifest.
    #[must_use]
    pub fn size(&self) -> usize {
        self.bytes.len()
    }

    /// Returns the exact SHA-256 digest represented in the manifest.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

impl fmt::Debug for McpServerSkillFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerSkillFile")
            .field("relative_path", &self.relative_path)
            .field("mime_type", &self.mime_type)
            .field("size", &self.bytes.len())
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// Invalid Skill file metadata or bytes.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerSkillFileError {
    /// The relative path was non-canonical or unsafe for a URI path.
    #[error("invalid MCP Skill relative path")]
    InvalidRelativePath,
    /// The MIME type was malformed or oversized.
    #[error("invalid MCP Skill file MIME type")]
    InvalidMimeType,
    /// A text file was not UTF-8.
    #[error("MCP Skill text file is not UTF-8")]
    InvalidUtf8,
    /// One file exceeded the complete Skill byte ceiling.
    #[error("MCP Skill file is too large")]
    TooLarge,
}

/// One fully materialized Agent Skill and its complete file manifest.
#[derive(Clone)]
pub struct McpServerSkillDefinition {
    uri: Arc<str>,
    name: Arc<str>,
    description: Arc<str>,
    frontmatter: Arc<Map<String, Value>>,
    files: Arc<BTreeMap<String, McpServerSkillFile>>,
    required_scopes: Arc<[Box<str>]>,
    total_bytes: usize,
}

impl McpServerSkillDefinition {
    /// Maximum files in one Skill, matching the SEP-2640 host support floor.
    pub const HARD_MAXIMUM_FILES: usize = 512;
    /// Maximum aggregate bytes in one Skill, matching the SEP-2640 host support floor.
    pub const HARD_MAXIMUM_BYTES: usize = 16 * MEBIBYTE;

    /// Freezes a complete static Skill from exact file bytes.
    pub fn new(
        uri: impl Into<String>,
        files: impl IntoIterator<Item = McpServerSkillFile>,
    ) -> Result<Self, McpServerSkillDefinitionError> {
        let uri = uri.into();
        let uri_parent = validate_skill_uri(&uri)?;
        let mut by_path = BTreeMap::new();
        let mut total_bytes = 0_usize;
        for file in files {
            if by_path.len() == Self::HARD_MAXIMUM_FILES {
                return Err(McpServerSkillDefinitionError::TooManyFiles);
            }
            total_bytes = total_bytes
                .checked_add(file.bytes.len())
                .ok_or(McpServerSkillDefinitionError::TooLarge)?;
            if total_bytes > Self::HARD_MAXIMUM_BYTES {
                return Err(McpServerSkillDefinitionError::TooLarge);
            }
            if by_path
                .insert(file.relative_path.to_string(), file)
                .is_some()
            {
                return Err(McpServerSkillDefinitionError::DuplicateFile);
            }
        }
        if by_path.is_empty() {
            return Err(McpServerSkillDefinitionError::Empty);
        }
        let skill_document = by_path
            .get(SKILL_DOCUMENT)
            .ok_or(McpServerSkillDefinitionError::MissingSkillDocument)?;
        if !matches!(
            skill_document.representation,
            McpServerSkillFileRepresentation::Text
        ) {
            return Err(McpServerSkillDefinitionError::SkillDocumentNotText);
        }
        let text = std::str::from_utf8(&skill_document.bytes)
            .map_err(|_| McpServerSkillDefinitionError::SkillDocumentNotText)?;
        let frontmatter = parse_frontmatter(text)?;
        validate_frontmatter(&frontmatter)?;
        let name = frontmatter
            .get("name")
            .and_then(Value::as_str)
            .expect("validated Skill frontmatter has a name");
        if name != uri_parent {
            return Err(McpServerSkillDefinitionError::NameUriMismatch);
        }
        let description = frontmatter
            .get("description")
            .and_then(Value::as_str)
            .expect("validated Skill frontmatter has a description");
        for relative_path in by_path.keys() {
            resource_uri(&uri, relative_path)?;
        }
        Ok(Self {
            uri: Arc::from(uri),
            name: Arc::from(name),
            description: Arc::from(description),
            frontmatter: Arc::new(frontmatter),
            files: Arc::new(by_path),
            required_scopes: Arc::from([]),
            total_bytes,
        })
    }

    /// Requires every listed OAuth-style scope for discovery and reads.
    pub fn with_required_scopes(
        mut self,
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, McpServerSkillDefinitionError> {
        self.required_scopes = validate_scopes(scopes)?;
        Ok(self)
    }

    /// Returns the resource URI of this Skill's `SKILL.md`.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the Agent Skills name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the Agent Skills description.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the complete preserved frontmatter object.
    #[must_use]
    pub fn frontmatter(&self) -> &Map<String, Value> {
        &self.frontmatter
    }

    /// Returns files in canonical relative-path order.
    pub fn files(&self) -> impl ExactSizeIterator<Item = &McpServerSkillFile> {
        self.files.values()
    }

    /// Returns required scopes in canonical order.
    pub fn required_scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.required_scopes.iter().map(AsRef::as_ref)
    }

    fn has_required_scopes(&self, principal: &McpServerPrincipal) -> bool {
        self.required_scopes()
            .all(|scope| principal.has_scope(scope))
    }

    fn to_wire(&self) -> Value {
        let resources = self
            .files
            .values()
            .map(|file| {
                json!({
                    "uri": resource_uri(&self.uri, file.relative_path())
                        .expect("validated Skill paths always produce valid URIs"),
                    "digest": file.digest(),
                    "size": file.size(),
                })
            })
            .collect::<Vec<_>>();
        json!({
            "uri": self.uri(),
            "frontmatter": self.frontmatter(),
            "resources": resources,
        })
    }
}

impl fmt::Debug for McpServerSkillDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerSkillDefinition")
            .field("uri", &self.uri)
            .field("name", &self.name)
            .field("files", &self.files.len())
            .field("total_bytes", &self.total_bytes)
            .field("required_scopes", &self.required_scopes)
            .finish_non_exhaustive()
    }
}

/// Invalid Agent Skill definition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerSkillDefinitionError {
    /// The `SKILL.md` resource URI was malformed or non-canonical.
    #[error("invalid MCP Skill URI")]
    InvalidUri,
    /// No files were supplied.
    #[error("MCP Skill has no files")]
    Empty,
    /// The required `SKILL.md` file was absent.
    #[error("MCP Skill is missing SKILL.md")]
    MissingSkillDocument,
    /// The required `SKILL.md` file was not UTF-8 text.
    #[error("MCP Skill SKILL.md is not text")]
    SkillDocumentNotText,
    /// A relative path appeared twice.
    #[error("duplicate MCP Skill file")]
    DuplicateFile,
    /// One Skill exceeded the 512-file ceiling.
    #[error("too many MCP Skill files")]
    TooManyFiles,
    /// One Skill exceeded the 16 MiB byte ceiling.
    #[error("MCP Skill exceeds the byte ceiling")]
    TooLarge,
    /// YAML frontmatter was missing, malformed, duplicated, or non-object.
    #[error("invalid MCP Skill frontmatter")]
    InvalidFrontmatter,
    /// A required or defined Agent Skills frontmatter field was invalid.
    #[error("invalid MCP Skill frontmatter field")]
    InvalidFrontmatterField,
    /// The URI parent name did not match frontmatter `name`.
    #[error("MCP Skill URI parent does not match its name")]
    NameUriMismatch,
    /// Too many authorization scopes were configured.
    #[error("too many MCP Skill scopes")]
    TooManyScopes,
    /// One authorization scope was malformed.
    #[error("invalid MCP Skill scope")]
    InvalidScope,
    /// One authorization scope appeared twice.
    #[error("duplicate MCP Skill scope")]
    DuplicateScope,
}

/// Aggregate limits for a frozen MCP Skill catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpServerSkillCatalogLimits {
    skills: usize,
    files: usize,
    bytes: usize,
}

impl McpServerSkillCatalogLimits {
    /// Hard Skill-count ceiling.
    pub const HARD_MAXIMUM_SKILLS: usize = 4096;
    /// Hard aggregate file-count ceiling.
    pub const HARD_MAXIMUM_FILES: usize = 65_536;
    /// Hard aggregate in-memory byte ceiling.
    pub const HARD_MAXIMUM_BYTES: usize = 1024 * MEBIBYTE;

    /// Constructs explicit positive catalog limits within implementation ceilings.
    pub const fn new(
        maximum_skills: usize,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<Self, McpServerSkillCatalogLimitsError> {
        if maximum_skills == 0 || maximum_files == 0 || maximum_bytes == 0 {
            return Err(McpServerSkillCatalogLimitsError::ZeroLimit);
        }
        if maximum_skills > Self::HARD_MAXIMUM_SKILLS
            || maximum_files > Self::HARD_MAXIMUM_FILES
            || maximum_bytes > Self::HARD_MAXIMUM_BYTES
        {
            return Err(McpServerSkillCatalogLimitsError::AboveHardMaximum);
        }
        Ok(Self {
            skills: maximum_skills,
            files: maximum_files,
            bytes: maximum_bytes,
        })
    }
}

impl Default for McpServerSkillCatalogLimits {
    fn default() -> Self {
        Self {
            skills: 256,
            files: 4096,
            bytes: 64 * MEBIBYTE,
        }
    }
}

/// Invalid Skill catalog limit policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerSkillCatalogLimitsError {
    /// Every configured limit must be positive.
    #[error("MCP Skill catalog limits must be positive")]
    ZeroLimit,
    /// A configured limit exceeded its implementation ceiling.
    #[error("MCP Skill catalog limit exceeds the hard maximum")]
    AboveHardMaximum,
}

/// Startup-only builder for an immutable Skill catalog.
#[derive(Debug)]
pub struct McpServerSkillCatalogBuilder {
    limits: McpServerSkillCatalogLimits,
    skills: BTreeMap<String, McpServerSkillDefinition>,
    resource_uris: BTreeSet<String>,
    total_files: usize,
    total_bytes: usize,
}

impl McpServerSkillCatalogBuilder {
    /// Creates an empty catalog builder with explicit aggregate limits.
    #[must_use]
    pub fn new(limits: McpServerSkillCatalogLimits) -> Self {
        Self {
            limits,
            skills: BTreeMap::new(),
            resource_uris: BTreeSet::new(),
            total_files: 0,
            total_bytes: 0,
        }
    }

    /// Registers one fully validated static Skill.
    pub fn register(
        &mut self,
        definition: McpServerSkillDefinition,
    ) -> Result<(), McpServerSkillCatalogError> {
        if self.skills.len() == self.limits.skills {
            return Err(McpServerSkillCatalogError::TooManySkills);
        }
        if self.skills.contains_key(definition.uri()) {
            return Err(McpServerSkillCatalogError::DuplicateSkill);
        }
        let file_count = definition.files.len();
        let next_files = self
            .total_files
            .checked_add(file_count)
            .ok_or(McpServerSkillCatalogError::TooManyFiles)?;
        if next_files > self.limits.files {
            return Err(McpServerSkillCatalogError::TooManyFiles);
        }
        let next_bytes = self
            .total_bytes
            .checked_add(definition.total_bytes)
            .ok_or(McpServerSkillCatalogError::TooLarge)?;
        if next_bytes > self.limits.bytes {
            return Err(McpServerSkillCatalogError::TooLarge);
        }
        let uris = definition
            .files
            .keys()
            .map(|path| {
                resource_uri(definition.uri(), path)
                    .expect("validated Skill paths always produce valid URIs")
            })
            .collect::<Vec<_>>();
        if uris.iter().any(|uri| self.resource_uris.contains(uri)) {
            return Err(McpServerSkillCatalogError::DuplicateResource);
        }
        self.resource_uris.extend(uris);
        self.total_files = next_files;
        self.total_bytes = next_bytes;
        self.skills.insert(definition.uri().to_owned(), definition);
        Ok(())
    }

    /// Freezes a non-empty catalog in deterministic URI order.
    pub fn build(self) -> Result<McpServerSkillCatalog, McpServerSkillCatalogError> {
        if self.skills.is_empty() {
            return Err(McpServerSkillCatalogError::Empty);
        }
        let mut digest_material = b"stateknot/mcp-skills/catalog/v1".to_vec();
        let mut resources = BTreeMap::new();
        for definition in self.skills.values() {
            append_digest_part(&mut digest_material, definition.uri().as_bytes());
            for file in definition.files.values() {
                let uri = resource_uri(definition.uri(), file.relative_path())
                    .expect("validated Skill paths always produce valid URIs");
                append_digest_part(&mut digest_material, uri.as_bytes());
                append_digest_part(&mut digest_material, file.digest().as_bytes());
                resources.insert(
                    uri,
                    McpServerSkillResourceBinding {
                        skill_uri: definition.uri.clone(),
                        file: file.clone(),
                    },
                );
            }
        }
        let digest = digest_hex(digest_material);
        Ok(McpServerSkillCatalog {
            inner: Arc::new(McpServerSkillCatalogInner {
                skills: self.skills,
                resources,
                digest: Arc::from(digest),
                total_files: self.total_files,
                total_bytes: self.total_bytes,
            }),
        })
    }
}

impl Default for McpServerSkillCatalogBuilder {
    fn default() -> Self {
        Self::new(McpServerSkillCatalogLimits::default())
    }
}

/// Immutable MCP Skill metadata and file bytes.
#[derive(Clone)]
pub struct McpServerSkillCatalog {
    inner: Arc<McpServerSkillCatalogInner>,
}

struct McpServerSkillCatalogInner {
    skills: BTreeMap<String, McpServerSkillDefinition>,
    resources: BTreeMap<String, McpServerSkillResourceBinding>,
    digest: Arc<str>,
    total_files: usize,
    total_bytes: usize,
}

#[derive(Clone)]
struct McpServerSkillResourceBinding {
    skill_uri: Arc<str>,
    file: McpServerSkillFile,
}

impl McpServerSkillCatalog {
    /// Returns a Skill by exact `SKILL.md` URI.
    #[must_use]
    pub fn get(&self, uri: &str) -> Option<&McpServerSkillDefinition> {
        self.inner.skills.get(uri)
    }

    /// Returns the number of Skills.
    #[must_use]
    pub fn skill_count(&self) -> usize {
        self.inner.skills.len()
    }

    /// Returns the aggregate file count.
    #[must_use]
    pub fn file_count(&self) -> usize {
        self.inner.total_files
    }

    /// Returns the aggregate raw byte count.
    #[must_use]
    pub fn byte_count(&self) -> usize {
        self.inner.total_bytes
    }

    pub(crate) fn contains_resource(&self, uri: &str) -> bool {
        self.inner.resources.contains_key(uri)
    }

    fn contains_scoped_skills(&self) -> bool {
        self.inner
            .skills
            .values()
            .any(|skill| !skill.required_scopes.is_empty())
    }
}

impl fmt::Debug for McpServerSkillCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerSkillCatalog")
            .field("skills", &self.skill_count())
            .field("files", &self.file_count())
            .field("bytes", &self.byte_count())
            .field("catalog_digest", &self.inner.digest)
            .finish()
    }
}

/// Invalid Skill catalog mutation or build.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerSkillCatalogError {
    /// No Skill was registered.
    #[error("MCP Skill catalog is empty")]
    Empty,
    /// A `SKILL.md` URI appeared twice.
    #[error("duplicate MCP Skill URI")]
    DuplicateSkill,
    /// A resource URI was shared by multiple Skills.
    #[error("duplicate MCP Skill resource URI")]
    DuplicateResource,
    /// The configured Skill-count limit was reached.
    #[error("too many MCP Skills")]
    TooManySkills,
    /// The configured aggregate file-count limit was exceeded.
    #[error("too many MCP Skill files")]
    TooManyFiles,
    /// The configured aggregate byte limit was exceeded.
    #[error("MCP Skill catalog is too large")]
    TooLarge,
}

/// Skill operation presented to the authorization policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpServerSkillOperation {
    /// Include one Skill in discovery.
    List,
    /// Resolve one exact Skill entry.
    Get,
    /// Read one file belonging to a Skill.
    Read,
}

/// Owned facts for an authorization decision.
#[derive(Clone, Debug)]
pub struct McpServerSkillAuthorizationRequest {
    principal: McpServerPrincipal,
    operation: McpServerSkillOperation,
    uri: Arc<str>,
}

impl McpServerSkillAuthorizationRequest {
    /// Returns the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &McpServerPrincipal {
        &self.principal
    }

    /// Returns the requested operation.
    #[must_use]
    pub const fn operation(&self) -> McpServerSkillOperation {
        self.operation
    }

    /// Returns the untrusted requested Skill or file URI.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

/// Decoded Skill authorization policy.
pub trait McpServerSkillAuthorization: Send + Sync + 'static {
    /// Authorizes before direct lookup discloses whether a URI exists.
    fn authorize(
        &self,
        request: McpServerSkillAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerSkillAuthorizationError>>;

    /// Returns whether decisions are identical for every principal for the cache TTL.
    ///
    /// The safe default is `false`. Implementations must return `true` only when
    /// neither identity, scopes, external policy state, nor time can change the
    /// visible catalog during the advertised cache lifetime.
    fn is_public_cache_safe(&self) -> bool {
        false
    }
}

/// Explicit policy allowing every scope-qualified Skill operation.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowMcpServerSkillAuthorization;

impl McpServerSkillAuthorization for AllowMcpServerSkillAuthorization {
    fn authorize(
        &self,
        _request: McpServerSkillAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerSkillAuthorizationError>> {
        Box::pin(async { Ok(()) })
    }

    fn is_public_cache_safe(&self) -> bool {
        true
    }
}

/// Public-safe Skill authorization failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerSkillAuthorizationError {
    /// Policy denied this operation.
    #[error("MCP Skill operation is forbidden")]
    Forbidden,
    /// The policy authority is unavailable.
    #[error("MCP Skill authorization is unavailable")]
    Unavailable,
}

/// Production server for the static-manifest SEP-2640 profile.
#[derive(Clone)]
pub struct McpServerSkillService {
    catalog: McpServerSkillCatalog,
    options: McpServerApplicationOptions,
    authorization: Arc<dyn McpServerSkillAuthorization>,
}

impl McpServerSkillService {
    /// Creates a service with an explicit decoded authorization policy.
    pub fn new<A>(
        catalog: McpServerSkillCatalog,
        options: McpServerApplicationOptions,
        authorization: A,
    ) -> Result<Self, McpServerSkillServiceBuildError>
    where
        A: McpServerSkillAuthorization,
    {
        if matches!(options.cache_scope, McpServerCacheScope::Public) {
            if catalog.contains_scoped_skills() {
                return Err(McpServerSkillServiceBuildError::PublicCacheWithScopedSkills);
            }
            if !authorization.is_public_cache_safe() {
                return Err(McpServerSkillServiceBuildError::PublicCacheWithDynamicPolicy);
            }
        }
        Ok(Self {
            catalog,
            options,
            authorization: Arc::new(authorization),
        })
    }

    /// Returns the immutable backing catalog.
    #[must_use]
    pub const fn catalog(&self) -> &McpServerSkillCatalog {
        &self.catalog
    }

    pub(crate) fn contains_resource(&self, uri: &str) -> bool {
        self.catalog.contains_resource(uri)
    }

    pub(crate) fn resource_uris(&self) -> impl Iterator<Item = &str> {
        self.catalog.inner.resources.keys().map(String::as_str)
    }

    pub(crate) fn capabilities() -> ServerCapabilities {
        let mut capabilities = ServerCapabilities::default();
        capabilities.resources = Some(ResourcesCapability::default());
        let mut extensions = ExtensionCapabilities::new();
        extensions.insert(MCP_SKILLS_EXTENSION_ID.to_owned(), Map::new());
        capabilities.extensions = Some(extensions);
        capabilities
    }

    fn scope_tag(&self, principal: &McpServerPrincipal) -> String {
        if matches!(self.options.cache_scope, McpServerCacheScope::Public) {
            return "public".to_owned();
        }
        let mut material = principal.subject().as_bytes().to_vec();
        for scope in principal.scopes() {
            append_digest_part(&mut material, scope.as_bytes());
        }
        digest_hex(material)
    }

    fn cursor(&self, kind: &str, scope: &str, offset: usize) -> String {
        format!(
            "v1.{kind}.{}.{}.{}",
            self.catalog.inner.digest, scope, offset
        )
    }

    fn parse_cursor(&self, value: &str, kind: &str, scope: &str) -> Result<usize, ErrorData> {
        if value.len() > 224 {
            return Err(invalid_cursor());
        }
        let mut parts = value.split('.');
        let valid = parts.next() == Some("v1")
            && parts.next() == Some(kind)
            && parts.next() == Some(self.catalog.inner.digest.as_ref())
            && parts.next() == Some(scope);
        let offset = parts.next().and_then(|part| part.parse::<usize>().ok());
        if !valid || offset.is_none() || parts.next().is_some() {
            return Err(invalid_cursor());
        }
        Ok(offset.expect("validated Skill cursor offset exists"))
    }

    async fn authorized(
        &self,
        principal: &McpServerPrincipal,
        operation: McpServerSkillOperation,
        uri: &str,
    ) -> Result<bool, ErrorData> {
        let request = McpServerSkillAuthorizationRequest {
            principal: principal.clone(),
            operation,
            uri: Arc::from(uri),
        };
        match self.authorization.authorize(request).await {
            Ok(()) => Ok(true),
            Err(McpServerSkillAuthorizationError::Forbidden) => Ok(false),
            Err(McpServerSkillAuthorizationError::Unavailable) => Err(ErrorData::internal_error(
                "MCP Skill authorization is unavailable",
                None,
            )),
        }
    }

    async fn visible_skills(
        &self,
        principal: &McpServerPrincipal,
        operation: McpServerSkillOperation,
    ) -> Result<Vec<&McpServerSkillDefinition>, ErrorData> {
        let mut visible = Vec::new();
        for skill in self.catalog.inner.skills.values() {
            if skill.has_required_scopes(principal)
                && self.authorized(principal, operation, skill.uri()).await?
            {
                visible.push(skill);
            }
        }
        Ok(visible)
    }

    async fn list_skills(
        &self,
        request: &CustomRequest,
        context: &RequestContext<RoleServer>,
    ) -> Result<CustomResult, ErrorData> {
        let params = parse_list_params(request)?;
        let principal = required_principal(context)?;
        let scope = self.scope_tag(principal);
        let visible = self
            .visible_skills(principal, McpServerSkillOperation::List)
            .await?;
        let start = params
            .cursor
            .as_deref()
            .map_or(Ok(0), |cursor| self.parse_cursor(cursor, "skills", &scope))?;
        if start > visible.len() {
            return Err(invalid_cursor());
        }
        let end = start
            .saturating_add(self.options.page_size)
            .min(visible.len());
        let skills = visible[start..end]
            .iter()
            .map(|skill| skill.to_wire())
            .collect::<Vec<_>>();
        let next_cursor = (end < visible.len()).then(|| self.cursor("skills", &scope, end));
        let mut result = Map::new();
        result.insert("resultType".to_owned(), json!("complete"));
        result.insert("skills".to_owned(), Value::Array(skills));
        result.insert("ttlMs".to_owned(), json!(self.options.cache_ttl_ms));
        result.insert("cacheScope".to_owned(), json!(self.cache_scope_name()));
        if let Some(cursor) = next_cursor {
            result.insert("nextCursor".to_owned(), Value::String(cursor));
        }
        Ok(CustomResult::new(Value::Object(result)))
    }

    async fn get_skill(
        &self,
        request: &CustomRequest,
        context: &RequestContext<RoleServer>,
    ) -> Result<CustomResult, ErrorData> {
        let params = parse_get_params(request)?;
        validate_absolute_uri(&params.uri).map_err(|_| skill_not_found())?;
        let principal = required_principal(context)?;
        if !self
            .authorized(principal, McpServerSkillOperation::Get, &params.uri)
            .await?
        {
            return Err(skill_not_found());
        }
        let skill = self.catalog.get(&params.uri).ok_or_else(skill_not_found)?;
        if !skill.has_required_scopes(principal) {
            return Err(skill_not_found());
        }
        Ok(CustomResult::new(json!({
            "resultType": "complete",
            "skill": skill.to_wire(),
            "ttlMs": self.options.cache_ttl_ms,
            "cacheScope": self.cache_scope_name(),
        })))
    }

    async fn list_skill_resources(
        &self,
        request: Option<&PaginatedRequestParams>,
        context: &RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let principal = required_principal(context)?;
        let visible_skills = self
            .visible_skills(principal, McpServerSkillOperation::Read)
            .await?
            .into_iter()
            .map(McpServerSkillDefinition::uri)
            .collect::<BTreeSet<_>>();
        let visible = self
            .catalog
            .inner
            .resources
            .iter()
            .filter(|(_, binding)| visible_skills.contains(binding.skill_uri.as_ref()))
            .collect::<Vec<_>>();
        let scope = self.scope_tag(principal);
        let start = request
            .and_then(|params| params.cursor.as_deref())
            .map_or(Ok(0), |cursor| {
                self.parse_cursor(cursor, "resources", &scope)
            })?;
        if start > visible.len() {
            return Err(invalid_cursor());
        }
        let end = start
            .saturating_add(self.options.page_size)
            .min(visible.len());
        let resources = visible[start..end]
            .iter()
            .map(|(uri, binding)| {
                let mut resource =
                    Resource::new((*uri).to_owned(), binding.file.relative_path().to_owned());
                resource.mime_type = Some(binding.file.mime_type().to_owned());
                resource.size = u64::try_from(binding.file.size()).ok();
                resource
            })
            .collect();
        let mut result = ListResourcesResult::with_all_items(resources)
            .with_ttl_ms(self.options.cache_ttl_ms)
            .with_cache_scope(self.options.cache_scope.into());
        result.next_cursor = (end < visible.len()).then(|| self.cursor("resources", &scope, end));
        Ok(result)
    }

    async fn read_skill_resource(
        &self,
        request: ReadResourceRequestParams,
        context: &RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        validate_absolute_uri(&request.uri).map_err(|_| resource_not_found())?;
        if request.input_responses.is_some() || request.request_state.is_some() {
            return Err(ErrorData::invalid_params(
                "MCP Skill files do not accept request state or input responses",
                None,
            ));
        }
        let principal = required_principal(context)?;
        if !self
            .authorized(principal, McpServerSkillOperation::Read, &request.uri)
            .await?
        {
            return Err(resource_not_found());
        }
        let binding = self
            .catalog
            .inner
            .resources
            .get(&request.uri)
            .ok_or_else(resource_not_found)?;
        let skill = self
            .catalog
            .get(&binding.skill_uri)
            .expect("resource bindings always reference a frozen Skill");
        if !skill.has_required_scopes(principal) {
            return Err(resource_not_found());
        }
        let resource_content = match binding.file.representation {
            McpServerSkillFileRepresentation::Text => ResourceContents::TextResourceContents {
                uri: request.uri,
                mime_type: Some(binding.file.mime_type().to_owned()),
                text: std::str::from_utf8(&binding.file.bytes)
                    .expect("text Skill files are validated UTF-8")
                    .to_owned(),
                meta: None,
            },
            McpServerSkillFileRepresentation::Binary => ResourceContents::BlobResourceContents {
                uri: request.uri,
                mime_type: Some(binding.file.mime_type().to_owned()),
                blob: STANDARD.encode(&binding.file.bytes),
                meta: None,
            },
        };
        Ok(ReadResourceResult::new(vec![resource_content])
            .with_ttl_ms(self.options.cache_ttl_ms)
            .with_cache_scope(self.options.cache_scope.into())
            .into())
    }

    fn cache_scope_name(&self) -> &'static str {
        match self.options.cache_scope {
            McpServerCacheScope::Public => "public",
            McpServerCacheScope::Private => "private",
        }
    }

    pub(crate) async fn dispatch_custom(
        &self,
        request: CustomRequest,
        context: RequestContext<RoleServer>,
    ) -> Result<CustomResult, ErrorData> {
        match request.method.as_str() {
            SKILL_LIST_METHOD => self.list_skills(&request, &context).await,
            SKILL_GET_METHOD => self.get_skill(&request, &context).await,
            _ => Err(ErrorData::new(
                rmcp::model::ErrorCode::METHOD_NOT_FOUND,
                request.method,
                None,
            )),
        }
    }

    pub(crate) async fn dispatch_read(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        self.read_skill_resource(request, &context).await
    }
}

impl fmt::Debug for McpServerSkillService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerSkillService")
            .field("catalog", &self.catalog)
            .field("options", &self.options)
            .field("authorization", &"[POLICY]")
            .finish_non_exhaustive()
    }
}

impl ServerHandler for McpServerSkillService {
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }

    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::new(Self::capabilities()).with_server_info(Implementation::new(
            self.options.server_name.to_string(),
            self.options.server_version.to_string(),
        ));
        if let Some(instructions) = &self.options.instructions {
            info = info.with_instructions(instructions.to_string());
        }
        info
    }

    async fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> Result<DiscoverResult, ErrorData> {
        Ok(
            DiscoverResult::from_server_info(vec![ProtocolVersion::V_2026_07_28], self.get_info())
                .with_ttl_ms(self.options.cache_ttl_ms)
                .with_cache_scope(self.options.cache_scope.into()),
        )
    }

    async fn on_custom_request(
        &self,
        request: CustomRequest,
        context: RequestContext<RoleServer>,
    ) -> Result<CustomResult, ErrorData> {
        self.dispatch_custom(request, context).await
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        self.list_skill_resources(request.as_ref(), &context).await
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        Ok(ListResourceTemplatesResult::with_all_items(Vec::new())
            .with_ttl_ms(self.options.cache_ttl_ms)
            .with_cache_scope(self.options.cache_scope.into()))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        self.read_skill_resource(request, &context).await
    }
}

/// Invalid Skill service configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerSkillServiceBuildError {
    /// Scope-filtered Skills cannot be described as a public cache.
    #[error("scope-restricted MCP Skills require a private catalog cache")]
    PublicCacheWithScopedSkills,
    /// A principal-sensitive or mutable policy cannot produce public cache metadata.
    #[error("dynamic MCP Skill authorization requires a private catalog cache")]
    PublicCacheWithDynamicPolicy,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillListParams {
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillGetParams {
    uri: String,
}

fn parse_list_params(request: &CustomRequest) -> Result<SkillListParams, ErrorData> {
    let params = match &request.params {
        None => SkillListParams::default(),
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|_| ErrorData::invalid_params("Invalid skills/list parameters", None))?,
    };
    if params
        .cursor
        .as_ref()
        .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 224)
    {
        return Err(invalid_cursor());
    }
    Ok(params)
}

fn parse_get_params(request: &CustomRequest) -> Result<SkillGetParams, ErrorData> {
    let Some(value) = &request.params else {
        return Err(ErrorData::invalid_params(
            "Invalid skills/get parameters",
            None,
        ));
    };
    let params: SkillGetParams = serde_json::from_value(value.clone())
        .map_err(|_| ErrorData::invalid_params("Invalid skills/get parameters", None))?;
    if params.uri.is_empty() || params.uri.len() > MAX_URI_BYTES {
        return Err(skill_not_found());
    }
    Ok(params)
}

pub(crate) fn parse_frontmatter(
    document: &str,
) -> Result<Map<String, Value>, McpServerSkillDefinitionError> {
    let bytes = document.as_bytes();
    let Some(first_end) = bytes.iter().position(|byte| *byte == b'\n') else {
        return Err(McpServerSkillDefinitionError::InvalidFrontmatter);
    };
    if document[..first_end].trim_end_matches('\r') != "---" {
        return Err(McpServerSkillDefinitionError::InvalidFrontmatter);
    }
    let mut line_start = first_end + 1;
    let mut closing_start = None;
    while line_start <= bytes.len() {
        let line_end = bytes[line_start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(bytes.len(), |relative| line_start + relative);
        if document[line_start..line_end].trim_end_matches('\r') == "---" {
            closing_start = Some(line_start);
            break;
        }
        if line_end == bytes.len() {
            break;
        }
        line_start = line_end + 1;
    }
    let closing_start = closing_start.ok_or(McpServerSkillDefinitionError::InvalidFrontmatter)?;
    let yaml = &document[first_end + 1..closing_start];
    if yaml.is_empty() || yaml.len() > MAX_FRONTMATTER_BYTES {
        return Err(McpServerSkillDefinitionError::InvalidFrontmatter);
    }
    let value = BoundedUniqueJsonValueSeed::new(MAX_FRONTMATTER_BYTES)
        .deserialize(serde_yaml_ng::Deserializer::from_str(yaml))
        .map_err(|_| McpServerSkillDefinitionError::InvalidFrontmatter)?;
    value
        .as_object()
        .cloned()
        .ok_or(McpServerSkillDefinitionError::InvalidFrontmatter)
}

#[derive(Clone)]
struct BoundedUniqueJsonValueSeed {
    remaining_bytes: Rc<Cell<usize>>,
}

impl BoundedUniqueJsonValueSeed {
    fn new(maximum_bytes: usize) -> Self {
        Self {
            remaining_bytes: Rc::new(Cell::new(maximum_bytes)),
        }
    }

    fn consume<E>(&self, bytes: usize) -> Result<(), E>
    where
        E: de::Error,
    {
        let bytes = bytes.max(1);
        let remaining = self.remaining_bytes.get();
        if bytes > remaining {
            return Err(E::custom("expanded YAML value exceeds its byte budget"));
        }
        self.remaining_bytes.set(remaining - bytes);
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for BoundedUniqueJsonValueSeed {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(BoundedUniqueJsonValueVisitor { seed: self })
    }
}

struct BoundedUniqueJsonValueVisitor {
    seed: BoundedUniqueJsonValueSeed,
}

impl<'de> Visitor<'de> for BoundedUniqueJsonValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a YAML value representable as duplicate-free JSON")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.seed.consume::<E>(1)?;
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.seed.consume::<E>(std::mem::size_of::<i64>())?;
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.seed.consume::<E>(std::mem::size_of::<u64>())?;
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.seed.consume::<E>(std::mem::size_of::<f64>())?;
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("non-finite YAML number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.seed.consume::<E>(value.len())?;
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.seed.consume::<E>(value.len())?;
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.seed.consume::<E>(1)?;
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.seed.consume::<E>(1)?;
        Ok(Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.seed.deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        self.seed.consume::<A::Error>(1)?;
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or_default().min(1024));
        while let Some(value) = sequence.next_element_seed(self.seed.clone())? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut mapping: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        self.seed.consume::<A::Error>(1)?;
        let mut values = Map::new();
        while let Some(key) = mapping.next_key::<String>()? {
            self.seed.consume::<A::Error>(key.len())?;
            let value = mapping.next_value_seed(self.seed.clone())?;
            if values.insert(key, value).is_some() {
                return Err(de::Error::custom("duplicate YAML mapping key"));
            }
        }
        Ok(Value::Object(values))
    }
}

pub(crate) fn validate_frontmatter(
    frontmatter: &Map<String, Value>,
) -> Result<(), McpServerSkillDefinitionError> {
    let name = frontmatter
        .get("name")
        .and_then(Value::as_str)
        .ok_or(McpServerSkillDefinitionError::InvalidFrontmatterField)?;
    if !valid_skill_name(name) {
        return Err(McpServerSkillDefinitionError::InvalidFrontmatterField);
    }
    let description = frontmatter
        .get("description")
        .and_then(Value::as_str)
        .ok_or(McpServerSkillDefinitionError::InvalidFrontmatterField)?;
    if description.trim().is_empty() || description.chars().count() > 1024 {
        return Err(McpServerSkillDefinitionError::InvalidFrontmatterField);
    }
    for field in ["license", "allowed-tools"] {
        if frontmatter
            .get(field)
            .is_some_and(|value| value.as_str().is_none())
        {
            return Err(McpServerSkillDefinitionError::InvalidFrontmatterField);
        }
    }
    if frontmatter.get("compatibility").is_some_and(|value| {
        value
            .as_str()
            .is_none_or(|text| text.trim().is_empty() || text.chars().count() > 500)
    }) {
        return Err(McpServerSkillDefinitionError::InvalidFrontmatterField);
    }
    if frontmatter.get("metadata").is_some_and(|value| {
        value
            .as_object()
            .is_none_or(|metadata| metadata.values().any(|entry| entry.as_str().is_none()))
    }) {
        return Err(McpServerSkillDefinitionError::InvalidFrontmatterField);
    }
    Ok(())
}

fn valid_skill_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.contains("--")
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

pub(crate) fn validate_skill_uri(uri: &str) -> Result<String, McpServerSkillDefinitionError> {
    validate_absolute_uri(uri)?;
    let parsed = uri
        .parse::<http::Uri>()
        .map_err(|_| McpServerSkillDefinitionError::InvalidUri)?;
    if parsed.query().is_some() || !parsed.path().ends_with("/SKILL.md") {
        return Err(McpServerSkillDefinitionError::InvalidUri);
    }
    let path = parsed.path();
    if path.contains('%')
        || path.contains("//")
        || path.contains('\\')
        || path.split('/').any(|segment| matches!(segment, "." | ".."))
    {
        return Err(McpServerSkillDefinitionError::InvalidUri);
    }
    let parent_path = path
        .strip_suffix("/SKILL.md")
        .expect("checked Skill URI suffix")
        .trim_end_matches('/');
    let parent = parent_path
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            parsed
                .authority()
                .map(|authority| authority.host().to_owned())
        })
        .ok_or(McpServerSkillDefinitionError::InvalidUri)?;
    if !valid_skill_name(&parent) {
        return Err(McpServerSkillDefinitionError::InvalidUri);
    }
    Ok(parent)
}

fn validate_absolute_uri(uri: &str) -> Result<(), McpServerSkillDefinitionError> {
    if uri.is_empty()
        || uri.len() > MAX_URI_BYTES
        || uri.chars().any(char::is_control)
        || uri
            .parse::<http::Uri>()
            .map_or(true, |parsed| parsed.scheme().is_none())
    {
        return Err(McpServerSkillDefinitionError::InvalidUri);
    }
    Ok(())
}

pub(crate) fn validate_relative_path(path: &str) -> Result<(), McpServerSkillFileError> {
    if path.is_empty()
        || path.len() > MAX_RELATIVE_PATH_BYTES
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains('\\')
        || path.chars().any(char::is_control)
    {
        return Err(McpServerSkillFileError::InvalidRelativePath);
    }
    for segment in path.split('/') {
        if segment.is_empty()
            || matches!(segment, "." | "..")
            || !segment.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
            })
        {
            return Err(McpServerSkillFileError::InvalidRelativePath);
        }
    }
    Ok(())
}

fn resource_uri(
    skill_uri: &str,
    relative_path: &str,
) -> Result<String, McpServerSkillDefinitionError> {
    let prefix = skill_uri
        .strip_suffix(SKILL_DOCUMENT)
        .ok_or(McpServerSkillDefinitionError::InvalidUri)?;
    let uri = format!("{prefix}{relative_path}");
    validate_absolute_uri(&uri)?;
    if uri.len() > MAX_URI_BYTES {
        return Err(McpServerSkillDefinitionError::InvalidUri);
    }
    Ok(uri)
}

fn validate_scopes(
    scopes: impl IntoIterator<Item = impl Into<String>>,
) -> Result<Arc<[Box<str>]>, McpServerSkillDefinitionError> {
    let mut scopes = scopes.into_iter().map(Into::into).collect::<Vec<_>>();
    if scopes.len() > MAX_SCOPE_COUNT {
        return Err(McpServerSkillDefinitionError::TooManyScopes);
    }
    if scopes.iter().any(|scope| {
        scope.is_empty()
            || scope.len() > MAX_SCOPE_BYTES
            || scope
                .bytes()
                .any(|byte| !matches!(byte, b'!' | b'#'..=b'[' | b']'..=b'~'))
    }) {
        return Err(McpServerSkillDefinitionError::InvalidScope);
    }
    scopes.sort_unstable();
    if scopes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(McpServerSkillDefinitionError::DuplicateScope);
    }
    Ok(scopes
        .into_iter()
        .map(String::into_boxed_str)
        .collect::<Vec<_>>()
        .into())
}

fn required_principal(
    context: &RequestContext<RoleServer>,
) -> Result<&McpServerPrincipal, ErrorData> {
    mcp_server_principal(context)
        .ok_or_else(|| ErrorData::internal_error("MCP server boundary is missing", None))
}

fn invalid_cursor() -> ErrorData {
    ErrorData::invalid_params("Invalid MCP Skill catalog cursor", None)
}

fn skill_not_found() -> ErrorData {
    ErrorData::invalid_params("MCP Skill was not found or unavailable", None)
}

fn resource_not_found() -> ErrorData {
    ErrorData::resource_not_found("Resource not found or unavailable", None)
}

fn digest_hex(value: impl AsRef<[u8]>) -> String {
    Digest::sha256(value)
        .to_string()
        .strip_prefix("sha256:")
        .expect("StateKnot SHA-256 digest has a stable prefix")
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skill(name: &str) -> McpServerSkillDefinition {
        let document = format!(
            "---\nname: {name}\ndescription: Review code with a fixed checklist.\nmetadata:\n  owner: stateknot\n---\n\n# Review\n"
        );
        McpServerSkillDefinition::new(
            format!("skill://{name}/SKILL.md"),
            [
                McpServerSkillFile::text(SKILL_DOCUMENT, "text/markdown", document).unwrap(),
                McpServerSkillFile::text(
                    "references/checklist.md",
                    "text/markdown",
                    "# Checklist\n",
                )
                .unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn definition_freezes_complete_manifest() {
        let definition = skill("code-review");
        assert_eq!(definition.name(), "code-review");
        assert_eq!(definition.files().len(), 2);
        let wire = definition.to_wire();
        assert_eq!(
            wire.pointer("/frontmatter/name"),
            Some(&json!("code-review"))
        );
        assert_eq!(
            wire.pointer("/resources/0/uri"),
            Some(&json!("skill://code-review/SKILL.md"))
        );
        assert!(
            wire.pointer("/resources/0/digest")
                .and_then(Value::as_str)
                .is_some_and(|digest| digest.starts_with("sha256:"))
        );
    }

    #[test]
    fn definition_rejects_traversal_and_name_mismatch() {
        assert_eq!(
            McpServerSkillFile::text("../secret", "text/plain", "x").unwrap_err(),
            McpServerSkillFileError::InvalidRelativePath
        );
        let document = McpServerSkillFile::text(
            SKILL_DOCUMENT,
            "text/markdown",
            "---\nname: other\ndescription: Valid description.\n---\n",
        )
        .unwrap();
        assert_eq!(
            McpServerSkillDefinition::new("skill://expected/SKILL.md", [document]).unwrap_err(),
            McpServerSkillDefinitionError::NameUriMismatch
        );
        let document = McpServerSkillFile::text(
            SKILL_DOCUMENT,
            "text/markdown",
            "---\nname: expected\ndescription: Valid description.\n---\n",
        )
        .unwrap();
        assert_eq!(
            McpServerSkillDefinition::new("skill://catalog/../expected/SKILL.md", [document],)
                .unwrap_err(),
            McpServerSkillDefinitionError::InvalidUri
        );
    }

    #[test]
    fn definition_rejects_duplicate_or_malformed_frontmatter() {
        let duplicate = McpServerSkillFile::text(
            SKILL_DOCUMENT,
            "text/markdown",
            "---\nname: duplicate\nname: shadow\ndescription: Valid description.\n---\n",
        )
        .unwrap();
        assert_eq!(
            McpServerSkillDefinition::new("skill://duplicate/SKILL.md", [duplicate]).unwrap_err(),
            McpServerSkillDefinitionError::InvalidFrontmatter
        );
        let nested_duplicate = McpServerSkillFile::text(
            SKILL_DOCUMENT,
            "text/markdown",
            "---\nname: nested-duplicate\ndescription: Valid description.\nextra:\n  owner: first\n  owner: second\n---\n",
        )
        .unwrap();
        assert_eq!(
            McpServerSkillDefinition::new("skill://nested-duplicate/SKILL.md", [nested_duplicate],)
                .unwrap_err(),
            McpServerSkillDefinitionError::InvalidFrontmatter
        );
        let invalid_metadata = McpServerSkillFile::text(
            SKILL_DOCUMENT,
            "text/markdown",
            "---\nname: invalid\ndescription: Valid description.\nmetadata:\n  count: 1\n---\n",
        )
        .unwrap();
        assert_eq!(
            McpServerSkillDefinition::new("skill://invalid/SKILL.md", [invalid_metadata])
                .unwrap_err(),
            McpServerSkillDefinitionError::InvalidFrontmatterField
        );
    }

    #[test]
    fn definition_rejects_alias_expansion_above_the_materialized_budget() {
        let document = McpServerSkillFile::text(
            SKILL_DOCUMENT,
            "text/markdown",
            "---\nname: alias-expansion\ndescription: Valid description.\nbase: &base [0123456789abcdef]\na: &a [*base, *base]\nb: &b [*a, *a]\nc: &c [*b, *b]\nd: &d [*c, *c]\ne: &e [*d, *d]\nf: &f [*e, *e]\ng: &g [*f, *f]\nh: &h [*g, *g]\ni: &i [*h, *h]\nj: &j [*i, *i]\nk: [*j, *j, *j, *j, *j]\n---\n",
        )
        .unwrap();
        assert_eq!(
            McpServerSkillDefinition::new("skill://alias-expansion/SKILL.md", [document])
                .unwrap_err(),
            McpServerSkillDefinitionError::InvalidFrontmatter
        );
    }

    #[test]
    fn catalog_rejects_overlapping_resource_namespace() {
        let mut catalog = McpServerSkillCatalogBuilder::default();
        catalog.register(skill("code-review")).unwrap();
        assert_eq!(
            catalog.register(skill("code-review")).unwrap_err(),
            McpServerSkillCatalogError::DuplicateSkill
        );
    }
}
