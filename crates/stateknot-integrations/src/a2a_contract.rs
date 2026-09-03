// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded StateKnot-owned contracts for the A2A 1.0 adapter.
//!
//! The official SDK structs are deliberately private to the adapter. These
//! wrappers validate every inbound and outbound value, keep credentials out of
//! `Debug`, and prevent a wire dependency from becoming `StateKnot`'s domain API.

use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use a2a as wire;
use chrono::{DateTime, Utc};
use serde_json::Value;
use thiserror::Error;
use zeroize::Zeroizing;

/// A2A protocol version implemented by this profile.
pub const A2A_PROTOCOL_VERSION_1_0: &str = "1.0";

/// HTTP+JSON protocol-binding identifier used in Agent Cards.
pub const A2A_BINDING_HTTP_JSON: &str = "HTTP+JSON";

/// JSON-RPC protocol-binding identifier used in Agent Cards.
pub const A2A_BINDING_JSONRPC: &str = "JSONRPC";

/// Standard public Agent Card discovery path.
pub const A2A_AGENT_CARD_PATH: &str = "/.well-known/agent-card.json";

const MAX_ID_BYTES: usize = 512;
const MAX_LABEL_BYTES: usize = 512;
const MAX_DESCRIPTION_BYTES: usize = 16 * 1024;
const MAX_URL_BYTES: usize = 4096;
const MAX_MODE_BYTES: usize = 256;
const MAX_PARTS: usize = 128;
const MAX_PART_BYTES: usize = 1024 * 1024;
const MAX_METADATA_BYTES: usize = 256 * 1024;
const MAX_METADATA_ENTRIES: usize = 256;
const MAX_MESSAGES: usize = 256;
const MAX_ARTIFACTS: usize = 128;
const MAX_EXTENSIONS: usize = 64;
const MAX_REFERENCE_TASKS: usize = 128;
const MAX_SKILLS: usize = 256;
const MAX_INTERFACES: usize = 16;
const MAX_MODES: usize = 64;
const MAX_TAGS: usize = 64;
const MAX_EXAMPLES: usize = 64;
const MAX_SECURITY_SCHEMES: usize = 64;
const MAX_SECURITY_REQUIREMENTS: usize = 64;
const MAX_SCOPES: usize = 128;
const MAX_SECRET_BYTES: usize = 16 * 1024;

/// Invalid or unbounded A2A contract data.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum A2aContractError {
    /// A required value was absent.
    #[error("A2A {field} must not be empty")]
    Empty {
        /// Stable field label.
        field: &'static str,
    },
    /// A bounded collection exceeded its hard ceiling.
    #[error("A2A {field} has {actual} entries; maximum is {maximum}")]
    TooMany {
        /// Stable field label.
        field: &'static str,
        /// Maximum accepted count.
        maximum: usize,
        /// Observed count.
        actual: usize,
    },
    /// A string or encoded JSON value exceeded its hard byte ceiling.
    #[error("A2A {field} is {actual} bytes; maximum is {maximum}")]
    TooLarge {
        /// Stable field label.
        field: &'static str,
        /// Maximum accepted byte length.
        maximum: usize,
        /// Observed byte length.
        actual: usize,
    },
    /// Text contained boundary whitespace or control characters.
    #[error("A2A {field} contains invalid text")]
    InvalidText {
        /// Stable field label.
        field: &'static str,
    },
    /// A protocol identifier or enum value is unsupported.
    #[error("unsupported A2A {field}: {value}")]
    Unsupported {
        /// Stable field label.
        field: &'static str,
        /// Public-safe rejected value.
        value: String,
    },
    /// A URL is malformed or violates a contract-level rule.
    #[error("invalid A2A {field} URL")]
    InvalidUrl {
        /// Stable field label.
        field: &'static str,
    },
    /// JSON could not be encoded within the contract boundary.
    #[error("invalid A2A {field} JSON")]
    InvalidJson {
        /// Stable field label.
        field: &'static str,
    },
    /// A task or stream value violates a lifecycle invariant.
    #[error("invalid A2A lifecycle: {0}")]
    InvalidLifecycle(&'static str),
    /// A response union had no valid member.
    #[error("invalid A2A union: {0}")]
    InvalidUnion(&'static str),
}

fn validate_required_text(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), A2aContractError> {
    if value.is_empty() {
        return Err(A2aContractError::Empty { field });
    }
    if value.len() > maximum {
        return Err(A2aContractError::TooLarge {
            field,
            maximum,
            actual: value.len(),
        });
    }
    if value.trim() != value || value.chars().any(char::is_control) {
        return Err(A2aContractError::InvalidText { field });
    }
    Ok(())
}

fn validate_optional_text(
    field: &'static str,
    value: Option<&str>,
    maximum: usize,
) -> Result<(), A2aContractError> {
    if let Some(value) = value {
        validate_required_text(field, value, maximum)?;
    }
    Ok(())
}

fn validate_count(
    field: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), A2aContractError> {
    if actual > maximum {
        return Err(A2aContractError::TooMany {
            field,
            maximum,
            actual,
        });
    }
    Ok(())
}

fn validate_json(
    field: &'static str,
    value: &Value,
    maximum: usize,
) -> Result<(), A2aContractError> {
    let bytes = serde_json::to_vec(value).map_err(|_| A2aContractError::InvalidJson { field })?;
    if bytes.len() > maximum {
        return Err(A2aContractError::TooLarge {
            field,
            maximum,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn validate_metadata(
    field: &'static str,
    metadata: Option<&HashMap<String, Value>>,
) -> Result<(), A2aContractError> {
    let Some(metadata) = metadata else {
        return Ok(());
    };
    validate_count(field, metadata.len(), MAX_METADATA_ENTRIES)?;
    for key in metadata.keys() {
        validate_required_text(field, key, MAX_LABEL_BYTES)?;
    }
    validate_json(
        field,
        &serde_json::to_value(metadata).map_err(|_| A2aContractError::InvalidJson { field })?,
        MAX_METADATA_BYTES,
    )
}

fn validate_url(field: &'static str, value: &str) -> Result<(), A2aContractError> {
    validate_required_text(field, value, MAX_URL_BYTES)?;
    let parsed = reqwest::Url::parse(value).map_err(|_| A2aContractError::InvalidUrl { field })?;
    if parsed.host_str().is_none() || parsed.username() != "" || parsed.password().is_some() {
        return Err(A2aContractError::InvalidUrl { field });
    }
    Ok(())
}

/// Secret A2A material that is zeroized and never formatted in plaintext.
#[derive(Clone, PartialEq)]
pub struct A2aSecret(Zeroizing<String>);

impl A2aSecret {
    /// Constructs bounded, non-empty secret material.
    pub fn new(value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = Zeroizing::new(value.into());
        if value.is_empty() {
            return Err(A2aContractError::Empty { field: "secret" });
        }
        if value.len() > MAX_SECRET_BYTES {
            return Err(A2aContractError::TooLarge {
                field: "secret",
                maximum: MAX_SECRET_BYTES,
                actual: value.len(),
            });
        }
        if value.chars().any(char::is_control) {
            return Err(A2aContractError::InvalidText { field: "secret" });
        }
        Ok(Self(value))
    }

    /// Borrows the secret for immediate credential verification or encryption.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for A2aSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("A2aSecret([REDACTED])")
    }
}

/// Directional role of an A2A message.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum A2aMessageRole {
    /// Message sent by the calling agent or user side.
    User,
    /// Message sent by the serving agent side.
    Agent,
}

impl From<A2aMessageRole> for wire::Role {
    fn from(value: A2aMessageRole) -> Self {
        match value {
            A2aMessageRole::User => Self::User,
            A2aMessageRole::Agent => Self::Agent,
        }
    }
}

impl TryFrom<wire::Role> for A2aMessageRole {
    type Error = A2aContractError;

    fn try_from(value: wire::Role) -> Result<Self, Self::Error> {
        match value {
            wire::Role::User => Ok(Self::User),
            wire::Role::Agent => Ok(Self::Agent),
            wire::Role::Unspecified => Err(A2aContractError::Unsupported {
                field: "message role",
                value: "ROLE_UNSPECIFIED".to_string(),
            }),
        }
    }
}

/// Borrowed content view for an A2A part.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum A2aPartContent<'a> {
    /// UTF-8 text.
    Text(&'a str),
    /// Structured JSON data.
    Data(&'a Value),
    /// Inline decoded bytes.
    Raw(&'a [u8]),
    /// External resource URL.
    Url(&'a str),
}

/// Borrowed, allocation-free view of a bounded A2A content part.
///
/// Services use this view to enforce media and egress policy without cloning
/// potentially large inline payloads or depending on the upstream SDK model.
#[derive(Clone, Copy, Debug)]
pub struct A2aPartRef<'a> {
    inner: &'a wire::Part,
}

impl<'a> A2aPartRef<'a> {
    /// Returns the content variant without exposing SDK types.
    #[must_use]
    pub fn content(self) -> A2aPartContent<'a> {
        match &self.inner.content {
            wire::PartContent::Text(value) => A2aPartContent::Text(value),
            wire::PartContent::Data(value) => A2aPartContent::Data(value),
            wire::PartContent::Raw(value) => A2aPartContent::Raw(value),
            wire::PartContent::Url(value) => A2aPartContent::Url(value),
        }
    }

    /// Returns the optional display filename.
    #[must_use]
    pub fn filename(self) -> Option<&'a str> {
        self.inner.filename.as_deref()
    }

    /// Returns the optional media type.
    #[must_use]
    pub fn media_type(self) -> Option<&'a str> {
        self.inner.media_type.as_deref()
    }

    /// Returns untrusted protocol metadata.
    #[must_use]
    pub const fn metadata(self) -> Option<&'a HashMap<String, Value>> {
        self.inner.metadata.as_ref()
    }

    /// Clones this already validated part for an asynchronous ingestion job.
    #[must_use]
    pub fn to_owned(self) -> A2aPart {
        A2aPart {
            inner: self.inner.clone(),
        }
    }
}

/// A bounded A2A content part.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aPart {
    inner: wire::Part,
}

impl A2aPart {
    /// Creates a text part.
    pub fn text(value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        if value.len() > MAX_PART_BYTES {
            return Err(A2aContractError::TooLarge {
                field: "part text",
                maximum: MAX_PART_BYTES,
                actual: value.len(),
            });
        }
        Ok(Self {
            inner: wire::Part::text(value),
        })
    }

    /// Creates a structured-data part.
    pub fn data(value: Value) -> Result<Self, A2aContractError> {
        validate_json("part data", &value, MAX_PART_BYTES)?;
        Ok(Self {
            inner: wire::Part::data(value),
        })
    }

    /// Creates an inline binary part.
    ///
    /// Server ingress should normally materialize this through the artifact
    /// registry before constructing trusted core content.
    pub fn raw(value: Vec<u8>) -> Result<Self, A2aContractError> {
        if value.len() > MAX_PART_BYTES {
            return Err(A2aContractError::TooLarge {
                field: "part raw bytes",
                maximum: MAX_PART_BYTES,
                actual: value.len(),
            });
        }
        Ok(Self {
            inner: wire::Part::raw(value),
        })
    }

    /// Creates an external URL part.
    ///
    /// Constructing this value does not fetch or trust the URL. Callers must
    /// pass it through the configured egress and artifact-ingestion boundary.
    pub fn url(value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_url("part", &value)?;
        Ok(Self {
            inner: wire::Part::url(value),
        })
    }

    /// Adds a validated media type.
    pub fn with_media_type(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_required_text("part media type", &value, MAX_MODE_BYTES)?;
        value
            .parse::<mime::Mime>()
            .map_err(|_| A2aContractError::InvalidText {
                field: "part media type",
            })?;
        self.inner.media_type = Some(value);
        Ok(self)
    }

    /// Adds a bounded display filename.
    pub fn with_filename(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_required_text("part filename", &value, MAX_LABEL_BYTES)?;
        self.inner.filename = Some(value);
        Ok(self)
    }

    /// Adds bounded protocol metadata.
    pub fn with_metadata(
        mut self,
        value: HashMap<String, Value>,
    ) -> Result<Self, A2aContractError> {
        validate_metadata("part metadata", Some(&value))?;
        self.inner.metadata = Some(value);
        Ok(self)
    }

    /// Returns the content variant without exposing SDK types.
    #[must_use]
    pub fn content(&self) -> A2aPartContent<'_> {
        match &self.inner.content {
            wire::PartContent::Text(value) => A2aPartContent::Text(value),
            wire::PartContent::Data(value) => A2aPartContent::Data(value),
            wire::PartContent::Raw(value) => A2aPartContent::Raw(value),
            wire::PartContent::Url(value) => A2aPartContent::Url(value),
        }
    }

    /// Returns the optional filename.
    #[must_use]
    pub fn filename(&self) -> Option<&str> {
        self.inner.filename.as_deref()
    }

    /// Returns the optional media type.
    #[must_use]
    pub fn media_type(&self) -> Option<&str> {
        self.inner.media_type.as_deref()
    }

    /// Returns untrusted protocol metadata.
    #[must_use]
    pub const fn metadata(&self) -> Option<&HashMap<String, Value>> {
        self.inner.metadata.as_ref()
    }

    pub(crate) fn try_from_wire(inner: wire::Part) -> Result<Self, A2aContractError> {
        match &inner.content {
            wire::PartContent::Text(value) if value.len() > MAX_PART_BYTES => {
                return Err(A2aContractError::TooLarge {
                    field: "part text",
                    maximum: MAX_PART_BYTES,
                    actual: value.len(),
                });
            }
            wire::PartContent::Data(value) => validate_json("part data", value, MAX_PART_BYTES)?,
            wire::PartContent::Raw(value) if value.len() > MAX_PART_BYTES => {
                return Err(A2aContractError::TooLarge {
                    field: "part raw bytes",
                    maximum: MAX_PART_BYTES,
                    actual: value.len(),
                });
            }
            wire::PartContent::Url(value) => validate_url("part", value)?,
            _ => {}
        }
        validate_optional_text("part filename", inner.filename.as_deref(), MAX_LABEL_BYTES)?;
        validate_optional_text(
            "part media type",
            inner.media_type.as_deref(),
            MAX_MODE_BYTES,
        )?;
        if let Some(media_type) = inner.media_type.as_deref() {
            media_type
                .parse::<mime::Mime>()
                .map_err(|_| A2aContractError::InvalidText {
                    field: "part media type",
                })?;
        }
        validate_metadata("part metadata", inner.metadata.as_ref())?;
        Ok(Self { inner })
    }

    pub(crate) fn into_wire(self) -> wire::Part {
        self.inner
    }
}

/// A bounded A2A message.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aMessage {
    inner: wire::Message,
}

impl A2aMessage {
    /// Constructs a message with a sender-generated stable identifier.
    pub fn new(
        message_id: impl Into<String>,
        role: A2aMessageRole,
        parts: Vec<A2aPart>,
    ) -> Result<Self, A2aContractError> {
        let message_id = message_id.into();
        validate_required_text("message id", &message_id, MAX_ID_BYTES)?;
        if parts.is_empty() {
            return Err(A2aContractError::Empty {
                field: "message parts",
            });
        }
        validate_count("message parts", parts.len(), MAX_PARTS)?;
        Ok(Self {
            inner: wire::Message {
                message_id,
                context_id: None,
                task_id: None,
                role: role.into(),
                parts: parts.into_iter().map(A2aPart::into_wire).collect(),
                metadata: None,
                extensions: None,
                reference_task_ids: None,
            },
        })
    }

    /// Binds the message to an A2A context.
    pub fn with_context_id(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_required_text("context id", &value, MAX_ID_BYTES)?;
        self.inner.context_id = Some(value);
        Ok(self)
    }

    /// Binds the message to an A2A task.
    pub fn with_task_id(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_required_text("task id", &value, MAX_ID_BYTES)?;
        self.inner.task_id = Some(value);
        Ok(self)
    }

    /// Adds bounded message metadata.
    pub fn with_metadata(
        mut self,
        value: HashMap<String, Value>,
    ) -> Result<Self, A2aContractError> {
        validate_metadata("message metadata", Some(&value))?;
        self.inner.metadata = Some(value);
        Ok(self)
    }

    /// Adds negotiated extension URIs.
    pub fn with_extensions(mut self, values: Vec<String>) -> Result<Self, A2aContractError> {
        validate_string_list("message extensions", &values, MAX_EXTENSIONS, MAX_URL_BYTES)?;
        for value in &values {
            validate_url("message extension", value)?;
        }
        self.inner.extensions = Some(values);
        Ok(self)
    }

    /// Adds related task identifiers.
    pub fn with_reference_task_ids(
        mut self,
        values: Vec<String>,
    ) -> Result<Self, A2aContractError> {
        validate_string_list(
            "reference task ids",
            &values,
            MAX_REFERENCE_TASKS,
            MAX_ID_BYTES,
        )?;
        self.inner.reference_task_ids = Some(values);
        Ok(self)
    }

    /// Returns the stable sender-generated message identifier.
    #[must_use]
    pub fn message_id(&self) -> &str {
        &self.inner.message_id
    }

    /// Returns the context identifier when present.
    #[must_use]
    pub fn context_id(&self) -> Option<&str> {
        self.inner.context_id.as_deref()
    }

    /// Returns the task identifier when present.
    #[must_use]
    pub fn task_id(&self) -> Option<&str> {
        self.inner.task_id.as_deref()
    }

    /// Returns the directional message role.
    #[must_use]
    pub fn role(&self) -> A2aMessageRole {
        match self.inner.role {
            wire::Role::User => A2aMessageRole::User,
            wire::Role::Agent | wire::Role::Unspecified => A2aMessageRole::Agent,
        }
    }

    /// Iterates allocation-free borrowed views of the content parts.
    pub fn parts(&self) -> impl ExactSizeIterator<Item = A2aPartRef<'_>> {
        self.inner.parts.iter().map(|inner| A2aPartRef { inner })
    }

    /// Returns untrusted protocol metadata.
    #[must_use]
    pub const fn metadata(&self) -> Option<&HashMap<String, Value>> {
        self.inner.metadata.as_ref()
    }

    /// Returns negotiated extension URIs.
    pub fn extensions(&self) -> impl ExactSizeIterator<Item = &str> {
        self.inner
            .extensions
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(String::as_str)
    }

    /// Returns related task identifiers.
    pub fn reference_task_ids(&self) -> impl ExactSizeIterator<Item = &str> {
        self.inner
            .reference_task_ids
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(String::as_str)
    }

    pub(crate) fn try_from_wire(inner: wire::Message) -> Result<Self, A2aContractError> {
        validate_required_text("message id", &inner.message_id, MAX_ID_BYTES)?;
        validate_optional_text("context id", inner.context_id.as_deref(), MAX_ID_BYTES)?;
        validate_optional_text("task id", inner.task_id.as_deref(), MAX_ID_BYTES)?;
        A2aMessageRole::try_from(inner.role.clone())?;
        if inner.parts.is_empty() {
            return Err(A2aContractError::Empty {
                field: "message parts",
            });
        }
        validate_count("message parts", inner.parts.len(), MAX_PARTS)?;
        for part in inner.parts.iter().cloned() {
            A2aPart::try_from_wire(part)?;
        }
        validate_metadata("message metadata", inner.metadata.as_ref())?;
        if let Some(extensions) = inner.extensions.as_ref() {
            validate_string_list(
                "message extensions",
                extensions,
                MAX_EXTENSIONS,
                MAX_URL_BYTES,
            )?;
            for extension in extensions {
                validate_url("message extension", extension)?;
            }
        }
        if let Some(references) = inner.reference_task_ids.as_ref() {
            validate_string_list(
                "reference task ids",
                references,
                MAX_REFERENCE_TASKS,
                MAX_ID_BYTES,
            )?;
        }
        Ok(Self { inner })
    }

    pub(crate) fn into_wire(self) -> wire::Message {
        self.inner
    }
}

fn validate_string_list(
    field: &'static str,
    values: &[String],
    maximum_count: usize,
    maximum_bytes: usize,
) -> Result<(), A2aContractError> {
    validate_count(field, values.len(), maximum_count)?;
    for value in values {
        validate_required_text(field, value, maximum_bytes)?;
    }
    Ok(())
}

/// A2A task state, kept separate from `StateKnot` run lifecycle states.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum A2aTaskState {
    /// Accepted but not yet executing.
    Submitted,
    /// Actively executing.
    Working,
    /// Successfully completed.
    Completed,
    /// Failed terminally.
    Failed,
    /// Confirmed canceled.
    Canceled,
    /// Waiting for caller input.
    InputRequired,
    /// Rejected terminally.
    Rejected,
    /// Waiting for out-of-band authorization.
    AuthRequired,
}

impl A2aTaskState {
    /// Returns whether no later task state is permitted.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Canceled | Self::Rejected
        )
    }
}

impl From<A2aTaskState> for wire::TaskState {
    fn from(value: A2aTaskState) -> Self {
        match value {
            A2aTaskState::Submitted => Self::Submitted,
            A2aTaskState::Working => Self::Working,
            A2aTaskState::Completed => Self::Completed,
            A2aTaskState::Failed => Self::Failed,
            A2aTaskState::Canceled => Self::Canceled,
            A2aTaskState::InputRequired => Self::InputRequired,
            A2aTaskState::Rejected => Self::Rejected,
            A2aTaskState::AuthRequired => Self::AuthRequired,
        }
    }
}

impl TryFrom<wire::TaskState> for A2aTaskState {
    type Error = A2aContractError;

    fn try_from(value: wire::TaskState) -> Result<Self, Self::Error> {
        match value {
            wire::TaskState::Submitted => Ok(Self::Submitted),
            wire::TaskState::Working => Ok(Self::Working),
            wire::TaskState::Completed => Ok(Self::Completed),
            wire::TaskState::Failed => Ok(Self::Failed),
            wire::TaskState::Canceled => Ok(Self::Canceled),
            wire::TaskState::InputRequired => Ok(Self::InputRequired),
            wire::TaskState::Rejected => Ok(Self::Rejected),
            wire::TaskState::AuthRequired => Ok(Self::AuthRequired),
            wire::TaskState::Unspecified => Err(A2aContractError::Unsupported {
                field: "task state",
                value: "TASK_STATE_UNSPECIFIED".to_string(),
            }),
        }
    }
}

/// Status snapshot for an A2A task.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aTaskStatus {
    inner: wire::TaskStatus,
}

impl A2aTaskStatus {
    /// Constructs a status snapshot.
    pub fn new(
        state: A2aTaskState,
        message: Option<A2aMessage>,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<Self, A2aContractError> {
        if matches!(state, A2aTaskState::AuthRequired) && message.is_none() {
            return Err(A2aContractError::InvalidLifecycle(
                "auth-required status needs an explanatory message",
            ));
        }
        Ok(Self {
            inner: wire::TaskStatus {
                state: state.into(),
                message: message.map(A2aMessage::into_wire),
                timestamp,
            },
        })
    }

    /// Returns the A2A state.
    #[must_use]
    pub fn state(&self) -> A2aTaskState {
        A2aTaskState::try_from(self.inner.state.clone())
            .expect("A2aTaskStatus construction validates task state")
    }

    /// Returns the optional status timestamp.
    #[must_use]
    pub const fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.inner.timestamp
    }

    /// Clones the optional status message.
    #[must_use]
    pub fn message(&self) -> Option<A2aMessage> {
        self.inner.message.clone().map(|inner| A2aMessage { inner })
    }

    fn try_from_wire(inner: wire::TaskStatus) -> Result<Self, A2aContractError> {
        let state = A2aTaskState::try_from(inner.state.clone())?;
        if matches!(state, A2aTaskState::AuthRequired) && inner.message.is_none() {
            return Err(A2aContractError::InvalidLifecycle(
                "auth-required status needs an explanatory message",
            ));
        }
        if let Some(message) = inner.message.iter().next().cloned() {
            A2aMessage::try_from_wire(message)?;
        }
        Ok(Self { inner })
    }

    fn into_wire(self) -> wire::TaskStatus {
        self.inner
    }
}

/// Bounded output artifact attached to an A2A task.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aArtifact {
    inner: wire::Artifact,
}

impl A2aArtifact {
    /// Constructs an artifact from one or more bounded parts.
    pub fn new(
        artifact_id: impl Into<String>,
        parts: Vec<A2aPart>,
    ) -> Result<Self, A2aContractError> {
        let artifact_id = artifact_id.into();
        validate_required_text("artifact id", &artifact_id, MAX_ID_BYTES)?;
        if parts.is_empty() {
            return Err(A2aContractError::Empty {
                field: "artifact parts",
            });
        }
        validate_count("artifact parts", parts.len(), MAX_PARTS)?;
        Ok(Self {
            inner: wire::Artifact {
                artifact_id,
                name: None,
                description: None,
                parts: parts.into_iter().map(A2aPart::into_wire).collect(),
                metadata: None,
                extensions: None,
            },
        })
    }

    /// Adds a bounded display name.
    pub fn with_name(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_required_text("artifact name", &value, MAX_LABEL_BYTES)?;
        self.inner.name = Some(value);
        Ok(self)
    }

    /// Adds a bounded description.
    pub fn with_description(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_required_text("artifact description", &value, MAX_DESCRIPTION_BYTES)?;
        self.inner.description = Some(value);
        Ok(self)
    }

    /// Adds bounded metadata.
    pub fn with_metadata(
        mut self,
        value: HashMap<String, Value>,
    ) -> Result<Self, A2aContractError> {
        validate_metadata("artifact metadata", Some(&value))?;
        self.inner.metadata = Some(value);
        Ok(self)
    }

    /// Returns the stable artifact identifier.
    #[must_use]
    pub fn artifact_id(&self) -> &str {
        &self.inner.artifact_id
    }

    /// Returns the optional untrusted display name.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }

    /// Returns the optional untrusted description.
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }

    /// Iterates allocation-free borrowed views of the artifact parts.
    pub fn parts(&self) -> impl ExactSizeIterator<Item = A2aPartRef<'_>> {
        self.inner.parts.iter().map(|inner| A2aPartRef { inner })
    }

    /// Returns untrusted protocol metadata.
    #[must_use]
    pub const fn metadata(&self) -> Option<&HashMap<String, Value>> {
        self.inner.metadata.as_ref()
    }

    fn try_from_wire(inner: wire::Artifact) -> Result<Self, A2aContractError> {
        validate_required_text("artifact id", &inner.artifact_id, MAX_ID_BYTES)?;
        validate_optional_text("artifact name", inner.name.as_deref(), MAX_LABEL_BYTES)?;
        validate_optional_text(
            "artifact description",
            inner.description.as_deref(),
            MAX_DESCRIPTION_BYTES,
        )?;
        if inner.parts.is_empty() {
            return Err(A2aContractError::Empty {
                field: "artifact parts",
            });
        }
        validate_count("artifact parts", inner.parts.len(), MAX_PARTS)?;
        for part in inner.parts.iter().cloned() {
            A2aPart::try_from_wire(part)?;
        }
        validate_metadata("artifact metadata", inner.metadata.as_ref())?;
        if let Some(extensions) = inner.extensions.as_ref() {
            validate_string_list(
                "artifact extensions",
                extensions,
                MAX_EXTENSIONS,
                MAX_URL_BYTES,
            )?;
            for extension in extensions {
                validate_url("artifact extension", extension)?;
            }
        }
        Ok(Self { inner })
    }

    fn into_wire(self) -> wire::Artifact {
        self.inner
    }
}

/// A bounded A2A task projection.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aTask {
    inner: wire::Task,
}

impl A2aTask {
    /// Constructs a task projection. The identifier is an A2A identifier and
    /// must not be an exposed `StateKnot` run identifier.
    pub fn new(
        id: impl Into<String>,
        context_id: impl Into<String>,
        status: A2aTaskStatus,
    ) -> Result<Self, A2aContractError> {
        let id = id.into();
        let context_id = context_id.into();
        validate_required_text("task id", &id, MAX_ID_BYTES)?;
        validate_required_text("context id", &context_id, MAX_ID_BYTES)?;
        Ok(Self {
            inner: wire::Task {
                id,
                context_id,
                status: status.into_wire(),
                artifacts: None,
                history: None,
                metadata: None,
            },
        })
    }

    /// Adds a bounded artifact projection.
    pub fn with_artifacts(mut self, artifacts: Vec<A2aArtifact>) -> Result<Self, A2aContractError> {
        validate_count("task artifacts", artifacts.len(), MAX_ARTIFACTS)?;
        validate_unique_artifact_ids(&artifacts)?;
        self.inner.artifacts = Some(artifacts.into_iter().map(A2aArtifact::into_wire).collect());
        Ok(self)
    }

    /// Adds bounded message history in chronological order.
    pub fn with_history(mut self, history: Vec<A2aMessage>) -> Result<Self, A2aContractError> {
        validate_count("task history", history.len(), MAX_MESSAGES)?;
        self.inner.history = Some(history.into_iter().map(A2aMessage::into_wire).collect());
        Ok(self)
    }

    /// Adds bounded task metadata.
    pub fn with_metadata(
        mut self,
        metadata: HashMap<String, Value>,
    ) -> Result<Self, A2aContractError> {
        validate_metadata("task metadata", Some(&metadata))?;
        self.inner.metadata = Some(metadata);
        Ok(self)
    }

    /// Returns the opaque A2A task identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    /// Returns the opaque A2A context identifier.
    #[must_use]
    pub fn context_id(&self) -> &str {
        &self.inner.context_id
    }

    /// Returns the task state.
    #[must_use]
    pub fn state(&self) -> A2aTaskState {
        A2aTaskState::try_from(self.inner.status.state.clone())
            .expect("A2aTask construction validates task state")
    }

    /// Clones the status projection.
    #[must_use]
    pub fn status(&self) -> A2aTaskStatus {
        A2aTaskStatus {
            inner: self.inner.status.clone(),
        }
    }

    /// Clones message history for application mapping.
    #[must_use]
    pub fn history(&self) -> Vec<A2aMessage> {
        self.inner
            .history
            .as_deref()
            .unwrap_or_default()
            .iter()
            .cloned()
            .map(|inner| A2aMessage { inner })
            .collect()
    }

    /// Clones artifact projections for application mapping.
    #[must_use]
    pub fn artifacts(&self) -> Vec<A2aArtifact> {
        self.inner
            .artifacts
            .as_deref()
            .unwrap_or_default()
            .iter()
            .cloned()
            .map(|inner| A2aArtifact { inner })
            .collect()
    }

    pub(crate) fn history_len(&self) -> usize {
        self.inner.history.as_ref().map_or(0, Vec::len)
    }

    pub(crate) fn has_artifact_projection(&self) -> bool {
        self.inner.artifacts.is_some()
    }

    pub(crate) fn artifact_ids(&self) -> impl Iterator<Item = &str> {
        self.inner
            .artifacts
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|artifact| artifact.artifact_id.as_str())
    }

    pub(crate) fn try_from_wire(inner: wire::Task) -> Result<Self, A2aContractError> {
        validate_required_text("task id", &inner.id, MAX_ID_BYTES)?;
        validate_required_text("context id", &inner.context_id, MAX_ID_BYTES)?;
        A2aTaskStatus::try_from_wire(inner.status.clone())?;
        if let Some(artifacts) = inner.artifacts.as_ref() {
            validate_count("task artifacts", artifacts.len(), MAX_ARTIFACTS)?;
            let artifacts = artifacts
                .iter()
                .cloned()
                .map(A2aArtifact::try_from_wire)
                .collect::<Result<Vec<_>, _>>()?;
            validate_unique_artifact_ids(&artifacts)?;
        }
        if let Some(history) = inner.history.as_ref() {
            validate_count("task history", history.len(), MAX_MESSAGES)?;
            for message in history.iter().cloned() {
                A2aMessage::try_from_wire(message)?;
            }
        }
        validate_metadata("task metadata", inner.metadata.as_ref())?;
        Ok(Self { inner })
    }

    pub(crate) fn into_wire(self) -> wire::Task {
        self.inner
    }
}

fn validate_unique_artifact_ids(artifacts: &[A2aArtifact]) -> Result<(), A2aContractError> {
    let mut ids = HashSet::with_capacity(artifacts.len());
    if artifacts
        .iter()
        .any(|artifact| !ids.insert(artifact.artifact_id()))
    {
        return Err(A2aContractError::InvalidLifecycle(
            "artifact identifiers must be unique within a task",
        ));
    }
    Ok(())
}

/// Optional execution controls on a send request.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct A2aSendConfiguration {
    accepted_output_modes: Vec<String>,
    push_config: Option<A2aPushConfig>,
    history_length: Option<u32>,
    return_immediately: bool,
}

impl A2aSendConfiguration {
    /// Constructs default execution controls.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            accepted_output_modes: Vec::new(),
            push_config: None,
            history_length: None,
            return_immediately: false,
        }
    }

    /// Restricts acceptable response media modes.
    pub fn with_accepted_output_modes(
        mut self,
        modes: Vec<String>,
    ) -> Result<Self, A2aContractError> {
        validate_modes("accepted output modes", &modes)?;
        self.accepted_output_modes = modes;
        Ok(self)
    }

    /// Requests an initial push-notification configuration.
    #[must_use]
    pub fn with_push_config(mut self, config: A2aPushConfig) -> Self {
        self.push_config = Some(config);
        self
    }

    /// Requests a bounded suffix of task history in the response.
    pub fn with_history_length(mut self, length: u32) -> Result<Self, A2aContractError> {
        if usize::try_from(length).unwrap_or(usize::MAX) > MAX_MESSAGES {
            return Err(A2aContractError::TooLarge {
                field: "history length",
                maximum: MAX_MESSAGES,
                actual: usize::try_from(length).unwrap_or(usize::MAX),
            });
        }
        self.history_length = Some(length);
        Ok(self)
    }

    /// Requests a task snapshot as soon as the server durably accepts work.
    #[must_use]
    pub const fn return_immediately(mut self, enabled: bool) -> Self {
        self.return_immediately = enabled;
        self
    }

    /// Returns whether the caller requested immediate acknowledgement.
    #[must_use]
    pub const fn should_return_immediately(&self) -> bool {
        self.return_immediately
    }

    /// Returns the requested history length.
    #[must_use]
    pub const fn history_length(&self) -> Option<u32> {
        self.history_length
    }

    /// Returns acceptable output modes in preference order.
    pub fn accepted_output_modes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.accepted_output_modes.iter().map(String::as_str)
    }

    /// Returns an initial push configuration when supplied.
    #[must_use]
    pub const fn push_config(&self) -> Option<&A2aPushConfig> {
        self.push_config.as_ref()
    }

    fn try_from_wire(value: wire::SendMessageConfiguration) -> Result<Self, A2aContractError> {
        let accepted_output_modes = value.accepted_output_modes.unwrap_or_default();
        validate_modes("accepted output modes", &accepted_output_modes)?;
        let history_length = value
            .history_length
            .map(|value| {
                u32::try_from(value).map_err(|_| {
                    A2aContractError::InvalidLifecycle("history length must be non-negative")
                })
            })
            .transpose()?;
        if history_length.is_some_and(|value| value as usize > MAX_MESSAGES) {
            return Err(A2aContractError::TooLarge {
                field: "history length",
                maximum: MAX_MESSAGES,
                actual: history_length.unwrap_or_default() as usize,
            });
        }
        let push_config = value
            .task_push_notification_config
            .map(A2aPushConfig::try_from_wire)
            .transpose()?;
        if push_config
            .as_ref()
            .is_some_and(|config| config.task_id().is_some())
        {
            return Err(A2aContractError::InvalidLifecycle(
                "send-message push configuration must not contain a task id",
            ));
        }
        Ok(Self {
            accepted_output_modes,
            push_config,
            history_length,
            return_immediately: value.return_immediately.unwrap_or(false),
        })
    }

    fn into_wire(self) -> Result<wire::SendMessageConfiguration, A2aContractError> {
        if self
            .push_config
            .as_ref()
            .is_some_and(|config| config.task_id().is_some())
        {
            return Err(A2aContractError::InvalidLifecycle(
                "send-message push configuration must not contain a task id",
            ));
        }
        Ok(wire::SendMessageConfiguration {
            accepted_output_modes: (!self.accepted_output_modes.is_empty())
                .then_some(self.accepted_output_modes),
            task_push_notification_config: self.push_config.map(A2aPushConfig::into_wire),
            history_length: self
                .history_length
                .map(|value| {
                    i32::try_from(value).map_err(|_| {
                        A2aContractError::InvalidLifecycle(
                            "history length cannot be represented on the wire",
                        )
                    })
                })
                .transpose()?,
            return_immediately: self.return_immediately.then_some(true),
        })
    }
}

fn validate_modes(field: &'static str, modes: &[String]) -> Result<(), A2aContractError> {
    validate_string_list(field, modes, MAX_MODES, MAX_MODE_BYTES)?;
    for mode in modes {
        mode.parse::<mime::Mime>()
            .map_err(|_| A2aContractError::InvalidText { field })?;
    }
    Ok(())
}

/// A validated request to send a message.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aSendMessageRequest {
    message: A2aMessage,
    configuration: Option<A2aSendConfiguration>,
    metadata: Option<HashMap<String, Value>>,
}

impl A2aSendMessageRequest {
    /// Constructs a send request.
    #[must_use]
    pub const fn new(message: A2aMessage) -> Self {
        Self {
            message,
            configuration: None,
            metadata: None,
        }
    }

    /// Adds execution controls.
    #[must_use]
    pub fn with_configuration(mut self, configuration: A2aSendConfiguration) -> Self {
        self.configuration = Some(configuration);
        self
    }

    /// Adds bounded request metadata.
    pub fn with_metadata(
        mut self,
        metadata: HashMap<String, Value>,
    ) -> Result<Self, A2aContractError> {
        validate_metadata("send metadata", Some(&metadata))?;
        self.metadata = Some(metadata);
        Ok(self)
    }

    /// Returns the message.
    #[must_use]
    pub const fn message(&self) -> &A2aMessage {
        &self.message
    }

    /// Returns execution controls.
    #[must_use]
    pub const fn configuration(&self) -> Option<&A2aSendConfiguration> {
        self.configuration.as_ref()
    }

    /// Returns request metadata.
    #[must_use]
    pub const fn metadata(&self) -> Option<&HashMap<String, Value>> {
        self.metadata.as_ref()
    }

    pub(crate) fn try_from_wire(value: wire::SendMessageRequest) -> Result<Self, A2aContractError> {
        validate_metadata("send metadata", value.metadata.as_ref())?;
        if value.tenant.is_some() {
            return Err(A2aContractError::Unsupported {
                field: "request tenant override",
                value: "tenant".to_string(),
            });
        }
        let message = A2aMessage::try_from_wire(value.message)?;
        if message.role() != A2aMessageRole::User {
            return Err(A2aContractError::InvalidLifecycle(
                "send-message requests require a user-role message",
            ));
        }
        Ok(Self {
            message,
            configuration: value
                .configuration
                .map(A2aSendConfiguration::try_from_wire)
                .transpose()?,
            metadata: value.metadata,
        })
    }

    pub(crate) fn into_wire(
        self,
        tenant: Option<String>,
    ) -> Result<wire::SendMessageRequest, A2aContractError> {
        if self.message.role() != A2aMessageRole::User {
            return Err(A2aContractError::InvalidLifecycle(
                "send-message requests require a user-role message",
            ));
        }
        Ok(wire::SendMessageRequest {
            message: self.message.into_wire(),
            configuration: self
                .configuration
                .map(A2aSendConfiguration::into_wire)
                .transpose()?,
            metadata: self.metadata,
            tenant,
        })
    }
}

/// Unary response to a send operation.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum A2aSendMessageResponse {
    /// A durable task was accepted or updated.
    Task(A2aTask),
    /// The operation completed directly with a message.
    Message(A2aMessage),
}

impl A2aSendMessageResponse {
    pub(crate) fn try_from_wire(
        value: wire::SendMessageResponse,
    ) -> Result<Self, A2aContractError> {
        match value {
            wire::SendMessageResponse::Task(task) => Ok(Self::Task(A2aTask::try_from_wire(task)?)),
            wire::SendMessageResponse::Message(message) => {
                Ok(Self::Message(A2aMessage::try_from_wire(message)?))
            }
        }
    }

    /// Returns the standard field-presence JSON representation.
    pub fn to_json(&self) -> Result<Value, A2aContractError> {
        let wire = match self {
            Self::Task(task) => wire::SendMessageResponse::Task(task.inner.clone()),
            Self::Message(message) => wire::SendMessageResponse::Message(message.inner.clone()),
        };
        serde_json::to_value(wire).map_err(|_| A2aContractError::InvalidJson {
            field: "send response",
        })
    }

    /// Parses and bounds a standard field-presence JSON response.
    pub fn from_json(value: Value) -> Result<Self, A2aContractError> {
        validate_json("send response", &value, MAX_PART_BYTES * 2)?;
        let object = value.as_object().ok_or(A2aContractError::InvalidJson {
            field: "send response",
        })?;
        if object.len() != 1 || !(object.contains_key("task") || object.contains_key("message")) {
            return Err(A2aContractError::InvalidJson {
                field: "send response",
            });
        }
        let wire = serde_json::from_value(value).map_err(|_| A2aContractError::InvalidJson {
            field: "send response",
        })?;
        Self::try_from_wire(wire)
    }

    pub(crate) fn into_wire(self) -> wire::SendMessageResponse {
        match self {
            Self::Task(task) => wire::SendMessageResponse::Task(task.into_wire()),
            Self::Message(message) => wire::SendMessageResponse::Message(message.into_wire()),
        }
    }
}

/// A validated task lookup request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct A2aGetTaskRequest {
    id: Box<str>,
    history_length: Option<u32>,
}

impl A2aGetTaskRequest {
    /// Constructs a lookup request.
    pub fn new(id: impl Into<String>) -> Result<Self, A2aContractError> {
        let id = id.into();
        validate_required_text("task id", &id, MAX_ID_BYTES)?;
        Ok(Self {
            id: id.into_boxed_str(),
            history_length: None,
        })
    }

    /// Selects a bounded history suffix.
    pub fn with_history_length(mut self, length: u32) -> Result<Self, A2aContractError> {
        if length as usize > MAX_MESSAGES {
            return Err(A2aContractError::TooLarge {
                field: "history length",
                maximum: MAX_MESSAGES,
                actual: length as usize,
            });
        }
        self.history_length = Some(length);
        Ok(self)
    }

    /// Returns the task identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the requested history length.
    #[must_use]
    pub const fn history_length(&self) -> Option<u32> {
        self.history_length
    }

    pub(crate) fn try_from_wire(value: wire::GetTaskRequest) -> Result<Self, A2aContractError> {
        if value.tenant.is_some() {
            return Err(A2aContractError::Unsupported {
                field: "request tenant override",
                value: "tenant".to_string(),
            });
        }
        let mut request = Self::new(value.id)?;
        if let Some(length) = value.history_length {
            request = request.with_history_length(u32::try_from(length).map_err(|_| {
                A2aContractError::InvalidLifecycle("history length must be non-negative")
            })?)?;
        }
        Ok(request)
    }

    pub(crate) fn into_wire(
        self,
        tenant: Option<String>,
    ) -> Result<wire::GetTaskRequest, A2aContractError> {
        Ok(wire::GetTaskRequest {
            id: self.id.into(),
            history_length: self
                .history_length
                .map(|value| {
                    i32::try_from(value).map_err(|_| {
                        A2aContractError::InvalidLifecycle(
                            "history length cannot be represented on the wire",
                        )
                    })
                })
                .transpose()?,
            tenant,
        })
    }
}

/// Bounded filters and cursor for task listing.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct A2aListTasksRequest {
    context_id: Option<Box<str>>,
    status: Option<A2aTaskState>,
    page_size: u16,
    page_token: Option<Box<str>>,
    history_length: Option<u32>,
    status_timestamp_after: Option<DateTime<Utc>>,
    include_artifacts: bool,
}

impl A2aListTasksRequest {
    /// Protocol-defined default page size.
    pub const DEFAULT_PAGE_SIZE: u16 = 50;
    /// A2A 1.0 protocol page-size ceiling.
    pub const MAX_PAGE_SIZE: u16 = 100;

    /// Constructs an unfiltered first page.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            context_id: None,
            status: None,
            page_size: Self::DEFAULT_PAGE_SIZE,
            page_token: None,
            history_length: None,
            status_timestamp_after: None,
            include_artifacts: false,
        }
    }

    /// Filters by exact context identifier.
    pub fn with_context_id(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_required_text("context id", &value, MAX_ID_BYTES)?;
        self.context_id = Some(value.into_boxed_str());
        Ok(self)
    }

    /// Filters by exact task state.
    #[must_use]
    pub const fn with_status(mut self, value: A2aTaskState) -> Self {
        self.status = Some(value);
        self
    }

    /// Sets a bounded page size.
    pub fn with_page_size(mut self, value: u16) -> Result<Self, A2aContractError> {
        if value == 0 || value > Self::MAX_PAGE_SIZE {
            return Err(A2aContractError::InvalidLifecycle(
                "page size is outside the supported range",
            ));
        }
        self.page_size = value;
        Ok(self)
    }

    /// Sets an opaque, bounded backend cursor.
    pub fn with_page_token(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_required_text("page token", &value, MAX_ID_BYTES * 4)?;
        self.page_token = Some(value.into_boxed_str());
        Ok(self)
    }

    /// Selects a bounded history suffix for each task.
    pub fn with_history_length(mut self, value: u32) -> Result<Self, A2aContractError> {
        if value as usize > MAX_MESSAGES {
            return Err(A2aContractError::TooLarge {
                field: "history length",
                maximum: MAX_MESSAGES,
                actual: value as usize,
            });
        }
        self.history_length = Some(value);
        Ok(self)
    }

    /// Filters task status timestamps to this instant or later.
    #[must_use]
    pub const fn with_status_timestamp_after(mut self, value: DateTime<Utc>) -> Self {
        self.status_timestamp_after = Some(value);
        self
    }

    /// Requests artifact projections in listed tasks.
    #[must_use]
    pub const fn include_artifacts(mut self, include: bool) -> Self {
        self.include_artifacts = include;
        self
    }

    /// Returns the exact context filter.
    #[must_use]
    pub fn context_id(&self) -> Option<&str> {
        self.context_id.as_deref()
    }

    /// Returns the task-state filter.
    #[must_use]
    pub const fn status(&self) -> Option<A2aTaskState> {
        self.status
    }

    /// Returns the effective page size.
    #[must_use]
    pub const fn page_size(&self) -> u16 {
        self.page_size
    }

    /// Returns the opaque cursor.
    #[must_use]
    pub fn page_token(&self) -> Option<&str> {
        self.page_token.as_deref()
    }

    /// Returns the requested history length.
    #[must_use]
    pub const fn history_length(&self) -> Option<u32> {
        self.history_length
    }

    /// Returns the timestamp lower bound.
    #[must_use]
    pub const fn status_timestamp_after(&self) -> Option<DateTime<Utc>> {
        self.status_timestamp_after
    }

    /// Returns whether artifact projections were requested.
    #[must_use]
    pub const fn should_include_artifacts(&self) -> bool {
        self.include_artifacts
    }

    pub(crate) fn try_from_wire(value: wire::ListTasksRequest) -> Result<Self, A2aContractError> {
        if value.tenant.is_some() {
            return Err(A2aContractError::Unsupported {
                field: "request tenant override",
                value: "tenant".to_string(),
            });
        }
        let mut request = Self::new();
        if let Some(context_id) = value.context_id {
            request = request.with_context_id(context_id)?;
        }
        if let Some(status) = value.status {
            request = request.with_status(A2aTaskState::try_from(status)?);
        }
        if let Some(page_size) = value.page_size {
            request = request.with_page_size(u16::try_from(page_size).map_err(|_| {
                A2aContractError::InvalidLifecycle("page size is outside the supported range")
            })?)?;
        }
        if let Some(page_token) = value.page_token {
            request = request.with_page_token(page_token)?;
        }
        if let Some(history_length) = value.history_length {
            request =
                request.with_history_length(u32::try_from(history_length).map_err(|_| {
                    A2aContractError::InvalidLifecycle("history length must be non-negative")
                })?)?;
        }
        if let Some(timestamp) = value.status_timestamp_after {
            request = request.with_status_timestamp_after(timestamp);
        }
        request.include_artifacts = value.include_artifacts.unwrap_or(false);
        Ok(request)
    }

    pub(crate) fn into_wire(self, tenant: Option<String>) -> wire::ListTasksRequest {
        wire::ListTasksRequest {
            context_id: self.context_id.map(Into::into),
            status: self.status.map(Into::into),
            page_size: Some(i32::from(self.page_size)),
            page_token: self.page_token.map(Into::into),
            history_length: self
                .history_length
                .map(|value| i32::try_from(value).expect("bounded history length fits i32")),
            status_timestamp_after: self.status_timestamp_after,
            include_artifacts: self.include_artifacts.then_some(true),
            tenant,
        }
    }
}

/// One stable-snapshot page of A2A tasks.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aTaskPage {
    tasks: Vec<A2aTask>,
    next_page_token: Option<Box<str>>,
    page_size: u16,
    total_size: u64,
}

impl A2aTaskPage {
    /// Constructs a bounded page.
    pub fn new(
        tasks: Vec<A2aTask>,
        next_page_token: Option<String>,
        page_size: u16,
        total_size: u64,
    ) -> Result<Self, A2aContractError> {
        validate_count(
            "task page",
            tasks.len(),
            usize::from(A2aListTasksRequest::MAX_PAGE_SIZE),
        )?;
        if page_size == 0 || page_size > A2aListTasksRequest::MAX_PAGE_SIZE {
            return Err(A2aContractError::InvalidLifecycle(
                "task response page size is outside the protocol range",
            ));
        }
        if tasks.len() > usize::from(page_size) {
            return Err(A2aContractError::InvalidLifecycle(
                "task response contains more tasks than its page size",
            ));
        }
        if total_size < tasks.len() as u64 {
            return Err(A2aContractError::InvalidLifecycle(
                "task response total size is smaller than the returned page",
            ));
        }
        let mut task_ids = HashSet::with_capacity(tasks.len());
        let mut previous_timestamp = None;
        for task in &tasks {
            let timestamp = task.status().timestamp();
            if !task_ids.insert(task.id())
                || previous_timestamp
                    .zip(timestamp)
                    .is_some_and(|(previous, current)| previous < current)
            {
                return Err(A2aContractError::InvalidLifecycle(
                    "task page must contain unique IDs in newest-status-first order",
                ));
            }
            if timestamp.is_some() {
                previous_timestamp = timestamp;
            }
        }
        if let Some(token) = next_page_token.as_deref() {
            validate_required_text("page token", token, MAX_ID_BYTES * 4)?;
        }
        Ok(Self {
            tasks,
            next_page_token: next_page_token.map(String::into_boxed_str),
            page_size,
            total_size,
        })
    }

    /// Returns tasks in protocol-defined newest-status-first order.
    #[must_use]
    pub fn tasks(&self) -> &[A2aTask] {
        &self.tasks
    }

    /// Returns the stable-snapshot continuation cursor.
    #[must_use]
    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }

    /// Returns the page size used by the remote service.
    #[must_use]
    pub const fn page_size(&self) -> u16 {
        self.page_size
    }

    /// Returns the authorized total size when known by the backend.
    #[must_use]
    pub const fn total_size(&self) -> u64 {
        self.total_size
    }

    pub(crate) fn into_wire(self) -> Result<wire::ListTasksResponse, A2aContractError> {
        let total_size = i32::try_from(self.total_size).map_err(|_| {
            A2aContractError::InvalidLifecycle("task total count cannot be represented")
        })?;
        Ok(wire::ListTasksResponse {
            tasks: self.tasks.into_iter().map(A2aTask::into_wire).collect(),
            next_page_token: self.next_page_token.map(Into::into).unwrap_or_default(),
            page_size: i32::from(self.page_size),
            total_size,
        })
    }

    pub(crate) fn try_from_wire(value: wire::ListTasksResponse) -> Result<Self, A2aContractError> {
        if value.total_size < 0 {
            return Err(A2aContractError::InvalidLifecycle(
                "task page counts must be non-negative",
            ));
        }
        let page_size = u16::try_from(value.page_size).map_err(|_| {
            A2aContractError::InvalidLifecycle(
                "task response page size is outside the protocol range",
            )
        })?;
        let tasks = value
            .tasks
            .into_iter()
            .map(A2aTask::try_from_wire)
            .collect::<Result<Vec<_>, _>>()?;
        let next = (!value.next_page_token.is_empty()).then_some(value.next_page_token);
        Self::new(
            tasks,
            next,
            page_size,
            u64::try_from(value.total_size)
                .expect("non-negative i32 task total always fits in u64"),
        )
    }
}

/// Validated task-cancellation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct A2aCancelTaskRequest {
    id: Box<str>,
    metadata: Option<HashMap<String, Value>>,
}

impl A2aCancelTaskRequest {
    /// Constructs a cancellation request.
    pub fn new(id: impl Into<String>) -> Result<Self, A2aContractError> {
        let id = id.into();
        validate_required_text("task id", &id, MAX_ID_BYTES)?;
        Ok(Self {
            id: id.into_boxed_str(),
            metadata: None,
        })
    }

    /// Adds bounded cancellation metadata.
    pub fn with_metadata(
        mut self,
        value: HashMap<String, Value>,
    ) -> Result<Self, A2aContractError> {
        validate_metadata("cancel metadata", Some(&value))?;
        self.metadata = Some(value);
        Ok(self)
    }

    /// Returns the task identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns cancellation metadata.
    #[must_use]
    pub const fn metadata(&self) -> Option<&HashMap<String, Value>> {
        self.metadata.as_ref()
    }

    pub(crate) fn try_from_wire(value: wire::CancelTaskRequest) -> Result<Self, A2aContractError> {
        if value.tenant.is_some() {
            return Err(A2aContractError::Unsupported {
                field: "request tenant override",
                value: "tenant".to_string(),
            });
        }
        let mut request = Self::new(value.id)?;
        if let Some(metadata) = value.metadata {
            request = request.with_metadata(metadata)?;
        }
        Ok(request)
    }

    pub(crate) fn into_wire(self, tenant: Option<String>) -> wire::CancelTaskRequest {
        wire::CancelTaskRequest {
            id: self.id.into(),
            metadata: self.metadata,
            tenant,
        }
    }
}

/// Validated task-subscription request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct A2aSubscribeTaskRequest {
    id: Box<str>,
}

impl A2aSubscribeTaskRequest {
    /// Constructs a subscription request.
    pub fn new(id: impl Into<String>) -> Result<Self, A2aContractError> {
        let id = id.into();
        validate_required_text("task id", &id, MAX_ID_BYTES)?;
        Ok(Self {
            id: id.into_boxed_str(),
        })
    }

    /// Returns the task identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn try_from_wire(
        value: wire::SubscribeToTaskRequest,
    ) -> Result<Self, A2aContractError> {
        if value.tenant.is_some() {
            return Err(A2aContractError::Unsupported {
                field: "request tenant override",
                value: "tenant".to_string(),
            });
        }
        Self::new(value.id)
    }

    pub(crate) fn into_wire(self, tenant: Option<String>) -> wire::SubscribeToTaskRequest {
        wire::SubscribeToTaskRequest {
            id: self.id.into(),
            tenant,
        }
    }
}

/// Optional authentication attached to an A2A push destination.
#[derive(Clone)]
pub struct A2aPushAuthentication {
    scheme: Box<str>,
    credentials: Option<A2aSecret>,
}

impl A2aPushAuthentication {
    /// Constructs a bounded authentication descriptor.
    pub fn new(
        scheme: impl Into<String>,
        credentials: Option<A2aSecret>,
    ) -> Result<Self, A2aContractError> {
        let scheme = scheme.into();
        validate_required_text("push authentication scheme", &scheme, MAX_LABEL_BYTES)?;
        Ok(Self {
            scheme: scheme.into_boxed_str(),
            credentials,
        })
    }

    /// Returns the case-sensitive scheme token.
    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Returns secret credentials for immediate encryption or dispatch only.
    #[must_use]
    pub const fn credentials(&self) -> Option<&A2aSecret> {
        self.credentials.as_ref()
    }

    fn try_from_wire(value: wire::AuthenticationInfo) -> Result<Self, A2aContractError> {
        Self::new(
            value.scheme,
            value.credentials.map(A2aSecret::new).transpose()?,
        )
    }

    fn into_wire(self) -> wire::AuthenticationInfo {
        wire::AuthenticationInfo {
            scheme: self.scheme.into(),
            credentials: self
                .credentials
                .map(|credential| credential.expose_secret().to_string()),
        }
    }
}

impl fmt::Debug for A2aPushAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("A2aPushAuthentication")
            .field("scheme", &self.scheme)
            .field(
                "credentials",
                &self.credentials.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

impl PartialEq for A2aPushAuthentication {
    fn eq(&self, other: &Self) -> bool {
        self.scheme == other.scheme
            && self.credentials.as_ref().map(A2aSecret::expose_secret)
                == other.credentials.as_ref().map(A2aSecret::expose_secret)
    }
}

/// Bounded push-notification configuration.
///
/// The URL is untrusted until a push worker applies the configured egress
/// policy. Token and credentials are redacted from `Debug` and must be stored
/// encrypted by a production backend.
#[derive(Clone, PartialEq)]
pub struct A2aPushConfig {
    url: Box<str>,
    id: Option<Box<str>>,
    task_id: Option<Box<str>>,
    token: Option<A2aSecret>,
    authentication: Option<A2aPushAuthentication>,
}

impl A2aPushConfig {
    /// Constructs a destination without performing network access.
    pub fn new(url: impl Into<String>) -> Result<Self, A2aContractError> {
        let url = url.into();
        validate_url("push destination", &url)?;
        Ok(Self {
            url: url.into_boxed_str(),
            id: None,
            task_id: None,
            token: None,
            authentication: None,
        })
    }

    /// Adds a server-issued configuration identifier.
    pub fn with_id(mut self, id: impl Into<String>) -> Result<Self, A2aContractError> {
        let id = id.into();
        validate_required_text("push config id", &id, MAX_ID_BYTES)?;
        self.id = Some(id.into_boxed_str());
        Ok(self)
    }

    /// Binds the configuration to a task.
    pub fn with_task_id(mut self, id: impl Into<String>) -> Result<Self, A2aContractError> {
        let id = id.into();
        validate_required_text("task id", &id, MAX_ID_BYTES)?;
        self.task_id = Some(id.into_boxed_str());
        Ok(self)
    }

    /// Adds a caller-provided validation token.
    #[must_use]
    pub fn with_token(mut self, token: A2aSecret) -> Self {
        self.token = Some(token);
        self
    }

    /// Adds destination authentication.
    #[must_use]
    pub fn with_authentication(mut self, authentication: A2aPushAuthentication) -> Self {
        self.authentication = Some(authentication);
        self
    }

    /// Returns the untrusted callback URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the configuration identifier.
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Returns the bound task identifier.
    #[must_use]
    pub fn task_id(&self) -> Option<&str> {
        self.task_id.as_deref()
    }

    /// Returns the validation token for immediate encryption or dispatch only.
    #[must_use]
    pub const fn token(&self) -> Option<&A2aSecret> {
        self.token.as_ref()
    }

    /// Returns destination authentication.
    #[must_use]
    pub const fn authentication(&self) -> Option<&A2aPushAuthentication> {
        self.authentication.as_ref()
    }

    pub(crate) fn try_from_wire(
        value: wire::TaskPushNotificationConfig,
    ) -> Result<Self, A2aContractError> {
        if value.tenant.is_some() {
            return Err(A2aContractError::Unsupported {
                field: "request tenant override",
                value: "tenant".to_string(),
            });
        }
        let mut config = Self::new(value.url)?;
        if let Some(id) = value.id {
            config = config.with_id(id)?;
        }
        if !value.task_id.is_empty() {
            config = config.with_task_id(value.task_id)?;
        }
        if let Some(token) = value.token {
            config = config.with_token(A2aSecret::new(token)?);
        }
        if let Some(authentication) = value.authentication {
            config =
                config.with_authentication(A2aPushAuthentication::try_from_wire(authentication)?);
        }
        Ok(config)
    }

    pub(crate) fn into_wire(self) -> wire::TaskPushNotificationConfig {
        wire::TaskPushNotificationConfig {
            url: self.url.into(),
            id: self.id.map(Into::into),
            task_id: self.task_id.map(Into::into).unwrap_or_default(),
            token: self.token.map(|token| token.expose_secret().to_string()),
            authentication: self.authentication.map(A2aPushAuthentication::into_wire),
            tenant: None,
        }
    }

    pub(crate) fn into_wire_for_tenant(
        self,
        tenant: Option<String>,
    ) -> wire::TaskPushNotificationConfig {
        let mut value = self.into_wire();
        value.tenant = tenant;
        value
    }
}

impl fmt::Debug for A2aPushConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("A2aPushConfig")
            .field("url", &"[REDACTED]")
            .field("id", &self.id)
            .field("task_id", &self.task_id)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("authentication", &self.authentication)
            .finish()
    }
}

/// Request to read a push configuration after task authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct A2aGetPushConfigRequest {
    task_id: Box<str>,
    config_id: Box<str>,
}

impl A2aGetPushConfigRequest {
    /// Constructs a lookup request.
    pub fn new(
        task_id: impl Into<String>,
        config_id: impl Into<String>,
    ) -> Result<Self, A2aContractError> {
        let task_id = task_id.into();
        let config_id = config_id.into();
        validate_required_text("task id", &task_id, MAX_ID_BYTES)?;
        validate_required_text("push config id", &config_id, MAX_ID_BYTES)?;
        Ok(Self {
            task_id: task_id.into_boxed_str(),
            config_id: config_id.into_boxed_str(),
        })
    }

    /// Returns the task identifier.
    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Returns the configuration identifier.
    #[must_use]
    pub fn config_id(&self) -> &str {
        &self.config_id
    }

    pub(crate) fn try_from_wire(
        value: wire::GetTaskPushNotificationConfigRequest,
    ) -> Result<Self, A2aContractError> {
        if value.tenant.is_some() {
            return Err(A2aContractError::Unsupported {
                field: "request tenant override",
                value: "tenant".to_string(),
            });
        }
        Self::new(value.task_id, value.id)
    }

    pub(crate) fn into_get_wire(
        self,
        tenant: Option<String>,
    ) -> wire::GetTaskPushNotificationConfigRequest {
        wire::GetTaskPushNotificationConfigRequest {
            task_id: self.task_id.into(),
            id: self.config_id.into(),
            tenant,
        }
    }

    pub(crate) fn into_delete_wire(
        self,
        tenant: Option<String>,
    ) -> wire::DeleteTaskPushNotificationConfigRequest {
        wire::DeleteTaskPushNotificationConfigRequest {
            task_id: self.task_id.into(),
            id: self.config_id.into(),
            tenant,
        }
    }
}

/// Bounded request to list push configurations for one task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct A2aListPushConfigsRequest {
    task_id: Box<str>,
    page_size: u16,
    page_token: Option<Box<str>>,
}

impl A2aListPushConfigsRequest {
    /// Constructs a first-page request.
    pub fn new(task_id: impl Into<String>) -> Result<Self, A2aContractError> {
        let task_id = task_id.into();
        validate_required_text("task id", &task_id, MAX_ID_BYTES)?;
        Ok(Self {
            task_id: task_id.into_boxed_str(),
            page_size: 50,
            page_token: None,
        })
    }

    /// Sets a bounded page size.
    pub fn with_page_size(mut self, value: u16) -> Result<Self, A2aContractError> {
        if value == 0 || value > 256 {
            return Err(A2aContractError::InvalidLifecycle(
                "push-config page size is outside the supported range",
            ));
        }
        self.page_size = value;
        Ok(self)
    }

    /// Sets an opaque continuation cursor.
    pub fn with_page_token(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_required_text("page token", &value, MAX_ID_BYTES * 4)?;
        self.page_token = Some(value.into_boxed_str());
        Ok(self)
    }

    /// Returns the task identifier.
    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Returns the effective page size.
    #[must_use]
    pub const fn page_size(&self) -> u16 {
        self.page_size
    }

    /// Returns the opaque continuation cursor.
    #[must_use]
    pub fn page_token(&self) -> Option<&str> {
        self.page_token.as_deref()
    }

    pub(crate) fn try_from_wire(
        value: wire::ListTaskPushNotificationConfigsRequest,
    ) -> Result<Self, A2aContractError> {
        if value.tenant.is_some() {
            return Err(A2aContractError::Unsupported {
                field: "request tenant override",
                value: "tenant".to_string(),
            });
        }
        let mut request = Self::new(value.task_id)?;
        if let Some(page_size) = value.page_size {
            request = request.with_page_size(u16::try_from(page_size).map_err(|_| {
                A2aContractError::InvalidLifecycle(
                    "push-config page size is outside the supported range",
                )
            })?)?;
        }
        if let Some(page_token) = value.page_token {
            request = request.with_page_token(page_token)?;
        }
        Ok(request)
    }

    pub(crate) fn into_wire(
        self,
        tenant: Option<String>,
    ) -> wire::ListTaskPushNotificationConfigsRequest {
        wire::ListTaskPushNotificationConfigsRequest {
            task_id: self.task_id.into(),
            page_size: Some(i32::from(self.page_size)),
            page_token: self.page_token.map(Into::into),
            tenant,
        }
    }
}

/// One stable page of push configurations.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aPushConfigPage {
    configs: Vec<A2aPushConfig>,
    next_page_token: Option<Box<str>>,
}

impl A2aPushConfigPage {
    /// Constructs a bounded page.
    pub fn new(
        configs: Vec<A2aPushConfig>,
        next_page_token: Option<String>,
    ) -> Result<Self, A2aContractError> {
        validate_count("push config page", configs.len(), 256)?;
        if let Some(token) = next_page_token.as_deref() {
            validate_required_text("page token", token, MAX_ID_BYTES * 4)?;
        }
        Ok(Self {
            configs,
            next_page_token: next_page_token.map(String::into_boxed_str),
        })
    }

    /// Returns configurations in stable backend order.
    #[must_use]
    pub fn configs(&self) -> &[A2aPushConfig] {
        &self.configs
    }

    /// Returns the continuation cursor.
    #[must_use]
    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }

    pub(crate) fn into_wire(self) -> wire::ListTaskPushNotificationConfigsResponse {
        wire::ListTaskPushNotificationConfigsResponse {
            configs: self
                .configs
                .into_iter()
                .map(A2aPushConfig::into_wire)
                .collect(),
            next_page_token: self.next_page_token.map(Into::into),
        }
    }

    pub(crate) fn try_from_wire(
        value: wire::ListTaskPushNotificationConfigsResponse,
    ) -> Result<Self, A2aContractError> {
        let configs = value
            .configs
            .into_iter()
            .map(A2aPushConfig::try_from_wire)
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(configs, value.next_page_token)
    }
}

/// Request to delete one push configuration.
pub type A2aDeletePushConfigRequest = A2aGetPushConfigRequest;

/// Ordered task stream event.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum A2aStreamEvent {
    /// Complete task snapshot.
    Task(A2aTask),
    /// Direct message response.
    Message(A2aMessage),
    /// Task status transition.
    StatusUpdate(A2aStatusUpdate),
    /// Artifact or artifact chunk update.
    ArtifactUpdate(A2aArtifactUpdate),
}

impl A2aStreamEvent {
    /// Returns whether this event commits a terminal task state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::Task(task) => task.state().is_terminal(),
            Self::StatusUpdate(update) => update.status.state().is_terminal(),
            Self::Message(_) | Self::ArtifactUpdate(_) => false,
        }
    }

    pub(crate) fn into_wire(self) -> wire::StreamResponse {
        match self {
            Self::Task(task) => wire::StreamResponse::Task(task.into_wire()),
            Self::Message(message) => wire::StreamResponse::Message(message.into_wire()),
            Self::StatusUpdate(update) => wire::StreamResponse::StatusUpdate(update.into_wire()),
            Self::ArtifactUpdate(update) => {
                wire::StreamResponse::ArtifactUpdate(update.into_wire())
            }
        }
    }

    pub(crate) fn try_from_wire(value: wire::StreamResponse) -> Result<Self, A2aContractError> {
        match value {
            wire::StreamResponse::Task(task) => Ok(Self::Task(A2aTask::try_from_wire(task)?)),
            wire::StreamResponse::Message(message) => {
                Ok(Self::Message(A2aMessage::try_from_wire(message)?))
            }
            wire::StreamResponse::StatusUpdate(update) => {
                Ok(Self::StatusUpdate(A2aStatusUpdate::try_from_wire(update)?))
            }
            wire::StreamResponse::ArtifactUpdate(update) => Ok(Self::ArtifactUpdate(
                A2aArtifactUpdate::try_from_wire(update)?,
            )),
        }
    }
}

/// Task status update with exact task/context identity.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aStatusUpdate {
    task_id: Box<str>,
    context_id: Box<str>,
    status: A2aTaskStatus,
    metadata: Option<HashMap<String, Value>>,
}

impl A2aStatusUpdate {
    /// Constructs an ordered status update.
    pub fn new(
        task_id: impl Into<String>,
        context_id: impl Into<String>,
        status: A2aTaskStatus,
    ) -> Result<Self, A2aContractError> {
        let task_id = task_id.into();
        let context_id = context_id.into();
        validate_required_text("task id", &task_id, MAX_ID_BYTES)?;
        validate_required_text("context id", &context_id, MAX_ID_BYTES)?;
        Ok(Self {
            task_id: task_id.into_boxed_str(),
            context_id: context_id.into_boxed_str(),
            status,
            metadata: None,
        })
    }

    /// Adds bounded event metadata.
    pub fn with_metadata(
        mut self,
        metadata: HashMap<String, Value>,
    ) -> Result<Self, A2aContractError> {
        validate_metadata("status update metadata", Some(&metadata))?;
        self.metadata = Some(metadata);
        Ok(self)
    }

    /// Returns the task identifier.
    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Returns the context identifier.
    #[must_use]
    pub fn context_id(&self) -> &str {
        &self.context_id
    }

    /// Returns the task status.
    #[must_use]
    pub const fn status(&self) -> &A2aTaskStatus {
        &self.status
    }

    fn into_wire(self) -> wire::TaskStatusUpdateEvent {
        wire::TaskStatusUpdateEvent {
            task_id: self.task_id.into(),
            context_id: self.context_id.into(),
            status: self.status.into_wire(),
            metadata: self.metadata,
        }
    }

    fn try_from_wire(value: wire::TaskStatusUpdateEvent) -> Result<Self, A2aContractError> {
        let mut update = Self::new(
            value.task_id,
            value.context_id,
            A2aTaskStatus::try_from_wire(value.status)?,
        )?;
        if let Some(metadata) = value.metadata {
            update = update.with_metadata(metadata)?;
        }
        Ok(update)
    }
}

/// Artifact update or ordered artifact chunk.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aArtifactUpdate {
    task_id: Box<str>,
    context_id: Box<str>,
    artifact: A2aArtifact,
    append: bool,
    last_chunk: bool,
    metadata: Option<HashMap<String, Value>>,
}

impl A2aArtifactUpdate {
    /// Constructs an artifact update.
    pub fn new(
        task_id: impl Into<String>,
        context_id: impl Into<String>,
        artifact: A2aArtifact,
    ) -> Result<Self, A2aContractError> {
        let task_id = task_id.into();
        let context_id = context_id.into();
        validate_required_text("task id", &task_id, MAX_ID_BYTES)?;
        validate_required_text("context id", &context_id, MAX_ID_BYTES)?;
        Ok(Self {
            task_id: task_id.into_boxed_str(),
            context_id: context_id.into_boxed_str(),
            artifact,
            append: false,
            last_chunk: true,
            metadata: None,
        })
    }

    /// Marks this event as an append chunk and declares whether it is final.
    #[must_use]
    pub const fn as_chunk(mut self, last_chunk: bool) -> Self {
        self.append = true;
        self.last_chunk = last_chunk;
        self
    }

    /// Marks this event as the first, non-final chunk of a new or replaced artifact.
    /// Subsequent chunks must use [`Self::as_chunk`].
    #[must_use]
    pub const fn as_initial_chunk(mut self) -> Self {
        self.append = false;
        self.last_chunk = false;
        self
    }

    /// Adds bounded event metadata.
    pub fn with_metadata(
        mut self,
        metadata: HashMap<String, Value>,
    ) -> Result<Self, A2aContractError> {
        validate_metadata("artifact update metadata", Some(&metadata))?;
        self.metadata = Some(metadata);
        Ok(self)
    }

    /// Returns the task identifier.
    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Returns the context identifier.
    #[must_use]
    pub fn context_id(&self) -> &str {
        &self.context_id
    }

    /// Returns the artifact projection.
    #[must_use]
    pub const fn artifact(&self) -> &A2aArtifact {
        &self.artifact
    }

    /// Returns whether this event appends to an existing artifact.
    #[must_use]
    pub const fn append(&self) -> bool {
        self.append
    }

    /// Returns whether this is the final artifact chunk.
    #[must_use]
    pub const fn last_chunk(&self) -> bool {
        self.last_chunk
    }

    fn into_wire(self) -> wire::TaskArtifactUpdateEvent {
        wire::TaskArtifactUpdateEvent {
            task_id: self.task_id.into(),
            context_id: self.context_id.into(),
            artifact: self.artifact.into_wire(),
            append: Some(self.append),
            last_chunk: Some(self.last_chunk),
            metadata: self.metadata,
        }
    }

    fn try_from_wire(value: wire::TaskArtifactUpdateEvent) -> Result<Self, A2aContractError> {
        let append = value.append.unwrap_or(false);
        let last_chunk = value.last_chunk.unwrap_or(false);
        let mut update = Self::new(
            value.task_id,
            value.context_id,
            A2aArtifact::try_from_wire(value.artifact)?,
        )?;
        update.append = append;
        update.last_chunk = last_chunk;
        if let Some(metadata) = value.metadata {
            update = update.with_metadata(metadata)?;
        }
        Ok(update)
    }
}

/// Transport binding supported by the `StateKnot` A2A v1 profile.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum A2aBinding {
    /// HTTP+JSON resource-oriented binding.
    HttpJson,
    /// JSON-RPC 2.0 over HTTP binding.
    JsonRpc,
}

impl A2aBinding {
    /// Returns the exact Agent Card protocol-binding value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HttpJson => A2A_BINDING_HTTP_JSON,
            Self::JsonRpc => A2A_BINDING_JSONRPC,
        }
    }
}

impl TryFrom<&str> for A2aBinding {
    type Error = A2aContractError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            A2A_BINDING_HTTP_JSON => Ok(Self::HttpJson),
            A2A_BINDING_JSONRPC => Ok(Self::JsonRpc),
            other => Err(A2aContractError::Unsupported {
                field: "protocol binding",
                value: other.to_string(),
            }),
        }
    }
}

/// One A2A endpoint advertised in preference order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct A2aAgentInterface {
    url: Box<str>,
    binding: A2aBinding,
    tenant: Option<Box<str>>,
}

impl A2aAgentInterface {
    /// Constructs an A2A 1.0 endpoint descriptor.
    pub fn new(url: impl Into<String>, binding: A2aBinding) -> Result<Self, A2aContractError> {
        let url = url.into();
        validate_url("agent interface", &url)?;
        Ok(Self {
            url: url.into_boxed_str(),
            binding,
            tenant: None,
        })
    }

    /// Binds the opaque routing tenant required by this interface.
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Result<Self, A2aContractError> {
        let tenant = tenant.into();
        validate_required_text("interface tenant", &tenant, MAX_ID_BYTES)?;
        self.tenant = Some(tenant.into_boxed_str());
        Ok(self)
    }

    /// Returns the endpoint URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the supported binding.
    #[must_use]
    pub const fn binding(&self) -> A2aBinding {
        self.binding
    }

    /// Returns the opaque routing tenant advertised for this interface.
    #[must_use]
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    pub(crate) fn try_from_wire(value: wire::AgentInterface) -> Result<Self, A2aContractError> {
        if value.protocol_version != A2A_PROTOCOL_VERSION_1_0 {
            return Err(A2aContractError::Unsupported {
                field: "protocol version",
                value: value.protocol_version,
            });
        }
        let tenant = value.tenant;
        let mut interface = Self::new(
            value.url,
            A2aBinding::try_from(value.protocol_binding.as_str())?,
        )?;
        if let Some(tenant) = tenant {
            interface = interface.with_tenant(tenant)?;
        }
        Ok(interface)
    }

    fn into_wire(self) -> wire::AgentInterface {
        wire::AgentInterface {
            url: self.url.into(),
            protocol_binding: self.binding.as_str().to_string(),
            protocol_version: A2A_PROTOCOL_VERSION_1_0.to_string(),
            tenant: self.tenant.map(Into::into),
        }
    }
}

fn validate_wire_interface(value: &wire::AgentInterface) -> Result<(), A2aContractError> {
    validate_required_text("agent interface address", &value.url, MAX_URL_BYTES)?;
    validate_required_text(
        "protocol binding",
        value.protocol_binding.as_str(),
        MAX_LABEL_BYTES,
    )?;
    validate_required_text(
        "protocol version",
        value.protocol_version.as_str(),
        MAX_LABEL_BYTES,
    )?;
    validate_optional_text("interface tenant", value.tenant.as_deref(), MAX_ID_BYTES)?;
    if A2aBinding::try_from(value.protocol_binding.as_str()).is_ok() {
        validate_url("agent interface", &value.url)?;
    }
    Ok(())
}

/// A declared A2A extension.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aAgentExtension {
    uri: Box<str>,
    description: Option<Box<str>>,
    required: bool,
    parameters: Option<HashMap<String, Value>>,
}

impl A2aAgentExtension {
    /// Constructs an extension declaration by absolute URI.
    pub fn new(uri: impl Into<String>) -> Result<Self, A2aContractError> {
        let uri = uri.into();
        validate_url("extension", &uri)?;
        Ok(Self {
            uri: uri.into_boxed_str(),
            description: None,
            required: false,
            parameters: None,
        })
    }

    /// Adds a public description.
    pub fn with_description(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_required_text("extension description", &value, MAX_DESCRIPTION_BYTES)?;
        self.description = Some(value.into_boxed_str());
        Ok(self)
    }

    /// Marks support for this extension as required for interoperable calls.
    #[must_use]
    pub const fn required(mut self, value: bool) -> Self {
        self.required = value;
        self
    }

    /// Adds bounded extension parameters. Parameters remain inert data.
    pub fn with_parameters(
        mut self,
        value: HashMap<String, Value>,
    ) -> Result<Self, A2aContractError> {
        validate_metadata("extension parameters", Some(&value))?;
        self.parameters = Some(value);
        Ok(self)
    }

    /// Returns the exact extension URI.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns whether clients must request this extension.
    #[must_use]
    pub const fn is_required(&self) -> bool {
        self.required
    }

    fn try_from_wire(value: wire::AgentExtension) -> Result<Self, A2aContractError> {
        let mut extension = Self::new(value.uri)?;
        if let Some(description) = value.description {
            extension = extension.with_description(description)?;
        }
        extension.required = value.required.unwrap_or(false);
        if let Some(parameters) = value.params {
            extension = extension.with_parameters(parameters)?;
        }
        Ok(extension)
    }

    fn into_wire(self) -> wire::AgentExtension {
        wire::AgentExtension {
            uri: self.uri.into(),
            description: self.description.map(Into::into),
            required: self.required.then_some(true),
            params: self.parameters,
        }
    }
}

/// Honest capability flags advertised by an Agent Card.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct A2aAgentCapabilities {
    streaming: bool,
    push_notifications: bool,
    extended_agent_card: bool,
    extensions: Vec<A2aAgentExtension>,
}

impl A2aAgentCapabilities {
    /// Constructs an empty capability set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            streaming: false,
            push_notifications: false,
            extended_agent_card: false,
            extensions: Vec::new(),
        }
    }

    /// Advertises durable streaming/subscription support.
    #[must_use]
    pub const fn streaming(mut self, value: bool) -> Self {
        self.streaming = value;
        self
    }

    /// Advertises durable push-configuration and delivery support.
    #[must_use]
    pub const fn push_notifications(mut self, value: bool) -> Self {
        self.push_notifications = value;
        self
    }

    /// Advertises an authenticated extended Agent Card.
    #[must_use]
    pub const fn extended_agent_card(mut self, value: bool) -> Self {
        self.extended_agent_card = value;
        self
    }

    /// Adds one declared extension.
    pub fn with_extension(
        mut self,
        extension: A2aAgentExtension,
    ) -> Result<Self, A2aContractError> {
        if self.extensions.len() == MAX_EXTENSIONS {
            return Err(A2aContractError::TooMany {
                field: "agent extensions",
                maximum: MAX_EXTENSIONS,
                actual: self.extensions.len() + 1,
            });
        }
        if self
            .extensions
            .iter()
            .any(|existing| existing.uri == extension.uri)
        {
            return Err(A2aContractError::InvalidLifecycle(
                "duplicate Agent Card extension URI",
            ));
        }
        self.extensions.push(extension);
        Ok(self)
    }

    /// Returns whether streaming is advertised.
    #[must_use]
    pub const fn supports_streaming(&self) -> bool {
        self.streaming
    }

    /// Returns whether push notifications are advertised.
    #[must_use]
    pub const fn supports_push_notifications(&self) -> bool {
        self.push_notifications
    }

    /// Returns whether an extended Agent Card is advertised.
    #[must_use]
    pub const fn supports_extended_agent_card(&self) -> bool {
        self.extended_agent_card
    }

    /// Returns declared extensions in preference order.
    #[must_use]
    pub fn extensions(&self) -> &[A2aAgentExtension] {
        &self.extensions
    }

    fn try_from_wire(value: wire::AgentCapabilities) -> Result<Self, A2aContractError> {
        let extensions = value
            .extensions
            .unwrap_or_default()
            .into_iter()
            .map(A2aAgentExtension::try_from_wire)
            .collect::<Result<Vec<_>, _>>()?;
        validate_count("agent extensions", extensions.len(), MAX_EXTENSIONS)?;
        let mut seen = std::collections::HashSet::new();
        if extensions
            .iter()
            .any(|extension| !seen.insert(extension.uri().to_string()))
        {
            return Err(A2aContractError::InvalidLifecycle(
                "duplicate Agent Card extension URI",
            ));
        }
        Ok(Self {
            streaming: value.streaming.unwrap_or(false),
            push_notifications: value.push_notifications.unwrap_or(false),
            extended_agent_card: value.extended_agent_card.unwrap_or(false),
            extensions,
        })
    }

    fn into_wire(self) -> wire::AgentCapabilities {
        wire::AgentCapabilities {
            streaming: Some(self.streaming),
            push_notifications: Some(self.push_notifications),
            extensions: (!self.extensions.is_empty()).then_some(
                self.extensions
                    .into_iter()
                    .map(A2aAgentExtension::into_wire)
                    .collect(),
            ),
            extended_agent_card: Some(self.extended_agent_card),
        }
    }
}

/// One discoverable agent skill. Mapping to a local executable capability is
/// an explicit deployment decision; the external ID is never normalized into
/// a `StateKnot` capability name.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aAgentSkill {
    inner: wire::AgentSkill,
}

impl A2aAgentSkill {
    /// Constructs a skill descriptor.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        description: impl Into<String>,
        tags: Vec<String>,
    ) -> Result<Self, A2aContractError> {
        let id = id.into();
        let name = name.into();
        let description = description.into();
        validate_required_text("skill id", &id, MAX_ID_BYTES)?;
        validate_required_text("skill name", &name, MAX_LABEL_BYTES)?;
        validate_required_text("skill description", &description, MAX_DESCRIPTION_BYTES)?;
        validate_string_list("skill tags", &tags, MAX_TAGS, MAX_LABEL_BYTES)?;
        Ok(Self {
            inner: wire::AgentSkill {
                id,
                name,
                description,
                tags,
                examples: None,
                input_modes: None,
                output_modes: None,
                security_requirements: None,
            },
        })
    }

    /// Adds bounded examples.
    pub fn with_examples(mut self, values: Vec<String>) -> Result<Self, A2aContractError> {
        validate_string_list(
            "skill examples",
            &values,
            MAX_EXAMPLES,
            MAX_DESCRIPTION_BYTES,
        )?;
        self.inner.examples = Some(values);
        Ok(self)
    }

    /// Narrows accepted input media modes.
    pub fn with_input_modes(mut self, values: Vec<String>) -> Result<Self, A2aContractError> {
        validate_modes("skill input modes", &values)?;
        self.inner.input_modes = Some(values);
        Ok(self)
    }

    /// Narrows produced output media modes.
    pub fn with_output_modes(mut self, values: Vec<String>) -> Result<Self, A2aContractError> {
        validate_modes("skill output modes", &values)?;
        self.inner.output_modes = Some(values);
        Ok(self)
    }

    /// Sets skill-specific security requirements.
    pub fn with_security_requirements(
        mut self,
        values: Vec<HashMap<String, Vec<String>>>,
    ) -> Result<Self, A2aContractError> {
        validate_security_requirements(&values)?;
        self.inner.security_requirements = Some(values);
        Ok(self)
    }

    /// Returns the exact external skill identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    /// Returns skill-specific input modes; an empty slice means card defaults.
    #[must_use]
    pub fn input_modes(&self) -> &[String] {
        self.inner.input_modes.as_deref().unwrap_or_default()
    }

    /// Returns skill-specific output modes; an empty slice means card defaults.
    #[must_use]
    pub fn output_modes(&self) -> &[String] {
        self.inner.output_modes.as_deref().unwrap_or_default()
    }

    pub(crate) fn security_requirements(&self) -> Option<&[wire::SecurityRequirement]> {
        self.inner.security_requirements.as_deref()
    }

    fn try_from_wire(value: wire::AgentSkill) -> Result<Self, A2aContractError> {
        let mut skill = Self::new(value.id, value.name, value.description, value.tags)?;
        if let Some(examples) = value.examples {
            skill = skill.with_examples(examples)?;
        }
        if let Some(modes) = value.input_modes {
            skill = skill.with_input_modes(modes)?;
        }
        if let Some(modes) = value.output_modes {
            skill = skill.with_output_modes(modes)?;
        }
        if let Some(requirements) = value.security_requirements {
            skill = skill.with_security_requirements(requirements)?;
        }
        Ok(skill)
    }

    fn into_wire(self) -> wire::AgentSkill {
        self.inner
    }
}

/// Bounded A2A security scheme. The inner SDK representation remains private.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aSecurityScheme {
    inner: wire::SecurityScheme,
}

impl A2aSecurityScheme {
    /// Constructs an HTTP Bearer scheme.
    pub fn http_bearer(
        bearer_format: Option<String>,
        description: Option<String>,
    ) -> Result<Self, A2aContractError> {
        validate_optional_text("bearer format", bearer_format.as_deref(), MAX_LABEL_BYTES)?;
        validate_optional_text(
            "security description",
            description.as_deref(),
            MAX_DESCRIPTION_BYTES,
        )?;
        Ok(Self {
            inner: wire::SecurityScheme::HttpAuth(wire::HttpAuthSecurityScheme {
                scheme: "Bearer".to_string(),
                description,
                bearer_format,
            }),
        })
    }

    /// Constructs an `OpenID` Connect discovery scheme.
    pub fn open_id_connect(
        discovery_url: impl Into<String>,
        description: Option<String>,
    ) -> Result<Self, A2aContractError> {
        let discovery_url = discovery_url.into();
        validate_url("OpenID Connect discovery", &discovery_url)?;
        validate_optional_text(
            "security description",
            description.as_deref(),
            MAX_DESCRIPTION_BYTES,
        )?;
        Ok(Self {
            inner: wire::SecurityScheme::OpenIdConnect(wire::OpenIdConnectSecurityScheme {
                open_id_connect_url: discovery_url,
                description,
            }),
        })
    }

    /// Validates and preserves any standard A2A 1.0 security-scheme JSON.
    pub fn from_json(value: Value) -> Result<Self, A2aContractError> {
        validate_json("security scheme", &value, MAX_METADATA_BYTES)?;
        let inner = serde_json::from_value(value).map_err(|_| A2aContractError::InvalidJson {
            field: "security scheme",
        })?;
        validate_security_scheme(&inner)?;
        Ok(Self { inner })
    }

    /// Returns the standard A2A JSON form without secret material.
    pub fn to_json(&self) -> Result<Value, A2aContractError> {
        serde_json::to_value(&self.inner).map_err(|_| A2aContractError::InvalidJson {
            field: "security scheme",
        })
    }

    fn into_wire(self) -> wire::SecurityScheme {
        self.inner
    }
}

fn validate_security_scheme(value: &wire::SecurityScheme) -> Result<(), A2aContractError> {
    match value {
        wire::SecurityScheme::ApiKey(value) => {
            validate_required_text("API key location", &value.location, MAX_LABEL_BYTES)?;
            validate_required_text("API key name", &value.name, MAX_LABEL_BYTES)?;
            validate_optional_text(
                "security description",
                value.description.as_deref(),
                MAX_DESCRIPTION_BYTES,
            )?;
        }
        wire::SecurityScheme::HttpAuth(value) => {
            validate_required_text("HTTP authentication scheme", &value.scheme, MAX_LABEL_BYTES)?;
            validate_optional_text(
                "security description",
                value.description.as_deref(),
                MAX_DESCRIPTION_BYTES,
            )?;
            validate_optional_text(
                "bearer format",
                value.bearer_format.as_deref(),
                MAX_LABEL_BYTES,
            )?;
        }
        wire::SecurityScheme::OAuth2(value) => {
            validate_optional_text(
                "security description",
                value.description.as_deref(),
                MAX_DESCRIPTION_BYTES,
            )?;
            if let Some(url) = value.oauth2_metadata_url.as_deref() {
                validate_url("OAuth metadata", url)?;
            }
            validate_oauth_flow(&value.flows)?;
        }
        wire::SecurityScheme::OpenIdConnect(value) => {
            validate_url("OpenID Connect discovery", &value.open_id_connect_url)?;
            validate_optional_text(
                "security description",
                value.description.as_deref(),
                MAX_DESCRIPTION_BYTES,
            )?;
        }
        wire::SecurityScheme::MutualTls(value) => {
            validate_optional_text(
                "security description",
                value.description.as_deref(),
                MAX_DESCRIPTION_BYTES,
            )?;
        }
    }
    Ok(())
}

fn validate_oauth_flow(value: &wire::OAuthFlows) -> Result<(), A2aContractError> {
    match value {
        wire::OAuthFlows::AuthorizationCode(flow) => {
            validate_url("OAuth authorization", &flow.authorization_url)?;
            validate_url("OAuth token", &flow.token_url)?;
            validate_scope_map(&flow.scopes)?;
            if let Some(url) = flow.refresh_url.as_deref() {
                validate_url("OAuth refresh", url)?;
            }
        }
        wire::OAuthFlows::ClientCredentials(flow) => {
            validate_url("OAuth token", &flow.token_url)?;
            validate_scope_map(&flow.scopes)?;
            if let Some(url) = flow.refresh_url.as_deref() {
                validate_url("OAuth refresh", url)?;
            }
        }
        wire::OAuthFlows::DeviceCode(flow) => {
            validate_url("OAuth device authorization", &flow.device_authorization_url)?;
            validate_url("OAuth token", &flow.token_url)?;
            validate_scope_map(&flow.scopes)?;
            if let Some(url) = flow.refresh_url.as_deref() {
                validate_url("OAuth refresh", url)?;
            }
        }
        wire::OAuthFlows::Implicit(flow) => {
            validate_url("OAuth authorization", &flow.authorization_url)?;
            validate_scope_map(&flow.scopes)?;
            if let Some(url) = flow.refresh_url.as_deref() {
                validate_url("OAuth refresh", url)?;
            }
        }
        wire::OAuthFlows::Password(flow) => {
            validate_url("OAuth token", &flow.token_url)?;
            validate_scope_map(&flow.scopes)?;
            if let Some(url) = flow.refresh_url.as_deref() {
                validate_url("OAuth refresh", url)?;
            }
        }
    }
    Ok(())
}

fn validate_scope_map(scopes: &HashMap<String, String>) -> Result<(), A2aContractError> {
    validate_count("OAuth scopes", scopes.len(), MAX_SCOPES)?;
    for (scope, description) in scopes {
        validate_required_text("OAuth scope", scope, MAX_LABEL_BYTES)?;
        validate_required_text(
            "OAuth scope description",
            description,
            MAX_DESCRIPTION_BYTES,
        )?;
    }
    Ok(())
}

fn validate_security_requirements(
    requirements: &[HashMap<String, Vec<String>>],
) -> Result<(), A2aContractError> {
    validate_count(
        "security requirements",
        requirements.len(),
        MAX_SECURITY_REQUIREMENTS,
    )?;
    for requirement in requirements {
        validate_count(
            "security requirement schemes",
            requirement.len(),
            MAX_SECURITY_SCHEMES,
        )?;
        for (name, scopes) in requirement {
            validate_required_text("security scheme name", name, MAX_LABEL_BYTES)?;
            validate_string_list("security scopes", scopes, MAX_SCOPES, MAX_LABEL_BYTES)?;
        }
    }
    Ok(())
}

fn validate_security_requirement_references(
    requirements: &[HashMap<String, Vec<String>>],
    schemes: Option<&HashMap<String, wire::SecurityScheme>>,
) -> Result<(), A2aContractError> {
    for name in requirements.iter().flat_map(HashMap::keys) {
        if schemes.is_none_or(|schemes| !schemes.contains_key(name)) {
            return Err(A2aContractError::InvalidLifecycle(
                "security requirement references an unknown scheme",
            ));
        }
    }
    Ok(())
}

/// Detached JWS signature declared on an Agent Card.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aAgentCardSignature {
    inner: wire::AgentCardSignature,
}

impl A2aAgentCardSignature {
    /// Constructs a bounded JWS signature record.
    pub fn new(
        protected: impl Into<String>,
        signature: impl Into<String>,
        header: Option<HashMap<String, Value>>,
    ) -> Result<Self, A2aContractError> {
        let protected = protected.into();
        let signature = signature.into();
        validate_required_text("card protected header", &protected, MAX_PART_BYTES)?;
        validate_required_text("card signature", &signature, MAX_PART_BYTES)?;
        validate_metadata("card signature header", header.as_ref())?;
        Ok(Self {
            inner: wire::AgentCardSignature {
                protected,
                signature,
                header,
            },
        })
    }

    fn try_from_wire(value: wire::AgentCardSignature) -> Result<Self, A2aContractError> {
        Self::new(value.protected, value.signature, value.header)
    }

    fn into_wire(self) -> wire::AgentCardSignature {
        self.inner
    }
}

/// Validated public or extended A2A Agent Card.
#[derive(Clone, Debug, PartialEq)]
pub struct A2aAgentCard {
    inner: wire::AgentCard,
}

impl A2aAgentCard {
    /// Begins an Agent Card with all required scalar fields.
    pub fn builder(
        name: impl Into<String>,
        description: impl Into<String>,
        agent_version: impl Into<String>,
    ) -> Result<A2aAgentCardBuilder, A2aContractError> {
        let name = name.into();
        let description = description.into();
        let agent_version = agent_version.into();
        validate_required_text("agent name", &name, MAX_LABEL_BYTES)?;
        validate_required_text("agent description", &description, MAX_DESCRIPTION_BYTES)?;
        validate_required_text("agent version", &agent_version, MAX_LABEL_BYTES)?;
        Ok(A2aAgentCardBuilder {
            card: wire::AgentCard {
                name,
                description,
                version: agent_version,
                supported_interfaces: Vec::new(),
                capabilities: wire::AgentCapabilities::default(),
                default_input_modes: Vec::new(),
                default_output_modes: Vec::new(),
                skills: Vec::new(),
                provider: None,
                documentation_url: None,
                icon_url: None,
                security_schemes: None,
                security_requirements: None,
                signatures: None,
            },
        })
    }

    /// Returns the display name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns the serving agent's application version.
    #[must_use]
    pub fn agent_version(&self) -> &str {
        &self.inner.version
    }

    /// Returns capabilities advertised by this exact card snapshot.
    #[must_use]
    pub fn capabilities(&self) -> A2aAgentCapabilities {
        A2aAgentCapabilities::try_from_wire(self.inner.capabilities.clone())
            .expect("A2aAgentCard construction validates capabilities")
    }

    /// Returns StateKnot-supported A2A 1.0 interfaces in preference order.
    #[must_use]
    pub fn interfaces(&self) -> Vec<A2aAgentInterface> {
        self.inner
            .supported_interfaces
            .iter()
            .cloned()
            .filter_map(|interface| A2aAgentInterface::try_from_wire(interface).ok())
            .collect()
    }

    /// Returns skill descriptors in declaration order.
    #[must_use]
    pub fn skills(&self) -> Vec<A2aAgentSkill> {
        self.inner
            .skills
            .iter()
            .cloned()
            .map(A2aAgentSkill::try_from_wire)
            .collect::<Result<Vec<_>, _>>()
            .expect("A2aAgentCard construction validates skills")
    }

    /// Returns the card-wide accepted input modes in preference order.
    #[must_use]
    pub fn default_input_modes(&self) -> &[String] {
        &self.inner.default_input_modes
    }

    /// Returns the card-wide produced output modes in preference order.
    #[must_use]
    pub fn default_output_modes(&self) -> &[String] {
        &self.inner.default_output_modes
    }

    /// Returns the number of interfaces advertised, including unknown future
    /// bindings that are preserved for signature and digest verification.
    #[must_use]
    pub fn advertised_interface_count(&self) -> usize {
        self.inner.supported_interfaces.len()
    }

    pub(crate) fn wire(&self) -> &wire::AgentCard {
        &self.inner
    }

    /// Serializes the standard A2A 1.0 JSON representation.
    pub fn to_json(&self) -> Result<Value, A2aContractError> {
        serde_json::to_value(&self.inner).map_err(|_| A2aContractError::InvalidJson {
            field: "Agent Card",
        })
    }

    /// Parses and validates the standard A2A 1.0 JSON representation.
    pub fn from_json(value: Value) -> Result<Self, A2aContractError> {
        validate_json("Agent Card", &value, MAX_PART_BYTES)?;
        let inner = serde_json::from_value(value).map_err(|_| A2aContractError::InvalidJson {
            field: "Agent Card",
        })?;
        Self::try_from_wire(inner)
    }

    pub(crate) fn try_from_wire(inner: wire::AgentCard) -> Result<Self, A2aContractError> {
        validate_required_text("agent name", &inner.name, MAX_LABEL_BYTES)?;
        validate_required_text(
            "agent description",
            &inner.description,
            MAX_DESCRIPTION_BYTES,
        )?;
        validate_required_text("agent version", &inner.version, MAX_LABEL_BYTES)?;
        if inner.supported_interfaces.is_empty() {
            return Err(A2aContractError::Empty {
                field: "supported interfaces",
            });
        }
        validate_count(
            "supported interfaces",
            inner.supported_interfaces.len(),
            MAX_INTERFACES,
        )?;
        let mut interfaces = HashSet::with_capacity(inner.supported_interfaces.len());
        for interface in &inner.supported_interfaces {
            validate_wire_interface(interface)?;
            if !interfaces.insert((
                interface.url.as_str(),
                interface.protocol_binding.as_str(),
                interface.protocol_version.as_str(),
                interface.tenant.as_deref(),
            )) {
                return Err(A2aContractError::InvalidLifecycle(
                    "duplicate Agent Card interface",
                ));
            }
        }
        A2aAgentCapabilities::try_from_wire(inner.capabilities.clone())?;
        validate_modes("default input modes", &inner.default_input_modes)?;
        validate_modes("default output modes", &inner.default_output_modes)?;
        if inner.default_input_modes.is_empty() || inner.default_output_modes.is_empty() {
            return Err(A2aContractError::Empty {
                field: "default media modes",
            });
        }
        validate_count("agent skills", inner.skills.len(), MAX_SKILLS)?;
        let mut skill_ids = HashSet::with_capacity(inner.skills.len());
        for skill in inner.skills.iter().cloned() {
            let skill = A2aAgentSkill::try_from_wire(skill)?;
            if !skill_ids.insert(skill.id().to_owned()) {
                return Err(A2aContractError::InvalidLifecycle(
                    "duplicate Agent Skill identifier",
                ));
            }
        }
        if let Some(provider) = inner.provider.as_ref() {
            validate_required_text(
                "provider organization",
                &provider.organization,
                MAX_LABEL_BYTES,
            )?;
            validate_url("provider", &provider.url)?;
        }
        if let Some(url) = inner.documentation_url.as_deref() {
            validate_url("documentation", url)?;
        }
        if let Some(url) = inner.icon_url.as_deref() {
            validate_url("icon", url)?;
        }
        if let Some(schemes) = inner.security_schemes.as_ref() {
            validate_count("security schemes", schemes.len(), MAX_SECURITY_SCHEMES)?;
            for (name, scheme) in schemes {
                validate_required_text("security scheme name", name, MAX_LABEL_BYTES)?;
                validate_security_scheme(scheme)?;
            }
        }
        if let Some(requirements) = inner.security_requirements.as_ref() {
            validate_security_requirements(requirements)?;
            validate_security_requirement_references(
                requirements,
                inner.security_schemes.as_ref(),
            )?;
        }
        for skill in &inner.skills {
            if let Some(requirements) = skill.security_requirements.as_deref() {
                validate_security_requirement_references(
                    requirements,
                    inner.security_schemes.as_ref(),
                )?;
            }
        }
        if let Some(signatures) = inner.signatures.as_ref() {
            validate_count(
                "Agent Card signatures",
                signatures.len(),
                MAX_SECURITY_SCHEMES,
            )?;
            for signature in signatures.iter().cloned() {
                A2aAgentCardSignature::try_from_wire(signature)?;
            }
        }
        Ok(Self { inner })
    }

    pub(crate) fn into_wire(self) -> wire::AgentCard {
        self.inner
    }
}

/// Builder that requires at least one interface and one default input/output
/// mode before producing an Agent Card.
#[derive(Clone, Debug)]
pub struct A2aAgentCardBuilder {
    card: wire::AgentCard,
}

impl A2aAgentCardBuilder {
    /// Sets capability flags and extension declarations.
    #[must_use]
    pub fn capabilities(mut self, capabilities: A2aAgentCapabilities) -> Self {
        self.card.capabilities = capabilities.into_wire();
        self
    }

    /// Adds an interface at the end of preference order.
    pub fn interface(mut self, interface: A2aAgentInterface) -> Result<Self, A2aContractError> {
        if self.card.supported_interfaces.len() == MAX_INTERFACES {
            return Err(A2aContractError::TooMany {
                field: "supported interfaces",
                maximum: MAX_INTERFACES,
                actual: self.card.supported_interfaces.len() + 1,
            });
        }
        let wire = interface.into_wire();
        if self
            .card
            .supported_interfaces
            .iter()
            .any(|existing| existing.protocol_binding == wire.protocol_binding)
        {
            return Err(A2aContractError::InvalidLifecycle(
                "only one interface per StateKnot-supported binding is allowed",
            ));
        }
        self.card.supported_interfaces.push(wire);
        Ok(self)
    }

    /// Sets required default input media modes.
    pub fn default_input_modes(mut self, values: Vec<String>) -> Result<Self, A2aContractError> {
        validate_modes("default input modes", &values)?;
        if values.is_empty() {
            return Err(A2aContractError::Empty {
                field: "default input modes",
            });
        }
        self.card.default_input_modes = values;
        Ok(self)
    }

    /// Sets required default output media modes.
    pub fn default_output_modes(mut self, values: Vec<String>) -> Result<Self, A2aContractError> {
        validate_modes("default output modes", &values)?;
        if values.is_empty() {
            return Err(A2aContractError::Empty {
                field: "default output modes",
            });
        }
        self.card.default_output_modes = values;
        Ok(self)
    }

    /// Adds one discoverable skill.
    pub fn skill(mut self, skill: A2aAgentSkill) -> Result<Self, A2aContractError> {
        if self.card.skills.len() == MAX_SKILLS {
            return Err(A2aContractError::TooMany {
                field: "agent skills",
                maximum: MAX_SKILLS,
                actual: self.card.skills.len() + 1,
            });
        }
        if self
            .card
            .skills
            .iter()
            .any(|existing| existing.id == skill.inner.id)
        {
            return Err(A2aContractError::InvalidLifecycle(
                "duplicate Agent Skill identifier",
            ));
        }
        self.card.skills.push(skill.into_wire());
        Ok(self)
    }

    /// Adds provider attribution.
    pub fn provider(
        mut self,
        organization: impl Into<String>,
        url: impl Into<String>,
    ) -> Result<Self, A2aContractError> {
        let organization = organization.into();
        let url = url.into();
        validate_required_text("provider organization", &organization, MAX_LABEL_BYTES)?;
        validate_url("provider", &url)?;
        self.card.provider = Some(wire::AgentProvider { organization, url });
        Ok(self)
    }

    /// Adds a documentation URL.
    pub fn documentation_url(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_url("documentation", &value)?;
        self.card.documentation_url = Some(value);
        Ok(self)
    }

    /// Adds an icon URL.
    pub fn icon_url(mut self, value: impl Into<String>) -> Result<Self, A2aContractError> {
        let value = value.into();
        validate_url("icon", &value)?;
        self.card.icon_url = Some(value);
        Ok(self)
    }

    /// Adds a named security scheme.
    pub fn security_scheme(
        mut self,
        name: impl Into<String>,
        scheme: A2aSecurityScheme,
    ) -> Result<Self, A2aContractError> {
        let name = name.into();
        validate_required_text("security scheme name", &name, MAX_LABEL_BYTES)?;
        let schemes = self.card.security_schemes.get_or_insert_with(HashMap::new);
        if schemes.len() == MAX_SECURITY_SCHEMES {
            return Err(A2aContractError::TooMany {
                field: "security schemes",
                maximum: MAX_SECURITY_SCHEMES,
                actual: schemes.len() + 1,
            });
        }
        if schemes.insert(name, scheme.into_wire()).is_some() {
            return Err(A2aContractError::InvalidLifecycle(
                "duplicate security scheme name",
            ));
        }
        Ok(self)
    }

    /// Sets alternative security requirements. Each map is an AND group and
    /// the vector is evaluated as OR, following the A2A/OpenAPI model.
    pub fn security_requirements(
        mut self,
        requirements: Vec<HashMap<String, Vec<String>>>,
    ) -> Result<Self, A2aContractError> {
        validate_security_requirements(&requirements)?;
        self.card.security_requirements = Some(requirements);
        Ok(self)
    }

    /// Adds one detached card signature.
    pub fn signature(mut self, signature: A2aAgentCardSignature) -> Result<Self, A2aContractError> {
        let signatures = self.card.signatures.get_or_insert_with(Vec::new);
        if signatures.len() == MAX_SECURITY_SCHEMES {
            return Err(A2aContractError::TooMany {
                field: "Agent Card signatures",
                maximum: MAX_SECURITY_SCHEMES,
                actual: signatures.len() + 1,
            });
        }
        signatures.push(signature.into_wire());
        Ok(self)
    }

    /// Validates all cross-field invariants and builds the card.
    pub fn build(self) -> Result<A2aAgentCard, A2aContractError> {
        A2aAgentCard::try_from_wire(self.card)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card() -> A2aAgentCard {
        A2aAgentCard::builder("StateKnot test", "Bounded test agent", "0.0.0")
            .unwrap()
            .capabilities(A2aAgentCapabilities::new().streaming(true))
            .interface(
                A2aAgentInterface::new("https://agent.example/a2a/rest", A2aBinding::HttpJson)
                    .unwrap(),
            )
            .unwrap()
            .default_input_modes(vec!["text/plain".to_string()])
            .unwrap()
            .default_output_modes(vec!["application/json".to_string()])
            .unwrap()
            .skill(
                A2aAgentSkill::new(
                    "assess-supplier",
                    "Assess supplier",
                    "Returns a bounded supplier assessment.",
                    vec!["procurement".to_string()],
                )
                .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn card_round_trips_without_sdk_types() {
        let original = card();
        let decoded = A2aAgentCard::from_json(original.to_json().unwrap()).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(decoded.interfaces()[0].binding(), A2aBinding::HttpJson);
    }

    #[test]
    fn rejects_unspecified_roles_and_states() {
        let message = wire::Message {
            message_id: "m-1".to_string(),
            context_id: None,
            task_id: None,
            role: wire::Role::Unspecified,
            parts: vec![wire::Part::text("hello")],
            metadata: None,
            extensions: None,
            reference_task_ids: None,
        };
        assert!(A2aMessage::try_from_wire(message).is_err());
    }

    #[test]
    fn secrets_are_redacted() {
        let config = A2aPushConfig::new("https://hooks.example/a2a")
            .unwrap()
            .with_token(A2aSecret::new("top-secret").unwrap())
            .with_authentication(
                A2aPushAuthentication::new(
                    "Bearer",
                    Some(A2aSecret::new("super-private").unwrap()),
                )
                .unwrap(),
            );
        let formatted = format!("{config:?}");
        assert!(!formatted.contains("top-secret"));
        assert!(!formatted.contains("super-private"));
    }

    #[test]
    fn auth_required_needs_explanation() {
        assert!(A2aTaskStatus::new(A2aTaskState::AuthRequired, None, None).is_err());
    }

    #[test]
    fn blocks_wire_tenant_override() {
        let request = wire::GetTaskRequest {
            id: "task-1".to_string(),
            history_length: None,
            tenant: Some("other".to_string()),
        };
        assert!(A2aGetTaskRequest::try_from_wire(request).is_err());
    }

    #[test]
    fn task_pagination_enforces_a2a_protocol_bounds() {
        assert!(A2aListTasksRequest::new().with_page_size(100).is_ok());
        assert!(A2aListTasksRequest::new().with_page_size(101).is_err());

        let response = wire::ListTasksResponse {
            tasks: Vec::new(),
            next_page_token: String::new(),
            page_size: 101,
            total_size: 0,
        };
        assert!(A2aTaskPage::try_from_wire(response).is_err());
    }

    #[test]
    fn inert_future_interfaces_are_preserved_without_becoming_executable() {
        let mut value = card().to_json().unwrap();
        value["supportedInterfaces"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "url": "grpc.example.test:443",
                "protocolBinding": "GRPC",
                "protocolVersion": "1.0"
            }));

        let decoded = A2aAgentCard::from_json(value).unwrap();
        assert_eq!(decoded.advertised_interface_count(), 2);
        assert_eq!(decoded.interfaces().len(), 1);
    }

    #[test]
    fn duplicate_skill_ids_are_rejected_on_untrusted_cards() {
        let mut value = card().to_json().unwrap();
        let duplicate = value["skills"][0].clone();
        value["skills"].as_array_mut().unwrap().push(duplicate);
        assert!(A2aAgentCard::from_json(value).is_err());
    }

    #[test]
    fn card_and_skill_security_requirements_must_reference_declared_schemes() {
        let missing = HashMap::from([("missing".to_string(), Vec::new())]);

        let mut card_requirement = card().into_wire();
        card_requirement.security_requirements = Some(vec![missing.clone()]);
        assert!(A2aAgentCard::try_from_wire(card_requirement).is_err());

        let mut skill_requirement = card().into_wire();
        skill_requirement.skills[0].security_requirements = Some(vec![missing]);
        assert!(A2aAgentCard::try_from_wire(skill_requirement).is_err());
    }

    #[test]
    fn artifact_update_wire_flags_are_preserved_exactly() {
        let update = wire::TaskArtifactUpdateEvent {
            task_id: "task-1".to_string(),
            context_id: "context-1".to_string(),
            artifact: A2aArtifact::new("artifact-1", vec![A2aPart::text("first chunk").unwrap()])
                .unwrap()
                .into_wire(),
            append: Some(false),
            last_chunk: Some(false),
            metadata: None,
        };

        let decoded = A2aArtifactUpdate::try_from_wire(update).unwrap();
        assert!(!decoded.append());
        assert!(!decoded.last_chunk());
    }

    #[test]
    fn outbound_send_rejects_agent_roles_and_bound_push_tasks() {
        let agent_message = A2aMessage::new(
            "message-1",
            A2aMessageRole::Agent,
            vec![A2aPart::text("not a client message").unwrap()],
        )
        .unwrap();
        assert!(
            A2aSendMessageRequest::new(agent_message)
                .into_wire(None)
                .is_err()
        );

        let push = A2aPushConfig::new("https://hooks.example.test/a2a")
            .unwrap()
            .with_task_id("task-1")
            .unwrap();
        let configuration = A2aSendConfiguration::new().with_push_config(push);
        let user_message = A2aMessage::new(
            "message-2",
            A2aMessageRole::User,
            vec![A2aPart::text("valid role").unwrap()],
        )
        .unwrap();
        assert!(
            A2aSendMessageRequest::new(user_message)
                .with_configuration(configuration)
                .into_wire(None)
                .is_err()
        );
    }
}
