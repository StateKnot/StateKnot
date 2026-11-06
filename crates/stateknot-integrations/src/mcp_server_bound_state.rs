// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! One host identity and state-consumption policy shared by owned MCP surfaces.

use std::{fmt, num::NonZeroU64, sync::Arc, time::Duration};

use rmcp::{ErrorData, RoleServer, service::RequestContext};
use serde_json::{Map, Value};
use stateknot_core::{Digest, PrincipalIdentity, TenantId};
use thiserror::Error;

use crate::mcp_server_tool::append_digest_part;
use crate::{McpServerPrincipal, McpServerRequestStateCodec, mcp_server_principal};

/// Trusted host inputs identifying one authenticated-to-local identity mapping.
///
/// Construction validates syntax, not authentication or mapping provenance.
/// The host must check its current mapping at authorization after queueing, and
/// never reuse a generation, including revoke/reactivate and A-to-B-to-A changes.
/// Replicas accepting the same state must share the namespace and mapping.
/// This value is not a serialized credential or durable authorization grant.
///
/// ```compile_fail,E0277
/// fn durable_record<T: serde::Serialize>() {}
/// durable_record::<stateknot_integrations::McpServerHostBinding>();
/// ```
#[derive(Clone, Eq, PartialEq)]
pub struct McpServerHostBinding {
    namespace: Box<str>,
    authenticated_identity: PrincipalIdentity,
    tenant_id: TenantId,
    effective_principal: PrincipalIdentity,
    mapping_generation: NonZeroU64,
}

impl McpServerHostBinding {
    /// Maximum namespace length in bytes.
    pub const MAX_NAMESPACE_BYTES: usize = 128;

    /// Constructs a binding from already verified host mapping inputs.
    ///
    /// Namespace accepts ASCII alphanumerics and `._:-`, except `.` and `..`.
    /// Generation overflow must be refused by the host, never wrapped or reused.
    pub fn new(
        namespace: impl Into<String>,
        authenticated_identity: PrincipalIdentity,
        tenant_id: TenantId,
        effective_principal: PrincipalIdentity,
        mapping_generation: NonZeroU64,
    ) -> Result<Self, McpServerHostBindingError> {
        let namespace = namespace.into();
        if namespace.is_empty()
            || namespace.len() > Self::MAX_NAMESPACE_BYTES
            || matches!(namespace.as_str(), "." | "..")
            || !namespace.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            })
        {
            return Err(McpServerHostBindingError::InvalidNamespace);
        }
        Ok(Self {
            namespace: namespace.into_boxed_str(),
            authenticated_identity,
            tenant_id,
            effective_principal,
            mapping_generation,
        })
    }

    /// Returns the exact stable deployment namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the verified external identity retained by the host.
    #[must_use]
    pub const fn authenticated_identity(&self) -> &PrincipalIdentity {
        &self.authenticated_identity
    }

    /// Returns the mapped local tenant.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the mapped effective local principal.
    #[must_use]
    pub const fn effective_principal(&self) -> &PrincipalIdentity {
        &self.effective_principal
    }

    /// Returns the host-owned, non-reused mapping generation.
    #[must_use]
    pub const fn mapping_generation(&self) -> NonZeroU64 {
        self.mapping_generation
    }

    pub(crate) fn host_key(&self) -> [u8; 32] {
        let mut material = b"stateknot/mcp-server/host-binding/v1".to_vec();
        let generation = self.mapping_generation.get().to_be_bytes();
        for part in [
            self.namespace().as_bytes(),
            self.authenticated_identity.issuer().as_str().as_bytes(),
            self.authenticated_identity.subject().as_str().as_bytes(),
            self.tenant_id.as_str().as_bytes(),
            self.effective_principal.issuer().as_str().as_bytes(),
            self.effective_principal.subject().as_str().as_bytes(),
            &generation,
        ] {
            append_digest_part(&mut material, part);
        }
        sha256(material)
    }
}

impl fmt::Debug for McpServerHostBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpServerHostBinding([REDACTED])")
    }
}

/// Invalid host mapping syntax, without reflecting identity values.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerHostBindingError {
    /// Namespace is empty, oversized, path-like, or outside its ASCII grammar.
    #[error("invalid MCP host binding namespace")]
    InvalidNamespace,
}

/// Explicit immutable profile enforcing identity-bound cursors and MRTR state.
///
/// Attach this profile to the composite builder or each direct service. It
/// requires Private catalog caching; independently selected Public Resource
/// contents retain their existing application-owned cache contract. The codec
/// authenticates state without encryption or single-use redemption. The host's
/// existing admission and decoded policies still own current mapping checks.
///
/// ```compile_fail,E0277
/// fn durable_record<T: serde::Serialize>() {}
/// durable_record::<stateknot_integrations::McpServerBoundStateProfile>();
/// ```
#[derive(Clone)]
pub struct McpServerBoundStateProfile {
    codec: McpServerRequestStateCodec,
    ttl: Duration,
}

impl McpServerBoundStateProfile {
    /// Maximum positive state lifetime, matching the existing codec ceiling.
    pub const MAX_TTL: Duration = Duration::from_secs(24 * 60 * 60);
    /// Maximum complete JSON envelope, including escaping and property names.
    pub const MAX_ENVELOPE_PAYLOAD_BYTES: usize = 64 * 1024;
    /// Maximum complete integrity-protected wire token.
    pub const MAX_WIRE_STATE_BYTES: usize = 128 * 1024;

    /// Creates an explicit profile without clamping or a default lifetime.
    pub fn new(
        codec: McpServerRequestStateCodec,
        ttl: Duration,
    ) -> Result<Self, McpServerBoundStateProfileError> {
        if ttl.is_zero() || ttl > Self::MAX_TTL {
            return Err(McpServerBoundStateProfileError::InvalidTtl);
        }
        Ok(Self { codec, ttl })
    }

    /// Returns the configured state lifetime.
    #[must_use]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub(crate) fn open_inbound(
        &self,
        wire_state: Option<&str>,
        input_responses_was_present: bool,
        associated_data: &[u8],
    ) -> Result<Option<Arc<str>>, ErrorData> {
        let Some(wire_state) = wire_state else {
            return if input_responses_was_present {
                Err(invalid_state())
            } else {
                Ok(None)
            };
        };
        let value = self
            .codec
            .open_json(wire_state, associated_data)
            .map_err(|_| invalid_state())?;
        let object = value.as_object().ok_or_else(invalid_state)?;
        if object.len() != 2 || object.get("version").and_then(Value::as_u64) != Some(1) {
            return Err(invalid_state());
        }
        match object.get("applicationState") {
            Some(Value::Null) => Ok(None),
            Some(Value::String(state))
                if !state.is_empty() && state.len() <= Self::MAX_WIRE_STATE_BYTES =>
            {
                Ok(Some(Arc::from(state.as_str())))
            }
            _ => Err(invalid_state()),
        }
    }

    pub(crate) fn seal_outbound(
        &self,
        inner_application_state: Option<&str>,
        associated_data: &[u8],
    ) -> Result<String, ErrorData> {
        if inner_application_state
            .is_some_and(|state| state.is_empty() || state.len() > Self::MAX_WIRE_STATE_BYTES)
        {
            return Err(outbound_state_error());
        }
        let value = serde_json::json!({"version": 1, "applicationState": inner_application_state});
        self.codec
            .seal_json(&value, associated_data, self.ttl)
            .map_err(|_| outbound_state_error())
    }
}

impl fmt::Debug for McpServerBoundStateProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerBoundStateProfile")
            .field("ttl", &self.ttl)
            .field("codec", &"[REDACTED]")
            .finish()
    }
}

/// Invalid bound-state profile configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerBoundStateProfileError {
    /// TTL was zero or exceeded the unchanged codec ceiling.
    #[error("invalid MCP bound-state TTL")]
    InvalidTtl,
}

pub(crate) struct BoundStateFingerprint([u8; 32]);

impl BoundStateFingerprint {
    pub(crate) fn hex(&self) -> String {
        hex(&self.0)
    }
}

pub(crate) fn hex(bytes: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(64);
    for byte in bytes {
        result.push(char::from(DIGITS[usize::from(byte >> 4)]));
        result.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    result
}

pub(crate) fn principal_fingerprint(
    host_key: &[u8; 32],
    scopes: impl ExactSizeIterator<Item = impl AsRef<str>>,
) -> [u8; 32] {
    let mut material = b"stateknot/mcp-server/bound-state/principal/v1".to_vec();
    append_digest_part(&mut material, host_key);
    material.extend_from_slice(&(scopes.len() as u64).to_be_bytes());
    for scope in scopes {
        append_digest_part(&mut material, scope.as_ref().as_bytes());
    }
    sha256(material)
}

pub(crate) fn require_bound_principal(
    principal: &McpServerPrincipal,
) -> Result<BoundStateFingerprint, ErrorData> {
    match (
        principal.host_binding(),
        principal.bound_state_fingerprint(),
    ) {
        (Some(_), Some(fingerprint)) => Ok(BoundStateFingerprint(fingerprint)),
        _ => Err(ErrorData::internal_error(
            "MCP host binding is unavailable",
            None,
        )),
    }
}

pub(crate) fn check_context(
    profile: Option<&McpServerBoundStateProfile>,
    context: &RequestContext<RoleServer>,
) -> Result<(), ErrorData> {
    if profile.is_some() {
        let principal = mcp_server_principal(context)
            .ok_or_else(|| ErrorData::internal_error("MCP host binding is unavailable", None))?;
        require_bound_principal(principal)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) enum BoundOperation<'a> {
    Tool {
        name: &'a str,
        arguments: &'a Map<String, Value>,
    },
    Resource {
        uri: &'a str,
    },
    Prompt {
        name: &'a str,
        arguments: &'a Map<String, Value>,
    },
}

pub(crate) fn operation_binding(
    fingerprint: &BoundStateFingerprint,
    operation: BoundOperation<'_>,
) -> Result<[u8; 32], ErrorData> {
    let (domain, name, arguments) = match operation {
        BoundOperation::Tool { name, arguments } => ("tool", name, Some(arguments)),
        BoundOperation::Resource { uri } => ("resource", uri, None),
        BoundOperation::Prompt { name, arguments } => ("prompt", name, Some(arguments)),
    };
    let mut material = format!("stateknot/mcp-server/request-state/{domain}/v3").into_bytes();
    append_digest_part(&mut material, &fingerprint.0);
    append_digest_part(&mut material, name.as_bytes());
    if let Some(arguments) = arguments {
        // Bind the parsed values the handler receives. JCS rounds wide integers
        // through f64 and would also merge integer/float and signed-zero values.
        let mut sorted = Value::Object(arguments.clone());
        sorted.sort_all_objects();
        let canonical = serde_json::to_vec(&sorted)
            .map_err(|_| ErrorData::internal_error("Failed to bind MCP request state", None))?;
        append_digest_part(&mut material, &canonical);
    }
    Ok(sha256(material))
}

fn invalid_state() -> ErrorData {
    ErrorData::invalid_params("Invalid MCP request state", None)
}

fn outbound_state_error() -> ErrorData {
    ErrorData::internal_error("Failed to bind MCP request state", None)
}

fn sha256(material: impl AsRef<[u8]>) -> [u8; 32] {
    Digest::sha256(material)
        .as_bytes()
        .try_into()
        .expect("SHA-256 digest contains exactly 32 bytes")
}

pub(crate) fn validate_wire_state(wire_state: Option<&str>) -> Result<(), ErrorData> {
    if wire_state.is_some_and(|state| {
        state.is_empty()
            || state.len() > McpServerBoundStateProfile::MAX_WIRE_STATE_BYTES
            || state.chars().any(char::is_control)
    }) {
        return Err(invalid_state());
    }
    Ok(())
}

// Preserve the pinned SDK's original empty/default responses for operations a
// direct service does not mount, after applying the explicit profile guard.
// This is a private concrete default, not a configurable application seam.
pub(crate) struct UnmountedMcpSurface;

impl rmcp::ServerHandler for UnmountedMcpSurface {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn identity(subject: &str) -> PrincipalIdentity {
        PrincipalIdentity::new(
            "https://issuer.example.com".parse().unwrap(),
            subject.parse().unwrap(),
        )
    }

    fn binding(tenant: &str, effective: &str, generation: u64) -> McpServerHostBinding {
        McpServerHostBinding::new(
            "test-deployment",
            identity("external-subject"),
            tenant.parse().unwrap(),
            identity(effective),
            NonZeroU64::new(generation).unwrap(),
        )
        .unwrap()
    }

    fn principal(tenant: &str, effective: &str, generation: u64) -> McpServerPrincipal {
        McpServerPrincipal::from_host_binding(
            binding(tenant, effective, generation),
            ["read", "write"],
        )
        .unwrap()
    }

    fn codec() -> McpServerRequestStateCodec {
        McpServerRequestStateCodec::new(vec![73; 32]).unwrap()
    }

    fn profile() -> McpServerBoundStateProfile {
        McpServerBoundStateProfile::new(codec(), Duration::from_secs(30)).unwrap()
    }

    fn tool_binding(principal: &McpServerPrincipal, name: &str) -> [u8; 32] {
        operation_binding(
            &require_bound_principal(principal).unwrap(),
            BoundOperation::Tool {
                name,
                arguments: &Map::new(),
            },
        )
        .unwrap()
    }

    #[test]
    fn namespace_validation_and_generation_are_explicit() {
        for namespace in [
            "",
            ".",
            "..",
            " leading",
            "trailing ",
            "a/b",
            "a\\b",
            "a\n",
            "é",
        ] {
            assert_eq!(
                McpServerHostBinding::new(
                    namespace,
                    identity("external"),
                    "tenant".parse().unwrap(),
                    identity("local"),
                    NonZeroU64::new(1).unwrap(),
                ),
                Err(McpServerHostBindingError::InvalidNamespace)
            );
        }
        for namespace in ["a", "deployment._:-1", &"a".repeat(128)] {
            assert!(
                McpServerHostBinding::new(
                    namespace,
                    identity("external"),
                    "tenant".parse().unwrap(),
                    identity("local"),
                    NonZeroU64::new(u64::MAX).unwrap(),
                )
                .is_ok()
            );
        }
        assert!(
            McpServerHostBinding::new(
                "a".repeat(129),
                identity("external"),
                "tenant".parse().unwrap(),
                identity("local"),
                NonZeroU64::new(1).unwrap(),
            )
            .is_err()
        );
        assert_eq!(
            binding("tenant", "local", u64::MAX)
                .mapping_generation()
                .get(),
            u64::MAX
        );
    }

    #[test]
    fn canonical_scopes_and_every_mapping_component_change_state_identity() {
        let initial = principal("tenant-a", "local-a", 1);
        let initial_fp = require_bound_principal(&initial).unwrap().hex();
        let reordered = McpServerPrincipal::from_host_binding(
            binding("tenant-a", "local-a", 1),
            ["write", "read"],
        )
        .unwrap();
        assert_eq!(initial, reordered);
        for changed in [
            principal("tenant-b", "local-a", 1),
            principal("tenant-a", "local-b", 1),
            principal("tenant-a", "local-a", 2),
            principal("tenant-a", "local-a", 3),
            McpServerPrincipal::from_host_binding(binding("tenant-a", "local-a", 1), ["read"])
                .unwrap(),
        ] {
            assert_ne!(initial_fp, require_bound_principal(&changed).unwrap().hex());
        }
        let old = binding("tenant-a", "local-a", 1);
        for changed in [
            McpServerHostBinding::new(
                "other",
                old.authenticated_identity().clone(),
                old.tenant_id().clone(),
                old.effective_principal().clone(),
                old.mapping_generation(),
            )
            .unwrap(),
            McpServerHostBinding::new(
                old.namespace(),
                identity("other"),
                old.tenant_id().clone(),
                old.effective_principal().clone(),
                old.mapping_generation(),
            )
            .unwrap(),
            McpServerHostBinding::new(
                old.namespace(),
                PrincipalIdentity::new(
                    "https://other.example.com".parse().unwrap(),
                    "external-subject".parse().unwrap(),
                ),
                old.tenant_id().clone(),
                old.effective_principal().clone(),
                old.mapping_generation(),
            )
            .unwrap(),
            McpServerHostBinding::new(
                old.namespace(),
                old.authenticated_identity().clone(),
                old.tenant_id().clone(),
                PrincipalIdentity::new(
                    "https://other.example.com".parse().unwrap(),
                    "local-a".parse().unwrap(),
                ),
                old.mapping_generation(),
            )
            .unwrap(),
        ] {
            let changed =
                McpServerPrincipal::from_host_binding(changed, ["read", "write"]).unwrap();
            assert_ne!(initial.subject(), changed.subject());
            assert_ne!(initial_fp, require_bound_principal(&changed).unwrap().hex());
        }
    }

    #[test]
    fn legacy_subject_cannot_forge_a_host_binding_and_debug_redacts_new_identity() {
        let bound = principal("tenant-secret", "local-secret", 17);
        let legacy = McpServerPrincipal::new(bound.subject(), ["read", "write"]).unwrap();
        assert!(legacy.host_binding().is_none());
        assert!(require_bound_principal(&legacy).is_err());
        let debug = format!(
            "{bound:?} {:?} {:?}",
            bound.host_binding().unwrap(),
            profile()
        );
        for secret in [
            "test-deployment",
            "tenant-secret",
            "local-secret",
            "external-subject",
            "issuer.example.com",
            "write",
            bound.subject(),
        ] {
            assert!(!debug.contains(secret));
        }
        assert_eq!(
            format!("{bound:?}"),
            "McpServerPrincipal { host_bound: true, scope_count: 2, .. }"
        );
        assert_eq!(
            format!("{:?}", bound.host_binding().unwrap()),
            "McpServerHostBinding([REDACTED])"
        );
    }

    #[test]
    fn ttl_has_original_positive_twenty_four_hour_ceiling() {
        assert!(McpServerBoundStateProfile::new(codec(), Duration::ZERO).is_err());
        assert!(
            McpServerBoundStateProfile::new(
                codec(),
                McpServerBoundStateProfile::MAX_TTL + Duration::from_nanos(1)
            )
            .is_err()
        );
        let maximum =
            McpServerBoundStateProfile::new(codec(), McpServerBoundStateProfile::MAX_TTL).unwrap();
        assert_eq!(maximum.ttl(), Duration::from_secs(86_400));
    }

    #[test]
    fn null_inner_state_is_valid_but_present_responses_require_outer_state() {
        let profile = profile();
        let ad = tool_binding(&principal("tenant", "local", 1), "tool");
        assert!(profile.open_inbound(None, false, &ad).unwrap().is_none());
        assert!(profile.open_inbound(None, true, &ad).is_err());
        let sealed = profile.seal_outbound(None, &ad).unwrap();
        assert!(
            profile
                .open_inbound(Some(&sealed), true, &ad)
                .unwrap()
                .is_none()
        );
        assert!(
            profile
                .open_inbound(Some(&sealed), false, &ad)
                .unwrap()
                .is_none()
        );
        let inner = "opaque application \\\" state\n";
        let sealed = profile.seal_outbound(Some(inner), &ad).unwrap();
        assert_eq!(
            profile
                .open_inbound(Some(&sealed), true, &ad)
                .unwrap()
                .as_deref(),
            Some(inner)
        );
    }

    #[test]
    fn strict_envelope_requires_both_keys_and_integer_version() {
        let profile = profile();
        let ad = tool_binding(&principal("tenant", "local", 1), "tool");
        for value in [
            json!({"version":1}),
            json!({"applicationState":null}),
            json!({"version":1,"applicationState":null,"extra":true}),
            json!({"version":2,"applicationState":null}),
            json!({"version":1.0,"applicationState":null}),
            json!({"version":"1","applicationState":null}),
            json!({"version":1,"applicationState":""}),
            json!({"version":1,"applicationState":false}),
            json!({"version":1,"applicationState":{}}),
            json!(null),
            json!([]),
            json!("legacy"),
        ] {
            let sealed = codec()
                .seal_json(&value, &ad, Duration::from_secs(30))
                .unwrap();
            let error = profile.open_inbound(Some(&sealed), true, &ad).unwrap_err();
            assert_eq!(error.message.as_ref(), "Invalid MCP request state");
        }
        for value in [
            json!({"version":1,"applicationState":null}),
            json!({"version":1,"applicationState":"valid"}),
        ] {
            let sealed = codec()
                .seal_json(&value, &ad, Duration::from_secs(30))
                .unwrap();
            assert!(profile.open_inbound(Some(&sealed), true, &ad).is_ok());
        }
    }

    #[test]
    fn envelope_counts_escaping_and_all_properties_at_original_payload_ceiling() {
        let profile = profile();
        let ad = tool_binding(&principal("tenant", "local", 1), "tool");
        let overhead = serde_json::to_vec(&json!({"version":1,"applicationState":""}))
            .unwrap()
            .len();
        let state = "a".repeat(McpServerBoundStateProfile::MAX_ENVELOPE_PAYLOAD_BYTES - overhead);
        let token = profile.seal_outbound(Some(&state), &ad).unwrap();
        assert_eq!(
            profile
                .open_inbound(Some(&token), true, &ad)
                .unwrap()
                .as_deref(),
            Some(state.as_str())
        );
        assert!(profile.seal_outbound(Some(&(state + "a")), &ad).is_err());
        assert!(
            profile
                .seal_outbound(Some(&"\"".repeat(33 * 1024)), &ad)
                .is_err()
        );
        assert!(
            profile
                .seal_outbound(Some(&"a".repeat(128 * 1024)), &ad)
                .is_err()
        );
        assert!(
            profile
                .open_inbound(Some(&"a".repeat(128 * 1024 + 1)), true, &ad)
                .is_err()
        );
    }

    #[test]
    fn binding_changes_and_operation_arguments_cannot_redeem_old_state() {
        let profile = profile();
        let first = principal("tenant-a", "local", 1);
        let ad = tool_binding(&first, "tool");
        let sealed = profile.seal_outbound(Some("state"), &ad).unwrap();
        for changed in [
            principal("tenant-b", "local", 1),
            principal("tenant-a", "other", 1),
            principal("tenant-a", "local", 2),
        ] {
            assert!(
                profile
                    .open_inbound(Some(&sealed), true, &tool_binding(&changed, "tool"))
                    .is_err()
            );
        }
        assert!(
            profile
                .open_inbound(Some(&sealed), true, &tool_binding(&first, "other"))
                .is_err()
        );
        let fingerprint = require_bound_principal(&first).unwrap();
        let changed_args = serde_json::from_value(json!({"changed":true})).unwrap();
        for other in [
            BoundOperation::Tool {
                name: "tool",
                arguments: &changed_args,
            },
            BoundOperation::Resource { uri: "tool" },
            BoundOperation::Prompt {
                name: "tool",
                arguments: &Map::new(),
            },
        ] {
            let other = operation_binding(&fingerprint, other).unwrap();
            assert!(profile.open_inbound(Some(&sealed), true, &other).is_err());
        }
        assert!(
            profile
                .open_inbound(Some("old-opaque-application-state"), true, &ad)
                .is_err()
        );
        let other_key = McpServerBoundStateProfile::new(
            McpServerRequestStateCodec::new(vec![91; 32]).unwrap(),
            Duration::from_secs(30),
        )
        .unwrap();
        assert!(other_key.open_inbound(Some(&sealed), true, &ad).is_err());
    }

    #[test]
    fn argument_binding_preserves_parsed_numbers_and_ignores_object_key_order() {
        let principal = principal("tenant", "local", 1);
        let fingerprint = require_bound_principal(&principal).unwrap();
        let profile = profile();
        // Prompt's public schema currently accepts strings only. Exercising its
        // shared private binding here does not extend that public contract.
        for prompt in [false, true] {
            let bind = |arguments: &Map<String, Value>| {
                operation_binding(
                    &fingerprint,
                    if prompt {
                        BoundOperation::Prompt {
                            name: "item",
                            arguments,
                        }
                    } else {
                        BoundOperation::Tool {
                            name: "item",
                            arguments,
                        }
                    },
                )
                .unwrap()
            };
            for (first, second) in [
                (r#"{"n":9007199254740992}"#, r#"{"n":9007199254740993}"#),
                (
                    r#"{"n":18446744073709551614}"#,
                    r#"{"n":18446744073709551615}"#,
                ),
                (r#"{"n":-9007199254740992}"#, r#"{"n":-9007199254740993}"#),
                (
                    r#"{"n":-9223372036854775808}"#,
                    r#"{"n":-9223372036854775807}"#,
                ),
                (r#"{"n":1}"#, r#"{"n":1.0}"#),
                (r#"{"n":0.0}"#, r#"{"n":-0.0}"#),
                (
                    r#"{"items":[{"n":9007199254740992}]}"#,
                    r#"{"items":[{"n":9007199254740993}]}"#,
                ),
            ] {
                let first: Map<String, Value> = serde_json::from_str(first).unwrap();
                let second: Map<String, Value> = serde_json::from_str(second).unwrap();
                assert_ne!(
                    serde_json::to_vec(&first).unwrap(),
                    serde_json::to_vec(&second).unwrap()
                );
                let first_ad = bind(&first);
                let second_ad = bind(&second);
                assert_ne!(first_ad, second_ad);
                let token = profile
                    .seal_outbound(Some("numeric-state"), &first_ad)
                    .unwrap();
                assert_eq!(
                    profile
                        .open_inbound(Some(&token), true, &first_ad)
                        .unwrap()
                        .as_deref(),
                    Some("numeric-state")
                );
                assert_eq!(
                    profile
                        .open_inbound(Some(&token), true, &second_ad)
                        .unwrap_err()
                        .message
                        .as_ref(),
                    "Invalid MCP request state"
                );
            }
            let ordered: Map<String, Value> = serde_json::from_str(
                r#"{"a":{"x":1,"y":9007199254740993},"b":[{"c":-0.0,"d":18446744073709551615}]}"#,
            )
            .unwrap();
            let reordered: Map<String, Value> = serde_json::from_str(
                r#"{"b":[{"d":18446744073709551615,"c":-0.0}],"a":{"y":9007199254740993,"x":1}}"#,
            )
            .unwrap();
            let ad = bind(&ordered);
            assert_eq!(ad, bind(&reordered));
            let token = profile.seal_outbound(Some("ordered-state"), &ad).unwrap();
            assert_eq!(
                profile
                    .open_inbound(Some(&token), true, &bind(&reordered))
                    .unwrap()
                    .as_deref(),
                Some("ordered-state")
            );
        }
    }

    #[test]
    fn every_strict_surface_refuses_valid_same_key_v2_state() {
        let principal = principal("tenant", "local", 1);
        let fingerprint = require_bound_principal(&principal).unwrap();
        let profile = profile();
        let arguments = Map::new();
        for (surface, operation) in [
            (
                "tool",
                BoundOperation::Tool {
                    name: "item",
                    arguments: &arguments,
                },
            ),
            ("resource", BoundOperation::Resource { uri: "item" }),
            (
                "prompt",
                BoundOperation::Prompt {
                    name: "item",
                    arguments: &arguments,
                },
            ),
        ] {
            // Independently reconstruct the old AD, with the same host, key,
            // operation and valid envelope. A domain mismatch must reject it.
            let mut old_material =
                format!("stateknot/mcp-server/request-state/{surface}/v2").into_bytes();
            append_digest_part(&mut old_material, &fingerprint.0);
            append_digest_part(&mut old_material, b"item");
            if surface != "resource" {
                append_digest_part(
                    &mut old_material,
                    &serde_json_canonicalizer::to_vec(&arguments).unwrap(),
                );
            }
            let old_ad = sha256(old_material);
            let new_ad = operation_binding(&fingerprint, operation).unwrap();
            assert_ne!(old_ad, new_ad);
            let token = profile.seal_outbound(Some("v2-state"), &old_ad).unwrap();
            assert_eq!(
                profile
                    .open_inbound(Some(&token), true, &old_ad)
                    .unwrap()
                    .as_deref(),
                Some("v2-state")
            );
            assert_eq!(
                profile
                    .open_inbound(Some(&token), true, &new_ad)
                    .unwrap_err()
                    .message
                    .as_ref(),
                "Invalid MCP request state"
            );
        }
    }
}
