// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stateknot_core::{AgentRequest, AgentSubmissionKey, CapabilityIdentity, EventId};
use stateknot_runtime::AgentRunSnapshot;
use std::fmt;

/// Closed submission envelope. Tenant, caller, Run IDs and initial state are host-owned.
/// Persist the key and complete logical request before the first HTTP attempt.
#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHttpSubmission {
    /// Opaque tenant-scoped idempotency key, not an authentication credential.
    pub submission_key: AgentSubmissionKey,
    /// Exact installed Agent capability identity and version.
    pub agent: CapabilityIdentity,
    /// Input schema, bounded input and requested (only narrowing) budget limits.
    pub request: AgentRequest,
}

impl fmt::Debug for AgentHttpSubmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentHttpSubmission")
            .field("agent", &self.agent)
            .finish_non_exhaustive()
    }
}

/// Closed body-based lookup; opaque submission keys never enter a URL.
#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHttpLookup {
    /// Previously retained submission identity.
    pub submission_key: AgentSubmissionKey,
}

impl fmt::Debug for AgentHttpLookup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AgentHttpLookup { .. }")
    }
}

/// Public v1 response; a snapshot is an observation, not an execution acknowledgement.
/// A later replay can contain a newer lifecycle revision. Terminal data appears
/// only when the durable runtime has confirmed it.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHttpRunResponse {
    /// Fresh correlation identity; not the durable submission/cancellation key.
    pub request_id: EventId,
    /// Existing integrity-verified public snapshot, never raw journal/DB state.
    pub snapshot: AgentRunSnapshot,
}
