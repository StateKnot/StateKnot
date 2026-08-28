// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Provider-independent domain types shared across `StateKnot` runtime boundaries.
//!
//! This crate is intentionally independent of model providers, wire protocols,
//! databases, HTTP servers, and async executors. Implemented contracts are
//! introduced only with strict validation, schemas, and versioned wire
//! fixtures from RFC-0001.

#![forbid(unsafe_code)]

mod accounting;
mod capability;
mod decimal;
mod digest;
mod ids;
mod schema;
mod scope;
mod time;
mod version;

pub use accounting::{
    ByteCount, CountParseError, CurrencyCode, CurrencyCodeError, Money, MoneyArithmeticError,
    TokenCount,
};
pub use capability::{CapabilityName, CapabilityNameError};
pub use digest::{Digest, DigestAlgorithm, DigestError};
pub use ids::{
    ArtifactId, AttemptId, EventId, GeneratedIdError, InterruptId, InvocationId, RunId, TenantId,
    TenantIdError, ThreadId,
};
pub use schema::{SchemaId, SchemaIdError, SchemaReference};
pub use scope::{Scope, ScopeError, ScopeSet, ScopeSetError};
pub use time::{DurationMillis, DurationMillisError, Timestamp, TimestampError};
pub use version::{Version, VersionComponent, VersionError};
