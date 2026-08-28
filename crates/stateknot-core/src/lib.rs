// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Provider-independent domain types shared across `StateKnot` runtime boundaries.
//!
//! This crate is intentionally independent of model providers, wire protocols,
//! databases, HTTP servers, and async executors. Its first implemented contract
//! is the validated identifier model from RFC-0001.

#![forbid(unsafe_code)]

mod accounting;
mod decimal;
mod digest;
mod ids;
mod schema;
mod time;
mod version;

pub use accounting::{
    ByteCount, CountParseError, CurrencyCode, CurrencyCodeError, Money, MoneyArithmeticError,
    TokenCount,
};
pub use digest::{Digest, DigestAlgorithm, DigestError};
pub use ids::{
    ArtifactId, AttemptId, EventId, GeneratedIdError, InterruptId, InvocationId, RunId, TenantId,
    TenantIdError, ThreadId,
};
pub use schema::{SchemaId, SchemaIdError, SchemaReference};
pub use time::{DurationMillis, DurationMillisError, Timestamp, TimestampError};
pub use version::{Version, VersionComponent, VersionError};
