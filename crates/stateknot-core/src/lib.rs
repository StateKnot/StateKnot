// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Provider-independent domain types shared across `StateKnot` runtime boundaries.
//!
//! This crate is intentionally independent of model providers, wire protocols,
//! databases, HTTP servers, and async executors. Its first implemented contract
//! is the validated identifier model from RFC-0001.

#![forbid(unsafe_code)]

mod ids;

pub use ids::{
    ArtifactId, AttemptId, EventId, GeneratedIdError, InterruptId, InvocationId, RunId, TenantId,
    TenantIdError, ThreadId,
};
