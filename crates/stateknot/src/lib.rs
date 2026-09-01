// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! # `StateKnot`
//!
//! Durable agent orchestration for Rust.
//!
//! The facade exposes the implementation-backed core contracts, executable
//! graph runtime, and `PostgreSQL` 16/17 durability provider under explicit
//! modules. The project remains pre-alpha: these modules are usable for the
//! current vertical validation slice, but they are not a stable compatibility
//! promise. See the
//! [project repository](https://github.com/StateKnot/StateKnot) for the current
//! status, architecture plan, and roadmap.

#![forbid(unsafe_code)]

/// Protocol-neutral domain, graph, journal, invocation, and lifecycle contracts.
pub use stateknot_core as core;
/// First-party model-provider and protocol adapters.
pub use stateknot_integrations as integrations;
/// Executable schema/reducer/node registries and the durable graph driver.
pub use stateknot_runtime as runtime;
/// `PostgreSQL` 16/17 durable storage and recovery provider.
pub use stateknot_store_postgres as postgres;
