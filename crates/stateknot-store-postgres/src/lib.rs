// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL` 16/17 durability provider for `StateKnot`.
//!
//! The provider persists canonical journal bytes, serializes each run under a
//! row lock, checks worker fencing with the database clock, and commits journal
//! facts with their lifecycle projection in one transaction. It never holds a
//! database transaction across a model, tool, remote agent, or human wait.
//!
//! This pre-alpha slice assumes a trusted server-side pool. Do not distribute
//! its database credentials to untrusted workers; role-separated procedures and
//! the final worker/control-plane service boundary remain release blockers.

#![forbid(unsafe_code)]

mod config;
mod error;
mod model;
mod store;

pub use config::{PostgresStoreOptions, PostgresTransportSecurity};
pub use error::{ConfigurationError, StoreError};
pub use model::{
    AdmissionOutcome, AppendOutcome, CheckpointCommitOutcome, CheckpointLineagePage,
    CheckpointLineagePageSize, CheckpointPointer, JournalPage, JournalPageSize, LeaseClaimOutcome,
    LeaseReleaseOutcome, LeaseRenewalOutcome, RunProjection, StoredRun,
};
pub use store::PostgresStore;
