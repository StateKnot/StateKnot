// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Executable, offline-frozen runtime bindings for `StateKnot` graphs.
//!
//! Declarative graph descriptors remain in `stateknot-core`. This crate binds
//! those immutable descriptors to locally installed JSON Schemas, reducers,
//! and node executors, then drives them against a durable provider without
//! loading code or schemas from the network while a run is active.

#![forbid(unsafe_code)]

mod driver;
mod driver_schema;
mod registry;
mod schema;

pub use driver_schema::{
    STANDARD_GRAPH_DRIVER_EVENT_SCHEMA_ID, StandardGraphDriverSchemaError,
    StandardGraphDriverSchemaRegistrationError, register_standard_graph_driver_event_schema,
    standard_graph_driver_event_schema,
};

pub use driver::{
    DurableGraphDriver, DurableGraphDriverBuildError, DurableGraphDriverOptions,
    DurableGraphDriverOptionsError, GraphBlockedHandoff, GraphDriveBlockers, GraphDriveOutcome,
    GraphDriveReport, GraphDriveResult, GraphDriverError, GraphLifecycleBarrierHandoff,
};
pub use registry::{
    ExecutableGraph, ExecutableGraphRegistry, ExecutableGraphRegistryBuilder,
    ExecutableGraphRegistryError, GraphNodeContext, GraphNodeContextError, GraphNodeExecution,
    GraphNodeExecutionError, GraphNodeExecutionErrorBuildError, GraphNodeExecutor,
};
pub use schema::{
    JsonSchemaRegistry, JsonSchemaRegistryBuilder, JsonSchemaRegistryError,
    JsonSchemaRegistryLimits, JsonSchemaRegistryLimitsError,
};
