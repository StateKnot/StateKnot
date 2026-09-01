// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Executable, offline-frozen runtime bindings for `StateKnot` graphs.
//!
//! Declarative graph descriptors remain in `stateknot-core`. This crate binds
//! those immutable descriptors to locally installed JSON Schemas, reducers,
//! and node executors, then drives them against a durable provider without
//! loading code or schemas from the network while a run is active.
//! [`DurableAgentAdmission`] validates one authenticated immutable Agent intent
//! against that same deployment snapshot before atomically initializing its
//! executable `PostgreSQL` run.

#![forbid(unsafe_code)]

mod admission_schema;
mod agent_loop;
mod agent_typed;
mod driver;
mod driver_schema;
mod durable_admission;
mod fair_scheduler;
mod invocation_executor;
mod invocation_schema;
mod lifecycle;
mod lifecycle_schema;
mod provider_registry;
mod registry;
mod schema;
mod tenant_scheduler;

pub use admission_schema::{
    STANDARD_AGENT_ADMISSION_EVENT_SCHEMA_ID, StandardAgentAdmissionSchemaError,
    StandardAgentAdmissionSchemaRegistrationError, register_standard_agent_admission_event_schema,
    standard_agent_admission_event_schema,
};
pub use agent_loop::{
    AgentLoopError, AgentLoopOutcome, AgentLoopResult, DurableAgentLoop, DurableAgentLoopBuildError,
};
pub use agent_typed::{
    AgentBuilder, AgentBuilderError, AgentSchemaRegistrationError, AgentSchemaRole, TypedAgent,
    TypedAgentBindError, TypedAgentDefinition, TypedAgentInputError, TypedAgentOutputError,
};
pub use driver_schema::{
    STANDARD_GRAPH_DRIVER_EVENT_SCHEMA_ID, StandardGraphDriverSchemaError,
    StandardGraphDriverSchemaRegistrationError, register_standard_graph_driver_event_schema,
    standard_graph_driver_event_schema,
};
pub use durable_admission::{
    AgentRunIds, DurableAgentAdmission, DurableAgentAdmissionBuildError,
    DurableAgentAdmissionError, DurableAgentAdmissionRequest, DurableAgentAdmissionRequestError,
};
pub use invocation_executor::{
    DurableInvocationExecutor, DurableInvocationExecutorBuildError,
    DurableInvocationExecutorOptions, DurableInvocationExecutorOptionsError,
    InvocationAttemptEventIds, InvocationAttemptEventIdsError, InvocationAttemptHandoffError,
    InvocationBoundaryKind, InvocationBudgetContext, InvocationBudgetProvider,
    InvocationBudgetProviderError, InvocationClock, InvocationClockError,
    InvocationClockObservation, InvocationEventPayloadError, InvocationTerminalCommitFailure,
    ModelAttemptExecutionError, ModelAttemptHandoff, ModelAttemptOutcome, ModelAttemptTerminalKind,
    ModelEventSink, ModelEventSinkError, ModelTerminalCommitError, ModelTerminalCommitHandoff,
    SystemInvocationClock, ToolAttemptExecutionError, ToolAttemptHandoff, ToolAttemptOutcome,
    ToolAttemptTerminalKind, ToolTerminalCommitError, ToolTerminalCommitHandoff,
};
pub use invocation_schema::{
    STANDARD_INVOCATION_EXECUTION_EVENT_SCHEMA_ID, StandardInvocationExecutionSchemaError,
    StandardInvocationExecutionSchemaRegistrationError,
    register_standard_invocation_execution_event_schema,
    standard_invocation_execution_event_schema,
};
pub use lifecycle_schema::{
    STANDARD_GRAPH_LIFECYCLE_EVENT_SCHEMA_ID, StandardGraphLifecycleSchemaError,
    StandardGraphLifecycleSchemaRegistrationError, register_standard_graph_lifecycle_event_schema,
    standard_graph_lifecycle_event_schema,
};

pub use driver::{
    DurableGraphDriver, DurableGraphDriverBuildError, DurableGraphDriverOptions,
    DurableGraphDriverOptionsError, GraphBlockedHandoff, GraphDriveBlockers, GraphDriveOutcome,
    GraphDriveReport, GraphDriveResult, GraphDriverError, GraphLifecycleBarrierHandoff,
};
pub use fair_scheduler::{
    DurableFairScheduler, DurableFairSchedulerBuildError, DurableFairSchedulerOptions,
    DurableFairSchedulerOptionsError, FairSchedulerError, FairSchedulerTick, TenantFairnessWeight,
    TenantFairnessWeightError, TenantStarvationBound, WeightedFairnessPolicy,
    WeightedFairnessPolicyError,
};
pub use lifecycle::{
    DurableGraphLifecycle, DurableGraphLifecycleBuildError, DurableGraphLifecycleOptions,
    DurableGraphLifecycleOptionsError, GraphBarrierLifecycleOutcome, GraphFailureEvidence,
    GraphFailureEvidenceContext, GraphLifecycleError, GraphLifecycleEvidenceError,
    GraphLifecycleEvidenceProvider, GraphTerminalEvidence, GraphTerminalEvidenceContext,
};
pub use provider_registry::{
    ModelProviderRegistry, ModelProviderRegistryBuilder, ModelProviderRegistryError,
    ToolProviderRegistry, ToolProviderRegistryBuilder, ToolProviderRegistryError,
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
pub use tenant_scheduler::{
    DurableTenantScheduler, DurableTenantSchedulerBuildError, DurableTenantSchedulerOptions,
    DurableTenantSchedulerOptionsError, TenantSchedulerError, TenantSchedulerOutcome,
    TenantSchedulerReport, TenantSchedulerTick,
};
