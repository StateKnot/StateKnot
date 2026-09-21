// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Compile with:
//! `cargo check -p stateknot --example in_process_agent --locked`

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stateknot::{
    core::{AgentSubmissionKey, BudgetLimits},
    in_process_agent::{
        InProcessAgentBinding, InProcessAgentDependencies, InProcessAgentRequest,
        InProcessAgentRun, InProcessAgentRunOptions, InProcessAgentRuntime,
        InProcessAgentRuntimeOptions,
    },
    runtime::{AgentServiceCaller, TypedAgent},
};
use std::{error::Error, io, time::Duration};

/// Application request whose generated JSON Schema is pinned at startup.
#[derive(JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChatRequest {
    /// User message.
    pub message: String,
}

/// Application result decoded only after durable evidence validation.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChatResponse {
    /// Agent answer.
    pub answer: String,
}

/// Runs one recoverable request over an already-qualified deployment.
///
/// The application must persist `submission_key` with its own logical request.
/// Returning `None` means the durable run is still active; call this function
/// again with the same key and byte-equivalent input after this runtime or a
/// replacement runtime is available.
pub async fn run_chat(
    binding: InProcessAgentBinding,
    dependencies: InProcessAgentDependencies,
    codec: TypedAgent<ChatRequest, ChatResponse>,
    caller: AgentServiceCaller,
    submission_key: AgentSubmissionKey,
    input: ChatRequest,
) -> Result<Option<ChatResponse>, Box<dyn Error>> {
    let mut runtime = InProcessAgentRuntime::start(
        binding,
        dependencies,
        InProcessAgentRuntimeOptions::default(),
    )
    .await?;
    let agent = runtime
        .agent(codec, caller)?
        .with_options(InProcessAgentRunOptions::new(
            Duration::from_millis(50),
            Duration::from_secs(30),
        )?);
    let result = agent
        .run(InProcessAgentRequest::new(
            submission_key,
            input,
            BudgetLimits::empty(),
        ))
        .await;
    drop(agent);
    let report = runtime.shutdown().await?;
    if report.failure.is_some()
        || report.worker.is_none_or(|result| result.is_err())
        || report.maintenance.is_none_or(|result| result.is_err())
    {
        return Err(io::Error::other("in-process Agent roles did not drain cleanly").into());
    }
    match result? {
        InProcessAgentRun::Succeeded { output, .. } => Ok(Some(output)),
        InProcessAgentRun::Pending { .. } => Ok(None),
        InProcessAgentRun::Failed { .. }
        | InProcessAgentRun::Cancelled { .. }
        | InProcessAgentRun::Quarantined { .. } => {
            Err(io::Error::other("durable Agent run did not succeed").into())
        }
        _ => Err(io::Error::other("unsupported Agent run outcome").into()),
    }
}

fn main() {
    println!("assemble the qualified deployment, then call run_chat");
}
