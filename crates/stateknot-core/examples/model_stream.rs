// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Validates a complete provider-neutral model event stream offline.
//!
//! A concrete provider adapter implements `Model::stream`; the core contract
//! itself needs no provider SDK, executor, transport, or credential.

mod support;

use std::{error::Error, time::Instant};

use stateknot_core::{
    AttemptId, BudgetUsage, ByteCount, CancellationSignal, ContentMetadata, ContentSource,
    ExecutionCount, Extensions, Model, ModelContext, ModelEvent, ModelEventAccumulator,
    ModelEventKind, ModelFinishReason, ModelOutputDelta, ModelOutputStart, ModelRequest,
    ModelRequestLimits, ModelResponseMode, ModelResponseProvenance, ModelStreamChunk, ModelUsage,
    RedactionState, RunId, SecurityLabel, TenantId, ThreadId, TokenCount,
};

fn enter_model_boundary(model: &dyn Model, context: ModelContext, request: ModelRequest) {
    // Dropping the stream is cancellation, not successful completion. A runtime
    // must consume and validate it before committing an attempt result.
    drop(model.stream(context, request));
}

fn main() -> Result<(), Box<dyn Error>> {
    let descriptor = support::model_descriptor()?;
    let request = ModelRequest::builder(ModelRequestLimits::new(
        TokenCount::new(1_024),
        TokenCount::new(256),
        ByteCount::new(16 * 1024),
    )?)
    .instruction(support::instruction(
        "stream-example.base",
        "Return one concise incident summary.",
    )?)
    .response_mode(ModelResponseMode::Streaming)
    .build()?;
    descriptor
        .capabilities()
        .satisfies(request.requirements())?;

    let observed_at = "2030-01-01T00:00:00.000000Z".parse()?;
    let resolved = stateknot_core::ResolvedBudget::resolve(&[support::complete_budget()?])?;
    let remaining = resolved.remaining(&BudgetUsage::zero(), observed_at)?;
    let attempt_id: AttemptId = "01912345-6789-7abc-8def-0123456789ab".parse()?;
    let context = ModelContext::new(
        "tenant-production".parse::<TenantId>()?,
        "01912345-6789-7abc-8def-0123456789ac".parse::<RunId>()?,
        "01912345-6789-7abc-8def-0123456789ad".parse::<ThreadId>()?,
        attempt_id,
        remaining,
        observed_at,
        Instant::now(),
        CancellationSignal::never(),
    )?;
    assert_eq!(context.attempt_id(), attempt_id);

    // Retaining this typed function pointer proves the public Model boundary
    // compiles without inventing a provider or an executor for the example.
    let provider_entry: fn(&dyn Model, ModelContext, ModelRequest) = enter_model_boundary;
    std::hint::black_box(provider_entry);

    let provenance = ModelResponseProvenance::new(
        attempt_id,
        descriptor.metadata().identity().clone(),
        None,
        None,
    );
    let metadata = ContentMetadata::new(
        ContentSource::Model,
        stateknot_core::ContentTrust::Untrusted,
        "internal".parse::<SecurityLabel>()?,
        RedactionState::NotApplied,
    );
    let usage = ModelUsage::new(TokenCount::new(8), None, TokenCount::new(3), None)?;
    let events = [
        ModelEvent::new(
            attempt_id,
            ExecutionCount::new(0),
            ModelEventKind::Started { provenance },
        )?,
        ModelEvent::new(
            attempt_id,
            ExecutionCount::new(1),
            ModelEventKind::OutputStarted {
                output_index: ExecutionCount::ZERO,
                start: Box::new(ModelOutputStart::text(Some("en".parse()?), metadata)?),
            },
        )?,
        ModelEvent::new(
            attempt_id,
            ExecutionCount::new(2),
            ModelEventKind::OutputDelta {
                output_index: ExecutionCount::ZERO,
                delta: ModelOutputDelta::Text(ModelStreamChunk::new("Service recovered.")?),
            },
        )?,
        ModelEvent::new(
            attempt_id,
            ExecutionCount::new(3),
            ModelEventKind::OutputCompleted {
                output_index: ExecutionCount::ZERO,
            },
        )?,
        ModelEvent::new(
            attempt_id,
            ExecutionCount::new(4),
            ModelEventKind::Completed {
                finish_reason: ModelFinishReason::Completed,
                usage,
                extensions: Extensions::default(),
            },
        )?,
    ];

    let mut accumulator = ModelEventAccumulator::new(attempt_id, &descriptor, &request)?;
    for event in events {
        accumulator.push(event)?;
    }
    let response = accumulator.finish()?;
    assert_eq!(response.output().len(), 1);
    println!("validated five contiguous events into one bounded response");
    println!("no provider, executor, transport, or durable commit was used");
    Ok(())
}
