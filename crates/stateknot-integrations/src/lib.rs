// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded first-party protocol adapters for the provider-neutral `StateKnot` core.
//!
//! Provider wire models remain private to this crate. Each model attempt makes
//! exactly one upstream HTTP exchange: redirects and client retries are
//! disabled so the durable runtime remains the only retry authority.

#![forbid(unsafe_code)]

mod adapter;
mod anthropic;
mod credential;
mod http;
mod openai;
mod sse;

pub use adapter::ModelAdapterBuildError;
pub use anthropic::AnthropicMessagesModel;
pub use credential::{ApiKey, ApiKeyError, ApiKeyProvider, ApiKeyResolutionError, StaticApiKey};
pub use http::{
    ProviderEndpoint, ProviderEndpointError, ProviderHttpOptions, ProviderHttpOptionsError,
};
pub use openai::OpenAiResponsesModel;
