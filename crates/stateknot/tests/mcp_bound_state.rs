// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Public TCP ingress qualification for the explicitly configured bound-state profile.
//! Database cases follow the existing mandatory `PostgreSQL` environment contract.
#![allow(
    clippy::wildcard_imports,
    clippy::too_many_lines,
    clippy::similar_names
)]

use axum::Router;
use serde_json::{Value, json};
use stateknot::{core::*, integrations::*, postgres::*};
use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

#[path = "mcp_bound_state/support.rs"]
mod support;
use support::*;
#[path = "mcp_bound_state/durable.rs"]
mod durable;
#[path = "mcp_bound_state/network.rs"]
mod network;
