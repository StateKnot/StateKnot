// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

#![doc = include_str!("../../../README.md")]
// The included project README uses StateKnot as a brand name, not a Rust item.
#![allow(clippy::doc_markdown)]
#![forbid(unsafe_code)]

// This facade intentionally exports no public API during the architecture-contract
// phase. Public types are added only after their RFC and vertical validation pass.
