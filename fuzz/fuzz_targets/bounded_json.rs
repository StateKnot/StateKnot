// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    stateknot_fuzz::check_bounded_json(data);
});
