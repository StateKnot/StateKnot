// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Private operator entry point for one contained Linux MCP process generation.

use std::process::ExitCode;

#[cfg(target_os = "linux")]
#[path = "mcp_stdio_owner/linux.rs"]
mod linux;

fn main() -> ExitCode {
    #[cfg(target_os = "linux")]
    {
        linux::main()
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("stateknot-mcp-stdio-owner: unsupported-platform");
        ExitCode::FAILURE
    }
}
