// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Native consumer contract for the experimental Linux MCP stdio session.

#![cfg(all(target_os = "linux", feature = "experimental-mcp-stdio-linux-owner"))]

use std::{collections::BTreeMap, env, ffi::OsString, fs, path::Path, path::PathBuf, sync::Arc};

use serde_json::{Value, json};
use stateknot_core::{Digest, SchemaReference, Version};
use stateknot_integrations::{
    McpClientIdentity, McpStdioCommand, McpStdioConfig, McpStdioFailure, McpStdioOwnerProfile,
    McpStdioSendPhase, McpStdioSession, McpStdioToolPin, McpToolCall, mcp_tool_descriptor_digest,
};
use stateknot_runtime::JsonSchemaRegistryBuilder;

const TEST_SERVER: &str = r#"
import json
import os
import signal
import sys

assert os.getresuid() == (65534, 65534, 65534)
assert os.getresgid() == (65534, 65534, 65534)
assert not os.getgroups()
assert "NOTIFY_SOCKET" not in os.environ
tool = json.loads(sys.argv[1])
expected = iter(("server/discover", "tools/list", "tools/call"))
for line in sys.stdin.buffer:
    request = json.loads(line)
    method = request["method"]
    assert request["jsonrpc"] == "2.0" and method == next(expected)
    result = {"resultType": "complete"}
    if method == "server/discover":
        result.update(supportedVersions=["2026-07-28"], capabilities={"tools": {}},
                      ttlMs=0, cacheScope="private", _meta={
                          "io.modelcontextprotocol/serverInfo": {
                              "name": "stateknot-stdio-fixture", "version": "1.0.0"
                          }
                      })
    elif method == "tools/list":
        result["tools"] = [tool]
    else:
        params = request["params"]
        assert params["name"] == "echo" and params["arguments"] == {"message": "from consumer"}
        result.update(content=[{"type": "text", "text": "from consumer"}],
                      structuredContent={"echo": "from consumer"}, isError=False)
    print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}), flush=True)
# Keep the fixture alive if the application pipe closes first. The owner must
# retire the generation, rather than relying on the target to exit on EOF.
while True:
    signal.pause()
"#;

fn schema(name: &str, property: &str) -> (SchemaReference, Value) {
    let id = format!("https://stknot.com/schemas/tests/mcp-stdio-{name}/1.0.0");
    let document = json!({
        "$id": id,
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": { (property): { "type": "string" } },
        "required": [property],
        "additionalProperties": false
    });
    let reference = SchemaReference::new(
        id.parse().unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(serde_json_canonicalizer::to_vec(&document).unwrap()),
    );
    (reference, document)
}

fn path_from_env(key: &str) -> PathBuf {
    PathBuf::from(env::var_os(key).unwrap_or_else(|| panic!("native fixture requires {key}")))
}

fn configured_session() -> McpStdioSession {
    let work = path_from_env("STATEKNOT_STDIO_TEST_WORK");
    let server = work.join("fixture.py");
    fs::write(&server, TEST_SERVER).unwrap();
    let (input, input_schema) = schema("input", "message");
    let (output, output_schema) = schema("output", "echo");
    let descriptor = json!({
        "name": "echo",
        "description": "Echo one bounded message.",
        "inputSchema": input_schema,
        "outputSchema": output_schema
    });
    let digest = mcp_tool_descriptor_digest(&descriptor).unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    schemas.register(input.clone(), input_schema).unwrap();
    schemas.register(output.clone(), output_schema).unwrap();
    let profile = McpStdioOwnerProfile::new(
        work.join("stateknot-mcp-stdio-owner"),
        work.join("control"),
        env::var("STATEKNOT_STDIO_TEST_GENERATION").unwrap(),
    )
    .unwrap();
    let command = McpStdioCommand::new(
        Path::new("/usr/bin/python3"),
        &work,
        &[
            OsString::from("-I"),
            server.into_os_string(),
            OsString::from(descriptor.to_string()),
        ],
        &BTreeMap::new(),
    )
    .unwrap();
    let config = McpStdioConfig::new(
        profile,
        command,
        McpClientIdentity::new("stateknot-stdio-consumer", "1.0.0").unwrap(),
        McpClientIdentity::new("stateknot-stdio-fixture", "1.0.0").unwrap(),
        vec![McpStdioToolPin::new("echo".to_owned(), digest, input, Some(output)).unwrap()],
        Arc::new(schemas.build().unwrap()),
    )
    .unwrap();
    McpStdioSession::new(config)
}

#[tokio::test]
#[ignore = "requires the qualified root-owned Linux systemd fixture"]
async fn owned_session_discovers_calls_and_retires() {
    let mut session = configured_session();
    assert!(session.client().is_none());
    session.start().await.expect("start and admit the fixture");
    let client = session.client().expect("admitted session");
    assert_eq!(client.catalog().tools().len(), 1);
    let tool = client.catalog().find("echo").unwrap();
    let rejected = client
        .call_tool(tool, json!({"message": 17}))
        .await
        .expect_err("the local input schema must reject before dispatch");
    let response = client
        .call_tool(tool, json!({"message": "from consumer"}))
        .await;
    session
        .shutdown()
        .await
        .expect("verified generation retirement");
    assert!(session.client().is_none());
    assert_eq!(rejected.failure(), McpStdioFailure::Input);
    assert_eq!(rejected.send_phase(), McpStdioSendPhase::NotSent);
    let response = response.expect("successful Tool call");
    let McpToolCall::Complete(result) = response.outcome() else {
        panic!("expected a complete Tool result");
    };
    assert!(!result.is_error());
    assert_eq!(
        result.structured_content(),
        Some(&json!({"echo": "from consumer"}))
    );
    let closed = client
        .call_tool(tool, json!({"message": "after shutdown"}))
        .await
        .expect_err("retained client must be closed");
    assert_eq!(closed.failure(), McpStdioFailure::Closed);
    assert_eq!(closed.send_phase(), McpStdioSendPhase::NotSent);
    println!("STATEKNOT_MCP_STDIO_SESSION_DISCOVERY_CALL_RETIREMENT=passed");
}
