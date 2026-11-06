// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Native consumer contract for the experimental Linux MCP stdio session.

#![cfg(target_os = "linux")]

use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    fs,
    io::{ErrorKind, Read},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde_json::{Value, json};
use stateknot_core::{Digest, SchemaReference, Version};
use stateknot_integrations::{
    McpClientIdentity, McpStdioCommand, McpStdioConfig, McpStdioError, McpStdioFailure,
    McpStdioOwnerProfile, McpStdioSendPhase, McpStdioSession, McpStdioToolPin, McpToolCall,
    mcp_tool_descriptor_digest,
};
use stateknot_runtime::JsonSchemaRegistryBuilder;
use tokio::time::{sleep, timeout};

const MAX_WITNESS_BYTES: u64 = 256;
const WITNESS_UID: u32 = 65534;

const TEST_SERVER: &str = r#"
import json
import os
from pathlib import Path
import signal
import sys
import time

assert os.getresuid() == (65534, 65534, 65534)
assert os.getresgid() == (65534, 65534, 65534)
assert not os.getgroups()
assert "NOTIFY_SOCKET" not in os.environ
tool = json.loads(sys.argv[1])
case = sys.argv[2]

def idle():
    while True:
        signal.pause()

def witness(name, value):
    path = Path("witness") / name
    pending = path.with_suffix(".pending")
    raw = json.dumps(value, separators=(",", ":")).encode("utf-8")
    assert 0 < len(raw) <= 256
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    with os.fdopen(os.open(pending, flags, 0o600), "wb") as stream:
        stream.write(raw)
    pending.replace(path)

def notify(method, params):
    print(json.dumps({"jsonrpc": "2.0", "method": method, "params": params}), flush=True)

descendant = os.fork()
if descendant == 0:
    # Keep the inherited application pipes open until the owner retires the tree.
    idle()
witness("tree.json", {"target_pid": os.getpid(), "descendant_pid": descendant})
observe_deadline = time.monotonic() + 8
while not Path("observed").is_file():
    assert time.monotonic() < observe_deadline, "root observer acknowledgement did not arrive"
    time.sleep(0.01)

expected = iter(("server/discover", "tools/list", "tools/call"))
for request_id, line in enumerate(sys.stdin.buffer, 1):
    request = json.loads(line)
    method = request["method"]
    assert request["jsonrpc"] == "2.0" and method == next(expected)
    assert request["id"] == request_id
    result = {"resultType": "complete"}
    response = {"jsonrpc": "2.0", "id": request_id, "result": result}
    if method == "server/discover":
        if case == "handshake-timeout":
            witness("discovery.json", {"id": request_id, "method": method})
            idle()
        versions = ["2025-11-25"] if case == "wrong-version" else ["2026-07-28"]
        result.update(supportedVersions=versions, capabilities={"tools": {}},
                      ttlMs=0, cacheScope="private", _meta={
                          "io.modelcontextprotocol/serverInfo": {
                              "name": "stateknot-stdio-fixture", "version": "1.0.0"
                          }
                      })
    elif method == "tools/list":
        result["tools"] = [tool]
    else:
        params = request["params"]
        expected_message = "external-cancel-canary" if case == "cancelled" else "from consumer"
        assert params["name"] == "echo" and params["arguments"] == {"message": expected_message}
        if case == "cancelled":
            witness("request.json", {"id": request_id, "method": method,
                                     "nonce": params["arguments"]["message"]})
            idle()
        if case == "malformed":
            print('{"jsonrpc":"2.0","id":', flush=True)
            idle()
        result.update(content=[{"type": "text", "text": "from consumer"}],
                      structuredContent={"echo": "from consumer"}, isError=False)
        if case == "oversized":
            # One byte past the frame bound, with otherwise valid JSON and strings.
            maximum_frame = 256 * 1024
            result["padding"] = ["a" * (32 * 1024)] * 7 + [""]
            result["padding"][-1] = "a" * (maximum_frame + 1 - len(json.dumps(response)))
            frame = json.dumps(response)
            assert len(frame) == maximum_frame + 1
            print(frame, flush=True)
            idle()
        if case == "wrong-id":
            response["id"] = request_id + 1
        if case == "happy":
            token = params["_meta"]["progressToken"]
            assert token == request_id
            notify("notifications/progress", {"progressToken": token, "progress": 1, "total": 2})
            notify("notifications/message", {"level": "info", "data": "unrelated notice"})
            notify("notifications/progress", {"progressToken": token, "progress": 2, "total": 2})
    print(json.dumps(response), flush=True)
# An application EOF cannot serve as evidence that the owner retired the tree.
idle()
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

fn configured_session(work: &Path, case: &str) -> McpStdioSession {
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
        work,
        &[
            OsString::from("-I"),
            server.into_os_string(),
            OsString::from(descriptor.to_string()),
            OsString::from(case),
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

fn assert_fault(error: McpStdioError, failure: McpStdioFailure, phase: McpStdioSendPhase) {
    assert_eq!(error.failure(), failure);
    assert_eq!(error.send_phase(), phase);
}

fn witness_identity(metadata: &fs::Metadata) -> (u64, u64) {
    assert!(metadata.is_file());
    assert_eq!((metadata.uid(), metadata.gid()), (WITNESS_UID, WITNESS_UID));
    assert_eq!(metadata.mode() & 0o7777, 0o600);
    assert!((1..=MAX_WITNESS_BYTES).contains(&metadata.len()));
    (metadata.dev(), metadata.ino())
}

// Open witness contents only after verified retirement, when the target can
// no longer replace a path in its witness directory.
fn read_witness(path: &Path) -> ((u64, u64), Value) {
    let identity = witness_identity(&fs::symlink_metadata(path).unwrap());
    let file = fs::File::open(path).unwrap();
    assert_eq!(witness_identity(&file.metadata().unwrap()), identity);
    let mut raw = Vec::new();
    file.take(MAX_WITNESS_BYTES + 1)
        .read_to_end(&mut raw)
        .unwrap();
    assert!(raw.len() as u64 <= MAX_WITNESS_BYTES);
    (identity, serde_json::from_slice(&raw).unwrap())
}

async fn wait_for_witness(path: &Path) -> (u64, u64) {
    timeout(Duration::from_secs(5), async {
        loop {
            match fs::symlink_metadata(path) {
                Ok(metadata) => return witness_identity(&metadata),
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("request witness stat failed: {error}"),
            }
        }
    })
    .await
    .expect("target must receive the exact request before cancellation")
}

#[tokio::test]
#[ignore = "requires the qualified root-owned Linux systemd fixture"]
async fn owned_session_discovers_calls_and_retires() {
    use McpStdioFailure::{Admission, Busy, Closed, Deadline, Input, Limit, Protocol};
    use McpStdioSendPhase::{MayHaveBeenSent, NotSent};

    let work = path_from_env("STATEKNOT_STDIO_TEST_WORK");
    let case = env::var("STATEKNOT_STDIO_TEST_CASE").expect("native fixture case");
    let start_failure = match case.as_str() {
        "handshake-timeout" => Some(Deadline),
        "wrong-version" => Some(Admission),
        "happy" | "malformed" | "oversized" | "wrong-id" | "cancelled" => None,
        _ => panic!("unknown native fixture case: {case}"),
    };
    let mut session = configured_session(&work, &case);
    assert!(session.client().is_none());
    let start = session.start().await;
    if let Some(expected) = start_failure {
        assert_fault(start.expect_err("discovery must fail"), expected, NotSent);
        assert!(session.client().is_none());
        session
            .wait()
            .await
            .expect("verified retirement after failed discovery");
        if case == "handshake-timeout" {
            let (_, discovery) = read_witness(&work.join("witness/discovery.json"));
            assert_eq!(discovery, json!({"id": 1, "method": "server/discover"}));
        }
    } else {
        start.expect("start and admit the fixture");
        let client = session.client().expect("admitted session");
        assert_eq!(client.catalog().tools().len(), 1);
        let tool = client.catalog().find("echo").unwrap();

        if case == "cancelled" {
            let witness = work.join("witness/request.json");
            assert!(!witness.try_exists().unwrap());
            let mut call =
                Box::pin(client.call_tool(tool, json!({"message": "external-cancel-canary"})));
            let identity = tokio::select! {
                biased;
                result = &mut call => panic!("call completed before cancellation: {result:?}"),
                identity = wait_for_witness(&witness) => identity,
            };
            let busy = client
                .call_tool(tool, json!({"message": "unexpected second request"}))
                .await
                .expect_err("the witnessed request must still hold the dispatch slot");
            assert_fault(busy, Busy, NotSent);
            drop(call);
            assert!(session.client().is_none());
            session
                .wait()
                .await
                .expect("verified retirement after dropped dispatched call");
            let (retired_identity, request) = read_witness(&witness);
            assert_eq!(retired_identity, identity);
            assert_eq!(
                request,
                json!({"id": 3, "method": "tools/call", "nonce": "external-cancel-canary"})
            );
        } else {
            if case == "happy" {
                let rejected = client
                    .call_tool(tool, json!({"message": 17}))
                    .await
                    .expect_err("the local input schema must reject before dispatch");
                assert_fault(rejected, Input, NotSent);
            }
            let response = client
                .call_tool(tool, json!({"message": "from consumer"}))
                .await;
            if case == "happy" {
                let response = response.expect("successful Tool call");
                let McpToolCall::Complete(result) = response.outcome() else {
                    panic!("expected a complete Tool result");
                };
                assert!(!result.is_error());
                assert_eq!(
                    result.structured_content(),
                    Some(&json!({"echo": "from consumer"}))
                );
                let notifications = response.notifications();
                assert_eq!(notifications.len(), 2);
                for (notification, progress) in notifications.iter().zip([1, 2]) {
                    assert_eq!(notification.method(), "notifications/progress");
                    assert_eq!(
                        notification.params(),
                        Some(&json!({
                            "progressToken": 3, "progress": progress, "total": 2
                        }))
                    );
                }
                session
                    .shutdown()
                    .await
                    .expect("verified generation retirement");
            } else {
                let failure = match case.as_str() {
                    "malformed" | "wrong-id" => Protocol,
                    "oversized" => Limit,
                    _ => unreachable!("validated Tool response case"),
                };
                assert_fault(
                    response.expect_err("the dispatched Tool request must fail"),
                    failure,
                    MayHaveBeenSent,
                );
                assert!(session.client().is_none());
                session
                    .wait()
                    .await
                    .expect("verified retirement after Tool response fault");
            }
        }

        let closed = client
            .call_tool(tool, json!({"message": "after retirement"}))
            .await
            .expect_err("retained client must be closed");
        assert_fault(closed, Closed, NotSent);
    }

    assert!(session.client().is_none());
    println!("STATEKNOT_MCP_STDIO_SESSION_CASE={case}:passed");
}
