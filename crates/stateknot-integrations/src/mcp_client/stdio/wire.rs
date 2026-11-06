// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::io::{self, Write};

use serde_json::{Map, Value, json};
use stateknot_core::{BoundedJson, BoundedJsonError, JsonLimits};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{ChildStdin, ChildStdout},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::super::{
    MCP_META_CLIENT_CAPABILITIES, MCP_META_CLIENT_INFO, MCP_META_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION_2026_07_28, McpClientIdentity, McpNotification, ParsedMessage,
    classify_rpc_message,
};

const MAX_FRAME_BYTES: usize = JsonLimits::DEFAULT.max_bytes();
const MAX_OUTPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_NOTIFICATION_BYTES: usize = 256 * 1024;
const MAX_NOTIFICATIONS: usize = 32;
const PIPE_CHUNK_BYTES: usize = 8192;
const MAX_PRELUDE_BYTES: usize = 4 + 32 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WireError {
    Io,
    Limit,
    Protocol,
    Remote,
    Closed,
    Deadline,
}

pub(super) struct Wire {
    stdin: ChildStdin,
    stdout: ChildStdout,
    read_buffer: [u8; PIPE_CHUNK_BYTES],
    read_start: usize,
    read_end: usize,
    output_bytes: usize,
    next_id: u64,
}

impl Wire {
    pub(super) fn new(stdin: ChildStdin, stdout: ChildStdout) -> Self {
        Self {
            stdin,
            stdout,
            read_buffer: [0; PIPE_CHUNK_BYTES],
            read_start: 0,
            read_end: 0,
            output_bytes: 0,
            next_id: 1,
        }
    }

    pub(super) async fn prelude(&mut self, bytes: &[u8]) -> Result<(), WireError> {
        if bytes.len() > MAX_PRELUDE_BYTES {
            return Err(WireError::Limit);
        }
        self.stdin.write_all(bytes).await.map_err(|_| WireError::Io)
    }

    pub(super) async fn rpc(
        &mut self,
        identity: &McpClientIdentity,
        method: &str,
        mut params: Map<String, Value>,
        sent: &mut bool,
        deadline: Instant,
        stop: &CancellationToken,
    ) -> Result<(Value, Vec<McpNotification>), WireError> {
        *sent = false;
        let id = self.next_id;
        let next_id = id.checked_add(1).ok_or(WireError::Limit)?;
        params.insert(
            "_meta".to_owned(),
            json!({
                (MCP_META_PROTOCOL_VERSION): MCP_PROTOCOL_VERSION_2026_07_28,
                (MCP_META_CLIENT_INFO): {"name": identity.name(), "version": identity.version()},
                (MCP_META_CLIENT_CAPABILITIES): {},
                "progressToken": id,
            }),
        );
        let request = Value::Object(Map::from_iter([
            ("jsonrpc".to_owned(), Value::from("2.0")),
            ("id".to_owned(), Value::from(id)),
            ("method".to_owned(), Value::from(method)),
            ("params".to_owned(), Value::Object(params)),
        ]));
        let request = BoundedJson::try_from_value(request).map_err(json_error)?;
        let mut frame = FrameBuffer::default();
        serde_json::to_writer(&mut frame, &request).map_err(|_| WireError::Limit)?;
        frame.0.push(b'\n');
        if stop.is_cancelled() {
            return Err(WireError::Closed);
        }
        if Instant::now() >= deadline {
            return Err(WireError::Deadline);
        }
        self.next_id = next_id;
        // From this point a failed or interrupted write leaves dispatch uncertain.
        *sent = true;
        self.stdin
            .write_all(&frame.0)
            .await
            .map_err(|_| WireError::Io)?;

        let mut notifications = Vec::new();
        let mut notification_count = 0;
        let mut notification_bytes = 0;
        let mut last_progress = None;
        loop {
            let (value, bytes) = self.read_frame().await?;
            if value
                .get("method")
                .is_some_and(|method| !method.is_string())
            {
                return Err(WireError::Protocol);
            }
            match classify_rpc_message(value, id).map_err(|_| WireError::Protocol)? {
                ParsedMessage::Result(result) => {
                    self.require_response_boundary()?;
                    return Ok((result, notifications));
                }
                ParsedMessage::Remote(_) => {
                    self.require_response_boundary()?;
                    return Err(WireError::Remote);
                }
                ParsedMessage::Notification(notification) => {
                    notification_count += 1;
                    notification_bytes += bytes;
                    if notification_count > MAX_NOTIFICATIONS
                        || notification_bytes > MAX_NOTIFICATION_BYTES
                    {
                        return Err(WireError::Limit);
                    }
                    if let Some(progress) = correlated_progress(&notification, id)? {
                        if last_progress.is_some_and(|last| progress <= last) {
                            return Err(WireError::Protocol);
                        }
                        last_progress = Some(progress);
                        notifications.push(notification);
                    }
                }
            }
        }
    }

    fn require_response_boundary(&self) -> Result<(), WireError> {
        // The serialized profile has no next request yet. Reject any bytes
        // already prefetched beyond the final response before admitting work.
        if self.read_start == self.read_end {
            Ok(())
        } else {
            Err(WireError::Protocol)
        }
    }

    async fn read_frame(&mut self) -> Result<(Value, usize), WireError> {
        let mut frame = Vec::new();
        loop {
            if self.read_start == self.read_end {
                self.read_start = 0;
                self.read_end = read_chunk(
                    &mut self.stdout,
                    &mut self.read_buffer,
                    &mut self.output_bytes,
                )
                .await?;
                if self.read_end == 0 {
                    return Err(if frame.is_empty() {
                        WireError::Io
                    } else {
                        WireError::Protocol
                    });
                }
            }
            let remaining = &self.read_buffer[self.read_start..self.read_end];
            let newline = remaining.iter().position(|byte| *byte == b'\n');
            let length = newline.unwrap_or(remaining.len());
            if length > MAX_FRAME_BYTES - frame.len() {
                return Err(WireError::Limit);
            }
            frame.extend_from_slice(&remaining[..length]);
            self.read_start += length;
            if newline.is_some() {
                self.read_start += 1;
                let body = frame.strip_suffix(b"\r").unwrap_or(&frame);
                if body.is_empty() || body.contains(&b'\r') {
                    return Err(WireError::Protocol);
                }
                let value = BoundedJson::from_slice(body).map_err(json_error)?;
                return Ok((value.into_value(), frame.len() + 1));
            }
        }
    }

    pub(super) async fn finish(self) -> Result<(), WireError> {
        let Self {
            stdin,
            mut stdout,
            mut read_buffer,
            mut output_bytes,
            ..
        } = self;
        drop(stdin);
        // read_chunk counted every prefetched byte, including buffered data
        // after the final frame. Discard that data and continue to true EOF.
        while read_chunk(&mut stdout, &mut read_buffer, &mut output_bytes).await? != 0 {}
        Ok(())
    }
}

async fn read_chunk(
    stdout: &mut ChildStdout,
    buffer: &mut [u8; PIPE_CHUNK_BYTES],
    output_bytes: &mut usize,
) -> Result<usize, WireError> {
    if *output_bytes > MAX_OUTPUT_BYTES {
        return Err(WireError::Limit);
    }
    // One probe byte beyond the budget distinguishes EOF from further output.
    let maximum_read = (MAX_OUTPUT_BYTES - *output_bytes + 1).min(buffer.len());
    let count = stdout
        .read(&mut buffer[..maximum_read])
        .await
        .map_err(|_| WireError::Io)?;
    *output_bytes += count;
    if *output_bytes > MAX_OUTPUT_BYTES {
        return Err(WireError::Limit);
    }
    Ok(count)
}

fn correlated_progress(notification: &McpNotification, id: u64) -> Result<Option<f64>, WireError> {
    if notification.method().is_empty() || notification.params().is_some_and(|p| !p.is_object()) {
        return Err(WireError::Protocol);
    }
    if notification.method() != "notifications/progress" {
        return Ok(None);
    }
    let params = notification
        .params()
        .and_then(Value::as_object)
        .ok_or(WireError::Protocol)?;
    let token = params.get("progressToken").ok_or(WireError::Protocol)?;
    if !(token.is_string() || token.is_i64() || token.is_u64()) {
        return Err(WireError::Protocol);
    }
    let progress = params
        .get("progress")
        .and_then(Value::as_f64)
        .ok_or(WireError::Protocol)?;
    if params.get("total").is_some_and(|total| !total.is_number())
        || params
            .get("message")
            .is_some_and(|message| !message.is_string())
    {
        return Err(WireError::Protocol);
    }
    Ok((token.as_u64() == Some(id)).then_some(progress))
}

fn json_error(error: BoundedJsonError) -> WireError {
    match error {
        BoundedJsonError::InputTooLarge { .. }
        | BoundedJsonError::CompactRepresentationTooLarge { .. }
        | BoundedJsonError::NestingTooDeep { .. }
        | BoundedJsonError::TooManyContainerEntries { .. }
        | BoundedJsonError::TooManyNodes { .. }
        | BoundedJsonError::StringTooLong { .. }
        | BoundedJsonError::ObjectKeyTooLong { .. } => WireError::Limit,
        _ => WireError::Protocol,
    }
}

#[derive(Default)]
struct FrameBuffer(Vec<u8>);

impl Write for FrameBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_FRAME_BYTES - self.0.len() {
            return Err(io::Error::from(io::ErrorKind::FileTooLarge));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
