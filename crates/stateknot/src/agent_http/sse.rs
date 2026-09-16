// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::{
    AgentHttpAuthenticationError, AgentHttpCredential, AgentHttpOperation, AgentHttpOptionsError,
    AgentHttpRunResponse, HttpError, Inner, LimitedWriter, encode, error_status, response,
    service_error, single,
};
use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::stream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stateknot_core::{EventId, JournalHead, JournalSequence, RunId, Timestamp};
use stateknot_runtime::{AgentRunActivityPage, AgentServiceCaller};
use std::{convert::Infallible, io::Write as _, sync::Arc, time::Duration};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, timeout, timeout_at},
};

const MAX_CURSOR_BYTES: usize = 1024;
const MAX_STREAM_BYTES: usize = 64 * 1024 * 1024;

/// Explicit finite policy for activity SSE. JSON request limits remain separate.
#[derive(Clone, Debug)]
pub struct AgentHttpSseOptions {
    pub(super) max_streams: usize,
    lifetime: Duration,
    poll_interval: Duration,
    send_timeout: Duration,
}

impl AgentHttpSseOptions {
    /// Validates concurrency (1..=128), lifetime (1..=600 seconds), polling and
    /// producer queue wait (50 ms..=15 seconds). A stream emits at most 64 MiB.
    /// Polling also supplies comment heartbeats when no public observation changes.
    pub fn new(
        max_streams: usize,
        lifetime: Duration,
        poll_interval: Duration,
        send_timeout: Duration,
    ) -> Result<Self, AgentHttpOptionsError> {
        let interval = Duration::from_millis(50)..=Duration::from_secs(15);
        if !(1..=128).contains(&max_streams)
            || !(Duration::from_secs(1)..=Duration::from_secs(600)).contains(&lifetime)
            || !interval.contains(&poll_interval)
            || !interval.contains(&send_timeout)
            || poll_interval > lifetime
            || send_timeout > lifetime
        {
            return Err(AgentHttpOptionsError);
        }
        Ok(Self {
            max_streams,
            lifetime,
            poll_interval,
            send_timeout,
        })
    }
}

impl Default for AgentHttpSseOptions {
    fn default() -> Self {
        Self {
            max_streams: 16,
            lifetime: Duration::from_secs(60),
            poll_interval: Duration::from_secs(1),
            send_timeout: Duration::from_secs(5),
        }
    }
}

/// Public-safe activity notification. Its SSE id is an opaque exact replay cursor.
/// This is not a token delta, lifecycle transition, or historical snapshot.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHttpActivity {
    /// Contiguous journal order; not lifecycle revision.
    pub sequence: JournalSequence,
    /// Database recording time; sequence remains authoritative if clocks regress.
    pub recorded_at: Timestamp,
}

pub(super) fn validate_media(headers: &HeaderMap) -> Result<(), HttpError> {
    if headers.contains_key(header::CONTENT_ENCODING) {
        return Err(HttpError::Media);
    }
    if single(headers, header::ACCEPT.as_str())? != Some("text/event-stream") {
        return Err(HttpError::Accept);
    }
    Ok(())
}

fn cursor(head: &JournalHead) -> Result<String, HttpError> {
    let bytes = encode(head, MAX_CURSOR_BYTES / 2)?;
    Ok(format!("sk1.{}", URL_SAFE_NO_PAD.encode(bytes)))
}

fn parse_cursor(value: &str) -> Result<JournalHead, HttpError> {
    if value.len() > MAX_CURSOR_BYTES {
        return Err(HttpError::Invalid);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(value.strip_prefix("sk1.").ok_or(HttpError::Invalid)?)
        .map_err(|_| HttpError::Invalid)?;
    let head = serde_json::from_slice::<JournalHead>(&bytes).map_err(|_| HttpError::Invalid)?;
    if cursor(&head)? != value {
        return Err(HttpError::Invalid);
    }
    Ok(head)
}

pub(super) async fn open(
    inner: Arc<Inner>,
    caller: AgentServiceCaller,
    credential: AgentHttpCredential,
    run: RunId,
    headers: &HeaderMap,
    request_id: EventId,
) -> Result<Response, HttpError> {
    let options = inner.options.sse.clone().ok_or(HttpError::NotFound)?;
    let permit = inner
        .stream_permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| HttpError::Overloaded)?;
    let after = single(headers, "last-event-id")?
        .map(parse_cursor)
        .transpose()?;
    let page = inner
        .service
        .load_activity(caller.clone(), run, after.as_ref())
        .await
        .map_err(service_error)?;
    let mut observation = Observation {
        after,
        snapshot: Vec::new(),
        request_id,
        maximum: inner.options.max_response_bytes,
        total: 0,
    };
    // Validate the first bounded frame before committing HTTP 200.
    let first = observation.batch(&page)?;
    let (sender, receiver) = mpsc::channel(1);
    let expires = Instant::now() + options.lifetime;
    let tracker = inner.stream_tasks.clone();
    let task = tracker.spawn(async move {
        let _permit = permit;
        let result = tokio::select! {
            biased;
            () = inner.shutdown.cancelled() => Err(HttpError::Unavailable),
            () = sender.closed() => return,
            result = timeout_at(expires, produce(&sender, &inner, &options, &caller, &credential,
                run, &mut observation, page.has_more(), first)) => result.unwrap_or(Err(HttpError::Unavailable)),
        };
        if let Err(error) = result {
            // Never block drain or retain a stream permit to report an error.
            let code = error_status(error).1;
            let bytes = Bytes::from(format!(
                "event: error\ndata: {{\"error\":{{\"code\":\"agent_http.{code}\",\"request_id\":\"{request_id}\"}}}}\n\n"
            ));
            if bytes.len() <= MAX_STREAM_BYTES.saturating_sub(observation.total) {
                let _ = sender.try_send(bytes);
            }
        }
    });
    let body = Body::from_stream(stream::unfold(
        Consumer { receiver, task },
        |mut consumer| async {
            consumer
                .receiver
                .recv()
                .await
                .map(|bytes| (Ok::<_, Infallible>(bytes), consumer))
        },
    ));
    let mut result = response(StatusCode::OK, request_id, Vec::new());
    *result.body_mut() = body;
    result.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/event-stream".parse().expect("static SSE media"),
    );
    result.headers_mut().insert(
        header::CACHE_CONTROL,
        "private, no-store, no-transform"
            .parse()
            .expect("static SSE cache policy"),
    );
    result.headers_mut().insert(
        "x-accel-buffering",
        "no".parse().expect("static buffering hint"),
    );
    Ok(result)
}

struct Consumer {
    receiver: mpsc::Receiver<Bytes>,
    task: JoinHandle<()>,
}
impl Drop for Consumer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Observation {
    after: Option<JournalHead>,
    snapshot: Vec<u8>,
    request_id: EventId,
    maximum: usize,
    total: usize,
}
impl Observation {
    fn batch(&mut self, page: &AgentRunActivityPage) -> Result<Bytes, HttpError> {
        let snapshot = encode(
            &AgentHttpRunResponse {
                request_id: self.request_id,
                snapshot: page.snapshot().clone(),
            },
            self.maximum,
        )?;
        let mut writer = LimitedWriter {
            bytes: Vec::new(),
            maximum: self.maximum,
        };
        let write = |writer: &mut LimitedWriter, bytes: &[u8]| {
            writer
                .write_all(bytes)
                .map_err(|_| HttpError::ResponseTooLarge)
        };
        if self.snapshot != snapshot {
            write(&mut writer, b"event: snapshot\ndata: ")?;
            write(&mut writer, &snapshot)?;
            write(&mut writer, b"\n\n")?;
        }
        if let Some(head) = page.head() {
            write(
                &mut writer,
                format!("event: activity\nid: {}\ndata: ", cursor(head)?).as_bytes(),
            )?;
            write(
                &mut writer,
                &encode(
                    &AgentHttpActivity {
                        sequence: head.sequence(),
                        recorded_at: head.recorded_at(),
                    },
                    1024,
                )?,
            )?;
            write(&mut writer, b"\n\n")?;
        }
        if writer.bytes.is_empty() {
            write(&mut writer, b": keep-alive\n\n")?;
        }
        self.total = self
            .total
            .checked_add(writer.bytes.len())
            .ok_or(HttpError::Unavailable)?;
        if self.total > MAX_STREAM_BYTES {
            return Err(HttpError::Unavailable);
        }
        self.snapshot = snapshot;
        if let Some(head) = page.head() {
            self.after = Some(head.clone());
        }
        Ok(Bytes::from(writer.bytes))
    }
}

#[allow(clippy::too_many_arguments)] // One owned connection context, no global worker/cache.
async fn produce(
    sender: &mpsc::Sender<Bytes>,
    inner: &Inner,
    options: &AgentHttpSseOptions,
    caller: &AgentServiceCaller,
    credential: &AgentHttpCredential,
    run: RunId,
    observation: &mut Observation,
    mut has_more: bool,
    first: Bytes,
) -> Result<(), HttpError> {
    sender
        .send(first)
        .await
        .map_err(|_| HttpError::Unavailable)?;
    loop {
        if !has_more {
            tokio::time::sleep(options.poll_interval).await;
        }
        // Reserve BEFORE authorization/DB work: never queue work behind a slow reader.
        let slot = timeout(options.send_timeout, sender.reserve())
            .await
            .map_err(|_| HttpError::Unavailable)?
            .map_err(|_| HttpError::Unavailable)?;
        let page = timeout(inner.options.deadline, async {
            let principal = inner
                .authenticator
                .authenticate(credential.clone())
                .await
                .map_err(|error| match error {
                    AgentHttpAuthenticationError::Unauthenticated => HttpError::Unauthenticated,
                    AgentHttpAuthenticationError::Unavailable => HttpError::Unavailable,
                })?;
            if principal.caller() != caller || !principal.allows(AgentHttpOperation::Read) {
                return Err(HttpError::Denied);
            }
            inner
                .service
                .load_activity(caller.clone(), run, observation.after.as_ref())
                .await
                .map_err(service_error)
        })
        .await
        .map_err(|_| HttpError::Unavailable)??;
        has_more = page.has_more();
        slot.send(observation.batch(&page)?);
    }
}

#[cfg(test)]
mod tests;
