// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Authenticated JSON HTTP v1 and opt-in activity SSE over the durable Agent service.
//! No constructor starts a listener, scheduler or provider. TLS, credential
//! verification, replica-wide ingress quotas and worker deployment are explicit
//! host responsibilities. Activity SSE is opt-in; stable release acceptance is separate.
//!
//! ```
//! use std::sync::Arc;
//! use stateknot::{agent_http::*, runtime::AgentServiceV1};
//!
//! // The application supplies a real credential verifier and resource policy.
//! fn ingress(service: AgentServiceV1, verifier: Arc<dyn AgentHttpAuthenticator>)
//!     -> Result<AgentHttpService, AgentHttpOptionsError>
//! {
//!     let options = AgentHttpOptions::new(["agents.example.com".to_owned()])?;
//!     Ok(AgentHttpService::new(service, verifier, options))
//! }
//! ```

mod auth;
mod options;
mod sse;
mod wire;
pub use auth::{
    AgentHttpAuthenticationError, AgentHttpAuthenticator, AgentHttpCredential, AgentHttpOperation,
    AgentHttpPrincipal,
};
pub use options::{AgentHttpOptions, AgentHttpOptionsError};
pub use sse::{AgentHttpActivity, AgentHttpSseOptions};
pub use wire::{AgentHttpLookup, AgentHttpRunResponse, AgentHttpSubmission};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::Response,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use stateknot_core::{AgentAdmissionIntentError, BoundedJson, EventId, JsonLimits, RunId};
use stateknot_runtime::{
    AgentCancellationIds, AgentCancellationOutcome, AgentRunAdmissionOutcome, AgentRunSnapshot,
    AgentServiceAuthorizationError, AgentServiceError, AgentServiceRegistryError, AgentServiceV1,
    DurableAgentAdmissionError, DurableAgentAdmissionRequestError, DurableAgentRunsError,
};
use stateknot_store_postgres::StoreError;
use std::{
    io::{self, Write},
    sync::Arc,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const ROOT: &str = "/v1/agent-runs";

/// Clonable bounded HTTP ingress, sharing concurrency and shutdown across routers.
#[derive(Clone)]
pub struct AgentHttpService {
    inner: Arc<Inner>,
}

struct Inner {
    service: AgentServiceV1,
    authenticator: Arc<dyn AgentHttpAuthenticator>,
    options: AgentHttpOptions,
    permits: Semaphore,
    stream_permits: Arc<Semaphore>,
    shutdown: CancellationToken,
}

impl AgentHttpService {
    /// Binds a prevalidated service and mandatory credential verifier.
    /// Configure TLS and replica/tenant quotas outside this per-process boundary.
    #[must_use]
    pub fn new(
        service: AgentServiceV1,
        authenticator: Arc<dyn AgentHttpAuthenticator>,
        options: AgentHttpOptions,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                service,
                authenticator,
                permits: Semaphore::new(options.max_in_flight),
                stream_permits: Arc::new(Semaphore::new(
                    options.sse.as_ref().map_or(0, |sse| sse.max_streams),
                )),
                options,
                shutdown: CancellationToken::new(),
            }),
        }
    }

    /// Returns the exact v1 router. No cookies, redirects, CORS or public discovery.
    pub fn router(&self) -> Router {
        Router::new()
            .fallback(handle)
            .with_state(self.inner.clone())
    }

    /// Refuses new requests and cooperatively cancels in-flight operations.
    /// Cancellation never proves rollback; callers must recover using original IDs.
    /// The host must separately stop accepting connections and bound graceful drain.
    pub fn shutdown(&self) {
        self.inner.shutdown.cancel();
    }
}

async fn handle(State(inner): State<Arc<Inner>>, request: Request) -> Response {
    let request_id = EventId::generate();
    let Ok(_permit) = inner.permits.try_acquire() else {
        return failure(HttpError::Overloaded, request_id);
    };
    let result = tokio::select! {
        biased;
        () = inner.shutdown.cancelled() => Err(HttpError::Unavailable),
        result = tokio::time::timeout(inner.options.deadline, execute(&inner, request, request_id)) =>
            result.unwrap_or(Err(HttpError::Unavailable)),
    };
    match result {
        Ok(response) => response,
        Err(error) => failure(error, request_id),
    }
}

#[allow(clippy::too_many_lines)] // Keep ingress validation order visible in one place.
async fn execute(
    inner: &Arc<Inner>,
    request: Request,
    request_id: EventId,
) -> Result<Response, HttpError> {
    let (parts, body) = request.into_parts();
    validate_origin_host(&parts.headers, &parts.uri, &inner.options)?;
    let authorization = single(&parts.headers, header::AUTHORIZATION.as_str())?
        .ok_or(HttpError::Unauthenticated)?;
    let (scheme, value) = authorization
        .split_once(' ')
        .ok_or(HttpError::Unauthenticated)?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return Err(HttpError::Unauthenticated);
    }
    let credential = AgentHttpCredential::new(value).map_err(|_| HttpError::Unauthenticated)?;
    let principal = inner
        .authenticator
        .authenticate(credential.clone())
        .await
        .map_err(|error| match error {
            AgentHttpAuthenticationError::Unauthenticated => HttpError::Unauthenticated,
            AgentHttpAuthenticationError::Unavailable => HttpError::Unavailable,
        })?;
    // Authentication precedes path/body decoding and any resource existence lookup.
    let route = route(&parts.method, &parts.uri)?;
    if !principal.allows(route.operation()) {
        return Err(HttpError::Denied);
    }
    if matches!(route, Route::Events(_)) {
        sse::validate_media(&parts.headers)?;
    } else {
        validate_media(&parts.headers, &parts.method)?;
        if parts.headers.contains_key("last-event-id") {
            return Err(HttpError::Invalid);
        }
    }
    if let Some(length) = single(&parts.headers, header::CONTENT_LENGTH.as_str())? {
        if length.is_empty() || !length.bytes().all(|b| b.is_ascii_digit()) {
            return Err(HttpError::Invalid);
        }
        if length.parse::<u64>().map_err(|_| HttpError::TooLarge)?
            > inner.options.max_request_bytes as u64
        {
            return Err(HttpError::TooLarge);
        }
    }
    let bytes = to_bytes(body, inner.options.max_request_bytes)
        .await
        .map_err(|_| HttpError::TooLarge)?;
    let caller = principal.caller().clone();
    let (status, snapshot) = match route {
        Route::Events(run) => {
            if !bytes.is_empty() {
                return Err(HttpError::Invalid);
            }
            return sse::open(
                inner.clone(),
                caller,
                credential,
                run,
                &parts.headers,
                request_id,
            )
            .await;
        }
        Route::Submit => {
            let input: AgentHttpSubmission = decode(&bytes, inner.options.max_request_bytes)?;
            let outcome = inner
                .service
                .submit(caller, &input.submission_key, &input.agent, input.request)
                .await
                .map_err(service_error)?;
            let status = if matches!(outcome, AgentRunAdmissionOutcome::Committed(_)) {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            (status, outcome.snapshot().clone())
        }
        Route::Read(run) => {
            if !bytes.is_empty() {
                return Err(HttpError::Invalid);
            }
            (
                StatusCode::OK,
                inner
                    .service
                    .load(caller, run)
                    .await
                    .map_err(service_error)?,
            )
        }
        Route::Lookup => {
            let input: AgentHttpLookup = decode(&bytes, inner.options.max_request_bytes)?;
            (
                StatusCode::OK,
                inner
                    .service
                    .load_by_key(caller, &input.submission_key)
                    .await
                    .map_err(service_error)?,
            )
        }
        Route::Cancel(run) => {
            let input: AgentCancellationIds = decode(&bytes, inner.options.max_request_bytes)?;
            let outcome = inner
                .service
                .request_cancellation(caller, run, input)
                .await
                .map_err(service_error)?;
            let status = if matches!(outcome, AgentCancellationOutcome::Committed(_)) {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            };
            (status, outcome.snapshot().clone())
        }
    };
    success(
        status,
        request_id,
        snapshot,
        inner.options.max_response_bytes,
    )
}

enum Route {
    Submit,
    Lookup,
    Read(RunId),
    Cancel(RunId),
    Events(RunId),
}
impl Route {
    const fn operation(&self) -> AgentHttpOperation {
        match self {
            Self::Submit => AgentHttpOperation::Submit,
            Self::Read(_) | Self::Lookup | Self::Events(_) => AgentHttpOperation::Read,
            Self::Cancel(_) => AgentHttpOperation::Cancel,
        }
    }
}

fn route(method: &Method, uri: &axum::http::Uri) -> Result<Route, HttpError> {
    if uri.query().is_some() || uri.path().len() > 256 || uri.path().contains('%') {
        return Err(HttpError::Invalid);
    }
    let (route, allowed) = if uri.path() == ROOT {
        (Route::Submit, Method::POST)
    } else if uri.path() == "/v1/agent-runs/lookup" {
        (Route::Lookup, Method::POST)
    } else {
        let rest = uri
            .path()
            .strip_prefix("/v1/agent-runs/")
            .ok_or(HttpError::NotFound)?;
        if let Some(id) = rest.strip_suffix("/cancellation") {
            (
                Route::Cancel(id.parse().map_err(|_| HttpError::Invalid)?),
                Method::POST,
            )
        } else {
            (
                if let Some(id) = rest.strip_suffix("/events") {
                    Route::Events(id.parse().map_err(|_| HttpError::Invalid)?)
                } else {
                    Route::Read(rest.parse().map_err(|_| HttpError::Invalid)?)
                },
                Method::GET,
            )
        }
    };
    if method != allowed {
        return Err(HttpError::Method(if allowed == Method::POST {
            "POST"
        } else {
            "GET"
        }));
    }
    Ok(route)
}

fn single<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a str>, HttpError> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(HttpError::Invalid);
    }
    first
        .map(|value| value.to_str().map_err(|_| HttpError::Invalid))
        .transpose()
}

fn validate_origin_host(
    headers: &HeaderMap,
    uri: &axum::http::Uri,
    options: &AgentHttpOptions,
) -> Result<(), HttpError> {
    let host = single(headers, header::HOST.as_str())?;
    let authority = uri.authority().map(axum::http::uri::Authority::as_str);
    if let (Some(host), Some(authority)) = (host, authority) {
        if !host.eq_ignore_ascii_case(authority) {
            return Err(HttpError::Invalid);
        }
    }
    let host = host.or(authority).ok_or(HttpError::Invalid)?;
    if !options.hosts.contains(&host.to_ascii_lowercase()) {
        return Err(HttpError::Denied);
    }
    if let Some(origin) = single(headers, header::ORIGIN.as_str())? {
        if !options.origins.contains(origin) {
            return Err(HttpError::Denied);
        }
    }
    Ok(())
}

fn validate_media(headers: &HeaderMap, method: &Method) -> Result<(), HttpError> {
    if headers.contains_key(header::CONTENT_ENCODING) {
        return Err(HttpError::Media);
    }
    if let Some(accept) = single(headers, header::ACCEPT.as_str())? {
        // JSON operations have one explicit representation.
        if !matches!(accept, "application/json" | "*/*") {
            return Err(HttpError::Accept);
        }
    }
    if method == Method::POST {
        let content = single(headers, header::CONTENT_TYPE.as_str())?.ok_or(HttpError::Media)?;
        let media = content
            .parse::<mime::Mime>()
            .map_err(|_| HttpError::Media)?;
        if media.essence_str() != "application/json"
            || media
                .params()
                .any(|(name, value)| name != mime::CHARSET || value != "utf-8")
        {
            return Err(HttpError::Media);
        }
    }
    Ok(())
}

fn decode<T: DeserializeOwned>(bytes: &[u8], max_bytes: usize) -> Result<T, HttpError> {
    let defaults = JsonLimits::DEFAULT;
    let limits = JsonLimits::try_new(
        max_bytes,
        defaults.max_depth(),
        defaults.max_container_entries(),
        defaults.max_nodes(),
        defaults.max_string_bytes(),
        defaults.max_object_key_bytes(),
    )
    .map_err(|_| HttpError::Invalid)?;
    let value =
        BoundedJson::from_slice_with_limits(bytes, limits).map_err(|_| HttpError::Invalid)?;
    serde_json::from_value(value.into_value()).map_err(|_| HttpError::Invalid)
}

// Stop serialization at the byte ceiling instead of allocating then checking.
struct LimitedWriter {
    bytes: Vec<u8>,
    maximum: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("response limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode(value: &impl Serialize, maximum: usize) -> Result<Vec<u8>, HttpError> {
    let mut writer = LimitedWriter {
        bytes: Vec::new(),
        maximum,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| HttpError::ResponseTooLarge)?;
    Ok(writer.bytes)
}

fn success(
    status: StatusCode,
    request_id: EventId,
    snapshot: AgentRunSnapshot,
    maximum: usize,
) -> Result<Response, HttpError> {
    let body = encode(
        &AgentHttpRunResponse {
            request_id,
            snapshot,
        },
        maximum,
    )?;
    Ok(response(status, request_id, body))
}

fn response(status: StatusCode, request_id: EventId, bytes: Vec<u8>) -> Response {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        "application/json".parse().expect("static media type"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        "private, no-store".parse().expect("static cache policy"),
    );
    headers.insert(header::PRAGMA, "no-cache".parse().expect("static pragma"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        "nosniff".parse().expect("static nosniff"),
    );
    headers.insert(
        "stateknot-api-version",
        "1".parse().expect("static version"),
    );
    headers.insert(
        "x-request-id",
        request_id.to_string().parse().expect("UUID header"),
    );
    response
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HttpError {
    Invalid,
    Unauthenticated,
    Denied,
    NotFound,
    Conflict,
    TooLarge,
    Media,
    Accept,
    Method(&'static str),
    Overloaded,
    Unavailable,
    Internal,
    ResponseTooLarge,
    Cursor,
}

fn error_status(error: HttpError) -> (StatusCode, &'static str) {
    match error {
        HttpError::Cursor => (StatusCode::CONFLICT, "invalid_cursor"),
        HttpError::Invalid => (StatusCode::BAD_REQUEST, "invalid_request"),
        HttpError::Unauthenticated => (StatusCode::UNAUTHORIZED, "unauthenticated"),
        HttpError::Denied => (StatusCode::FORBIDDEN, "denied"),
        HttpError::NotFound => (StatusCode::NOT_FOUND, "not_found"),
        HttpError::Conflict => (StatusCode::CONFLICT, "conflict"),
        HttpError::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "request_too_large"),
        HttpError::Media => (StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type"),
        HttpError::Accept => (StatusCode::NOT_ACCEPTABLE, "not_acceptable"),
        HttpError::Method(_) => (StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed"),
        HttpError::Overloaded => (StatusCode::TOO_MANY_REQUESTS, "overloaded"),
        HttpError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
        HttpError::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        HttpError::ResponseTooLarge => (StatusCode::INTERNAL_SERVER_ERROR, "response_too_large"),
    }
}

fn failure(error: HttpError, request_id: EventId) -> Response {
    let (status, code) = error_status(error);
    let body = serde_json::to_vec(
        &json!({"error":{"code":format!("agent_http.{code}"),"request_id":request_id}}),
    )
    .expect("bounded error envelope");
    let mut response = response(status, request_id, body);
    if error == HttpError::Unauthenticated {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            "Bearer realm=\"stateknot-agent\""
                .parse()
                .expect("static challenge"),
        );
    }
    if matches!(error, HttpError::Overloaded | HttpError::Unavailable) {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, "1".parse().expect("static backoff"));
    }
    if let HttpError::Method(allowed) = error {
        response
            .headers_mut()
            .insert(header::ALLOW, allowed.parse().expect("static method"));
    }
    response
}

#[allow(clippy::needless_pass_by_value)]
fn service_error(error: AgentServiceError) -> HttpError {
    match error {
        AgentServiceError::Authorization(error) => match error {
            AgentServiceAuthorizationError::Unauthenticated => HttpError::Unauthenticated,
            AgentServiceAuthorizationError::Denied => HttpError::Denied,
            AgentServiceAuthorizationError::Unavailable => HttpError::Unavailable,
            _ => HttpError::Internal,
        },
        AgentServiceError::Registry(AgentServiceRegistryError::MissingBinding { .. }) => {
            HttpError::NotFound
        }
        AgentServiceError::SubmissionConflict
        | AgentServiceError::ConflictingCancellation
        | AgentServiceError::TerminalRun
        | AgentServiceError::AdmissionRequest(DurableAgentAdmissionRequestError::Intent(
            AgentAdmissionIntentError::RetiredAgent,
        )) => HttpError::Conflict,
        AgentServiceError::AdmissionRequest(DurableAgentAdmissionRequestError::Intent(
            AgentAdmissionIntentError::Request { .. }
            | AgentAdmissionIntentError::NonInteroperableNumber,
        ))
        | AgentServiceError::Runs(DurableAgentRunsError::Admission(
            DurableAgentAdmissionError::InputSchema { .. },
        )) => HttpError::Invalid,
        AgentServiceError::AdmissionRequest(DurableAgentAdmissionRequestError::Intent(
            AgentAdmissionIntentError::InsufficientGrantedScopes,
        )) => HttpError::Denied,
        AgentServiceError::Store(error)
        | AgentServiceError::Runs(
            DurableAgentRunsError::Store(error)
            | DurableAgentRunsError::Admission(DurableAgentAdmissionError::Store(error)),
        ) => store_error(error),
        _ => HttpError::Internal,
    }
}

#[cfg(test)]
mod tests;

#[allow(clippy::needless_pass_by_value)]
fn store_error(error: StoreError) -> HttpError {
    match error {
        StoreError::InvalidJournalCursor => HttpError::Cursor,
        StoreError::RunNotFound
        | StoreError::AgentSubmissionNotFound
        | StoreError::AgentAdmissionNotFound => HttpError::NotFound,
        StoreError::AgentSubmissionConflict
        | StoreError::EventIdConflict
        | StoreError::ProjectionIntentConflict
        | StoreError::StaleJournalHead
        | StoreError::StaleLifecycleRevision
        | StoreError::AgentAdmissionRejected
        | StoreError::RunFailureClosing
        | StoreError::RunQuarantined
        | StoreError::RunNotRunnable => HttpError::Conflict,
        StoreError::Database { .. } => HttpError::Unavailable,
        _ => HttpError::Internal,
    }
}
