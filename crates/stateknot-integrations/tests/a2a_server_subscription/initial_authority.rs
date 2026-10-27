// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Initial preparation checks through the real router. In-process Body ownership
//! controls are named separately; client close is never used as a server Drop oracle.

use super::{
    A2A_PROTOCOL_VERSION_1_0, A2aPushConfig, A2aRequestContext, A2aServerAdmissionError,
    A2aServerAdmissionRequest, A2aServerAuthorizationError, A2aServerAuthorizationRequest,
    A2aServerOperation, A2aTaskServiceCapabilities, A2aTaskServiceError, Arc, Binding, BoxFuture,
    Context, Duration, HARNESS_WAIT, HEALTHY, Mutex, Ordering, Pin, Poll, Probe, Response, Server,
    Setup, TASK, Value, json, next_event, principal,
};
use axum::body::Body;
use bytes::Bytes;
use futures_core::Stream;
use http::{Method, Request, StatusCode};
use http_body_util::BodyExt as _;
use std::future::Future as _;
use std::sync::atomic::AtomicUsize;
use tower_service::Service as _;

pub(super) const ORIGIN: &str = "https://browser.example";
const BUDGET: Duration = Duration::from_millis(240);
const PART: Duration = Duration::from_millis(160);

#[derive(Clone, Copy, Default)]
pub(super) enum Action {
    #[default]
    Allow,
    Pending,
    Gate,
    Delay(Duration),
    Block(Duration),
    BlockRefuse(Duration),
    Refuse,
    Unavailable,
    Cancel,
    CancelRefuse,
}

#[derive(Clone)]
pub(super) struct Controls {
    pub(super) authorization: Action,
    pub(super) admission: Action,
    pub(super) authentication_delay: Duration,
    pub(super) backend_delay: Duration,
    pub(super) maximum_body_bytes: usize,
    pub(super) request_capacity: usize,
}

impl Default for Controls {
    fn default() -> Self {
        Self {
            authorization: Action::Allow,
            admission: Action::Allow,
            authentication_delay: Duration::ZERO,
            backend_delay: Duration::ZERO,
            maximum_body_bytes: 2048,
            request_capacity: 8,
        }
    }
}

#[derive(Default)]
pub(super) struct Observations {
    admissions: Mutex<Vec<A2aServerAdmissionRequest>>,
    business: Mutex<Vec<A2aServerOperation>>,
    authorization_drops: AtomicUsize,
    admission_drops: AtomicUsize,
}

struct CallbackOwner {
    probe: Arc<Probe>,
    admission: bool,
}

impl Drop for CallbackOwner {
    fn drop(&mut self) {
        let drops = if self.admission {
            &self.probe.initial.admission_drops
        } else {
            &self.probe.initial.authorization_drops
        };
        drops.fetch_add(1, Ordering::SeqCst);
        self.probe.changed.notify_one();
    }
}

async fn act(probe: &Arc<Probe>, action: Action) {
    match action {
        Action::Pending => {
            probe.pending_entered.fetch_add(1, Ordering::SeqCst);
            probe.changed.notify_one();
            std::future::pending::<()>().await;
        }
        Action::Gate => {
            probe.pending_entered.fetch_add(1, Ordering::SeqCst);
            probe.changed.notify_one();
            probe.release.acquire().await.unwrap().forget();
        }
        Action::Delay(delay) => tokio::time::sleep(delay).await,
        Action::Block(delay) | Action::BlockRefuse(delay) => std::thread::sleep(delay),
        Action::Cancel | Action::CancelRefuse => probe.shutdown.cancel(),
        Action::Allow | Action::Refuse | Action::Unavailable => {}
    }
}

pub(super) fn authorize(
    probe: Arc<Probe>,
    request: A2aServerAuthorizationRequest,
) -> BoxFuture<'static, Result<(), A2aServerAuthorizationError>> {
    probe.checks.fetch_add(1, Ordering::SeqCst);
    let healthy = request.task_id() == Some(HEALTHY);
    let action = if healthy {
        Action::Allow
    } else {
        probe.setup.initial.as_ref().unwrap().authorization
    };
    probe.requests.lock().unwrap().push(request);
    probe.changed.notify_one();
    Box::pin(async move {
        // The healthy B control still executes its real callback, but its
        // completion must not stand in for the controlled request's Drop.
        let _owner = (!healthy).then(|| CallbackOwner {
            probe: probe.clone(),
            admission: false,
        });
        act(&probe, action).await;
        match action {
            Action::Refuse | Action::CancelRefuse | Action::BlockRefuse(_) => {
                Err(A2aServerAuthorizationError::Forbidden)
            }
            Action::Unavailable => Err(A2aServerAuthorizationError::Unavailable),
            _ => Ok(()),
        }
    })
}

pub(super) fn admit(
    probe: Arc<Probe>,
    request: A2aServerAdmissionRequest,
) -> BoxFuture<'static, Result<(), A2aServerAdmissionError>> {
    // The admission API deliberately has no task/config hints. The request is
    // healthy only after the preceding recorded authorization identifies it.
    let healthy = probe.requests.lock().unwrap().last().unwrap().task_id() == Some(HEALTHY);
    let action = if healthy {
        Action::Allow
    } else {
        probe.setup.initial.as_ref().unwrap().admission
    };
    probe.admissions.fetch_add(1, Ordering::SeqCst);
    probe.initial.admissions.lock().unwrap().push(request);
    probe.changed.notify_one();
    Box::pin(async move {
        // The healthy B control still executes its real callback, but its
        // completion must not stand in for the controlled request's Drop.
        let _owner = (!healthy).then(|| CallbackOwner {
            probe: probe.clone(),
            admission: true,
        });
        act(&probe, action).await;
        match action {
            Action::Refuse | Action::CancelRefuse | Action::BlockRefuse(_) => {
                Err(A2aServerAdmissionError::Rejected)
            }
            Action::Unavailable => Err(A2aServerAdmissionError::Unavailable),
            _ => Ok(()),
        }
    })
}

pub(super) fn business<T: Send + 'static>(
    probe: Arc<Probe>,
    context: &A2aRequestContext,
    operation: A2aServerOperation,
    result: T,
) -> BoxFuture<'static, Result<T, A2aTaskServiceError>> {
    probe.initial.business.lock().unwrap().push(operation);
    probe
        .contexts
        .lock()
        .unwrap()
        .push(context.principal().clone());
    probe.changed.notify_one();
    Box::pin(async move {
        tokio::time::sleep(probe.setup.initial.as_ref().unwrap().backend_delay).await;
        Ok(result)
    })
}

pub(super) fn push_config() -> A2aPushConfig {
    A2aPushConfig::new("https://callback.example/notify")
        .unwrap()
        .with_id("cfg-a")
        .unwrap()
        .with_task_id(TASK)
        .unwrap()
}

fn setup(controls: Controls) -> Setup {
    Setup {
        enabled: false,
        service_capabilities: A2aTaskServiceCapabilities {
            streaming: true,
            push_notifications: true,
            extended_agent_card: true,
        },
        request_timeout: BUDGET,
        initial: Some(controls),
        ..Setup::default()
    }
}

#[derive(Clone, Copy, Debug)]
struct Operation {
    name: &'static str,
    kind: A2aServerOperation,
    method: MethodKind,
    path: &'static str,
    task: Option<&'static str>,
    config: Option<&'static str>,
}

#[derive(Clone, Copy, Debug)]
enum MethodKind {
    Get,
    Post,
    Delete,
}

const OPERATIONS: [Operation; 11] = [
    Operation {
        name: "SendMessage",
        kind: A2aServerOperation::SendMessage,
        method: MethodKind::Post,
        path: "/message:send",
        task: Some(TASK),
        config: None,
    },
    Operation {
        name: "SendStreamingMessage",
        kind: A2aServerOperation::SendStreamingMessage,
        method: MethodKind::Post,
        path: "/message:stream",
        task: Some(TASK),
        config: None,
    },
    Operation {
        name: "GetTask",
        kind: A2aServerOperation::GetTask,
        method: MethodKind::Get,
        path: "/tasks/task-a",
        task: Some(TASK),
        config: None,
    },
    Operation {
        name: "ListTasks",
        kind: A2aServerOperation::ListTasks,
        method: MethodKind::Get,
        path: "/tasks",
        task: None,
        config: None,
    },
    Operation {
        name: "CancelTask",
        kind: A2aServerOperation::CancelTask,
        method: MethodKind::Post,
        path: "/tasks/task-a:cancel",
        task: Some(TASK),
        config: None,
    },
    Operation {
        name: "SubscribeToTask",
        kind: A2aServerOperation::SubscribeToTask,
        method: MethodKind::Post,
        path: "/tasks/task-a:subscribe",
        task: Some(TASK),
        config: None,
    },
    Operation {
        name: a2a::methods::CREATE_PUSH_CONFIG,
        kind: A2aServerOperation::CreatePushConfig,
        method: MethodKind::Post,
        path: "/tasks/task-a/pushNotificationConfigs",
        task: Some(TASK),
        config: None,
    },
    Operation {
        name: a2a::methods::GET_PUSH_CONFIG,
        kind: A2aServerOperation::GetPushConfig,
        method: MethodKind::Get,
        path: "/tasks/task-a/pushNotificationConfigs/cfg-a",
        task: Some(TASK),
        config: Some("cfg-a"),
    },
    Operation {
        name: a2a::methods::LIST_PUSH_CONFIGS,
        kind: A2aServerOperation::ListPushConfigs,
        method: MethodKind::Get,
        path: "/tasks/task-a/pushNotificationConfigs",
        task: Some(TASK),
        config: None,
    },
    Operation {
        name: a2a::methods::DELETE_PUSH_CONFIG,
        kind: A2aServerOperation::DeletePushConfig,
        method: MethodKind::Delete,
        path: "/tasks/task-a/pushNotificationConfigs/cfg-a",
        task: Some(TASK),
        config: Some("cfg-a"),
    },
    Operation {
        name: "GetExtendedAgentCard",
        kind: A2aServerOperation::GetExtendedAgentCard,
        method: MethodKind::Get,
        path: "/extendedAgentCard",
        task: None,
        config: None,
    },
];

fn params(operation: Operation) -> Value {
    match operation.kind {
        A2aServerOperation::SendMessage | A2aServerOperation::SendStreamingMessage => {
            json!({"message":{"messageId":"initial-authority-message","role":"ROLE_USER","parts":[{"text":"fixture"}],"taskId":TASK}})
        }
        A2aServerOperation::GetTask
        | A2aServerOperation::CancelTask
        | A2aServerOperation::SubscribeToTask => json!({"id":TASK}),
        A2aServerOperation::CreatePushConfig => {
            json!({"taskId":TASK,"id":"cfg-a","url":"https://callback.example/notify"})
        }
        A2aServerOperation::GetPushConfig | A2aServerOperation::DeletePushConfig => {
            json!({"taskId":TASK,"id":"cfg-a"})
        }
        A2aServerOperation::ListPushConfigs => json!({"taskId":TASK}),
        _ => json!({}),
    }
}

fn request(server: &Server, binding: Binding, operation: Operation) -> reqwest::RequestBuilder {
    let builder = match binding {
        Binding::Rpc => server.client.post(format!("{}/a2a/jsonrpc", server.url))
            .json(&json!({"jsonrpc":"2.0","id":42,"method":operation.name,"params":params(operation)})),
        Binding::Rest => {
            let url = format!("{}/a2a/rest{}", server.url, operation.path);
            match operation.method {
                MethodKind::Get => server.client.get(url),
                MethodKind::Delete => server.client.delete(url),
                MethodKind::Post => server.client.post(url).header("Content-Type", "application/a2a+json").json(&params(operation)),
            }
        }
    };
    authenticate(builder)
}

fn authenticate(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    builder
        .header("Host", "agent.example")
        .header("A2A-Version", A2A_PROTOCOL_VERSION_1_0)
        .bearer_auth("subscription-token")
}

async fn send(builder: reqwest::RequestBuilder) -> Response {
    tokio::time::timeout(HARNESS_WAIT, builder.send())
        .await
        .expect("HTTP fixture deadline")
        .unwrap()
}

fn assert_facts(probe: &Probe, operation: Operation, admissions: usize, business_count: usize) {
    let requests = probe.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.principal(), &principal(false));
    assert_eq!(request.operation(), operation.kind);
    assert_eq!(request.task_id(), operation.task);
    assert_eq!(request.config_id(), operation.config);
    assert_eq!(
        request.principal().scopes().collect::<Vec<_>>(),
        ["scope-a", "scope-z"]
    );
    let facts = probe.initial.admissions.lock().unwrap();
    assert_eq!(facts.len(), admissions);
    for fact in facts.iter() {
        assert_eq!(fact.principal(), &principal(false));
        assert_eq!(fact.operation(), operation.kind);
    }
    let business = probe.initial.business.lock().unwrap();
    assert_eq!(business.len(), business_count);
    for actual in business.iter() {
        assert_eq!(*actual, operation.kind);
    }
    for context in probe.contexts.lock().unwrap().iter() {
        assert_eq!(context, &principal(false));
    }
}

fn expected_rest(error: &a2a::A2AError) -> Value {
    let rpc = serde_json::to_value(error.to_jsonrpc_error()).unwrap();
    let status = error.http_status_code();
    let grpc_status = match rpc["code"].as_i64().unwrap() {
        -32001 => "NOT_FOUND",
        _ => "INTERNAL",
    };
    json!({"error":{"code":status,"status":grpc_status,"message":rpc["message"],"details":rpc["data"]}})
}

fn normalize_rest(value: &mut Value, reason: &str) {
    let object = value.as_object_mut().unwrap();
    assert_eq!(object.len(), 1);
    let error = object.get_mut("error").unwrap().as_object_mut().unwrap();
    assert_eq!(error.len(), 4);
    assert!(
        ["code", "status", "message", "details"]
            .iter()
            .all(|k| error.contains_key(*k))
    );
    let mut rpc_shape =
        json!({"code":error["code"],"message":error["message"],"data":error["details"]});
    super::normalize_error_timestamp(&mut rpc_shape, reason);
    error.insert("details".to_owned(), rpc_shape["data"].clone());
}

async fn wire_error(binding: Binding, response: Response, expected: &a2a::A2AError) {
    let status = match binding {
        Binding::Rest => StatusCode::from_u16(expected.http_status_code()).unwrap(),
        Binding::Rpc => StatusCode::OK,
    };
    assert_eq!(response.status(), status);
    assert!(
        response.headers()["Content-Type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    let value: Value = response.json().await.unwrap();
    if matches!(binding, Binding::Rpc) {
        super::assert_error(binding, &value, expected);
    } else {
        let mut actual = value.clone();
        let mut wanted = expected_rest(expected);
        let reason = wanted["error"]["details"][0]["reason"]
            .as_str()
            .unwrap()
            .to_owned();
        normalize_rest(&mut actual, &reason);
        normalize_rest(&mut wanted, &reason);
        assert_eq!(actual, wanted);
        assert!(!value.to_string().contains("subscription-token"));
        assert!(!value.to_string().contains(TASK));
    }
}

async fn normal(binding: Binding, operation: Operation) {
    let server = Server::start(setup(Controls::default())).await;
    let mut response = send(request(&server, binding, operation)).await;
    assert!(
        response.status().is_success(),
        "valid {:?}: {}",
        operation,
        response.status()
    );
    if matches!(
        operation.kind,
        A2aServerOperation::SendStreamingMessage | A2aServerOperation::SubscribeToTask
    ) {
        let event = next_event(&mut response, &mut Vec::new()).await.unwrap();
        assert_eq!(super::payload(binding, &event)["task"]["id"], TASK);
    } else {
        let body = response.bytes().await.unwrap();
        if !body.is_empty() {
            let value: Value = serde_json::from_slice(&body).unwrap();
            assert!(
                value.get("error").is_none(),
                "normal request returned error: {value}"
            );
        }
    }
    assert_facts(&server.probe, operation, 1, 1);
}

async fn policy_case(
    binding: Binding,
    operation: Operation,
    admission: bool,
    action: Action,
    error: &a2a::A2AError,
) {
    let controls = if admission {
        Controls {
            admission: action,
            ..Controls::default()
        }
    } else {
        Controls {
            authorization: action,
            ..Controls::default()
        }
    };
    let server = Server::start(setup(controls)).await;
    wire_error(
        binding,
        send(request(&server, binding, operation)).await,
        error,
    )
    .await;
    assert_facts(&server.probe, operation, usize::from(admission), 0);
    assert_eq!(
        server
            .probe
            .initial
            .authorization_drops
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(
        server.probe.initial.admission_drops.load(Ordering::SeqCst),
        usize::from(admission)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_policy_all_eleven_operations_preserve_facts_errors_and_shared_timeout() {
    for binding in [Binding::Rest, Binding::Rpc] {
        for operation in OPERATIONS {
            normal(binding, operation).await;
            policy_case(
                binding,
                operation,
                false,
                Action::Refuse,
                &a2a::A2AError::task_not_found("authorized resource"),
            )
            .await;
            policy_case(
                binding,
                operation,
                false,
                Action::Unavailable,
                &a2a::A2AError::internal("A2A authorization is unavailable"),
            )
            .await;
            policy_case(
                binding,
                operation,
                true,
                Action::Refuse,
                &a2a::A2AError::internal("A2A request is rate limited"),
            )
            .await;
            policy_case(
                binding,
                operation,
                true,
                Action::Unavailable,
                &a2a::A2AError::internal("A2A admission is unavailable"),
            )
            .await;
            for admission in [false, true] {
                policy_case(
                    binding,
                    operation,
                    admission,
                    Action::Pending,
                    &a2a::A2AError::internal("A2A operation timed out"),
                )
                .await;
            }
        }
    }
}

async fn cumulative(binding: Binding) {
    let server = Server::start(setup(Controls {
        authorization: Action::Delay(PART),
        admission: Action::Delay(PART),
        ..Controls::default()
    }))
    .await;
    wire_error(
        binding,
        send(request(&server, binding, OPERATIONS[2])).await,
        &a2a::A2AError::internal("A2A operation timed out"),
    )
    .await;
    assert_facts(&server.probe, OPERATIONS[2], 1, 0);
    let server = Server::start(setup(Controls {
        authentication_delay: PART,
        authorization: Action::Delay(PART),
        backend_delay: PART,
        ..Controls::default()
    }))
    .await;
    let response = send(request(&server, binding, OPERATIONS[2])).await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert!(value.get("error").is_none());
    assert_facts(&server.probe, OPERATIONS[2], 1, 1);
}

async fn pending_recovery(binding: Binding, admission: bool) {
    let controls = if admission {
        Controls {
            admission: Action::Pending,
            request_capacity: 1,
            ..Controls::default()
        }
    } else {
        Controls {
            authorization: Action::Pending,
            request_capacity: 1,
            ..Controls::default()
        }
    };
    let server = Server::start(setup(controls)).await;
    // Keep the complete first error Response alive, including its unread body.
    let first = send(request(&server, binding, OPERATIONS[2])).await;
    assert_eq!(
        first.status(),
        match binding {
            Binding::Rest => StatusCode::INTERNAL_SERVER_ERROR,
            Binding::Rpc => StatusCode::OK,
        }
    );
    let second = send(server.request(binding, "GetTask", HEALTHY)).await;
    assert_eq!(second.status(), StatusCode::OK);
    let value: Value = second.json().await.unwrap();
    assert!(value.get("error").is_none());
    wire_error(
        binding,
        first,
        &a2a::A2AError::internal("A2A operation timed out"),
    )
    .await;
    assert_eq!(server.probe.initial.business.lock().unwrap().len(), 1);
}

async fn pending_does_not_hold_operation(binding: Binding, admission: bool) {
    let controls = if admission {
        Controls {
            admission: Action::Gate,
            ..Controls::default()
        }
    } else {
        Controls {
            authorization: Action::Gate,
            ..Controls::default()
        }
    };
    let mut options = setup(controls);
    options.request_timeout = Duration::from_secs(2);
    let server = Server::start(options).await;
    let builder = request(&server, binding, OPERATIONS[2]);
    let first = tokio::spawn(async move { send(builder).await });
    server
        .probe
        .wait_for(|| server.probe.pending_entered.load(Ordering::SeqCst) == 1)
        .await;
    let response = send(server.request(binding, "GetTask", HEALTHY)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let value: Value = response.json().await.unwrap();
    assert!(value.get("error").is_none());
    assert!(
        !first.is_finished(),
        "controlled policy barrier must still own A"
    );
    assert_eq!(server.probe.initial.business.lock().unwrap().len(), 1);
    let drops = if admission {
        &server.probe.initial.admission_drops
    } else {
        &server.probe.initial.authorization_drops
    };
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "A still owns its callback after B completes"
    );
    server.probe.release.add_permits(1);
    let response = first.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .json::<Value>()
            .await
            .unwrap()
            .get("error")
            .is_none()
    );
    assert_eq!(server.probe.initial.business.lock().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_policy_deadline_shutdown_and_capacity_are_owned() {
    for binding in [Binding::Rest, Binding::Rpc] {
        cumulative(binding).await;
        for admission in [false, true] {
            pending_recovery(binding, admission).await;
            pending_does_not_hold_operation(binding, admission).await;
            for action in [
                Action::Block(BUDGET + Duration::from_millis(60)),
                Action::BlockRefuse(BUDGET + Duration::from_millis(60)),
                Action::Cancel,
                Action::CancelRefuse,
            ] {
                let error = if matches!(action, Action::Block(_) | Action::BlockRefuse(_)) {
                    a2a::A2AError::internal("A2A operation timed out")
                } else {
                    a2a::A2AError::internal("A2A service is shutting down")
                };
                policy_case(binding, OPERATIONS[2], admission, action, &error).await;
            }
            let controls = if admission {
                Controls {
                    admission: Action::Pending,
                    ..Controls::default()
                }
            } else {
                Controls {
                    authorization: Action::Pending,
                    ..Controls::default()
                }
            };
            let server = Server::start(setup(controls)).await;
            let builder = request(&server, binding, OPERATIONS[2]);
            let running = tokio::spawn(async move { send(builder).await });
            server
                .probe
                .wait_for(|| server.probe.pending_entered.load(Ordering::SeqCst) == 1)
                .await;
            server.shutdown.cancel();
            wire_error(
                binding,
                running.await.unwrap(),
                &a2a::A2AError::internal("A2A service is shutting down"),
            )
            .await;
            assert_facts(&server.probe, OPERATIONS[2], usize::from(admission), 0);
            assert_eq!(
                if admission {
                    server.probe.initial.admission_drops.load(Ordering::SeqCst)
                } else {
                    server
                        .probe
                        .initial
                        .authorization_drops
                        .load(Ordering::SeqCst)
                },
                1
            );
        }
    }
}

// An actual upload stream: prefix is sent immediately, then a finite delay or
// an infinite pending body. These counters describe the client only. Server
// ownership is tested independently below with a direct Router future.
struct Upload {
    prefix: Option<Bytes>,
    tail: Option<Bytes>,
    delay: Pin<Box<tokio::time::Sleep>>,
    complete: bool,
}

impl Stream for Upload {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(prefix) = self.prefix.take() {
            return Poll::Ready(Some(Ok(prefix)));
        }
        if !self.complete {
            return Poll::Pending;
        }
        if self.delay.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        Poll::Ready(self.tail.take().map(Ok))
    }
}

fn upload(bytes: Vec<u8>, complete: bool) -> reqwest::Body {
    let mut bytes = bytes;
    let first = bytes.remove(0);
    let tail = complete.then(|| Bytes::from(bytes));
    reqwest::Body::wrap_stream(Upload {
        prefix: Some(Bytes::copy_from_slice(&[first])),
        tail,
        delay: Box::pin(tokio::time::sleep(PART)),
        complete,
    })
}

fn body_request(
    server: &Server,
    binding: Binding,
    operation: Operation,
    body: reqwest::Body,
) -> reqwest::RequestBuilder {
    // RequestBuilder::body replaces JSON's bytes while preserving its exact
    // media type. reqwest emits normal HTTP framing; no synthetic response.
    request(server, binding, operation).body(body)
}

fn body_bytes(binding: Binding, operation: Operation) -> Vec<u8> {
    let value = match binding {
        Binding::Rest => params(operation),
        Binding::Rpc => {
            json!({"jsonrpc":"2.0","id":42,"method":operation.name,"params":params(operation)})
        }
    };
    serde_json::to_vec(&value).unwrap()
}

async fn body_plain(response: Response, status: StatusCode, text: &str) {
    assert_eq!(response.status(), status);
    assert_eq!(
        response.headers()["Content-Type"],
        "text/plain; charset=utf-8"
    );
    assert_eq!(response.text().await.unwrap(), text);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_uploads_share_the_initial_policy_deadline_and_shutdown() {
    for (binding, operation) in [
        (Binding::Rest, OPERATIONS[0]),
        (Binding::Rest, OPERATIONS[1]),
        (Binding::Rest, OPERATIONS[6]),
        (Binding::Rpc, OPERATIONS[2]),
    ] {
        let server = Server::start(setup(Controls {
            request_capacity: 1,
            ..Controls::default()
        }))
        .await;
        let response = send(body_request(
            &server,
            binding,
            operation,
            upload(body_bytes(binding, operation), false),
        ))
        .await;
        assert_eq!(server.probe.authn.load(Ordering::SeqCst), 1);
        assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
        assert!(server.probe.initial.business.lock().unwrap().is_empty());
        body_plain(response, StatusCode::REQUEST_TIMEOUT, "request timed out").await;
        let response = send(server.request(binding, "GetTask", HEALTHY)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .json::<Value>()
                .await
                .unwrap()
                .get("error")
                .is_none()
        );

        let server = Server::start(setup(Controls {
            authorization: Action::Delay(PART),
            ..Controls::default()
        }))
        .await;
        wire_error(
            binding,
            send(body_request(
                &server,
                binding,
                operation,
                upload(body_bytes(binding, operation), true),
            ))
            .await,
            &a2a::A2AError::internal("A2A operation timed out"),
        )
        .await;
        assert_facts(&server.probe, operation, 0, 0);

        let server = Server::start(setup(Controls::default())).await;
        let builder = body_request(
            &server,
            binding,
            operation,
            upload(body_bytes(binding, operation), false),
        );
        let running = tokio::spawn(async move { send(builder).await });
        server
            .probe
            .wait_for(|| server.probe.authn.load(Ordering::SeqCst) == 1)
            .await;
        // Authentication returning is independent of the pending body; give
        // it a turn before requesting global shutdown, not a timer extension.
        tokio::task::yield_now().await;
        server.shutdown.cancel();
        body_plain(
            running.await.unwrap(),
            StatusCode::SERVICE_UNAVAILABLE,
            "service unavailable",
        )
        .await;
        assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
        assert!(server.probe.initial.business.lock().unwrap().is_empty());
    }
}

fn padded_push(size: usize) -> Vec<u8> {
    let mut body = serde_json::to_vec(&params(OPERATIONS[6])).unwrap();
    assert!(size >= body.len());
    body.resize(size, b' ');
    body
}

async fn push_bytes(server: &Server, bytes: Vec<u8>, streamed: bool) -> Response {
    let builder = request(server, Binding::Rest, OPERATIONS[6]).header("Origin", ORIGIN);
    let builder = if streamed {
        builder.body(reqwest::Body::wrap_stream(futures_util::stream::iter([
            Ok::<_, std::io::Error>(Bytes::from(bytes)),
        ])))
    } else {
        builder.body(bytes)
    };
    send(builder).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_buffering_uses_configured_limit_and_original_sdk_rejections() {
    let maximum = 2 * 1024 * 1024 + 1024;
    for streamed in [false, true] {
        let mut options = setup(Controls {
            maximum_body_bytes: maximum,
            ..Controls::default()
        });
        options.request_timeout = Duration::from_secs(2);
        let server = Server::start(options).await;
        let response = push_bytes(&server, padded_push(maximum), streamed).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .json::<Value>()
                .await
                .unwrap()
                .get("error")
                .is_none()
        );
        assert_facts(&server.probe, OPERATIONS[6], 1, 1);
        let server = Server::start(setup(Controls {
            maximum_body_bytes: maximum,
            ..Controls::default()
        }))
        .await;
        let response = push_bytes(&server, padded_push(maximum + 1), streamed).await;
        assert_eq!(response.headers()["Access-Control-Allow-Origin"], ORIGIN);
        assert_eq!(response.headers()["Vary"], "Origin");
        body_plain(
            response,
            StatusCode::PAYLOAD_TOO_LARGE,
            "Failed to buffer the request body: length limit exceeded",
        )
        .await;
        assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
        assert!(server.probe.initial.business.lock().unwrap().is_empty());
    }
    for raw in ["{", "{\"url\":4}"] {
        let baseline = sdk_push_error(raw).await;
        let server = Server::start(setup(Controls::default())).await;
        let response = send(request(&server, Binding::Rest, OPERATIONS[6]).body(raw)).await;
        assert_eq!(response.status(), baseline.0);
        assert_eq!(response.headers()["Content-Type"], baseline.1);
        let actual = response.bytes().await.unwrap();
        if raw == "{" {
            assert_eq!(actual, baseline.2);
        } else {
            let mut actual: Value = serde_json::from_slice(&actual).unwrap();
            let mut expected: Value = serde_json::from_slice(&baseline.2).unwrap();
            normalize_sdk_error_info(&mut actual);
            normalize_sdk_error_info(&mut expected);
            assert_eq!(actual, expected);
        }
        assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
        assert!(server.probe.initial.business.lock().unwrap().is_empty());
    }
    // Compound-invalid UTF-8 path plus body is deliberately only a rejection
    // oracle: the owner permits changed rejection precedence for this case.
    let server = Server::start(setup(Controls::default())).await;
    let response = send(
        authenticate(server.client.post(format!(
            "{}/a2a/rest/tasks/%FF/pushNotificationConfigs",
            server.url
        )))
        .header("Content-Type", "application/json")
        .body("{"),
    )
    .await;
    assert!(response.status().is_client_error());
    assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
}

fn owned_request(binding: Binding, operation: Operation, body: Body) -> Request<Body> {
    let (method, path) = match binding {
        Binding::Rpc => (Method::POST, "/a2a/jsonrpc".to_owned()),
        Binding::Rest => (
            match operation.method {
                MethodKind::Get => Method::GET,
                MethodKind::Post => Method::POST,
                MethodKind::Delete => Method::DELETE,
            },
            format!("/a2a/rest{}", operation.path),
        ),
    };
    Request::builder()
        .method(method)
        .uri(path)
        .header("Host", "agent.example")
        .header("A2A-Version", A2A_PROTOCOL_VERSION_1_0)
        .header("Authorization", "Bearer subscription-token")
        .header("Content-Type", "application/json")
        .body(body)
        .unwrap()
}

async fn owned_call(mut router: axum::Router, request: Request<Body>) -> http::Response<Body> {
    std::future::poll_fn(|cx| {
        <axum::Router as tower_service::Service<Request<Body>>>::poll_ready(&mut router, cx)
    })
    .await
    .unwrap();
    router.call(request).await.unwrap()
}

struct OwnedBody {
    bytes: Option<Bytes>,
    drops: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
    action: BodyAction,
}

enum BodyAction {
    Pending,
    Block(Duration),
    BlockError(Duration),
    Cancel(tokio_util::sync::CancellationToken),
    CancelError(tokio_util::sync::CancellationToken),
    Error,
}

impl Stream for OwnedBody {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        match &self.action {
            BodyAction::Pending => return Poll::Pending,
            BodyAction::Block(duration) => std::thread::sleep(*duration),
            BodyAction::BlockError(duration) => {
                std::thread::sleep(*duration);
                return Poll::Ready(Some(Err(std::io::Error::other("late controlled IO"))));
            }
            BodyAction::Cancel(token) => token.cancel(),
            BodyAction::CancelError(token) => {
                token.cancel();
                return Poll::Ready(Some(Err(std::io::Error::other("cancelled controlled IO"))));
            }
            BodyAction::Error => {
                return Poll::Ready(Some(Err(std::io::Error::other("controlled body IO"))));
            }
        }
        Poll::Ready(self.bytes.take().map(Ok))
    }
}

impl Drop for OwnedBody {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

async fn owned_body_case(action: BodyAction, status: StatusCode, text: &str) {
    let server = Server::start(setup(Controls::default())).await;
    let drops = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let body = Body::from_stream(OwnedBody {
        bytes: Some(Bytes::from(body_bytes(Binding::Rest, OPERATIONS[6]))),
        drops: drops.clone(),
        polls: polls.clone(),
        action,
    });
    let response = owned_call(
        server.router.clone(),
        owned_request(Binding::Rest, OPERATIONS[6], body),
    )
    .await;
    assert_eq!(response.status(), status);
    assert_eq!(
        response.headers()["Content-Type"],
        "text/plain; charset=utf-8"
    );
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .as_ref(),
        text.as_bytes()
    );
    assert!(polls.load(Ordering::SeqCst) > 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
    assert!(server.probe.initial.business.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_router_owns_body_and_callback_futures_and_checks_ready_clock() {
    owned_body_case(
        BodyAction::Block(BUDGET + Duration::from_millis(60)),
        StatusCode::REQUEST_TIMEOUT,
        "request timed out",
    )
    .await;
    owned_body_case(
        BodyAction::Error,
        StatusCode::BAD_REQUEST,
        "Failed to buffer the request body: controlled body IO",
    )
    .await;
    owned_body_case(
        BodyAction::BlockError(BUDGET + Duration::from_millis(60)),
        StatusCode::REQUEST_TIMEOUT,
        "request timed out",
    )
    .await;
    owned_request_drop().await;
    owned_ready_shutdown(false).await;
    owned_ready_shutdown(true).await;
}

async fn owned_request_drop() {
    // A direct owner drop is observable; no assertion equates client close
    // with a server future being dropped by Hyper.
    for phase in 0..3 {
        let controls = match phase {
            1 => Controls {
                authorization: Action::Pending,
                ..Controls::default()
            },
            2 => Controls {
                admission: Action::Pending,
                ..Controls::default()
            },
            _ => Controls::default(),
        };
        let server = Server::start(setup(controls)).await;
        let drops = Arc::new(AtomicUsize::new(0));
        let polls = Arc::new(AtomicUsize::new(0));
        let body = if phase == 0 {
            Body::from_stream(OwnedBody {
                bytes: None,
                drops: drops.clone(),
                polls: polls.clone(),
                action: BodyAction::Pending,
            })
        } else {
            Body::from(body_bytes(Binding::Rpc, OPERATIONS[2]))
        };
        let mut future = Box::pin(owned_call(
            server.router.clone(),
            owned_request(Binding::Rpc, OPERATIONS[2], body),
        ));
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        if phase == 0 {
            assert!(polls.load(Ordering::SeqCst) > 0);
        } else {
            assert_eq!(server.probe.pending_entered.load(Ordering::SeqCst), 1);
        }
        drop(future);
        if phase == 0 {
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(
                if phase == 1 {
                    server
                        .probe
                        .initial
                        .authorization_drops
                        .load(Ordering::SeqCst)
                } else {
                    server.probe.initial.admission_drops.load(Ordering::SeqCst)
                },
                1
            );
        }
        assert!(server.probe.initial.business.lock().unwrap().is_empty());
        let response = send(server.request(Binding::Rest, "GetTask", HEALTHY)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}

async fn owned_ready_shutdown(error: bool) {
    let server = Server::start(setup(Controls::default())).await;
    let drops = Arc::new(AtomicUsize::new(0));
    let body = Body::from_stream(OwnedBody {
        bytes: Some(Bytes::from(body_bytes(Binding::Rest, OPERATIONS[6]))),
        drops: drops.clone(),
        polls: Arc::new(AtomicUsize::new(0)),
        action: if error {
            BodyAction::CancelError(server.shutdown.clone())
        } else {
            BodyAction::Cancel(server.shutdown.clone())
        },
    });
    let response = owned_call(
        server.router.clone(),
        owned_request(Binding::Rest, OPERATIONS[6], body),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .as_ref(),
        b"service unavailable"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_body_routes_and_capability_checks_keep_their_original_order() {
    for index in [2, 3, 4, 5, 7, 8, 9, 10] {
        let server = Server::start(setup(Controls::default())).await;
        let response = send(body_request(
            &server,
            Binding::Rest,
            OPERATIONS[index],
            upload(vec![b'{'], false),
        ))
        .await;
        assert!(response.status().is_success());
        assert_facts(&server.probe, OPERATIONS[index], 1, 1);
    }
    for (path, method) in [
        ("/.well-known/agent-card.json", Method::GET),
        ("/a2a/rest/not-an-operation", Method::POST),
        ("/a2a/rest/tasks", Method::PATCH),
    ] {
        let server = Server::start(setup(Controls::default())).await;
        let response = send(
            authenticate(
                server
                    .client
                    .request(method, format!("{}{path}", server.url)),
            )
            .header("Content-Type", "application/json")
            .body(upload(vec![b'{'], false)),
        )
        .await;
        assert!(response.status().is_success() || response.status().is_client_error());
        assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
        assert!(server.probe.initial.business.lock().unwrap().is_empty());
    }
    for binding in [Binding::Rest, Binding::Rpc] {
        let mut options = setup(Controls::default());
        options.service_capabilities = A2aTaskServiceCapabilities::default();
        let server = Server::start(options).await;
        for index in [1, 5, 6, 7, 8, 9, 10] {
            let response = send(request(&server, binding, OPERATIONS[index])).await;
            assert!(response.status().is_client_error() || matches!(binding, Binding::Rpc));
            assert!(
                response
                    .json::<Value>()
                    .await
                    .unwrap()
                    .get("error")
                    .is_some()
            );
        }
        assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
        let server = Server::start(setup(Controls::default())).await;
        let response =
            send(request(&server, binding, OPERATIONS[2]).header("A2A-Extensions", "not a URI"))
                .await;
        assert!(
            response
                .json::<Value>()
                .await
                .unwrap()
                .get("error")
                .is_some()
        );
        assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
    }
    // Existing media-type rejection occurs before body polling.
    let server = Server::start(setup(Controls::default())).await;
    let response = send(
        authenticate(
            server
                .client
                .post(format!("{}/a2a/rest/message:send", server.url)),
        )
        .header("Content-Type", "text/plain")
        .body(upload(vec![b'{'], false)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(server.probe.authn.load(Ordering::SeqCst), 0);
    assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
}

struct NoSdkBusiness;

#[async_trait::async_trait]
impl a2a_server::AgentExecutor for NoSdkBusiness {
    fn execute(
        &self,
        _: a2a_server::ExecutorContext,
    ) -> futures_util::stream::BoxStream<'static, Result<a2a::StreamResponse, a2a::A2AError>> {
        panic!("invalid body unexpectedly entered the SDK executor")
    }
    fn cancel(
        &self,
        _: a2a_server::ExecutorContext,
    ) -> futures_util::stream::BoxStream<'static, Result<a2a::StreamResponse, a2a::A2AError>> {
        panic!("invalid body unexpectedly entered SDK cancellation")
    }
}

async fn sdk_push_error(raw: &'static str) -> (StatusCode, http::HeaderValue, Bytes) {
    let handler =
        a2a_server::DefaultRequestHandler::new(NoSdkBusiness, a2a_server::InMemoryTaskStore::new());
    let router = a2a_server::rest::rest_router(Arc::new(handler));
    let request = Request::builder()
        .method(Method::POST)
        .uri("/tasks/task-a/pushNotificationConfigs")
        .header("Content-Type", "application/a2a+json")
        .header("A2A-Version", A2A_PROTOCOL_VERSION_1_0)
        .body(Body::from(raw))
        .unwrap();
    let response = owned_call(router, request).await;
    let content_type = response.headers()["Content-Type"].clone();
    (
        response.status(),
        content_type,
        response.into_body().collect().await.unwrap().to_bytes(),
    )
}

fn normalize_sdk_error_info(value: &mut Value) {
    let outer = value.as_object_mut().unwrap();
    assert_eq!(outer.len(), 1);
    let error = outer.get_mut("error").unwrap().as_object_mut().unwrap();
    assert_eq!(error.len(), 4);
    assert!(
        ["code", "status", "message", "details"]
            .iter()
            .all(|k| error.contains_key(*k))
    );
    assert_eq!(error["code"], 400);
    assert_eq!(error["status"], "INVALID_ARGUMENT");
    assert_eq!(error["message"], "invalid request body");
    let details = error.get_mut("details").unwrap().as_array_mut().unwrap();
    assert_eq!(details.len(), 2);
    assert_eq!(
        details[0]["@type"],
        "type.googleapis.com/google.rpc.BadRequest"
    );
    let mut envelope =
        json!({"code":400,"message":"invalid request body","data":[details[1].clone()]});
    super::normalize_error_timestamp(&mut envelope, "INVALID_PARAMS");
    details[1] = envelope["data"][0].clone();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_rest_send_and_rpc_early_errors_keep_complete_wire_values() {
    for binding in [Binding::Rest, Binding::Rpc] {
        let server = Server::start(setup(Controls::default())).await;
        let response = send(
            request(&server, binding, OPERATIONS[0])
                .header("Origin", ORIGIN)
                .body(vec![b' '; 2049]),
        )
        .await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            !response
                .headers()
                .contains_key("Access-Control-Allow-Origin")
        );
        match binding {
            Binding::Rest => {
                assert_eq!(response.headers()["Content-Type"], "application/json");
                assert_eq!(
                    response.json::<Value>().await.unwrap(),
                    json!({"error":{"code":413,"status":"RESOURCE_EXHAUSTED","message":"A2A request exceeds the configured boundary","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"INVALID_REQUEST","domain":"a2a-protocol.org","metadata":{}}]}})
                );
            }
            Binding::Rpc => {
                body_plain(
                    response,
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body is too large",
                )
                .await;
            }
        }
        assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
        assert!(server.probe.initial.business.lock().unwrap().is_empty());
    }
    let server = Server::start(setup(Controls::default())).await;
    for (raw, code, message) in [
        ("{", -32700, "Parse error"),
        ("[]", -32600, "Invalid Request"),
    ] {
        let response = send(request(&server, Binding::Rpc, OPERATIONS[2]).body(raw)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.json::<Value>().await.unwrap(),
            json!({"jsonrpc":"2.0","id":null,"error":{"code":code,"message":message}})
        );
    }
    assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
    assert!(server.probe.initial.business.lock().unwrap().is_empty());
}

async fn truncated_request(address: &str, path: &str) -> Option<(u16, String, Vec<u8>)> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: agent.example\r\nA2A-Version: {A2A_PROTOCOL_VERSION_1_0}\r\nAuthorization: Bearer subscription-token\r\nContent-Type: application/a2a+json\r\nContent-Length: 256\r\nConnection: close\r\n\r\n{{"
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    socket.shutdown().await.unwrap();
    let mut raw = Vec::new();
    let result = tokio::time::timeout(HARNESS_WAIT, socket.read_to_end(&mut raw))
        .await
        .unwrap();
    if let Err(error) = result {
        assert!(matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
        ));
    }
    if raw.is_empty() {
        return None;
    }
    let end = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("complete response headers");
    let headers = String::from_utf8(raw[..end].to_vec()).unwrap();
    let mut lines = headers.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let headers = lines
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.to_ascii_lowercase(), value.trim().to_owned())
        })
        .collect::<std::collections::HashMap<_, _>>();
    let body = raw[end + 4..].to_vec();
    assert_eq!(
        headers
            .get("content-length")
            .unwrap()
            .parse::<usize>()
            .unwrap(),
        body.len()
    );
    Some((status, headers["content-type"].clone(), body))
}

struct ServerTask(tokio::task::JoinHandle<()>);
impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncated_tcp_push_compares_original_sdk_response_when_transport_exposes_one() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let handler =
        a2a_server::DefaultRequestHandler::new(NoSdkBusiness, a2a_server::InMemoryTaskStore::new());
    let router = a2a_server::rest::rest_router(Arc::new(handler));
    let _owner = ServerTask(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    let original = truncated_request(&address, "/tasks/task-a/pushNotificationConfigs").await;
    let server = Server::start(setup(Controls::default())).await;
    let current = truncated_request(
        server.url.strip_prefix("http://").unwrap(),
        "/a2a/rest/tasks/task-a/pushNotificationConfigs",
    )
    .await;
    assert_eq!(
        current, original,
        "compare an observable complete error; an empty close is a transport result, not an invented SDK400"
    );
    if let Some((status, content_type, _)) = current {
        assert_eq!(status, 400);
        assert_eq!(content_type, "text/plain; charset=utf-8");
    }
    assert_eq!(server.probe.checks.load(Ordering::SeqCst), 0);
    assert!(server.probe.initial.business.lock().unwrap().is_empty());
}
