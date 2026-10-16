// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real HTTP guard tests. The controlled stream/policy are test instruments,
//! not an identity provider, durable task backend, or production subscription implementation.

use futures_core::Stream;
use futures_util::stream;
use reqwest::{Client, Response};
use serde_json::{Value, json};
use stateknot_core::{BoxFuture, IssuerId, PrincipalIdentity, SubjectId, TenantId};
use stateknot_integrations::{
    A2A_PROTOCOL_VERSION_1_0, A2aAgentCapabilities, A2aAgentCard, A2aAgentInterface, A2aAgentSkill,
    A2aBinding, A2aCancelTaskRequest, A2aDeletePushConfigRequest, A2aEventStream,
    A2aGetPushConfigRequest, A2aGetTaskRequest, A2aListPushConfigsRequest, A2aListTasksRequest,
    A2aPushConfig, A2aPushConfigPage, A2aRequestContext, A2aSendMessageRequest,
    A2aSendMessageResponse, A2aServer, A2aServerAdmissionControl, A2aServerAdmissionError,
    A2aServerAdmissionRequest, A2aServerAuthenticationError, A2aServerAuthenticationRequest,
    A2aServerAuthenticator, A2aServerAuthorizationError, A2aServerAuthorizationRequest,
    A2aServerAuthorizer, A2aServerHttpOptions, A2aServerOperation, A2aServerPrincipal,
    A2aStreamEvent, A2aSubscribeTaskRequest, A2aTask, A2aTaskPage, A2aTaskService,
    A2aTaskServiceCapabilities, A2aTaskServiceError, A2aTaskState, A2aTaskStatus,
};
use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, Semaphore, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const TASK: &str = "task-a";
const HEALTHY: &str = "task-b";
const HARNESS_WAIT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
enum Binding {
    Rest,
    Rpc,
}

#[derive(Clone)]
struct Setup {
    enabled: bool,
    enable_before_limits: bool,
    service_capabilities: A2aTaskServiceCapabilities,
    gate_at: Option<usize>,
    pending_at: Option<usize>,
    block_at: Option<usize>,
    pre_delay: Duration,
    post_delay: Duration,
    request_timeout: Duration,
    idle_timeout: Duration,
    event_cap: u32,
    hold_admission: bool,
    initial: Option<initial_authority::Controls>,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            enabled: true,
            enable_before_limits: true,
            service_capabilities: A2aTaskServiceCapabilities {
                streaming: true,
                ..A2aTaskServiceCapabilities::default()
            },
            gate_at: None,
            pending_at: None,
            block_at: None,
            pre_delay: Duration::ZERO,
            post_delay: Duration::ZERO,
            request_timeout: Duration::from_secs(2),
            idle_timeout: Duration::from_secs(1),
            event_cap: 8,
            hold_admission: false,
            initial: None,
        }
    }
}

struct Probe {
    setup: Setup,
    initial: initial_authority::Observations,
    shutdown: CancellationToken,
    mode: AtomicUsize,
    remapped: AtomicBool,
    authn: AtomicUsize,
    checks: AtomicUsize,
    admissions: AtomicUsize,
    created: AtomicUsize,
    polls: AtomicUsize,
    yielded: AtomicUsize,
    dropped: AtomicUsize,
    pending_drops: AtomicUsize,
    pending_entered: AtomicUsize,
    changed: Notify,
    release: Semaphore,
    admission_release: Semaphore,
    requests: Mutex<Vec<A2aServerAuthorizationRequest>>,
    contexts: Mutex<Vec<A2aServerPrincipal>>,
    receiver: Mutex<Option<mpsc::Receiver<Result<A2aStreamEvent, A2aTaskServiceError>>>>,
}

impl Probe {
    async fn wait_for(&self, predicate: impl Fn() -> bool) {
        tokio::time::timeout(HARNESS_WAIT, async {
            while !predicate() {
                self.changed.notified().await;
            }
        })
        .await
        .expect("fixture phase did not arrive");
    }

    fn fixed_facts(&self) {
        let expected = principal(false);
        let requests = self.requests.lock().unwrap();
        for request in requests.iter().filter(|r| r.task_id() == Some(TASK)) {
            assert_eq!(request.principal(), &expected);
            assert_eq!(
                request.principal().scopes().collect::<Vec<_>>(),
                ["scope-a", "scope-z"]
            );
            assert_eq!(request.operation(), A2aServerOperation::SubscribeToTask);
            assert_eq!(request.config_id(), None);
        }
        for context in self.contexts.lock().unwrap().iter() {
            assert_eq!(context, &expected);
        }
    }
}

fn principal(remapped: bool) -> A2aServerPrincipal {
    A2aServerPrincipal::new(
        TenantId::new(if remapped {
            "tenant-remapped"
        } else {
            "tenant-original"
        })
        .unwrap(),
        PrincipalIdentity::new(
            IssuerId::new("https://issuer.example/subscription").unwrap(),
            SubjectId::new("original-caller").unwrap(),
        ),
        ["scope-z", "scope-a"],
    )
    .unwrap()
}

struct Authenticator(Arc<Probe>);

impl A2aServerAuthenticator for Authenticator {
    fn authenticate(
        &self,
        request: A2aServerAuthenticationRequest,
    ) -> BoxFuture<'_, Result<A2aServerPrincipal, A2aServerAuthenticationError>> {
        self.0.authn.fetch_add(1, Ordering::SeqCst);
        if self.0.setup.initial.is_some() {
            self.0.changed.notify_one();
        }
        let valid = request
            .bearer()
            .is_some_and(|v| v.expose_secret() == "subscription-token");
        let remapped = self.0.remapped.load(Ordering::SeqCst);
        let delay = self
            .0
            .setup
            .initial
            .as_ref()
            .map_or(Duration::ZERO, |c| c.authentication_delay);
        Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if valid {
                Ok(principal(remapped))
            } else {
                Err(A2aServerAuthenticationError::InvalidCredential)
            }
        })
    }
}

struct PendingPolicy(Arc<Probe>);

impl Drop for PendingPolicy {
    fn drop(&mut self) {
        self.0.pending_drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct Policy(Arc<Probe>);

impl A2aServerAuthorizer for Policy {
    fn authorize(
        &self,
        request: A2aServerAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), A2aServerAuthorizationError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::authorize(self.0.clone(), request);
        }
        let probe = self.0.clone();
        let is_original = request.task_id() == Some(TASK);
        let index = if is_original {
            probe.checks.fetch_add(1, Ordering::SeqCst) + 1
        } else {
            0
        };
        probe.requests.lock().unwrap().push(request.clone());
        probe.changed.notify_one();
        Box::pin(async move {
            if probe.setup.pending_at == Some(index) {
                let _pending = PendingPolicy(probe.clone());
                probe.pending_entered.fetch_add(1, Ordering::SeqCst);
                probe.changed.notify_one();
                std::future::pending::<()>().await;
            }
            if probe.setup.gate_at == Some(index) {
                let permit = probe.release.acquire().await.unwrap();
                permit.forget();
            }
            let delay = match index {
                2 => probe.setup.pre_delay,
                3 => probe.setup.post_delay,
                _ => Duration::ZERO,
            };
            if probe.setup.block_at == Some(index) {
                std::thread::sleep(delay);
            } else {
                tokio::time::sleep(delay).await;
            }
            if is_original
                && (probe.mode.load(Ordering::SeqCst) == 1
                    || (probe.remapped.load(Ordering::SeqCst)
                        && request.principal() == &principal(false)))
            {
                return Err(A2aServerAuthorizationError::Forbidden);
            }
            if is_original && probe.mode.load(Ordering::SeqCst) == 2 {
                return Err(A2aServerAuthorizationError::Unavailable);
            }
            Ok(())
        })
    }
}

struct Admission(Arc<Probe>);

impl A2aServerAdmissionControl for Admission {
    fn admit(
        &self,
        request: A2aServerAdmissionRequest,
    ) -> BoxFuture<'_, Result<(), A2aServerAdmissionError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::admit(self.0.clone(), request);
        }
        let probe = self.0.clone();
        let index = probe.admissions.fetch_add(1, Ordering::SeqCst);
        probe.changed.notify_one();
        Box::pin(async move {
            if probe.setup.hold_admission && index == 0 {
                let permit = probe.admission_release.acquire().await.unwrap();
                permit.forget();
            }
            Ok(())
        })
    }
}

fn task(id: &str, terminal: bool) -> A2aTask {
    A2aTask::new(
        id,
        "original-context",
        A2aTaskStatus::new(
            if terminal {
                A2aTaskState::Completed
            } else {
                A2aTaskState::Working
            },
            None,
            None,
        )
        .unwrap(),
    )
    .unwrap()
}

struct Backend {
    probe: Arc<Probe>,
    receiver: mpsc::Receiver<Result<A2aStreamEvent, A2aTaskServiceError>>,
}

impl Stream for Backend {
    type Item = Result<A2aStreamEvent, A2aTaskServiceError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.probe.polls.fetch_add(1, Ordering::SeqCst);
        self.probe.changed.notify_one();
        let result = self.receiver.poll_recv(cx);
        if matches!(&result, Poll::Ready(Some(_))) {
            self.probe.yielded.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.probe.dropped.fetch_add(1, Ordering::SeqCst);
        self.probe.changed.notify_one();
    }
}

struct Service(Arc<Probe>);

impl Service {
    fn open(&self, context: &A2aRequestContext, id: &str) -> A2aEventStream {
        if id == TASK {
            self.0.created.fetch_add(1, Ordering::SeqCst);
            self.0
                .contexts
                .lock()
                .unwrap()
                .push(context.principal().clone());
            self.0.changed.notify_one();
            Box::pin(Backend {
                probe: self.0.clone(),
                receiver: self.0.receiver.lock().unwrap().take().unwrap(),
            })
        } else {
            assert_eq!(id, HEALTHY);
            Box::pin(stream::iter([Ok(A2aStreamEvent::Task(task(
                HEALTHY, true,
            )))]))
        }
    }
}

impl A2aTaskService for Service {
    fn capabilities(&self) -> A2aTaskServiceCapabilities {
        self.0.setup.service_capabilities
    }
    fn send_message(
        &self,
        context: A2aRequestContext,
        _: A2aSendMessageRequest,
    ) -> BoxFuture<'_, Result<A2aSendMessageResponse, A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::SendMessage,
                A2aSendMessageResponse::Task(task(TASK, true)),
            );
        }
        Box::pin(async { Err(A2aTaskServiceError::UnsupportedOperation) })
    }
    fn send_streaming_message(
        &self,
        context: A2aRequestContext,
        _: A2aSendMessageRequest,
    ) -> BoxFuture<'_, Result<A2aEventStream, A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::SendStreamingMessage,
                Box::pin(stream::iter([Ok(A2aStreamEvent::Task(task(TASK, true)))]))
                    as A2aEventStream,
            );
        }
        let stream = self.open(&context, TASK);
        Box::pin(async { Ok(stream) })
    }
    fn get_task(
        &self,
        context: A2aRequestContext,
        request: A2aGetTaskRequest,
    ) -> BoxFuture<'_, Result<A2aTask, A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::GetTask,
                task(request.id(), true),
            );
        }
        let task = task(request.id(), true);
        Box::pin(async { Ok(task) })
    }
    fn list_tasks(
        &self,
        context: A2aRequestContext,
        _: A2aListTasksRequest,
    ) -> BoxFuture<'_, Result<A2aTaskPage, A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::ListTasks,
                A2aTaskPage::new(Vec::new(), None, 50, 0).unwrap(),
            );
        }
        Box::pin(async { Err(A2aTaskServiceError::UnsupportedOperation) })
    }
    fn cancel_task(
        &self,
        context: A2aRequestContext,
        _: A2aCancelTaskRequest,
    ) -> BoxFuture<'_, Result<A2aTask, A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::CancelTask,
                task(TASK, true),
            );
        }
        Box::pin(async { Err(A2aTaskServiceError::UnsupportedOperation) })
    }
    fn subscribe_to_task(
        &self,
        context: A2aRequestContext,
        request: A2aSubscribeTaskRequest,
    ) -> BoxFuture<'_, Result<A2aEventStream, A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::SubscribeToTask,
                Box::pin(stream::iter([Ok(A2aStreamEvent::Task(task(TASK, true)))]))
                    as A2aEventStream,
            );
        }
        let stream = self.open(&context, request.id());
        Box::pin(async { Ok(stream) })
    }
    fn create_push_config(
        &self,
        context: A2aRequestContext,
        _: A2aPushConfig,
    ) -> BoxFuture<'_, Result<A2aPushConfig, A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::CreatePushConfig,
                initial_authority::push_config(),
            );
        }
        Box::pin(async { Err(A2aTaskServiceError::PushNotificationsNotSupported) })
    }
    fn get_push_config(
        &self,
        context: A2aRequestContext,
        _: A2aGetPushConfigRequest,
    ) -> BoxFuture<'_, Result<A2aPushConfig, A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::GetPushConfig,
                initial_authority::push_config(),
            );
        }
        Box::pin(async { Err(A2aTaskServiceError::PushNotificationsNotSupported) })
    }
    fn list_push_configs(
        &self,
        context: A2aRequestContext,
        _: A2aListPushConfigsRequest,
    ) -> BoxFuture<'_, Result<A2aPushConfigPage, A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::ListPushConfigs,
                A2aPushConfigPage::new(vec![initial_authority::push_config()], None).unwrap(),
            );
        }
        Box::pin(async { Err(A2aTaskServiceError::PushNotificationsNotSupported) })
    }
    fn delete_push_config(
        &self,
        context: A2aRequestContext,
        _: A2aDeletePushConfigRequest,
    ) -> BoxFuture<'_, Result<(), A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::DeletePushConfig,
                (),
            );
        }
        Box::pin(async { Err(A2aTaskServiceError::PushNotificationsNotSupported) })
    }
    fn get_extended_agent_card(
        &self,
        context: A2aRequestContext,
    ) -> BoxFuture<'_, Result<A2aAgentCard, A2aTaskServiceError>> {
        if self.0.setup.initial.is_some() {
            return initial_authority::business(
                self.0.clone(),
                &context,
                A2aServerOperation::GetExtendedAgentCard,
                card(true),
            );
        }
        Box::pin(async { Err(A2aTaskServiceError::ExtendedCardNotConfigured) })
    }
}

fn card(streaming: bool) -> A2aAgentCard {
    card_with_capabilities(A2aTaskServiceCapabilities {
        streaming,
        ..A2aTaskServiceCapabilities::default()
    })
}

fn card_with_capabilities(capabilities: A2aTaskServiceCapabilities) -> A2aAgentCard {
    A2aAgentCard::builder(
        "Subscription guard fixture",
        "Controlled policy/stream only",
        "0.0.0",
    )
    .unwrap()
    .capabilities(
        A2aAgentCapabilities::new()
            .streaming(capabilities.streaming)
            .push_notifications(capabilities.push_notifications)
            .extended_agent_card(capabilities.extended_agent_card),
    )
    .interface(
        A2aAgentInterface::new("https://agent.example/a2a/rest", A2aBinding::HttpJson).unwrap(),
    )
    .unwrap()
    .interface(
        A2aAgentInterface::new("https://agent.example/a2a/jsonrpc", A2aBinding::JsonRpc).unwrap(),
    )
    .unwrap()
    .default_input_modes(vec!["text/plain".to_owned()])
    .unwrap()
    .default_output_modes(vec!["text/plain".to_owned()])
    .unwrap()
    .skill(
        A2aAgentSkill::new(
            "observe",
            "Observe",
            "Observe the fixture stream",
            vec!["test".to_owned()],
        )
        .unwrap(),
    )
    .unwrap()
    .build()
    .unwrap()
}

struct Server {
    url: String,
    client: Client,
    router: axum::Router,
    probe: Arc<Probe>,
    sender: Option<mpsc::Sender<Result<A2aStreamEvent, A2aTaskServiceError>>>,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

impl Server {
    async fn start(setup: Setup) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (sender, receiver) = mpsc::channel(4);
        sender
            .try_send(Ok(A2aStreamEvent::Task(task(TASK, false))))
            .unwrap();
        let shutdown = CancellationToken::new();
        let probe = Arc::new(Probe {
            setup: setup.clone(),
            initial: initial_authority::Observations::default(),
            shutdown: shutdown.clone(),
            mode: AtomicUsize::new(0),
            remapped: AtomicBool::new(false),
            authn: AtomicUsize::new(0),
            checks: AtomicUsize::new(0),
            admissions: AtomicUsize::new(0),
            created: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            yielded: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            pending_drops: AtomicUsize::new(0),
            pending_entered: AtomicUsize::new(0),
            changed: Notify::new(),
            release: Semaphore::new(0),
            admission_release: Semaphore::new(0),
            requests: Mutex::new(Vec::new()),
            contexts: Mutex::new(Vec::new()),
            receiver: Mutex::new(Some(receiver)),
        });
        let mut options = A2aServerHttpOptions::new();
        if setup.enabled && setup.enable_before_limits {
            options = options
                .with_subscription_authorization_rechecks()
                .with_subscription_authorization_rechecks();
        }
        options = options
            .with_allowed_authorities(["agent.example"])
            .unwrap()
            .with_limits(
                setup
                    .initial
                    .as_ref()
                    .map_or(2048, |c| c.maximum_body_bytes),
                setup.initial.as_ref().map_or(8, |c| c.request_capacity),
                1,
            )
            .unwrap()
            .with_timeouts(setup.request_timeout, setup.idle_timeout)
            .unwrap()
            .with_maximum_stream_events(setup.event_cap)
            .unwrap();
        if setup.enabled && !setup.enable_before_limits {
            options = options
                .with_subscription_authorization_rechecks()
                .with_subscription_authorization_rechecks();
        }
        if setup.initial.is_some() {
            options = options
                .with_allowed_origins([initial_authority::ORIGIN])
                .unwrap();
        }
        let server = A2aServer::new(
            card_with_capabilities(setup.service_capabilities),
            Authenticator(probe.clone()),
            Policy(probe.clone()),
            Admission(probe.clone()),
            Service(probe.clone()),
            options.clone(),
            shutdown.clone(),
        )
        .unwrap();
        let router = server.router();
        let served_router = router.clone();
        let stopped = shutdown.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, served_router)
                .with_graceful_shutdown(stopped.cancelled_owned())
                .await
                .unwrap();
        });
        let client = Client::builder()
            .no_proxy()
            .http1_only()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();
        Self {
            url,
            client,
            router,
            probe,
            sender: Some(sender),
            shutdown,
            task,
        }
    }

    fn request(&self, binding: Binding, operation: &str, task_id: &str) -> reqwest::RequestBuilder {
        let params = if operation == "SendStreamingMessage" {
            json!({"message":{"messageId":"original-message","role":"ROLE_USER","parts":[{"text":"fixture"}]}})
        } else {
            json!({"id":task_id})
        };
        let builder = match binding {
            Binding::Rest => {
                if operation == "GetTask" {
                    self.client
                        .get(format!("{}/a2a/rest/tasks/{task_id}", self.url))
                } else {
                    let path = if operation == "SendStreamingMessage" {
                        "/a2a/rest/message:stream".to_owned()
                    } else {
                        format!("/a2a/rest/tasks/{task_id}:subscribe")
                    };
                    self.client
                        .post(format!("{}{path}", self.url))
                        .header("Content-Type", "application/a2a+json")
                        .json(&params)
                }
            }
            Binding::Rpc => self
                .client
                .post(format!("{}/a2a/jsonrpc", self.url))
                .json(&json!({"jsonrpc":"2.0","id":42,"method":operation,"params":params})),
        };
        builder
            .header("Host", "agent.example")
            .header("A2A-Version", A2A_PROTOCOL_VERSION_1_0)
            .bearer_auth("subscription-token")
    }

    async fn subscribe(&self, binding: Binding, id: &str) -> Response {
        tokio::time::timeout(
            HARNESS_WAIT,
            self.request(binding, "SubscribeToTask", id).send(),
        )
        .await
        .unwrap()
        .unwrap()
    }

    async fn send(&self, value: Result<A2aStreamEvent, A2aTaskServiceError>) {
        self.sender.as_ref().unwrap().send(value).await.unwrap();
    }

    async fn healthy(&self, binding: Binding) {
        let mut response = self.subscribe(binding, HEALTHY).await;
        assert_eq!(response.status(), 200);
        let value = next_event(&mut response, &mut Vec::new()).await.unwrap();
        assert_eq!(payload(binding, &value)["task"]["id"], HEALTHY);
    }

    async fn retired(&self) {
        self.probe
            .wait_for(|| self.probe.dropped.load(Ordering::SeqCst) == 1)
            .await;
        assert_eq!(self.probe.dropped.load(Ordering::SeqCst), 1);
    }
}

fn payload(binding: Binding, value: &Value) -> &Value {
    match binding {
        Binding::Rest => value,
        Binding::Rpc => &value["result"],
    }
}

async fn next_event(response: &mut Response, buffered: &mut Vec<u8>) -> Option<Value> {
    tokio::time::timeout(HARNESS_WAIT, async {
        loop {
            if let Some(end) = buffered.windows(2).position(|v| v == b"\n\n") {
                let event = String::from_utf8(buffered.drain(..end + 2).collect()).unwrap();
                let data = event
                    .lines()
                    .find_map(|line| line.strip_prefix("data: "))
                    .unwrap();
                return Some(serde_json::from_str(data).unwrap());
            }
            if let Some(chunk) = response.chunk().await.unwrap() {
                buffered.extend_from_slice(&chunk);
            } else {
                assert!(buffered.is_empty());
                return None;
            }
        }
    })
    .await
    .expect("SSE event/EOF did not arrive")
}

fn assert_error(binding: Binding, value: &Value, error: &a2a::A2AError) {
    let mut expected = serde_json::to_value(error.to_jsonrpc_error()).unwrap();
    let expected_reason = expected["data"][0]["reason"].as_str().unwrap().to_owned();
    let mut actual = value.clone();
    let actual_error = match binding {
        Binding::Rest => &mut actual,
        Binding::Rpc => actual.get_mut("error").unwrap(),
    };
    normalize_error_timestamp(actual_error, &expected_reason);
    normalize_error_timestamp(&mut expected, &expected_reason);
    match binding {
        Binding::Rest => assert_eq!(actual, expected),
        Binding::Rpc => assert_eq!(actual, json!({"jsonrpc":"2.0","id":42,"error":expected})),
    }
    assert!(!value.to_string().contains("subscription-token"));
    assert!(!value.to_string().contains(TASK));
}

fn normalize_error_timestamp(error: &mut Value, expected_reason: &str) {
    let object = error.as_object_mut().unwrap();
    assert_eq!(object.len(), 3);
    assert!(
        ["code", "message", "data"]
            .iter()
            .all(|key| object.contains_key(*key))
    );
    let details = object.get_mut("data").unwrap().as_array_mut().unwrap();
    assert_eq!(details.len(), 1);
    let info = details[0].as_object_mut().unwrap();
    assert_eq!(info.len(), 4);
    assert!(
        ["@type", "domain", "reason", "metadata"]
            .iter()
            .all(|key| info.contains_key(*key))
    );
    assert_eq!(info["@type"], "type.googleapis.com/google.rpc.ErrorInfo");
    assert_eq!(info["domain"], "a2a-protocol.org");
    assert_eq!(info["reason"], expected_reason);
    let metadata = info.get_mut("metadata").unwrap().as_object_mut().unwrap();
    assert_eq!(metadata.len(), 1);
    let timestamp = metadata.get_mut("timestamp").unwrap();
    let parsed = chrono::DateTime::parse_from_rfc3339(timestamp.as_str().unwrap()).unwrap();
    assert_eq!(parsed.offset().local_minus_utc(), 0);
    // The locked SDK generates this single field with Utc::now() separately
    // for each conversion. Every other field remains in the complete equality.
    *timestamp = Value::String("validated-dynamic-UTC-timestamp".to_owned());
}

async fn first(server: &Server, binding: Binding, operation: &str) -> (Response, Vec<u8>, Value) {
    let mut response = tokio::time::timeout(
        HARNESS_WAIT,
        server.request(binding, operation, TASK).send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), 200);
    assert!(
        response.headers()["Content-Type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    let mut buffered = Vec::new();
    let value = next_event(&mut response, &mut buffered).await.unwrap();
    assert_eq!(payload(binding, &value)["task"]["id"], TASK);
    (response, buffered, value)
}

async fn success(binding: Binding, setup: Setup) -> Vec<Value> {
    let enabled = setup.enabled;
    let server = Server::start(setup).await;
    let (mut response, mut buffered, initial) = first(&server, binding, "SubscribeToTask").await;
    let mut events = vec![initial];
    server
        .send(Ok(A2aStreamEvent::Task(task(TASK, false))))
        .await;
    events.push(next_event(&mut response, &mut buffered).await.unwrap());
    server
        .send(Ok(A2aStreamEvent::Task(task(TASK, true))))
        .await;
    events.push(next_event(&mut response, &mut buffered).await.unwrap());
    assert!(next_event(&mut response, &mut buffered).await.is_none());
    server.retired().await;
    assert_eq!(server.probe.authn.load(Ordering::SeqCst), 1);
    assert_eq!(
        server.probe.checks.load(Ordering::SeqCst),
        if enabled { 7 } else { 1 }
    );
    assert_eq!(server.probe.admissions.load(Ordering::SeqCst), 1);
    assert_eq!(server.probe.created.load(Ordering::SeqCst), 1);
    assert_eq!(server.probe.yielded.load(Ordering::SeqCst), 3);
    server.probe.fixed_facts();
    server.healthy(binding).await;
    events
}

async fn refused(binding: Binding, post: bool, unavailable: bool, remapped: bool) {
    let server = Server::start(Setup {
        gate_at: if post { None } else { Some(4) },
        ..Setup::default()
    })
    .await;
    let (mut response, mut buffered, _) = first(&server, binding, "SubscribeToTask").await;
    server
        .probe
        .wait_for(|| server.probe.checks.load(Ordering::SeqCst) >= 4)
        .await;
    if post {
        server
            .probe
            .wait_for(|| server.probe.polls.load(Ordering::SeqCst) >= 2)
            .await;
    }
    let polls_before = server.probe.polls.load(Ordering::SeqCst);
    if remapped {
        server.probe.remapped.store(true, Ordering::SeqCst);
    } else {
        server
            .probe
            .mode
            .store(if unavailable { 2 } else { 1 }, Ordering::SeqCst);
    }
    server
        .send(Ok(A2aStreamEvent::Task(task(TASK, false))))
        .await;
    if !post {
        server.probe.release.add_permits(1);
    }
    let value = next_event(&mut response, &mut buffered).await.unwrap();
    assert_error(
        binding,
        &value,
        &if unavailable {
            a2a::A2AError::internal("A2A authorization is unavailable")
        } else {
            a2a::A2AError::task_not_found("authorized resource")
        },
    );
    server.retired().await;
    if !post {
        assert_eq!(server.probe.polls.load(Ordering::SeqCst), polls_before);
    }
    assert_eq!(
        server.probe.yielded.load(Ordering::SeqCst),
        if post { 2 } else { 1 }
    );
    assert_eq!(server.probe.authn.load(Ordering::SeqCst), 1);
    server.probe.fixed_facts();
    // Keep the rejected response/body alive during a distinct healthy request.
    server.healthy(binding).await;
    assert!(next_event(&mut response, &mut buffered).await.is_none());
}

async fn callback_timeout(binding: Binding, post: bool) {
    let server = Server::start(Setup {
        pending_at: Some(if post { 5 } else { 4 }),
        request_timeout: Duration::from_millis(40),
        ..Setup::default()
    })
    .await;
    let (mut response, mut buffered, _) = first(&server, binding, "SubscribeToTask").await;
    if post {
        server
            .send(Ok(A2aStreamEvent::Task(task(TASK, false))))
            .await;
    }
    let value = next_event(&mut response, &mut buffered).await.unwrap();
    assert_error(
        binding,
        &value,
        &a2a::A2AError::internal("A2A authorization is unavailable"),
    );
    server.retired().await;
    assert_eq!(server.probe.pending_drops.load(Ordering::SeqCst), 1);
    server.healthy(binding).await;
}

async fn cumulative_idle(binding: Binding) {
    let mut server = Server::start(Setup {
        pre_delay: Duration::from_millis(200),
        post_delay: Duration::from_millis(800),
        ..Setup::default()
    })
    .await;
    // Replace the preloaded event only in this explicit instrumented fixture.
    let receiver = server
        .probe
        .receiver
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .try_recv()
        .unwrap();
    assert!(receiver.is_ok());
    let request = server.request(binding, "SubscribeToTask", TASK);
    // Headers may arrive independently of the first SSE body item. The policy
    // phase, rather than HTTP header timing, starts this fixture's delay.
    let producer = async {
        server
            .probe
            .wait_for(|| server.probe.checks.load(Ordering::SeqCst) >= 2)
            .await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        server
            .send(Ok(A2aStreamEvent::Task(task(TASK, false))))
            .await;
    };
    let (response, ()) = tokio::join!(request.send(), producer);
    let mut response = response.unwrap();
    assert_eq!(response.status(), 200);
    let value = next_event(&mut response, &mut Vec::new()).await.unwrap();
    assert_error(
        binding,
        &value,
        &a2a::A2AError::internal("A2A stream idle timeout"),
    );
    server.retired().await;
    assert_eq!(server.probe.checks.load(Ordering::SeqCst), 3);
    server.sender.take();
    server.healthy(binding).await;
}

async fn ready_race(binding: Binding) {
    let server = Server::start(Setup {
        block_at: Some(2),
        pre_delay: Duration::from_millis(30),
        request_timeout: Duration::from_millis(10),
        ..Setup::default()
    })
    .await;
    let mut response = server.subscribe(binding, TASK).await;
    assert_eq!(response.status(), 200);
    let value = next_event(&mut response, &mut Vec::new()).await.unwrap();
    assert_error(
        binding,
        &value,
        &a2a::A2AError::internal("A2A authorization is unavailable"),
    );
    assert_eq!(server.probe.polls.load(Ordering::SeqCst), 0);
    server.retired().await;
    server.healthy(binding).await;
}

async fn pending_phase(binding: Binding, phase: usize, disconnect: bool) {
    let server = Server::start(Setup {
        pending_at: match phase {
            0 => Some(4),
            2 => Some(5),
            _ => None,
        },
        request_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(5),
        ..Setup::default()
    })
    .await;
    let started = tokio::time::Instant::now();
    let (mut response, mut buffered, _) = first(&server, binding, "SubscribeToTask").await;
    if phase == 2 {
        server
            .send(Ok(A2aStreamEvent::Task(task(TASK, false))))
            .await;
    }
    let needed = if phase == 2 { 5 } else { 4 };
    server
        .probe
        .wait_for(|| server.probe.checks.load(Ordering::SeqCst) >= needed)
        .await;
    if phase == 1 {
        server
            .probe
            .wait_for(|| server.probe.polls.load(Ordering::SeqCst) >= 2)
            .await;
    } else {
        server
            .probe
            .wait_for(|| server.probe.pending_entered.load(Ordering::SeqCst) == 1)
            .await;
    }
    if disconnect {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "disconnect control reached too late to exclude timeout cleanup"
        );
        drop(response);
        // The HTTP server, not local Response Drop timing, determines cleanup.
        tokio::time::timeout(Duration::from_secs(2), server.retired())
            .await
            .expect("disconnect must release before the5s policy/idle deadlines");
        server.healthy(binding).await;
    } else {
        server.shutdown.cancel();
        let value = next_event(&mut response, &mut buffered).await.unwrap();
        assert_error(
            binding,
            &value,
            &a2a::A2AError::internal("A2A service is shutting down"),
        );
        server.retired().await;
    }
    if phase != 1 {
        assert_eq!(server.probe.pending_drops.load(Ordering::SeqCst), 1);
    }
}

async fn lifecycle(binding: Binding, case: usize) {
    let mut server = Server::start(Setup {
        event_cap: if case == 3 { 1 } else { 8 },
        ..Setup::default()
    })
    .await;
    let (mut response, mut buffered, _) = first(&server, binding, "SubscribeToTask").await;
    match case {
        0 => {
            server.sender.take();
        }
        1 => server.send(Err(A2aTaskServiceError::Unavailable)).await,
        2 => {
            server
                .send(Ok(A2aStreamEvent::Task(task(TASK, true))))
                .await;
        }
        _ => {}
    }
    let value = next_event(&mut response, &mut buffered).await;
    match case {
        0 => assert!(value.is_none()),
        1 => assert_error(
            binding,
            &value.unwrap(),
            &a2a::A2AError::internal("A2A service is unavailable"),
        ),
        2 => assert_eq!(payload(binding, &value.unwrap())["task"]["id"], TASK),
        _ => assert_error(
            binding,
            &value.unwrap(),
            &a2a::A2AError::internal("A2A stream event limit reached"),
        ),
    }
    server.retired().await;
    server.healthy(binding).await;
}

async fn legacy(binding: Binding, enabled: bool, sending: bool) {
    let server = Server::start(Setup {
        enabled,
        ..Setup::default()
    })
    .await;
    let operation = if sending {
        "SendStreamingMessage"
    } else {
        "SubscribeToTask"
    };
    let (mut response, mut buffered, _) = first(&server, binding, operation).await;
    server.probe.mode.store(1, Ordering::SeqCst);
    server
        .send(Ok(A2aStreamEvent::Task(task(TASK, true))))
        .await;
    let value = next_event(&mut response, &mut buffered).await.unwrap();
    assert_eq!(payload(binding, &value)["task"]["id"], TASK);
    assert!(next_event(&mut response, &mut buffered).await.is_none());
    server.retired().await;
    // send's decoded request has no task hint; no new recheck was introduced.
    assert_eq!(server.probe.requests.lock().unwrap().len(), 1);
    assert_eq!(server.probe.admissions.load(Ordering::SeqCst), 1);
    assert_eq!(server.probe.created.load(Ordering::SeqCst), 1);
}

async fn ordinary_unary(binding: Binding) {
    let server = Server::start(Setup::default()).await;
    let response = server
        .request(binding, "GetTask", TASK)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let value: Value = response.json().await.unwrap();
    assert_eq!(payload(binding, &value)["id"], TASK);
    let requests = server.probe.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].operation(), A2aServerOperation::GetTask);
    assert_eq!(requests[0].principal(), &principal(false));
    assert_eq!(requests[0].task_id(), Some(TASK));
    assert_eq!(server.probe.admissions.load(Ordering::SeqCst), 1);
    assert_eq!(server.probe.created.load(Ordering::SeqCst), 0);
    assert_eq!(server.probe.checks.load(Ordering::SeqCst), 1);
}

async fn initial_boundary(binding: Binding) {
    let server = Server::start(Setup::default()).await;
    let builder = match binding {
        Binding::Rest => server
            .client
            .post(format!("{}/a2a/rest/message:send", server.url)),
        Binding::Rpc => server.client.post(format!("{}/a2a/jsonrpc", server.url)),
    };
    let response = builder
        .header("Host", "agent.example")
        .header("Content-Type", "application/json")
        .body("malformed")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    assert_eq!(server.probe.requests.lock().unwrap().len(), 0);
    server.probe.mode.store(1, Ordering::SeqCst);
    let response = server.subscribe(binding, TASK).await;
    if matches!(binding, Binding::Rest) {
        assert_eq!(response.status(), 404);
    } else {
        assert_eq!(response.status(), 200);
    }
    assert_eq!(server.probe.checks.load(Ordering::SeqCst), 1);
    assert_eq!(
        server.probe.requests.lock().unwrap()[0].task_id(),
        Some(TASK)
    );
    let body: Value = response.json().await.unwrap();
    if matches!(binding, Binding::Rpc) {
        assert_error(
            binding,
            &body,
            &a2a::A2AError::task_not_found("authorized resource"),
        );
    }
    assert!(!body.to_string().contains("subscription-token"));
    assert_eq!(server.probe.created.load(Ordering::SeqCst), 0);
    assert_eq!(server.probe.admissions.load(Ordering::SeqCst), 0);
}

async fn admission_change(binding: Binding) {
    let server = Server::start(Setup {
        hold_admission: true,
        ..Setup::default()
    })
    .await;
    let mut request = Box::pin(server.subscribe(binding, TASK));
    tokio::select! { () = server.probe.wait_for(|| server.probe.admissions.load(Ordering::SeqCst) == 1) => {}, value = &mut request => panic!("admission gate bypassed: {value:?}") }
    assert_eq!(server.probe.created.load(Ordering::SeqCst), 0);
    server.probe.mode.store(1, Ordering::SeqCst);
    server.probe.admission_release.add_permits(1);
    let mut response = request.await;
    let value = next_event(&mut response, &mut Vec::new()).await.unwrap();
    assert_error(
        binding,
        &value,
        &a2a::A2AError::task_not_found("authorized resource"),
    );
    // Creation follows the original one-time prepare decision; the new guard
    // then refuses delivery. It cannot claim the backend was never created.
    assert_eq!(server.probe.created.load(Ordering::SeqCst), 1);
    assert_eq!(server.probe.yielded.load(Ordering::SeqCst), 0);
    server.retired().await;
    server.healthy(binding).await;
}

async fn enable_lifecycle(binding: Binding) {
    for before in [false, true] {
        success(
            binding,
            Setup {
                enable_before_limits: before,
                ..Setup::default()
            },
        )
        .await;
        let server = Server::start(Setup {
            service_capabilities: A2aTaskServiceCapabilities::default(),
            enable_before_limits: before,
            ..Setup::default()
        })
        .await;
        let response = server.subscribe(binding, TASK).await;
        assert!(
            !response
                .headers()
                .get("Content-Type")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("text/event-stream")
        );
        assert_eq!(server.probe.created.load(Ordering::SeqCst), 0);
        assert_eq!(server.probe.requests.lock().unwrap().len(), 0);
    }
}

async fn backpressure(binding: Binding) {
    let server = Server::start(Setup {
        gate_at: Some(4),
        ..Setup::default()
    })
    .await;
    let (mut response, mut buffered, _) = first(&server, binding, "SubscribeToTask").await;
    server
        .probe
        .wait_for(|| server.probe.checks.load(Ordering::SeqCst) == 4)
        .await;
    assert_eq!(server.probe.polls.load(Ordering::SeqCst), 1);
    assert_eq!(server.probe.yielded.load(Ordering::SeqCst), 1);
    // A precise policy barrier, not client non-reading, proves this stage.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(server.probe.checks.load(Ordering::SeqCst), 4);
    assert_eq!(server.probe.polls.load(Ordering::SeqCst), 1);
    server.probe.mode.store(1, Ordering::SeqCst);
    server.probe.release.add_permits(1);
    assert_error(
        binding,
        &next_event(&mut response, &mut buffered).await.unwrap(),
        &a2a::A2AError::task_not_found("authorized resource"),
    );
    server.retired().await;
    server.healthy(binding).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_server_subscription_authorization_matrix_covers_both_bindings() {
    for binding in [Binding::Rest, Binding::Rpc] {
        // The 17 fixed matrix IDs; grouped cases additionally exercise each
        // pre/post/shutdown/retirement variant, all through the real router.
        legacy(binding, false, false).await; // default_legacy
        legacy(binding, false, true).await;
        enable_lifecycle(binding).await; // enable_lifecycle
        let strict = success(binding, Setup::default()).await; // success_fixed_facts
        let original = success(
            binding,
            Setup {
                enabled: false,
                ..Setup::default()
            },
        )
        .await;
        assert_eq!(strict, original, "wire event values changed");
        refused(binding, false, false, false).await; // pre_poll_forbidden
        refused(binding, true, false, false).await; // post_event_forbidden
        for post in [false, true] {
            refused(binding, post, true, false).await;
        } // pre_post_unavailable
        for post in [false, true] {
            callback_timeout(binding, post).await;
        } // callback_timeout
        cumulative_idle(binding).await; // cumulative_idle_budget
        ready_race(binding).await; // ready_deadline_race
        for case in 0..4 {
            lifecycle(binding, case).await;
        } // backend_lifecycle
        for phase in 0..3 {
            pending_phase(binding, phase, false).await;
        } // shutdown_each_phase
        for phase in 0..3 {
            pending_phase(binding, phase, true).await;
        } // tcp_disconnect
        refused(binding, false, false, false).await; // terminal_body_retained
        backpressure(binding).await; // backpressure_scope
        legacy(binding, true, true).await; // non_subscription_isolation
        ordinary_unary(binding).await;
        initial_boundary(binding).await; // initial_boundary_order
        admission_change(binding).await;
        refused(binding, true, false, true).await; // immutable_operation_context
    }
}

#[path = "a2a_server_subscription/initial_authority.rs"]
mod initial_authority;
