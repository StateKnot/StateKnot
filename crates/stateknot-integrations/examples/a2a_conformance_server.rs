// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic HTTP fixture for the official A2A 1.0 TCK.
//!
//! This binary deliberately uses an in-memory scenario backend because it is
//! a conformance fixture, not a deployable task service. Production users must
//! implement `A2aTaskService` with durable idempotency, projections, streams,
//! cancellation, and push outbox semantics as required by that trait.

use std::{
    collections::HashMap,
    env,
    error::Error,
    io::{Error as IoError, ErrorKind},
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::StreamExt as _;
use serde_json::json;
use stateknot_core::{BoxFuture, IssuerId, PrincipalIdentity, SubjectId, TenantId};
use stateknot_integrations::{
    A2aAgentCapabilities, A2aAgentCard, A2aAgentInterface, A2aAgentSkill, A2aArtifact, A2aBinding,
    A2aCancelTaskRequest, A2aDeletePushConfigRequest, A2aEventStream, A2aGetPushConfigRequest,
    A2aGetTaskRequest, A2aListPushConfigsRequest, A2aListTasksRequest, A2aMessage, A2aMessageRole,
    A2aPart, A2aPushConfig, A2aPushConfigPage, A2aRequestContext, A2aSendConfiguration,
    A2aSendMessageRequest, A2aSendMessageResponse, A2aServer, A2aServerAuthenticationError,
    A2aServerAuthenticationRequest, A2aServerAuthenticator, A2aServerHttpOptions,
    A2aServerPrincipal, A2aStreamEvent, A2aSubscribeTaskRequest, A2aTask, A2aTaskPage,
    A2aTaskService, A2aTaskServiceCapabilities, A2aTaskServiceError, A2aTaskState, A2aTaskStatus,
    AllowA2aServerAdmission, AllowA2aServerAuthorization,
};
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;

const DEFAULT_PORT: u16 = 3400;
const PORT_ENVIRONMENT_VARIABLE: &str = "STATEKNOT_A2A_CONFORMANCE_PORT";

#[derive(Clone)]
struct AnonymousConformanceAuthenticator {
    principal: A2aServerPrincipal,
}

impl AnonymousConformanceAuthenticator {
    fn new() -> Self {
        Self {
            principal: A2aServerPrincipal::new(
                TenantId::new("a2a-tck").expect("static tenant is valid"),
                PrincipalIdentity::new(
                    IssuerId::new("https://tck.a2a-protocol.org").expect("static issuer is valid"),
                    SubjectId::new("conformance-client").expect("static subject is valid"),
                ),
                ["a2a.conformance"],
            )
            .expect("static principal is valid"),
        }
    }
}

impl A2aServerAuthenticator for AnonymousConformanceAuthenticator {
    fn authenticate(
        &self,
        _request: A2aServerAuthenticationRequest,
    ) -> BoxFuture<'_, Result<A2aServerPrincipal, A2aServerAuthenticationError>> {
        Box::pin(async { Ok(self.principal.clone()) })
    }
}

#[derive(Clone)]
struct TaskRecord {
    id: String,
    context_id: String,
    state: A2aTaskState,
    updated_at: DateTime<Utc>,
    history: Vec<A2aMessage>,
    artifacts: Vec<A2aArtifact>,
}

impl TaskRecord {
    fn project(
        &self,
        history_length: Option<u32>,
        include_artifacts: bool,
    ) -> Result<A2aTask, A2aTaskServiceError> {
        let status = A2aTaskStatus::new(self.state, None, Some(self.updated_at))
            .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?;
        let mut task = A2aTask::new(self.id.clone(), self.context_id.clone(), status)
            .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?;
        if include_artifacts && !self.artifacts.is_empty() {
            task = task
                .with_artifacts(self.artifacts.clone())
                .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?;
        }
        if let Some(length) = history_length.filter(|length| *length > 0) {
            let length = usize::try_from(length).unwrap_or(usize::MAX);
            let start = self.history.len().saturating_sub(length);
            task = task
                .with_history(self.history[start..].to_vec())
                .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?;
        }
        Ok(task)
    }
}

struct StoredTask {
    record: TaskRecord,
    updates: watch::Sender<TaskRecord>,
}

struct ConformanceTaskService {
    port: u16,
    sequence: AtomicU64,
    tasks: Arc<RwLock<HashMap<String, StoredTask>>>,
    push_configs: Arc<RwLock<HashMap<String, HashMap<String, A2aPushConfig>>>>,
    push_client: reqwest::Client,
}

impl ConformanceTaskService {
    fn new(port: u16) -> Result<Self, reqwest::Error> {
        Ok(Self {
            port,
            sequence: AtomicU64::new(0),
            tasks: Arc::new(RwLock::new(HashMap::new())),
            push_configs: Arc::new(RwLock::new(HashMap::new())),
            push_client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()?,
        })
    }

    fn next_id(&self, prefix: &str) -> String {
        format!(
            "{prefix}-{}",
            self.sequence.fetch_add(1, Ordering::Relaxed) + 1
        )
    }

    fn now() -> DateTime<Utc> {
        DateTime::<Utc>::from(SystemTime::now())
    }

    fn requested_history(request: &A2aSendMessageRequest) -> Option<u32> {
        request
            .configuration()
            .and_then(A2aSendConfiguration::history_length)
    }

    fn validate_media(request: &A2aSendMessageRequest) -> Result<(), A2aTaskServiceError> {
        let unsupported = request.message().parts().any(|part| {
            part.media_type()
                .is_some_and(|mode| !matches!(mode, "text/plain" | "application/json"))
        });
        if unsupported {
            Err(A2aTaskServiceError::ContentTypeNotSupported)
        } else {
            Ok(())
        }
    }

    fn artifacts_for(message_id: &str) -> Result<Vec<A2aArtifact>, A2aTaskServiceError> {
        let part = if message_id.contains("artifact-file-url") {
            Some(
                A2aPart::url("https://example.com/output.txt")
                    .and_then(|part| part.with_filename("output.txt"))
                    .and_then(|part| part.with_media_type("text/plain")),
            )
        } else if message_id.contains("artifact-file") {
            Some(
                A2aPart::raw(b"Generated file content".to_vec())
                    .and_then(|part| part.with_filename("output.txt"))
                    .and_then(|part| part.with_media_type("text/plain")),
            )
        } else if message_id.contains("artifact-data") {
            Some(A2aPart::data(json!({"key": "value", "count": 42})))
        } else if message_id.contains("artifact-text") {
            Some(A2aPart::text("Generated text content"))
        } else {
            None
        };
        part.map_or_else(
            || Ok(Vec::new()),
            |part| {
                part.and_then(|part| A2aArtifact::new("artifact-1", vec![part]))
                    .map(|artifact| vec![artifact])
                    .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)
            },
        )
    }

    fn direct_message(
        &self,
        request: &A2aSendMessageRequest,
    ) -> Result<Option<A2aMessage>, A2aTaskServiceError> {
        if !request.message().message_id().contains("message-response") {
            return Ok(None);
        }
        let context_id = request
            .message()
            .context_id()
            .map_or_else(|| self.next_id("context"), ToOwned::to_owned);
        A2aMessage::new(
            self.next_id("message"),
            A2aMessageRole::Agent,
            vec![
                A2aPart::text("Direct message response")
                    .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?,
            ],
        )
        .and_then(|message| message.with_context_id(context_id))
        .map(Some)
        .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)
    }

    fn bind_push_config(
        &self,
        mut config: A2aPushConfig,
        task_id: &str,
    ) -> Result<A2aPushConfig, A2aTaskServiceError> {
        config = config
            .with_task_id(task_id)
            .map_err(|_| A2aTaskServiceError::InvalidRequest)?;
        if config.id().is_none() {
            config = config
                .with_id(self.next_id("push"))
                .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?;
        }
        Ok(config)
    }

    async fn store_push_config(&self, config: A2aPushConfig) {
        let task_id = config
            .task_id()
            .expect("bound conformance config has a task id")
            .to_string();
        let config_id = config
            .id()
            .expect("bound conformance config has an id")
            .to_string();
        self.push_configs
            .write()
            .await
            .entry(task_id)
            .or_default()
            .insert(config_id, config);
    }

    async fn push_configs_for(&self, task_id: &str) -> Vec<A2aPushConfig> {
        self.push_configs
            .read()
            .await
            .get(task_id)
            .map(|configs| configs.values().cloned().collect())
            .unwrap_or_default()
    }

    async fn deliver_push_updates(&self, record: &TaskRecord) {
        let configs = self.push_configs_for(&record.id).await;
        let payload = json!({
            "statusUpdate": {
                "taskId": record.id,
                "contextId": record.context_id,
                "status": {
                    "state": task_state_name(record.state),
                    "timestamp": record.updated_at.to_rfc3339_opts(SecondsFormat::Micros, true)
                }
            }
        });
        for config in configs {
            let Some(url) = loopback_webhook_url(config.url()) else {
                continue;
            };
            let mut request = self.push_client.post(url).json(&payload);
            if let Some(authentication) = config.authentication()
                && let Some(credentials) = authentication.credentials()
            {
                request = request.header(
                    reqwest::header::AUTHORIZATION,
                    format!(
                        "{} {}",
                        authentication.scheme(),
                        credentials.expose_secret()
                    ),
                );
            }
            for attempt in 0..3 {
                let Some(retry) = request.try_clone() else {
                    break;
                };
                if retry
                    .send()
                    .await
                    .is_ok_and(|response| response.status().is_success())
                {
                    break;
                }
                if attempt < 2 {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }
    }
}

fn task_state_name(state: A2aTaskState) -> &'static str {
    match state {
        A2aTaskState::Submitted => "TASK_STATE_SUBMITTED",
        A2aTaskState::Working => "TASK_STATE_WORKING",
        A2aTaskState::InputRequired => "TASK_STATE_INPUT_REQUIRED",
        A2aTaskState::AuthRequired => "TASK_STATE_AUTH_REQUIRED",
        A2aTaskState::Completed => "TASK_STATE_COMPLETED",
        A2aTaskState::Failed => "TASK_STATE_FAILED",
        A2aTaskState::Canceled => "TASK_STATE_CANCELED",
        A2aTaskState::Rejected => "TASK_STATE_REJECTED",
        _ => "TASK_STATE_UNSPECIFIED",
    }
}

fn loopback_webhook_url(value: &str) -> Option<reqwest::Url> {
    let url = reqwest::Url::parse(value).ok()?;
    let address = url.host_str()?.parse::<IpAddr>().ok()?;
    (url.scheme() == "http"
        && address.is_loopback()
        && url.username().is_empty()
        && url.password().is_none())
    .then_some(url)
}

impl A2aTaskService for ConformanceTaskService {
    fn capabilities(&self) -> A2aTaskServiceCapabilities {
        A2aTaskServiceCapabilities {
            streaming: true,
            push_notifications: true,
            extended_agent_card: true,
        }
    }

    fn send_message(
        &self,
        _context: A2aRequestContext,
        request: A2aSendMessageRequest,
    ) -> BoxFuture<'_, Result<A2aSendMessageResponse, A2aTaskServiceError>> {
        Box::pin(async move {
            Self::validate_media(&request)?;
            if let Some(message) = self.direct_message(&request)? {
                return Ok(A2aSendMessageResponse::Message(message));
            }

            let history_length = Self::requested_history(&request);
            let inline_push_config = request
                .configuration()
                .and_then(|configuration| configuration.push_config())
                .cloned();
            let message = request.message().clone();
            let mut tasks = self.tasks.write().await;
            if let Some(task_id) = message.task_id() {
                let stored = tasks
                    .get_mut(task_id)
                    .ok_or(A2aTaskServiceError::TaskNotFound)?;
                if message
                    .context_id()
                    .is_some_and(|context_id| context_id != stored.record.context_id)
                {
                    return Err(A2aTaskServiceError::InvalidRequest);
                }
                if stored.record.state.is_terminal() {
                    return Err(A2aTaskServiceError::UnsupportedOperation);
                }
                stored.record.history.push(message.clone());
                if message.message_id().contains("complete-task") {
                    stored.record.state = A2aTaskState::Completed;
                }
                stored.record.updated_at = Self::now();
                stored.updates.send_replace(stored.record.clone());
                let record = stored.record.clone();
                let task = record.project(history_length, true)?;
                drop(tasks);
                self.deliver_push_updates(&record).await;
                return Ok(A2aSendMessageResponse::Task(task));
            }

            let task_id = self.next_id("task");
            let context_id = message
                .context_id()
                .map_or_else(|| self.next_id("context"), ToOwned::to_owned);
            let return_immediately = request
                .configuration()
                .is_some_and(A2aSendConfiguration::should_return_immediately);
            let state = if message.message_id().contains("input-required") {
                A2aTaskState::InputRequired
            } else if return_immediately {
                A2aTaskState::Submitted
            } else {
                A2aTaskState::Completed
            };
            let record = TaskRecord {
                id: task_id.clone(),
                context_id,
                state,
                updated_at: Self::now(),
                history: vec![message.clone()],
                artifacts: Self::artifacts_for(message.message_id())?,
            };
            let task = record.project(history_length, true)?;
            let (updates, _receiver) = watch::channel(record.clone());
            tasks.insert(
                task_id.clone(),
                StoredTask {
                    record: record.clone(),
                    updates,
                },
            );
            drop(tasks);
            if let Some(config) = inline_push_config {
                let config = self.bind_push_config(config, &task_id)?;
                self.store_push_config(config).await;
                self.deliver_push_updates(&record).await;
            }
            Ok(A2aSendMessageResponse::Task(task))
        })
    }

    fn send_streaming_message(
        &self,
        _context: A2aRequestContext,
        request: A2aSendMessageRequest,
    ) -> BoxFuture<'_, Result<A2aEventStream, A2aTaskServiceError>> {
        Box::pin(async move {
            Self::validate_media(&request)?;
            if request.message().message_id().contains("stream-002") {
                let message = A2aMessage::new(
                    self.next_id("message"),
                    A2aMessageRole::Agent,
                    vec![
                        A2aPart::text("Single streaming message")
                            .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?,
                    ],
                )
                .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?;
                return Ok(Box::pin(futures_util::stream::once(async move {
                    Ok(A2aStreamEvent::Message(message))
                })) as A2aEventStream);
            }

            let message = request.message().clone();
            let task_id = self.next_id("task");
            let context_id = message
                .context_id()
                .map_or_else(|| self.next_id("context"), ToOwned::to_owned);
            let submitted = TaskRecord {
                id: task_id.clone(),
                context_id: context_id.clone(),
                state: A2aTaskState::Submitted,
                updated_at: Self::now(),
                history: vec![message.clone()],
                artifacts: Self::artifacts_for(message.message_id())?,
            };
            let submitted_task = submitted.project(Self::requested_history(&request), true)?;
            let working_status = A2aTaskStatus::new(A2aTaskState::Working, None, Some(Self::now()))
                .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?;
            let completed_status =
                A2aTaskStatus::new(A2aTaskState::Completed, None, Some(Self::now()))
                    .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?;
            let working = stateknot_integrations::A2aStatusUpdate::new(
                task_id.clone(),
                context_id.clone(),
                working_status,
            )
            .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?;
            let completed = stateknot_integrations::A2aStatusUpdate::new(
                task_id.clone(),
                context_id,
                completed_status,
            )
            .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)?;
            let mut final_record = submitted.clone();
            final_record.state = A2aTaskState::Completed;
            final_record.updated_at = Self::now();
            let (updates, _receiver) = watch::channel(final_record.clone());
            self.tasks.write().await.insert(
                task_id,
                StoredTask {
                    record: final_record,
                    updates,
                },
            );
            let events = vec![
                Ok(A2aStreamEvent::Task(submitted_task)),
                Ok(A2aStreamEvent::StatusUpdate(working)),
                Ok(A2aStreamEvent::StatusUpdate(completed)),
            ];
            Ok(Box::pin(futures_util::stream::iter(events)) as A2aEventStream)
        })
    }

    fn get_task(
        &self,
        _context: A2aRequestContext,
        request: A2aGetTaskRequest,
    ) -> BoxFuture<'_, Result<A2aTask, A2aTaskServiceError>> {
        Box::pin(async move {
            self.tasks
                .read()
                .await
                .get(request.id())
                .ok_or(A2aTaskServiceError::TaskNotFound)?
                .record
                .project(request.history_length(), true)
        })
    }

    fn list_tasks(
        &self,
        _context: A2aRequestContext,
        request: A2aListTasksRequest,
    ) -> BoxFuture<'_, Result<A2aTaskPage, A2aTaskServiceError>> {
        Box::pin(async move {
            let tasks = self.tasks.read().await;
            let mut records = tasks
                .values()
                .map(|stored| &stored.record)
                .filter(|record| {
                    request
                        .context_id()
                        .is_none_or(|context_id| context_id == record.context_id)
                        && request.status().is_none_or(|state| state == record.state)
                        && request
                            .status_timestamp_after()
                            .is_none_or(|timestamp| record.updated_at > timestamp)
                })
                .cloned()
                .collect::<Vec<_>>();
            records.sort_by(|left, right| {
                right
                    .updated_at
                    .cmp(&left.updated_at)
                    .then_with(|| right.id.cmp(&left.id))
            });
            let total_size = u64::try_from(records.len()).unwrap_or(u64::MAX);
            let offset = request
                .page_token()
                .map(str::parse::<usize>)
                .transpose()
                .map_err(|_| A2aTaskServiceError::InvalidRequest)?
                .unwrap_or_default();
            if offset > records.len() {
                return Err(A2aTaskServiceError::InvalidRequest);
            }
            let end = offset
                .saturating_add(usize::from(request.page_size()))
                .min(records.len());
            let page = records[offset..end]
                .iter()
                .map(|record| {
                    record.project(request.history_length(), request.should_include_artifacts())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let next_page_token = (end < records.len()).then(|| end.to_string());
            A2aTaskPage::new(page, next_page_token, total_size)
                .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)
        })
    }

    fn cancel_task(
        &self,
        _context: A2aRequestContext,
        request: A2aCancelTaskRequest,
    ) -> BoxFuture<'_, Result<A2aTask, A2aTaskServiceError>> {
        Box::pin(async move {
            let mut tasks = self.tasks.write().await;
            let stored = tasks
                .get_mut(request.id())
                .ok_or(A2aTaskServiceError::TaskNotFound)?;
            if stored.record.state.is_terminal() {
                return Err(A2aTaskServiceError::TaskNotCancelable);
            }
            stored.record.state = A2aTaskState::Canceled;
            stored.record.updated_at = Self::now();
            stored.updates.send_replace(stored.record.clone());
            let record = stored.record.clone();
            let task = record.project(None, true)?;
            drop(tasks);
            self.deliver_push_updates(&record).await;
            Ok(task)
        })
    }

    fn subscribe_to_task(
        &self,
        _context: A2aRequestContext,
        request: A2aSubscribeTaskRequest,
    ) -> BoxFuture<'_, Result<A2aEventStream, A2aTaskServiceError>> {
        Box::pin(async move {
            let (initial, receiver) = {
                let tasks = self.tasks.read().await;
                let stored = tasks
                    .get(request.id())
                    .ok_or(A2aTaskServiceError::TaskNotFound)?;
                if stored.record.state.is_terminal() {
                    return Err(A2aTaskServiceError::UnsupportedOperation);
                }
                (
                    stored.record.project(None, true)?,
                    stored.updates.subscribe(),
                )
            };
            let first =
                futures_util::stream::once(async move { Ok(A2aStreamEvent::Task(initial)) });
            let updates = futures_util::stream::unfold(receiver, |mut receiver| async move {
                if receiver.changed().await.is_err() {
                    return None;
                }
                let record = receiver.borrow_and_update().clone();
                let event = record.project(None, true).map(A2aStreamEvent::Task);
                Some((event, receiver))
            });
            Ok(Box::pin(first.chain(updates)) as A2aEventStream)
        })
    }

    fn create_push_config(
        &self,
        _context: A2aRequestContext,
        config: A2aPushConfig,
    ) -> BoxFuture<'_, Result<A2aPushConfig, A2aTaskServiceError>> {
        Box::pin(async move {
            let task_id = config
                .task_id()
                .ok_or(A2aTaskServiceError::InvalidRequest)?
                .to_string();
            if !self.tasks.read().await.contains_key(&task_id) {
                return Err(A2aTaskServiceError::TaskNotFound);
            }
            let config = self.bind_push_config(config, &task_id)?;
            self.store_push_config(config.clone()).await;
            Ok(config)
        })
    }

    fn get_push_config(
        &self,
        _context: A2aRequestContext,
        request: A2aGetPushConfigRequest,
    ) -> BoxFuture<'_, Result<A2aPushConfig, A2aTaskServiceError>> {
        Box::pin(async move {
            if !self.tasks.read().await.contains_key(request.task_id()) {
                return Err(A2aTaskServiceError::TaskNotFound);
            }
            self.push_configs
                .read()
                .await
                .get(request.task_id())
                .and_then(|configs| configs.get(request.config_id()))
                .cloned()
                .ok_or(A2aTaskServiceError::TaskNotFound)
        })
    }

    fn list_push_configs(
        &self,
        _context: A2aRequestContext,
        request: A2aListPushConfigsRequest,
    ) -> BoxFuture<'_, Result<A2aPushConfigPage, A2aTaskServiceError>> {
        Box::pin(async move {
            if !self.tasks.read().await.contains_key(request.task_id()) {
                return Err(A2aTaskServiceError::TaskNotFound);
            }
            let configs = self.push_configs.read().await;
            let mut values = configs
                .get(request.task_id())
                .map(|values| values.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            values.sort_by(|left, right| left.id().cmp(&right.id()));
            let offset = request
                .page_token()
                .map(str::parse::<usize>)
                .transpose()
                .map_err(|_| A2aTaskServiceError::InvalidRequest)?
                .unwrap_or_default();
            if offset > values.len() {
                return Err(A2aTaskServiceError::InvalidRequest);
            }
            let end = offset
                .saturating_add(usize::from(request.page_size()))
                .min(values.len());
            let page = values[offset..end].to_vec();
            let next_page_token = (end < values.len()).then(|| end.to_string());
            A2aPushConfigPage::new(page, next_page_token)
                .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)
        })
    }

    fn delete_push_config(
        &self,
        _context: A2aRequestContext,
        request: A2aDeletePushConfigRequest,
    ) -> BoxFuture<'_, Result<(), A2aTaskServiceError>> {
        Box::pin(async move {
            if !self.tasks.read().await.contains_key(request.task_id()) {
                return Err(A2aTaskServiceError::TaskNotFound);
            }
            if let Some(configs) = self.push_configs.write().await.get_mut(request.task_id()) {
                configs.remove(request.config_id());
            }
            Ok(())
        })
    }

    fn get_extended_agent_card(
        &self,
        _context: A2aRequestContext,
    ) -> BoxFuture<'_, Result<A2aAgentCard, A2aTaskServiceError>> {
        Box::pin(async move {
            agent_card(self.port).map_err(|_| A2aTaskServiceError::InvalidAgentResponse)
        })
    }
}

fn agent_card(port: u16) -> Result<A2aAgentCard, Box<dyn Error>> {
    let rest_url = format!("http://127.0.0.1:{port}/a2a/rest");
    let jsonrpc_url = format!("http://127.0.0.1:{port}/a2a/jsonrpc");
    Ok(A2aAgentCard::builder(
        "StateKnot A2A conformance agent",
        "Deterministic fixture for the official A2A 1.0 TCK.",
        env!("CARGO_PKG_VERSION"),
    )?
    .capabilities(
        A2aAgentCapabilities::new()
            .streaming(true)
            .push_notifications(true)
            .extended_agent_card(true),
    )
    .interface(A2aAgentInterface::new(rest_url, A2aBinding::HttpJson)?)?
    .interface(A2aAgentInterface::new(jsonrpc_url, A2aBinding::JsonRpc)?)?
    .default_input_modes(vec![
        "text/plain".to_string(),
        "application/json".to_string(),
    ])?
    .default_output_modes(vec![
        "text/plain".to_string(),
        "application/json".to_string(),
    ])?
    .skill(A2aAgentSkill::new(
        "conformance",
        "A2A conformance scenarios",
        "Executes deterministic, bounded A2A TCK scenarios.",
        vec!["a2a".to_string(), "conformance".to_string()],
    )?)?
    .documentation_url("https://stknot.com/docs/a2a-server")?
    .build()?)
}

fn configured_port() -> Result<u16, Box<dyn Error>> {
    let value = match env::var(PORT_ENVIRONMENT_VARIABLE) {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => return Ok(DEFAULT_PORT),
        Err(env::VarError::NotUnicode(_)) => {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("{PORT_ENVIRONMENT_VARIABLE} must be valid UTF-8"),
            )
            .into());
        }
    };
    let port = value.parse::<u16>().map_err(|_| {
        IoError::new(
            ErrorKind::InvalidInput,
            format!("{PORT_ENVIRONMENT_VARIABLE} must be an integer from 1 through 65535"),
        )
    })?;
    if port == 0 {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            format!("{PORT_ENVIRONMENT_VARIABLE} must be an integer from 1 through 65535"),
        )
        .into());
    }
    Ok(port)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port = configured_port()?;
    let bind_address = format!("127.0.0.1:{port}");
    let shutdown = CancellationToken::new();
    let server = A2aServer::new(
        agent_card(port)?,
        AnonymousConformanceAuthenticator::new(),
        AllowA2aServerAuthorization,
        AllowA2aServerAdmission,
        ConformanceTaskService::new(port)?,
        A2aServerHttpOptions::new()
            .with_allowed_authorities([format!("127.0.0.1:{port}"), format!("localhost:{port}")])?,
        shutdown,
    )?;
    let listener = tokio::net::TcpListener::bind(&bind_address).await?;
    println!("StateKnot A2A conformance fixture listening on http://{bind_address}");
    axum::serve(listener, server.router()).await?;
    Ok(())
}
