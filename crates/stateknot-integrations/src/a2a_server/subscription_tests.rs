// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Private ownership-order oracle, paired with the real HTTP subscription tests.

use super::{
    A2aEventStream, A2aServerAuthorizationError, A2aServerAuthorizationRequest,
    A2aServerAuthorizer, A2aServerHttpOptions, A2aServerOperation, A2aServerPrincipal,
    A2aStreamEvent, A2aTaskServiceError, AuthorizedSubscriptionStream, map_authorization_error,
    wire,
};
use crate::{A2aTask, A2aTaskState, A2aTaskStatus};
use futures_core::Stream;
use futures_util::{StreamExt, stream};
use stateknot_core::{BoxFuture, IssuerId, PrincipalIdentity, SubjectId, TenantId};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

fn principal() -> A2aServerPrincipal {
    A2aServerPrincipal::new(
        TenantId::new("subscription-tenant").unwrap(),
        PrincipalIdentity::new(
            IssuerId::new("https://issuer.example/subscription").unwrap(),
            SubjectId::new("original-caller").unwrap(),
        ),
        ["scope-z", "scope-a"],
    )
    .unwrap()
}

fn authorization() -> A2aServerAuthorizationRequest {
    A2aServerAuthorizationRequest {
        principal: principal(),
        operation: A2aServerOperation::SubscribeToTask,
        task_id: Some("original-task".into()),
        config_id: None,
    }
}

fn event(terminal: bool) -> A2aStreamEvent {
    A2aStreamEvent::Task(
        A2aTask::new(
            "original-task",
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
        .unwrap(),
    )
}

struct ObservedStream {
    inner: A2aEventStream,
    dropped: Arc<AtomicUsize>,
    entered: Arc<Notify>,
}

impl Stream for ObservedStream {
    type Item = Result<A2aStreamEvent, A2aTaskServiceError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.entered.notify_one();
        self.inner.as_mut().poll_next(cx)
    }
}

impl Drop for ObservedStream {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

struct Check {
    calls: AtomicUsize,
    fail_at: Option<usize>,
    wait_at: Option<usize>,
    delay: Duration,
    post_delay: Option<Duration>,
    block: bool,
    cancel_at: Option<usize>,
    shutdown: CancellationToken,
    entered: Notify,
    pending_drops: AtomicUsize,
}

impl Check {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            fail_at: None,
            wait_at: None,
            delay: Duration::ZERO,
            post_delay: None,
            block: false,
            cancel_at: None,
            shutdown: CancellationToken::new(),
            entered: Notify::new(),
            pending_drops: AtomicUsize::new(0),
        }
    }
}

struct PendingCheck<'a>(&'a AtomicUsize);

impl Drop for PendingCheck<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl A2aServerAuthorizer for Check {
    fn authorize(
        &self,
        request: A2aServerAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), A2aServerAuthorizationError>> {
        assert_eq!(request.principal(), &principal());
        assert_eq!(
            request.principal().scopes().collect::<Vec<_>>(),
            ["scope-a", "scope-z"]
        );
        assert_eq!(request.operation(), A2aServerOperation::SubscribeToTask);
        assert_eq!(request.task_id(), Some("original-task"));
        assert_eq!(request.config_id(), None);
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.wait_at == Some(index) {
                let _drop = PendingCheck(&self.pending_drops);
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            let delay = if index == 1 {
                self.post_delay.unwrap_or(self.delay)
            } else {
                self.delay
            };
            if self.block {
                // A deliberately non-cooperative fixture checks the explicit
                // post-await clock guard; production callbacks must not block.
                std::thread::sleep(delay);
            } else {
                tokio::time::sleep(delay).await;
            }
            if self.cancel_at == Some(index) {
                self.shutdown.cancel();
            }
            if self.fail_at == Some(index) {
                Err(A2aServerAuthorizationError::Forbidden)
            } else {
                Ok(())
            }
        })
    }
}

type StrictFixture = (
    futures_util::stream::BoxStream<'static, Result<wire::StreamResponse, wire::A2AError>>,
    Arc<Semaphore>,
    Arc<AtomicUsize>,
    Arc<Notify>,
);

fn strict(
    inner: A2aEventStream,
    check: Arc<Check>,
    request_timeout: Duration,
    remaining: u32,
    shutdown: CancellationToken,
) -> StrictFixture {
    let slots = Arc::new(Semaphore::new(1));
    let dropped = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let state = AuthorizedSubscriptionStream {
        stream: Box::pin(ObservedStream {
            inner,
            dropped: dropped.clone(),
            entered: entered.clone(),
        }),
        _permit: slots.clone().try_acquire_owned().unwrap(),
        authorizer: check,
        authorization: authorization(),
        shutdown,
        request_timeout,
        idle_timeout: Duration::from_secs(1),
        remaining,
    };
    (state.into_stream(), slots, dropped, entered)
}

#[test]
fn enabling_is_idempotent_clone_and_other_options_preserve_exact_limits() {
    assert!(!A2aServerHttpOptions::new().subscription_authorization_rechecks);
    assert!(!A2aServerHttpOptions::default().subscription_authorization_rechecks);
    let options = A2aServerHttpOptions::new()
        .with_subscription_authorization_rechecks()
        .with_subscription_authorization_rechecks()
        .with_limits(2048, 4, 1)
        .unwrap()
        .with_timeouts(Duration::from_millis(10), Duration::from_secs(1))
        .unwrap()
        .with_maximum_stream_events(2)
        .unwrap();
    let cloned = options.clone();
    assert!(cloned.subscription_authorization_rechecks);
    assert_eq!(cloned.maximum_body_bytes, 2048);
    assert_eq!(cloned.maximum_in_flight_requests, 4);
    assert_eq!(cloned.maximum_in_flight_operations, 1);
    assert_eq!(cloned.request_timeout, Duration::from_millis(10));
    assert_eq!(cloned.stream_idle_timeout, Duration::from_secs(1));
    assert_eq!(cloned.maximum_stream_events, 2);
    assert!(
        A2aServerHttpOptions::new()
            .with_limits(2048, 4, 1)
            .unwrap()
            .with_subscription_authorization_rechecks()
            .subscription_authorization_rechecks
    );
}

#[tokio::test]
async fn terminal_error_eof_and_limit_retire_before_the_first_return_not_next_poll() {
    for case in 0..4 {
        let values = match case {
            0 => vec![Ok(event(true))],
            1 => vec![Err(A2aTaskServiceError::Unavailable)],
            _ => Vec::new(),
        };
        let (mut output, slots, dropped, _) = strict(
            Box::pin(stream::iter(values)),
            Arc::new(Check::new()),
            Duration::from_secs(2),
            if case == 3 { 0 } else { 2 },
            CancellationToken::new(),
        );
        let first = output.next().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(slots.available_permits(), 1);
        match case {
            0 => assert!(first.unwrap().is_ok()),
            2 => assert!(first.is_none()),
            _ => assert!(first.unwrap().is_err()),
        }
        if case != 2 {
            assert!(output.next().await.is_none());
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn pre_and_post_denial_drop_resources_before_yielding_and_preserve_original_facts() {
    for fail_at in [0, 1] {
        let check = Arc::new(Check {
            fail_at: Some(fail_at),
            ..Check::new()
        });
        let (mut output, slots, dropped, _) = strict(
            Box::pin(stream::iter([Ok(event(false))])),
            check.clone(),
            Duration::from_secs(2),
            2,
            CancellationToken::new(),
        );
        let error = output.next().await.unwrap().unwrap_err();
        assert_eq!(
            error.message,
            map_authorization_error(A2aServerAuthorizationError::Forbidden).message
        );
        assert_eq!(check.calls.load(Ordering::SeqCst), fail_at + 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(slots.available_permits(), 1);
        assert!(output.next().await.is_none());
    }
}

#[tokio::test]
async fn one_idle_deadline_covers_pre_backend_and_post_without_restarts() {
    let check = Arc::new(Check {
        delay: Duration::from_millis(200),
        post_delay: Some(Duration::from_millis(800)),
        ..Check::new()
    });
    let inner = stream::once(async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(event(false))
    });
    let (mut output, slots, dropped, _) = strict(
        Box::pin(inner),
        check.clone(),
        Duration::from_secs(2),
        2,
        CancellationToken::new(),
    );
    let error = output.next().await.unwrap().unwrap_err();
    assert_eq!(error.message, "A2A stream idle timeout");
    assert_eq!(check.calls.load(Ordering::SeqCst), 2);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test]
async fn ready_after_request_deadline_cannot_bypass_the_actual_clock_check() {
    let check = Arc::new(Check {
        delay: Duration::from_millis(20),
        block: true,
        ..Check::new()
    });
    let (mut output, slots, dropped, _) = strict(
        Box::pin(stream::iter([Ok(event(false))])),
        check,
        Duration::from_millis(10),
        2,
        CancellationToken::new(),
    );
    let error = output.next().await.unwrap().unwrap_err();
    assert_eq!(error.message, "A2A authorization is unavailable");
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test]
async fn ready_after_idle_deadline_and_ready_cancelled_checks_cannot_deliver() {
    let check = Arc::new(Check {
        delay: Duration::from_millis(1020),
        block: true,
        ..Check::new()
    });
    let (mut output, slots, dropped, _) = strict(
        Box::pin(stream::iter([Ok(event(false))])),
        check,
        Duration::from_secs(2),
        2,
        CancellationToken::new(),
    );
    let error = output.next().await.unwrap().unwrap_err();
    assert_eq!(error.message, "A2A stream idle timeout");
    assert_eq!(slots.available_permits(), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    for index in [0, 1] {
        let shutdown = CancellationToken::new();
        let check = Arc::new(Check {
            cancel_at: Some(index),
            shutdown: shutdown.clone(),
            ..Check::new()
        });
        let (mut output, slots, dropped, _) = strict(
            Box::pin(stream::iter([Ok(event(false))])),
            check.clone(),
            Duration::from_secs(2),
            2,
            shutdown,
        );
        let error = output.next().await.unwrap().unwrap_err();
        assert_eq!(error.message, "A2A service is shutting down");
        assert_eq!(check.calls.load(Ordering::SeqCst), index + 1);
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(output.next().await.is_none());
    }
}

#[tokio::test]
async fn unpolled_stream_starts_no_idle_timer_or_authorization_task() {
    let check = Arc::new(Check::new());
    let (mut output, slots, dropped, _) = strict(
        Box::pin(stream::iter([Ok(event(true))])),
        check.clone(),
        Duration::from_secs(2),
        2,
        CancellationToken::new(),
    );
    tokio::time::sleep(Duration::from_millis(1020)).await;
    assert_eq!(check.calls.load(Ordering::SeqCst), 0);
    assert_eq!(slots.available_permits(), 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(output.next().await.unwrap().is_ok());
    assert_eq!(check.calls.load(Ordering::SeqCst), 2);
    assert_eq!(slots.available_permits(), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_and_owner_drop_cancel_each_pending_phase_and_release_once() {
    for phase in 0..3 {
        for disconnect in [false, true] {
            let check = Arc::new(Check {
                wait_at: match phase {
                    0 => Some(0),
                    2 => Some(1),
                    _ => None,
                },
                ..Check::new()
            });
            let inner: A2aEventStream = if phase == 1 {
                Box::pin(stream::pending())
            } else {
                Box::pin(stream::iter([Ok(event(false))]))
            };
            let shutdown = CancellationToken::new();
            let (mut output, slots, dropped, backend_entered) = strict(
                inner,
                check.clone(),
                Duration::from_secs(2),
                2,
                shutdown.clone(),
            );
            let mut next = Box::pin(output.next());
            if phase == 1 {
                tokio::select! { () = backend_entered.notified() => {}, value = &mut next => panic!("unexpected return: {value:?}"), () = tokio::time::sleep(Duration::from_secs(3)) => panic!("backend phase did not arrive") }
            } else {
                tokio::select! { () = check.entered.notified() => {}, value = &mut next => panic!("unexpected return: {value:?}"), () = tokio::time::sleep(Duration::from_secs(3)) => panic!("authorization phase did not arrive") }
            }
            if disconnect {
                drop(next);
                drop(output);
            } else {
                shutdown.cancel();
                assert!(next.await.unwrap().is_err());
                assert_eq!(slots.available_permits(), 1);
                assert_eq!(dropped.load(Ordering::SeqCst), 1);
                assert!(output.next().await.is_none());
            }
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
            assert_eq!(slots.available_permits(), 1);
            if phase != 1 {
                assert_eq!(check.pending_drops.load(Ordering::SeqCst), 1);
            }
        }
    }
}
