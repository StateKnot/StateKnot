// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::agent_http::AgentHttpPrincipal;
use stateknot_core::BoxFuture;
use std::time::Duration;

fn options() -> AgentHostOperationsOptions {
    AgentHostOperationsOptions::new(["ops.example.test".into()]).unwrap()
}
#[test]
fn configuration_rejects_unbounded_or_ambiguous_input() {
    for hosts in [vec![], vec!["*".into()], vec!["ops.example.test".into(); 2]] {
        assert!(AgentHostOperationsOptions::new(hosts).is_err());
    }
    for (count, deadline) in [
        (0, Duration::from_secs(1)),
        (257, Duration::from_secs(1)),
        (1, Duration::from_millis(9)),
        (1, Duration::from_millis(10001)),
    ] {
        assert!(options().with_request_limits(count, deadline).is_err());
    }
    assert!(
        options()
            .with_request_limits(256, Duration::from_secs(10))
            .is_ok()
    );
    assert!(
        options()
            .with_transport_limits(
                0,
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(1)
            )
            .is_err()
    );
    assert!(
        options()
            .with_transport_limits(
                1,
                Duration::from_secs(3),
                Duration::from_secs(2),
                Duration::from_secs(1)
            )
            .is_err()
    );
}
struct Deny;
impl AgentHttpAuthenticator for Deny {
    fn authenticate(
        &self,
        _: AgentHttpCredential,
    ) -> BoxFuture<'_, Result<AgentHttpPrincipal, AgentHttpAuthenticationError>> {
        Box::pin(async { Err(AgentHttpAuthenticationError::Unauthenticated) })
    }
}
#[tokio::test]
async fn cancelled_post_coordinator_cleanup_retains_completion() {
    let policy = Arc::new(
        AgentHostOperationsPolicy::new(AgentHostHealth::new(), vec![], Duration::from_secs(30))
            .unwrap(),
    );
    let mut ops = AgentHostOperations::start(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        policy,
        Arc::new(Deny),
        options(),
    )
    .unwrap();
    // Fault-inject delayed descendant destruction after coordinator failure.
    let cleanup = ConnectionGuard::new(ops.active.clone());
    ops.task.as_ref().unwrap().abort();
    assert!(
        timeout(Duration::from_millis(10), ops.wait())
            .await
            .is_err()
    );
    assert!(ops.completed.is_some());
    drop(cleanup);
    assert_eq!(ops.wait().await, Err(AgentHostOperationsError::Stopped));
    assert_eq!(ops.active_connections(), 0);
    assert!(ops.completed.is_none());
}
