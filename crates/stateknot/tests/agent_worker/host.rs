// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot::{agent_host::*, agent_maintenance::AgentMaintenanceStatus};
use tokio::net::{TcpListener, TcpStream};

impl AgentHttpReadiness for Host {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentHttpReadinessError>> {
        Box::pin(async {
            AgentWorkerReadiness::check(self)
                .await
                .map_err(|_| AgentHttpReadinessError)
        })
    }
}

fn maintenance(f: &Fixture) -> AgentMaintenanceBinding {
    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    register_standard_agent_deadline_event_schema(&mut schemas).unwrap();
    register_standard_child_reconciliation_event_schema(&mut schemas).unwrap();
    register_standard_child_join_event_schema(&mut schemas).unwrap();
    register_standard_run_failure_close_event_schema(&mut schemas).unwrap();
    AgentMaintenanceBinding::new(
        f.store.clone(),
        schemas.build().unwrap(),
        vec![f.caller.tenant_id().clone()],
        AgentMaintenanceMutationOptions::default(),
    )
    .unwrap()
}

pub(super) fn limits() -> AgentHostOptions {
    AgentHostOptions {
        http: AgentHttpServerOptions::default().with_transport_limits(16, Duration::from_secs(1), Duration::from_secs(60), Duration::from_millis(150)).unwrap()
            // Deliberately much slower than sibling probes: gate cannot depend on this interval.
            .with_readiness_limits(Duration::from_secs(60), Duration::from_secs(5), Duration::from_secs(90)).unwrap(),
        worker: options()
            .with_readiness_limits(
                Duration::from_millis(50),
                Duration::from_secs(5),
                Duration::from_secs(10),
            )
            .unwrap(),
        maintenance: AgentMaintenanceOptions::default()
            .with_deadlines(Duration::from_secs(20), Duration::from_secs(2))
            .unwrap()
            .with_pacing(Duration::from_millis(20), Duration::from_millis(50))
            .unwrap()
            .with_readiness_limits(
                Duration::from_millis(50),
                Duration::from_secs(5),
                Duration::from_secs(10),
            )
            .unwrap(),
    }
}

pub(super) fn checks() -> [Arc<Host>; 3] {
    std::array::from_fn(|_| Arc::new(Host::default()))
}
pub(super) async fn launch(
    f: &Fixture,
    checks: &[Arc<Host>; 3],
    ev: Arc<Evidence>,
    limits: AgentHostOptions,
) -> (AgentHost, AgentHttpService) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = AgentHttpService::new(
        f.service.clone(),
        f.auth.clone(),
        AgentHttpOptions::loopback(listener.local_addr().unwrap().port())
            .unwrap()
            .with_sse(AgentHttpSseOptions::default()),
    );
    let bindings = AgentHostBindings::new(http.clone(), binding(f, ev), maintenance(f));
    let dependencies = AgentHostDependencies {
        http: checks[0].clone(),
        worker: checks[1].clone(),
        maintenance: checks[2].clone(),
    };
    (
        AgentHost::launch(listener, bindings, dependencies, limits).unwrap(),
        http,
    )
}
pub(super) async fn ready(host: &AgentHost) {
    timeout(Duration::from_secs(15), host.wait_ready())
        .await
        .unwrap()
        .unwrap();
}
pub(super) fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}
pub(super) async fn submit(host: &AgentHost, f: &Fixture) -> reqwest::Response {
    client()
        .post(format!("http://{}/v1/agent-runs", host.local_addr()))
        .bearer_auth(fixture::TOKEN)
        .json(&f.submission())
        .send()
        .await
        .unwrap()
}
fn idle(health: &AgentHostHealth) -> bool {
    health
        .http()
        .is_none_or(|h| h.active_connections() == 0 && h.active_streams() == 0)
        && health
            .worker()
            .is_none_or(|h| h.active_ticks() == 0 && h.active_nodes() == 0)
        && health.maintenance().is_none_or(|h| h.active_ticks() == 0)
}
pub(super) async fn joined(host: &AgentHost, f: &Fixture) {
    assert_eq!(host.health().status(), AgentHostStatus::Stopped);
    assert!(idle(&host.health()));
    assert!(TcpStream::connect(host.local_addr()).await.is_err());
    assert!(f.store.observe_database_clock().await.is_ok());
}

#[tokio::test]
async fn postgres_host_end_to_end_and_independent_dependency_gates() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let checks = checks();
    let (mut host, _) = launch(&f, &checks, evidence(&f), limits()).await;
    ready(&host).await;
    let response = submit(&host, &f).await;
    assert_eq!(response.status(), 201);
    let snapshot = response
        .json::<AgentHttpRunResponse>()
        .await
        .unwrap()
        .snapshot;
    succeeded(&f, snapshot.provenance().run_id()).await;
    for index in [2, 1] {
        checks[index].mode.store(1, Ordering::SeqCst);
        until(|| host.health().status() == AgentHostStatus::Unavailable).await;
        assert!(host.health().http().unwrap().is_ready());
        assert_eq!(submit(&host, &f).await.status(), 503);
        until(|| host.health().worker().unwrap().active_ticks() == 0).await;
        let ticks = host.health().worker().unwrap().report().started_ticks;
        let calls = f.node_calls.load(Ordering::SeqCst);
        let queued = admit(&f).await;
        sleep(Duration::from_millis(200)).await;
        assert_eq!(f.node_calls.load(Ordering::SeqCst), calls);
        assert_eq!(
            host.health().worker().unwrap().report().started_ticks,
            ticks
        );
        checks[index].mode.store(0, Ordering::SeqCst);
        ready(&host).await;
        succeeded(&f, queued).await;
    }
    assert!(
        timeout(Duration::from_millis(50), host.wait())
            .await
            .is_err()
    );
    let report = host.shutdown().await.unwrap();
    assert_eq!(report.failure, None);
    assert_eq!(report.worker.unwrap().unwrap().failure, None);
    assert_eq!(report.maintenance.unwrap().unwrap().failure, None);
    joined(&host, &f).await;
    assert_eq!(host.wait().await, Err(AgentHostError::Stopped));
    f.store.close().await;
    println!(
        "\nSTATEKNOT_HOST_ADMISSION_EVIDENCE={{\"http_to_terminal\":true,\"sibling_gate\":true,\"maintenance_pauses_worker\":true,\"recovery\":true,\"cancelled_wait_retains_owner\":true}}"
    );
}

#[tokio::test]
async fn postgres_host_startup_failure_panic_and_cancel_join_started_roles() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    for (index, role) in [
        (2, AgentHostRole::Maintenance),
        (1, AgentHostRole::Worker),
        (0, AgentHostRole::Http),
    ] {
        for mode in [1, 3] {
            let checks = checks();
            checks[index].mode.store(mode, Ordering::SeqCst);
            let (mut host, service) = launch(&f, &checks, evidence(&f), limits()).await;
            let report = timeout(Duration::from_secs(15), host.wait())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(report.failure, Some(AgentHostFailure::Startup(role)));
            assert_eq!(report.maintenance.is_some(), index != 2);
            assert_eq!(report.worker.is_some(), index == 0);
            assert!(report.http.is_none());
            joined(&host, &f).await;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            assert!(matches!(
                AgentHttpServer::start(listener, service, Arc::new(Host::default()), limits().http)
                    .await,
                Err(AgentHttpServerError::AlreadyClaimed)
            ));
        }
        let checks = checks();
        checks[index].mode.store(2, Ordering::SeqCst);
        let (mut host, _) = launch(&f, &checks, evidence(&f), limits()).await;
        until(|| checks[index].calls.load(Ordering::SeqCst) > 0).await;
        assert!(
            timeout(Duration::from_millis(25), host.wait_ready())
                .await
                .is_err()
        );
        let report = host.shutdown().await.unwrap();
        assert_eq!(report.failure, None);
        assert_eq!(report.maintenance.is_some(), index != 2);
        joined(&host, &f).await;
    }
    f.store.close().await;
    println!(
        "\nSTATEKNOT_HOST_STARTUP_EVIDENCE={{\"all_stages_fail_closed\":true,\"panics_joined\":true,\"cancelled_startup_joined\":true,\"shared_ingress_closed\":true}}"
    );
}

#[tokio::test]
async fn postgres_host_unexpected_role_exit_drains_siblings() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    for (role, http_panic) in [
        (AgentHostRole::Http, false),
        (AgentHostRole::Worker, false),
        (AgentHostRole::Maintenance, false),
        (AgentHostRole::Http, true),
    ] {
        let checks = checks();
        let ev = evidence(&f);
        let mut options = limits();
        if http_panic {
            options.http = options
                .http
                .with_readiness_limits(
                    Duration::from_millis(50),
                    Duration::from_secs(5),
                    Duration::from_secs(10),
                )
                .unwrap();
        }
        let (mut host, service) = launch(&f, &checks, ev.clone(), options).await;
        ready(&host).await;
        let mut held_connection = None;
        match role {
            AgentHostRole::Http => {
                if http_panic {
                    held_connection = Some(TcpStream::connect(host.local_addr()).await.unwrap());
                    until(|| host.health().http().unwrap().active_connections() > 0).await;
                    checks[0].mode.store(3, Ordering::SeqCst);
                } else {
                    service.shutdown();
                }
            }
            AgentHostRole::Worker => {
                ev.mode.store(2, Ordering::SeqCst);
                admit(&f).await;
            }
            AgentHostRole::Maintenance => checks[2].mode.store(3, Ordering::SeqCst),
        }
        let report = timeout(Duration::from_secs(15), host.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.failure, Some(AgentHostFailure::Role(role)));
        if http_panic {
            assert_eq!(report.http, Some(Err(AgentHttpServerError::Stopped)));
        }
        joined(&host, &f).await;
        drop(held_connection);
    }
    f.store.close().await;
    println!(
        "\nSTATEKNOT_HOST_FAILURE_EVIDENCE={{\"ingress_exit\":true,\"worker_panic\":true,\"maintenance_panic\":true,\"siblings_joined\":true}}"
    );
}

#[tokio::test]
async fn postgres_host_ordered_forced_drain_joins_connections_streams_and_nodes() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let ev = evidence(&f);
    ev.mode.store(1, Ordering::SeqCst);
    let (mut host, _) = launch(&f, &checks(), ev.clone(), limits()).await;
    ready(&host).await;
    admit(&f).await;
    until(|| ev.entered.load(Ordering::SeqCst) == 1).await;
    f.node_block.store(true, Ordering::SeqCst);
    let admitted = submit(&host, &f).await;
    assert_eq!(admitted.status(), 201);
    let snapshot = admitted
        .json::<AgentHttpRunResponse>()
        .await
        .unwrap()
        .snapshot;
    until(|| host.health().worker().unwrap().active_nodes() == 1).await;
    let events = client()
        .get(format!(
            "http://{}/v1/agent-runs/{}/events",
            host.local_addr(),
            snapshot.provenance().run_id()
        ))
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .bearer_auth(fixture::TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(events.status(), 200);
    until(|| host.health().http().unwrap().active_streams() > 0).await;
    f.auth.block.store(true, Ordering::SeqCst);
    let request = client()
        .post(format!("http://{}/v1/agent-runs", host.local_addr()))
        .bearer_auth(fixture::TOKEN)
        .json(&f.submission())
        .send();
    tokio::pin!(request);
    tokio::select! { _ = &mut request => panic!("blocked authentication returned"), () = f.auth.entered.notified() => {} }
    host.begin_shutdown();
    assert_eq!(host.health().status(), AgentHostStatus::Draining);
    let health = host.health();
    let report = {
        let shutdown = host.wait();
        tokio::pin!(shutdown);
        tokio::select! {
            result = &mut shutdown => panic!("forced Worker drain completed before observation: {result:?}"),
            () = until(|| health.worker().unwrap().status() == AgentWorkerStatus::Draining) => {},
        }
        assert!(!health.http().unwrap().is_live());
        assert_eq!(
            health.maintenance().unwrap().status(),
            AgentMaintenanceStatus::Ready
        );
        timeout(Duration::from_secs(10), shutdown)
            .await
            .unwrap()
            .unwrap()
    };
    assert!(report.http.unwrap().unwrap().forced_connections > 0);
    assert!(report.worker.unwrap().unwrap().forced_ticks > 0);
    assert_eq!(report.failure, None);
    joined(&host, &f).await;
    drop(events);
    f.store.close().await;
    println!(
        "\nSTATEKNOT_HOST_DRAIN_EVIDENCE={{\"ordered_drain\":true,\"maintenance_during_worker_drain\":true,\"forced_connections\":true,\"joined_streams_and_nodes\":true}}"
    );
}

#[tokio::test]
async fn postgres_host_drop_before_poll_and_running_drop_close_ownership() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    for started in [false, true] {
        let (host, service) = launch(&f, &checks(), evidence(&f), limits()).await;
        if started {
            ready(&host).await;
            let c = checks();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            assert!(matches!(
                AgentHost::launch(
                    listener,
                    AgentHostBindings::new(
                        service.clone(),
                        binding(&f, evidence(&f)),
                        maintenance(&f)
                    ),
                    AgentHostDependencies {
                        http: c[0].clone(),
                        worker: c[1].clone(),
                        maintenance: c[2].clone()
                    },
                    limits()
                ),
                Err(AgentHostError::AlreadyClaimed)
            ));
            assert_eq!(submit(&host, &f).await.status(), 201);
        }
        let health = host.health();
        let address = host.local_addr();
        drop(host);
        assert_eq!(health.status(), AgentHostStatus::Stopped);
        until(|| idle(&health)).await;
        // Yield until the aborted coordinator's guard has actually been dropped.
        timeout(Duration::from_secs(5), async {
            while TcpStream::connect(address).await.is_ok() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        assert!(matches!(
            AgentHttpServer::start(listener, service, Arc::new(Host::default()), limits().http)
                .await,
            Err(AgentHttpServerError::AlreadyClaimed)
        ));
    }
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let service = AgentHttpService::new(
        f.service.clone(),
        f.auth.clone(),
        AgentHttpOptions::loopback(8080).unwrap(),
    );
    let c = checks();
    assert!(matches!(
        AgentHost::launch(
            listener,
            AgentHostBindings::new(service, binding(&f, evidence(&f)), maintenance(&f)),
            AgentHostDependencies {
                http: c[0].clone(),
                worker: c[1].clone(),
                maintenance: c[2].clone()
            },
            limits()
        ),
        Err(AgentHostError::InvalidListener)
    ));
    f.store.close().await;
    println!(
        "\nSTATEKNOT_HOST_DROP_EVIDENCE={{\"before_first_poll\":true,\"running_drop\":true,\"loopback_required\":true,\"eventual_idle\":true}}"
    );
}
