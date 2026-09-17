// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Private shared HTTP/1 connection bounds, not a general server framework.
use crate::agent_http::{AgentHttpDrainReport, AgentHttpServerOptions};
use axum::Router;
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{net::TcpStream, time::sleep};
use tokio_util::sync::CancellationToken;

pub(crate) struct ConnectionGuard(Arc<AtomicUsize>);
impl ConnectionGuard {
    pub(crate) fn new(active: Arc<AtomicUsize>) -> Self {
        active.fetch_add(1, Ordering::AcqRel);
        Self(active)
    }
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) fn count_failure(
    result: Option<&Result<bool, tokio::task::JoinError>>,
    report: &mut AgentHttpDrainReport,
) {
    if !matches!(result, Some(Ok(true))) {
        report.connection_failures = report.connection_failures.saturating_add(1);
    }
}

pub(crate) async fn connection(
    socket: TcpStream,
    router: Router,
    drain: CancellationToken,
    options: AgentHttpServerOptions,
    _active: ConnectionGuard,
) -> bool {
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .keep_alive(true)
        .half_close(false)
        .max_headers(64)
        .max_buf_size(32 * 1024)
        .header_read_timeout(options.header_timeout);
    let connection =
        builder.serve_connection(TokioIo::new(socket), TowerToHyperService::new(router));
    tokio::pin!(connection);
    let lifetime = sleep(options.connection_lifetime);
    tokio::pin!(lifetime);
    tokio::select! {
        biased;
        () = drain.cancelled() => connection.as_mut().graceful_shutdown(),
        () = &mut lifetime => return false,
        result = &mut connection => return result.is_ok(),
    }
    tokio::select! {
        biased;
        () = &mut lifetime => false,
        result = &mut connection => result.is_ok(),
    }
}
