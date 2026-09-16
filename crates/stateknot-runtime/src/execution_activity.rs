// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use tokio_util::task::TaskTracker;

/// Shared completion accounting for node futures dispatched by one driver.
/// Clones do not own the store or executors and do not cancel execution.
#[derive(Clone, Default, Debug)]
pub struct GraphExecutionActivity(TaskTracker);

impl GraphExecutionActivity {
    /// Number of registered node futures, including ones not yet polled.
    pub fn active_nodes(&self) -> usize {
        self.0.len()
    }

    /// Waits for destruction of all tracked node futures. The caller MUST first
    /// stop and join every producer using this driver (including its clones).
    /// This is not a dispatch lock: subsequent dispatch remains possible.
    /// Cancelling the wait does not lose task accounting.
    pub async fn wait_for_idle(&self) {
        self.0.close();
        self.0.wait().await;
    }

    pub(crate) fn track<F: Future>(&self, future: F) -> impl Future<Output = F::Output> + use<F> {
        self.0.track_future(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn tracks_before_poll_and_waits_for_future_destruction() {
        let activity = GraphExecutionActivity::default();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let future = activity.track(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        assert_eq!(activity.active_nodes(), 1);
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(future);
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        activity.wait_for_idle().await;
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(activity.active_nodes(), 0);
    }
}
