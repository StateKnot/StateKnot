// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Lease maintenance while bounded recovery and accounting await durable reads.

use super::{
    AtomicBool, BoxFuture, CancellationSignal, DurableGraphDriver, GraphDriveResult,
    GraphDriverError, Ordering, RunFence,
};
use std::sync::atomic::AtomicU32;

impl DurableGraphDriver {
    pub(super) fn drive_with_recovery_lease(
        &self,
        fence: RunFence,
        shutdown: CancellationSignal,
    ) -> BoxFuture<'_, Result<GraphDriveResult, GraphDriverError>> {
        Box::pin(async move {
            let node_phase = AtomicBool::new(false);
            let renewals = AtomicU32::new(0);
            let retries = AtomicU32::new(0);
            let mut driving = self.drive_inner(fence.clone(), shutdown, &node_phase);
            let maintenance = async {
                loop {
                    tokio::time::sleep(self.options.lease_renewal_interval).await;
                    // Node futures retain their existing guarded monotonic lease,
                    // cancellation and deadline maintenance. Recovery owns only
                    // the periods outside that concrete execution phase.
                    if node_phase.load(Ordering::Acquire) {
                        continue;
                    }
                    let (renewed, extra_attempts) = self.refresh_near_expiry_lease(&fence).await?;
                    renewals.fetch_add(renewed, Ordering::Relaxed);
                    retries.fetch_add(extra_attempts, Ordering::Relaxed);
                }
                #[allow(unreachable_code)]
                Ok::<(), GraphDriverError>(())
            };
            tokio::pin!(maintenance);
            tokio::select! {
                biased;
                result = &mut driving => {
                    result.map(|mut result| {
                        result.report.lease_renewals = result.report.lease_renewals.saturating_add(renewals.load(Ordering::Relaxed));
                        result.report.mutation_retries = result.report.mutation_retries.saturating_add(retries.load(Ordering::Relaxed));
                        result
                    })
                }
                result = &mut maintenance => {
                    match result {
                        Err(error) => Err(error),
                        Ok(()) => Err(GraphDriverError::RuntimeInvariant { operation: "maintain bounded recovery lease" }),
                    }
                }
            }
        })
    }
}
