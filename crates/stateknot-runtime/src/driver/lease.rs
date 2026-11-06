// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Lease maintenance while bounded recovery and accounting await durable reads.

use super::{
    AtomicBool, BoxFuture, CancellationObserver, CancellationSignal, DriverCancellation,
    DurableGraphDriver, GraphDriveOutcome, GraphDriveResult, GraphDriverError, Ordering, RunFence,
};
use stateknot_store_postgres::{DelayedRetryScheduleOutcome, LeaseReleaseOutcome};
use std::sync::atomic::AtomicU32;

// These results already own a complete Store ACK that releases ownership.
// Other handoffs still require the guardian's original live authority. An
// internal guardian stop alone must not relabel its error as user shutdown.
fn acknowledged_release(outcome: &GraphDriveOutcome, external_stop: bool) -> bool {
    match outcome {
        GraphDriveOutcome::FrameWaiting(_)
        | GraphDriveOutcome::ChildJoin(_)
        | GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released | LeaseReleaseOutcome::Idempotent,
        }
        | GraphDriveOutcome::Deferred {
            schedule:
                DelayedRetryScheduleOutcome::Scheduled { .. }
                | DelayedRetryScheduleOutcome::Idempotent { .. },
            ..
        } => true,
        GraphDriveOutcome::Cancelled {
            release: LeaseReleaseOutcome::Released | LeaseReleaseOutcome::Idempotent,
        } => external_stop,
        _ => false,
    }
}

// Observation control only. The original full owning readers and final live
// fence checks continue to decide recovery and launch authority.
struct RecoveryShutdown {
    shutdown: CancellationSignal,
    lease_lost: DriverCancellation,
}

impl CancellationObserver for RecoveryShutdown {
    fn is_cancelled(&self) -> bool {
        self.shutdown.is_cancelled() || self.lease_lost.is_cancelled()
    }

    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            tokio::select! {
                () = self.shutdown.cancelled() => {},
                () = self.lease_lost.cancelled() => {},
            }
        })
    }
}

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
            let lease_lost = DriverCancellation::new();
            let external_shutdown = shutdown.clone();
            let shutdown = CancellationSignal::new(RecoveryShutdown {
                shutdown,
                lease_lost: lease_lost.clone(),
            });
            let mut driving = self.drive_inner(fence.clone(), shutdown, &node_phase);
            let (stopping, mut stopped) = tokio::sync::oneshot::channel();
            let maintenance = async {
                loop {
                    // Only a pure wait can be interrupted. Once a lease read
                    // or renewal starts, its complete acknowledgement is owned.
                    tokio::select! {
                        biased;
                        _ = &mut stopped => return Ok::<(), GraphDriverError>(()),
                        () = tokio::time::sleep(self.options.lease_renewal_interval) => {},
                    }
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
            };
            tokio::pin!(maintenance);
            let result = tokio::select! {
                biased;
                result = &mut driving => {
                    let _ = stopping.send(());
                    // A completed drive owns its original result, including a
                    // release that can make the concurrent observation fail.
                    // Drain any earlier renewal before returning that result.
                    let _ = maintenance.await;
                    result
                }
                result = &mut maintenance => {
                    let external_stop = external_shutdown.is_cancelled();
                    lease_lost.cancel();
                    // Full owning recovery may be committing quarantine or a
                    // graph mutation. Signal cooperative stop and retain its
                    // whole ACK instead of dropping that durable operation.
                    match driving.await {
                        Err(error) => Err(error),
                        Ok(driven) if acknowledged_release(driven.outcome(), external_stop) => Ok(driven),
                        Ok(_) => match result {
                            Err(error) => Err(error),
                            Ok(()) => Err(GraphDriverError::RuntimeInvariant { operation: "maintain bounded recovery lease" }),
                        },
                    }
                }
            };
            result.map(|mut result| {
                result.report.lease_renewals = result
                    .report
                    .lease_renewals
                    .saturating_add(renewals.load(Ordering::Relaxed));
                result.report.mutation_retries = result
                    .report
                    .mutation_retries
                    .saturating_add(retries.load(Ordering::Relaxed));
                result
            })
        })
    }
}
