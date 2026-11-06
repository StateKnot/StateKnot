// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};

/// Allocate the current value, leaving the exclusive ceiling as an exhausted sentinel.
pub(crate) fn allocate(
    counter: &AtomicU64,
    ceiling: u64,
    set_order: Ordering,
    fetch_order: Ordering,
) -> Option<u64> {
    let mut current = counter.load(fetch_order);
    loop {
        if current >= ceiling {
            return None;
        }
        let next = current.checked_add(1)?;
        match counter.compare_exchange_weak(current, next, set_order, fetch_order) {
            Ok(allocated) => return Some(allocated),
            Err(observed) => current = observed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn concurrent_allocations_are_unique_and_stop_at_the_ceiling() {
        for (start, ceiling) in [(1, 129), (u64::MAX - 128, u64::MAX)] {
            let counter = AtomicU64::new(start);
            let barrier = Barrier::new(8);
            let mut allocated = std::thread::scope(|scope| {
                let workers: Vec<_> = (0..8)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            let mut ids = Vec::new();
                            while let Some(id) =
                                allocate(&counter, ceiling, Ordering::AcqRel, Ordering::Acquire)
                            {
                                ids.push(id);
                            }
                            ids
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .flat_map(|worker| worker.join().unwrap())
                    .collect::<Vec<_>>()
            });
            allocated.sort_unstable();
            assert_eq!(allocated, (start..ceiling).collect::<Vec<_>>());
            assert_eq!(counter.load(Ordering::Acquire), ceiling);
            assert_eq!(
                allocate(&counter, ceiling, Ordering::AcqRel, Ordering::Acquire),
                None
            );
        }
    }
}
