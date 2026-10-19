// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Raw immutable closure keys for bounded, tenant-scoped maintenance sweeps.
#[allow(clippy::wildcard_imports)]
use super::*;

const PAGE_LIMIT: usize = 16;
const SELECT_UPPER: &str = "SELECT run_id FROM stateknot.graph_frame_closures WHERE tenant_id=$1 ORDER BY run_id DESC LIMIT 1";
const SELECT_FIRST: &str = "SELECT run_id FROM stateknot.graph_frame_closures WHERE tenant_id=$1 AND run_id<=$2 ORDER BY run_id LIMIT 17";
const SELECT_AFTER: &str = "SELECT run_id FROM stateknot.graph_frame_closures WHERE tenant_id=$1 AND run_id>$2 AND run_id<=$3 ORDER BY run_id LIMIT 17";

/// An actual processed key within one tenant's fixed upper-bound sweep.
///
/// This in-memory cursor grants no authority and is not a durable watermark.
/// Start a new sweep with `None` after exhaustion or process loss.
#[derive(Clone, Debug)]
pub struct GraphFrameClosureCursor {
    tenant: TenantId,
    upper_run_id: RunId,
    after_run_id: RunId,
}

impl GraphFrameClosureCursor {
    /// Tenant explicitly selected by the authenticated maintenance host.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant
    }

    /// The actual candidate key processed before continuing this sweep.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.after_run_id
    }
}

/// At most sixteen raw closure keys and a seventeenth-row lookahead flag.
///
/// No lifecycle, quarantine, payload or owning-proof filtering is performed.
#[derive(Clone, Debug)]
pub struct GraphFrameClosurePage {
    candidates: Vec<GraphFrameClosureCursor>,
    has_more: bool,
}

impl GraphFrameClosurePage {
    /// Original PK-ordered candidates, including currently inapplicable records.
    #[must_use]
    pub fn candidates(&self) -> &[GraphFrameClosureCursor] {
        &self.candidates
    }

    /// Whether another raw key exists within this sweep's fixed upper bound.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

impl PostgresStore {
    /// Reads one bounded raw closure page under a short read-only snapshot.
    ///
    /// A new sweep captures the greatest existing PK in the same snapshot as
    /// its first page. Later pages retain that upper bound. Advance only after
    /// handling each actual candidate, including an explicit item error or
    /// inapplicable record; the lookahead row is never a returned cursor.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidGraphFrameClosureCursor`] before reading
    /// when the cursor crosses tenant scope or has advanced past its upper key.
    /// Otherwise returns database or corrupt stored-identity errors.
    pub fn graph_frame_closure_page<'a>(
        &'a self,
        tenant: &'a TenantId,
        after: Option<&'a GraphFrameClosureCursor>,
    ) -> stateknot_core::BoxFuture<'a, Result<GraphFrameClosurePage, StoreError>> {
        Box::pin(async move {
            if after.is_some_and(|cursor| {
                cursor.tenant_id() != tenant || cursor.after_run_id > cursor.upper_run_id
            }) {
                return Err(StoreError::InvalidGraphFrameClosureCursor);
            }
            if after.is_some_and(|cursor| cursor.after_run_id == cursor.upper_run_id) {
                return Ok(GraphFrameClosurePage {
                    candidates: Vec::new(),
                    has_more: false,
                });
            }
            let mut tx = self
                .begin_repeatable_read("frame closure discovery")
                .await?;
            let upper_run_id = if let Some(cursor) = after {
                cursor.upper_run_id
            } else {
                let upper = query_scalar::<_, Uuid>(SELECT_UPPER)
                    .bind(tenant.as_str())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|source| StoreError::database("frame closure upper key", source))?;
                let Some(upper) = upper else {
                    tx.commit().await.map_err(|source| {
                        StoreError::database("frame closure discovery commit", source)
                    })?;
                    return Ok(GraphFrameClosurePage {
                        candidates: Vec::new(),
                        has_more: false,
                    });
                };
                RunId::from_uuid(upper)
                    .map_err(|_| StoreError::corrupt("frame closure upper identity"))?
            };
            let rows = if let Some(cursor) = after {
                query_scalar::<_, Uuid>(SELECT_AFTER)
                    .bind(tenant.as_str())
                    .bind(*cursor.after_run_id.as_uuid())
                    .bind(*upper_run_id.as_uuid())
                    .fetch_all(&mut *tx)
                    .await
            } else {
                query_scalar::<_, Uuid>(SELECT_FIRST)
                    .bind(tenant.as_str())
                    .bind(*upper_run_id.as_uuid())
                    .fetch_all(&mut *tx)
                    .await
            }
            .map_err(|source| StoreError::database("frame closure discovery page", source))?;
            let has_more = rows.len() > PAGE_LIMIT;
            let candidates = rows
                .into_iter()
                .take(PAGE_LIMIT)
                .map(|run| {
                    Ok(GraphFrameClosureCursor {
                        tenant: tenant.clone(),
                        upper_run_id,
                        after_run_id: RunId::from_uuid(run)
                            .map_err(|_| StoreError::corrupt("frame closure candidate identity"))?,
                    })
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            tx.commit()
                .await
                .map_err(|source| StoreError::database("frame closure discovery commit", source))?;
            Ok(GraphFrameClosurePage {
                candidates,
                has_more,
            })
        })
    }
}
