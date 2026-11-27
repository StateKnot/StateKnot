// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::{
    BudgetUsage, Digest, NodeAttemptCompletion, NodeAttemptOutcome, NodeAttemptStart,
    NodeInvocationBinding, Postgres, StoreError, ToolInvocation, ToolInvocationHead, Transaction,
    query_scalar, verified_tool_revision,
};
use stateknot_core::{
    ByteCount, ExecutionCount, NodeInvocationBindings, ToolInvocationState, ToolRisk,
};

type Charge = (NodeInvocationBinding, BudgetUsage);
const DOMAIN: &[u8] = b"stateknot.node-failure-tool-charges.v1\0";
const MAX_BYTES: usize = 1_048_576;

pub(super) struct Receipt {
    pub(super) entries: Vec<Charge>,
    pub(super) bytes: Vec<u8>,
}

impl Receipt {
    pub(super) fn new(charges: &[Charge]) -> Result<Self, StoreError> {
        if charges.len() > NodeInvocationBindings::MAX_LEN {
            return Err(StoreError::InvalidNodeAttemptTransition);
        }
        let mut entries = charges.to_vec();
        let key = |entry: &Charge| {
            (
                entry.0.invocation_id(),
                entry.0.tool_head().and_then(ToolInvocationHead::attempt_id),
            )
        };
        entries.sort_by_key(key);
        if entries.iter().any(|entry| key(entry).1.is_none())
            || entries
                .windows(2)
                .any(|pair| key(&pair[0]) == key(&pair[1]))
        {
            return Err(StoreError::InvalidNodeAttemptTransition);
        }
        let bytes = serde_json_canonicalizer::to_vec(&entries)
            .map_err(|_| StoreError::encoding("failed attempt Tool charges"))?;
        if bytes.len() > MAX_BYTES {
            return Err(StoreError::InvalidNodeAttemptTransition);
        }
        Ok(Self { entries, bytes })
    }

    pub(super) async fn verify(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        completion: &NodeAttemptCompletion,
    ) -> Result<(), StoreError> {
        let activation = completion.start().activation();
        let mut total = BudgetUsage::zero();
        for (binding, charge) in &self.entries {
            let head = binding
                .tool_head()
                .ok_or(StoreError::InvalidNodeAttemptTransition)?;
            if binding.activation() != activation
                || head.journal_head().sequence() >= completion.journal_head().sequence()
            {
                return Err(StoreError::InvalidNodeAttemptTransition);
            }
            let (_, tool) = Box::pin(verified_tool_revision(
                tx,
                activation.tenant_id(),
                activation.run_id(),
                head.invocation_id(),
                head.revision(),
            ))
            .await?;
            if NodeInvocationBinding::from_tool(&tool).as_ref() != Ok(binding)
                || &terminal_usage(&tool, charge)? != charge
            {
                return Err(StoreError::InvalidNodeAttemptTransition);
            }
            total = total
                .checked_accumulate(charge)
                .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
        }
        if &total != completion.usage() {
            return Err(StoreError::InvalidNodeAttemptTransition);
        }
        Ok(())
    }

    // The run row serializes failure commits. The activation index bounds
    // candidate receipts to the physical attempts of this single activation.
    pub(super) async fn reject_previously_charged(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        start: &NodeAttemptStart,
    ) -> Result<(), StoreError> {
        if self.entries.is_empty() {
            return Ok(());
        }
        let mut invocations = Vec::with_capacity(self.entries.len());
        let mut attempts = Vec::with_capacity(self.entries.len());
        for (binding, _) in &self.entries {
            invocations.push(*binding.invocation_id().as_uuid());
            let attempt = binding
                .tool_head()
                .and_then(ToolInvocationHead::attempt_id)
                .ok_or(StoreError::InvalidNodeAttemptTransition)?;
            attempts.push(*attempt.as_uuid());
        }
        let duplicated = query_scalar::<_, bool>(r"
            SELECT EXISTS (
                SELECT 1 FROM stateknot.node_attempt_completions AS c
                CROSS JOIN LATERAL jsonb_array_elements(convert_from(c.tool_charges, 'UTF8')::jsonb) AS entry(value)
                JOIN unnest($4::uuid[], $5::uuid[]) AS proposed(invocation, attempt)
                  ON (entry.value #>> '{0,head,invocation_id}')::uuid = proposed.invocation
                 AND (entry.value #>> '{0,head,attempt_id}')::uuid = proposed.attempt
                WHERE c.tenant_id=$1 AND c.run_id=$2 AND c.activation_digest=$3
                  AND c.tool_charges IS NOT NULL
            )")
            .bind(start.activation().tenant_id().as_str())
            .bind(*start.activation().run_id().as_uuid())
            .bind(start.activation_digest().as_bytes())
            .bind(invocations)
            .bind(attempts)
            .fetch_one(&mut **tx)
            .await
            .map_err(|source| StoreError::database("previous failed attempt Tool charges", source))?;
        if duplicated {
            return Err(StoreError::InvalidNodeAttemptTransition);
        }
        Ok(())
    }
}

fn terminal_usage(tool: &ToolInvocation, charge: &BudgetUsage) -> Result<BudgetUsage, StoreError> {
    let (output_bytes, artifact_bytes) = match tool.state() {
        ToolInvocationState::Committed { result } => (
            ByteCount::new(result.output().stats().compact_bytes() as u64),
            result.artifacts().total_bytes(),
        ),
        ToolInvocationState::Failed { .. } => (ByteCount::ZERO, ByteCount::ZERO),
        _ => return Err(StoreError::InvalidNodeAttemptTransition),
    };
    BudgetUsage::builder()
        .tool_calls(ExecutionCount::new(1))
        .write_calls(
            if tool.intent().descriptor().semantics().risk() == ToolRisk::ReadOnly {
                ExecutionCount::ZERO
            } else {
                ExecutionCount::new(1)
            },
        )
        .input_bytes(ByteCount::new(
            tool.intent().input().value().stats().compact_bytes() as u64,
        ))
        .output_bytes(output_bytes)
        .artifact_bytes(artifact_bytes)
        .known_costs(charge.known_costs().clone())
        .build()
        .map_err(|_| StoreError::InvalidNodeAttemptTransition)
}

pub(super) fn projection(completion: &NodeAttemptCompletion, charges: Option<&[u8]>) -> Digest {
    let Some(bytes) = charges else {
        return completion.digest();
    };
    Digest::sha256([DOMAIN, completion.digest().as_bytes(), bytes].concat())
}

async fn load_bytes(
    tx: &mut Transaction<'_, Postgres>,
    completion: &NodeAttemptCompletion,
) -> Result<Option<Vec<u8>>, StoreError> {
    let start = completion.start();
    query_scalar::<_, Option<Vec<u8>>>("SELECT tool_charges FROM stateknot.node_attempt_completions WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3")
        .bind(start.activation().tenant_id().as_str())
        .bind(*start.activation().run_id().as_uuid())
        .bind(*start.attempt_id().as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|source| StoreError::database("failed attempt Tool charges", source))?
        .ok_or_else(|| StoreError::corrupt("missing failed attempt Tool receipt row"))
}

pub(super) async fn verified_projection(
    tx: &mut Transaction<'_, Postgres>,
    completion: &NodeAttemptCompletion,
) -> Result<(Digest, Option<Vec<Charge>>), StoreError> {
    if !matches!(completion.outcome(), NodeAttemptOutcome::Failed { .. }) {
        return Ok((completion.digest(), None));
    }
    let Some(bytes) = load_bytes(tx, completion).await? else {
        return Ok((completion.digest(), None));
    };
    let corrupt = || StoreError::corrupt("failed attempt Tool charges");
    if bytes.len() > MAX_BYTES {
        return Err(corrupt());
    }
    let entries: Vec<Charge> = serde_json::from_slice(&bytes).map_err(|_| corrupt())?;
    let receipt = Receipt::new(&entries).map_err(|_| corrupt())?;
    if receipt.bytes != bytes {
        return Err(corrupt());
    }
    receipt.verify(tx, completion).await.map_err(|e| match e {
        StoreError::InvalidNodeAttemptTransition | StoreError::ToolInvocationNotFound => corrupt(),
        other => other,
    })?;
    Ok((projection(completion, Some(&bytes)), Some(receipt.entries)))
}
