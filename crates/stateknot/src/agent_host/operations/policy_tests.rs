// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot_core::PrincipalIdentity;
fn caller(tenant: &str) -> AgentServiceCaller {
    AgentServiceCaller::new(
        tenant.parse().unwrap(),
        PrincipalIdentity::new(
            "https://issuer.example.test".parse().unwrap(),
            "fixture-subject".parse().unwrap(),
        ),
    )
}
#[tokio::test]
async fn bounded_exact_policy_cas_expiry_and_recovery() {
    let health = AgentHostHealth::new();
    let operator = caller("one");
    let principal = AgentHttpPrincipal::new(operator.clone(), [AgentHttpOperation::InspectHost]);
    for (callers, lease) in [
        (vec![], Duration::ZERO),
        (vec![], Duration::from_secs(3601)),
        (vec![operator.clone(); 2], Duration::from_secs(1)),
        (vec![operator.clone(); 129], Duration::from_secs(1)),
    ] {
        assert!(AgentHostOperationsPolicy::new(health.clone(), callers, lease).is_err());
    }
    let all: Vec<_> = (0..128)
        .map(|index| caller(&format!("tenant-{index}")))
        .collect();
    assert!(AgentHostOperationsPolicy::new(health.clone(), all, Duration::from_secs(1)).is_ok());
    let policy =
        AgentHostOperationsPolicy::new(health, vec![operator.clone()], Duration::from_secs(30))
            .unwrap();
    assert_eq!(policy.authorize(&principal), Ok(()));
    assert_eq!(
        policy.authorize(&AgentHttpPrincipal::new(
            operator.clone(),
            [AgentHttpOperation::Read]
        )),
        Err(HttpError::Denied)
    );
    assert_eq!(
        policy.authorize(&AgentHttpPrincipal::new(
            caller("two"),
            [AgentHttpOperation::InspectHost]
        )),
        Err(HttpError::Denied)
    );
    assert!(policy.replace(0, vec![], Duration::from_secs(1)).is_err());
    assert!(
        policy
            .replace(u64::MAX, vec![], Duration::from_secs(1))
            .is_err()
    );
    assert!(
        policy
            .replace(1, vec![operator.clone(); 2], Duration::from_secs(1))
            .is_err()
    );
    assert_eq!(policy.generation().unwrap(), 1);
    policy.replace(1, vec![], Duration::from_secs(30)).unwrap();
    assert_eq!(policy.authorize(&principal), Err(HttpError::Denied));
    policy
        .replace(2, vec![operator.clone()], Duration::from_millis(1))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(policy.authorize(&principal), Err(HttpError::Unavailable));
    policy
        .replace(3, vec![operator], Duration::from_secs(30))
        .unwrap();
    assert_eq!(policy.authorize(&principal), Ok(()));
    let _ = std::panic::catch_unwind(|| {
        let _lock = policy.snapshot.write().unwrap();
        panic!("fixture poison");
    });
    assert!(policy.generation().is_err());
    assert_eq!(policy.authorize(&principal), Err(HttpError::Unavailable));
    assert!(policy.replace(4, vec![], Duration::from_secs(1)).is_err());
}
