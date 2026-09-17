// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot::agent_host::{AgentHost, operations::*};
const OPERATOR: &str = "6d0680d3-9ab7-4cf2-82e9-9db41132870a";

pub(super) async fn qualify(
    host: &mut AgentHost,
    f: &Fixture,
    client: &reqwest::Client,
    issuer: &str,
    delivered_secret: &str,
    business_token: &str,
    business_auth: &AgentHttpIntrospection,
) {
    let ca = std::fs::read(std::env::var("STATEKNOT_TEST_IDENTITY_CA").unwrap()).unwrap();
    let caller = AgentServiceCaller::new(
        f.caller.tenant_id().clone(),
        PrincipalIdentity::new(issuer.parse().unwrap(), OPERATOR.parse().unwrap()),
    );
    let bindings = vec![
        TenantBinding::new(
            caller.tenant_id().clone(),
            caller.principal().clone(),
            &[AgentHttpOperation::InspectHost],
        ),
        // Explicit local grant cannot invent a scope absent from a business token.
        binding(
            f.caller.tenant_id().clone(),
            issuer,
            &[AgentHttpOperation::InspectHost],
        ),
    ];
    let tenants = Arc::new(TenantPolicy::new(bindings, Duration::from_secs(300)).unwrap());
    let default = AgentHttpIntrospection::new(
        config(issuer).with_root_certificate(&ca).unwrap(),
        secret(delivered_secret),
        tenants.clone(),
    )
    .unwrap();
    let token = access_token(client, issuer, "host-operator", "fixture-operator-secret").await;
    assert!(
        !default
            .authenticate(credential(&token))
            .await
            .unwrap()
            .allows(AgentHttpOperation::InspectHost)
    );
    let auth = Arc::new(
        AgentHttpIntrospection::new(
            config(issuer)
                .with_host_inspection_scope("stateknot:host:inspect".into())
                .unwrap()
                .with_root_certificate(&ca)
                .unwrap(),
            secret(delivered_secret),
            tenants,
        )
        .unwrap(),
    );
    let principal = auth.authenticate(credential(&token)).await.unwrap();
    assert!(principal.allows(AgentHttpOperation::InspectHost));
    for operation in [
        AgentHttpOperation::Submit,
        AgentHttpOperation::Read,
        AgentHttpOperation::Cancel,
    ] {
        assert!(!principal.allows(operation));
    }
    let policy = Arc::new(
        AgentHostOperationsPolicy::new(
            host.health(),
            vec![caller.clone(), f.caller.clone()],
            Duration::from_secs(300),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut ops = AgentHostOperations::start(
        listener,
        policy.clone(),
        auth.clone(),
        AgentHostOperationsOptions::new([address.to_string()]).unwrap(),
    )
    .unwrap();
    let url = format!("http://{address}/v1/host/status");
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(business_token)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .post(format!("http://{}/v1/agent-runs", host.local_addr()))
            .bearer_auth(&token)
            .json(&f.submission())
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    policy
        .replace(1, vec![f.caller.clone()], Duration::from_secs(300))
        .unwrap();
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    policy
        .replace(2, vec![caller.clone()], Duration::from_millis(1))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    policy
        .replace(3, vec![caller], Duration::from_secs(300))
        .unwrap();
    let admin = access_token(client, issuer, "fixture-admin", "fixture-admin-secret").await;
    let admin_url = format!(
        "{}/admin/realms/stateknot-qualification",
        issuer.split("/realms/").next().unwrap()
    );
    let rotate = client
        .post(format!(
            "{admin_url}/clients/stateknot-introspection-client/client-secret"
        ))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    assert_eq!(rotate.status(), 200);
    let rotated = rotate.json::<Value>().await.unwrap()["value"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
    business_auth
        .replace_client_secret(secret(&rotated))
        .unwrap();
    auth.replace_client_secret(secret(&rotated)).unwrap();
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let disable = client
        .put(format!("{admin_url}/users/{OPERATOR}"))
        .bearer_auth(&admin)
        .json(&json!({"enabled":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(disable.status(), 204);
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(ops.shutdown().await.unwrap().forced_connections, 0);
    assert_eq!(ops.active_connections(), 0);
    println!(
        "\nSTATEKNOT_OPERATIONS_IDENTITY_EVIDENCE={{\"verified_tls\":true,\"dedicated_ops_principal\":true,\"scope_opt_in\":true,\"business_token_denied\":true,\"ops_business_denied\":true,\"separate_acl\":true,\"expiry\":true,\"rotation_recovery\":true,\"idp_revocation\":true,\"joined\":true}}"
    );
}
