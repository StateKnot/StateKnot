// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::{super::AgentHttpAuthenticationError, IntrospectionOptions};
use serde_json::Value;
use stateknot_core::PrincipalIdentity;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn valid_scope(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| matches!(b, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
}

pub(super) fn verify(
    value: &Value,
    options: &IntrospectionOptions,
) -> Result<(PrincipalIdentity, Vec<String>), AgentHttpAuthenticationError> {
    let denied = AgentHttpAuthenticationError::Unauthenticated;
    if value["active"] != true
        || value.get("cnf").is_some()
        || value["iss"].as_str() != Some(options.issuer.as_str())
        || !value["token_type"]
            .as_str()
            .is_some_and(|v| v.eq_ignore_ascii_case("Bearer"))
    {
        return Err(denied);
    }
    let audiences = match &value["aud"] {
        Value::String(aud) => vec![aud.as_str()],
        Value::Array(aud) if !aud.is_empty() && aud.len() <= 32 => aud
            .iter()
            .map(|v| v.as_str().ok_or(denied))
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err(denied),
    };
    if !audiences.contains(&options.audience.as_str())
        || audiences.iter().any(|a| a.is_empty() || a.len() > 512)
    {
        return Err(denied);
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AgentHttpAuthenticationError::Unavailable)?
        .as_secs();
    let exp = value["exp"].as_u64().ok_or(denied)?;
    let iat = value["iat"].as_u64().ok_or(denied)?;
    if exp <= now || iat > now || iat >= exp || exp - iat > options.max_token_lifetime.as_secs() {
        return Err(denied);
    }
    if let Some(nbf) = value.get("nbf") {
        let nbf = nbf.as_u64().ok_or(denied)?;
        if nbf > now || nbf >= exp {
            return Err(denied);
        }
    }
    let subject = value["sub"]
        .as_str()
        .ok_or(denied)?
        .parse()
        .map_err(|_| denied)?;
    let scope = match value.get("scope") {
        None => "",
        Some(value) => value.as_str().ok_or(denied)?,
    };
    let mut scopes = Vec::new();
    if !scope.is_empty() {
        for scope in scope.split(' ') {
            if scopes.len() >= 64 || !valid_scope(scope) || scopes.iter().any(|s| s == scope) {
                return Err(denied);
            }
            scopes.push(scope.to_owned());
        }
    }
    Ok((
        PrincipalIdentity::new(options.issuer.clone(), subject),
        scopes,
    ))
}
