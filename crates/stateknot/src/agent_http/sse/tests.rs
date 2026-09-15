// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot_core::{Digest, TenantId};

#[test]
fn cursor_is_canonical_bounded_and_supports_maximum_scope() {
    let head = JournalHead::new(
        TenantId::new("t".repeat(TenantId::MAX_LEN)).unwrap(),
        RunId::generate(),
        JournalSequence::new(i64::MAX as u64).unwrap(),
        EventId::generate(),
        Timestamp::from_unix_micros(1).unwrap(),
        Digest::sha256(b"activity"),
    );
    let encoded = cursor(&head).unwrap();
    assert!(encoded.len() <= MAX_CURSOR_BYTES);
    assert_eq!(parse_cursor(&encoded).unwrap(), head);
    for invalid in [
        String::new(),
        "sk2.a".into(),
        "sk1.???".into(),
        format!("{encoded}="),
        "x".repeat(MAX_CURSOR_BYTES + 1),
        format!("sk1.{}", URL_SAFE_NO_PAD.encode(b"{}")),
        format!(
            "sk1.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_string_pretty(&head).unwrap())
        ),
    ] {
        assert!(parse_cursor(&invalid).is_err());
    }
}

#[test]
fn sse_limits_are_finite_and_opt_in() {
    let second = Duration::from_secs(1);
    for (streams, life, poll, wait) in [
        (0, second, second, second),
        (129, second, second, second),
        (1, Duration::ZERO, second, second),
        (1, Duration::from_secs(601), second, second),
        (1, second, Duration::ZERO, second),
        (1, second, second, Duration::ZERO),
        (1, second, Duration::from_secs(2), second),
    ] {
        assert!(AgentHttpSseOptions::new(streams, life, poll, wait).is_err());
    }
    assert!(
        super::super::AgentHttpOptions::loopback(1234)
            .unwrap()
            .sse
            .is_none()
    );
    let mut headers = HeaderMap::new();
    assert_eq!(validate_media(&headers), Err(HttpError::Accept));
    headers.insert(header::ACCEPT, "text/event-stream".parse().unwrap());
    assert_eq!(validate_media(&headers), Ok(()));
    headers.append(header::ACCEPT, "text/event-stream".parse().unwrap());
    assert_eq!(validate_media(&headers), Err(HttpError::Invalid));
}
