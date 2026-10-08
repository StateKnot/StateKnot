// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Independent models for bounded values, canonical bytes and authority narrowing.

use std::{collections::BTreeSet, fmt::Debug, time::Duration};

use proptest::{collection, prelude::*};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use stateknot_core::{
    AgentSubmissionKey, BoundedJson, ByteCount, CanonicalJson, CurrencyCode, Digest,
    DurationMillis, ExecutionCount, ExtensionKey, ExtensionLimits, ExtensionValue, Extensions,
    IssuerId, JsonLimits, Money, SchedulerShardId, SchemaId, Scope, ScopeSet, SubjectId, TenantId,
    Timestamp, TokenCount, Version,
};

fn round_trip<T: DeserializeOwned + Serialize + Eq + Debug>(value: &T) {
    let wire = serde_json::to_value(value).unwrap();
    let canonical = CanonicalJson::new(&BoundedJson::try_from_value(wire).unwrap()).unwrap();
    let recovered: T = serde_json::from_slice(canonical.as_bytes()).unwrap();
    assert_eq!(&recovered, value);
    let recovered = BoundedJson::try_from_value(serde_json::to_value(recovered).unwrap()).unwrap();
    assert_eq!(CanonicalJson::new(&recovered).unwrap(), canonical);
}

fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        collection::vec(any::<char>(), 0..530).prop_map(|value| value.into_iter().collect()),
        "[A-Za-z0-9._:~-]{0,530}",
    ]
}

fn scope_set(bits: u64) -> ScopeSet {
    ScopeSet::try_new(
        (0..64)
            .filter(|bit| bits & (1 << bit) != 0)
            .map(|bit| Scope::new(format!("scope:{bit:02}")).unwrap()),
    )
    .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn identity_constructor_and_serde_bounds_match_independent_grammars(value in text()) {
        let tenant_valid = !value.is_empty()
            && value.len() <= TenantId::MAX_LEN
            && value != "." && value != ".."
            && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte));
        prop_assert_eq!(TenantId::new(value.clone()).is_ok(), tenant_valid);
        prop_assert_eq!(serde_json::from_value::<TenantId>(json!(&value)).is_ok(), tenant_valid);
        prop_assert_eq!(SchedulerShardId::new(value.clone()).is_ok(), tenant_valid);
        prop_assert_eq!(serde_json::from_value::<SchedulerShardId>(json!(&value)).is_ok(), tenant_valid);

        let key_valid = (AgentSubmissionKey::MIN_LEN..=AgentSubmissionKey::MAX_LEN).contains(&value.len())
            && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._~-".contains(&byte));
        prop_assert_eq!(AgentSubmissionKey::new(value.clone()).is_ok(), key_valid);
        prop_assert_eq!(serde_json::from_value::<AgentSubmissionKey>(json!(&value)).is_ok(), key_valid);

        let subject_valid = !value.is_empty() && value.len() <= SubjectId::MAX_LEN
            && value.bytes().all(|byte| (0x20..=0x7e).contains(&byte));
        prop_assert_eq!(SubjectId::new(value.clone()).is_ok(), subject_valid);
        prop_assert_eq!(serde_json::from_value::<SubjectId>(json!(&value)).is_ok(), subject_valid);

        let scope_valid = !value.is_empty() && value.len() <= Scope::MAX_LEN
            && value.bytes().all(|byte| byte == 0x21 || (0x23..=0x5b).contains(&byte) || (0x5d..=0x7e).contains(&byte));
        prop_assert_eq!(Scope::new(value.clone()).is_ok(), scope_valid);
        prop_assert_eq!(serde_json::from_value::<Scope>(json!(&value)).is_ok(), scope_valid);
    }

    #[test]
    fn submission_digest_binds_exact_tenant_and_key(key in "[A-Za-z0-9._~-]{16,128}") {
        let key = AgentSubmissionKey::new(key).unwrap();
        let tenant = TenantId::new("tenant-a").unwrap();
        let other = TenantId::new("tenant-b").unwrap();
        let mut preimage = b"stateknot.agent-submission-key.v1\0tenant-a\0".to_vec();
        preimage.extend_from_slice(key.as_str().as_bytes());
        prop_assert_eq!(key.digest_for(&tenant), Digest::sha256(&preimage));
        prop_assert_ne!(key.digest_for(&tenant), key.digest_for(&other));
        let debug = format!("{key:?}");
        prop_assert!(!debug.contains(key.as_str()));
        round_trip(&key);
    }

    #[test]
    fn all_version_components_and_digest_bytes_round_trip(
        major in any::<u64>(), minor in any::<u64>(), patch in any::<u64>(), bytes in any::<[u8; 32]>()
    ) {
        let version = Version::new(major, minor, patch);
        let text = format!("{major}.{minor}.{patch}");
        prop_assert_eq!(text.parse::<Version>().unwrap(), version);
        for noncanonical in [format!("0{major}.{minor}.{patch}"), format!("{major}.0{minor}.{patch}"), format!("{major}.{minor}.0{patch}")] {
            prop_assert!(noncanonical.parse::<Version>().is_err());
            prop_assert!(serde_json::from_value::<Version>(json!(noncanonical)).is_err());
        }
        round_trip(&version);
        let digest = Digest::from_sha256(bytes);
        prop_assert_eq!(digest.as_bytes(), &bytes);
        prop_assert_eq!(digest.to_string().parse::<Digest>().unwrap(), digest);
        prop_assert!(digest.to_string().to_ascii_uppercase().parse::<Digest>().is_err());
        round_trip(&digest);
    }

    #[test]
    fn timestamp_constructor_bounds_are_exact(micros in any::<i64>()) {
        let valid = (Timestamp::MIN.unix_micros()..=Timestamp::MAX.unix_micros()).contains(&micros);
        prop_assert_eq!(Timestamp::from_unix_micros(micros).is_ok(), valid);
    }

    #[test]
    fn timestamp_full_range_has_no_precision_loss(micros in Timestamp::MIN.unix_micros()..=Timestamp::MAX.unix_micros()) {
        let value = Timestamp::from_unix_micros(micros).unwrap();
        prop_assert_eq!(value.to_string().parse::<Timestamp>().unwrap(), value);
        if let Ok(system_time) = value.to_system_time() {
            prop_assert_eq!(Timestamp::try_from(system_time).unwrap(), value);
        }
        round_trip(&value);
    }

    #[test]
    fn durations_reject_numeric_wire_overflow_and_precision_loss(value in any::<u64>(), nanos in 1..1_000_000_u32) {
        let valid = value <= i64::MAX.unsigned_abs();
        prop_assert_eq!(DurationMillis::try_from(value).is_ok(), valid);
        prop_assert_eq!(value.to_string().parse::<DurationMillis>().is_ok(), valid);
        prop_assert!(serde_json::from_value::<DurationMillis>(json!(value)).is_err());
        prop_assert!(DurationMillis::try_from(Duration::new(0, nanos)).is_err());
        if valid { round_trip(&DurationMillis::try_from(value).unwrap()); }
    }

    #[test]
    fn duration_arithmetic_matches_nonnegative_i64(left in 0..=i64::MAX, right in 0..=i64::MAX) {
        let a = DurationMillis::new(left).unwrap();
        let b = DurationMillis::new(right).unwrap();
        prop_assert_eq!(a.checked_add(b).map(DurationMillis::as_i64), left.checked_add(right));
        prop_assert_eq!(a.checked_sub(b).map(DurationMillis::as_i64), left.checked_sub(right).filter(|value| *value >= 0));
    }

    #[test]
    fn all_count_and_money_arithmetic_matches_u64(left in any::<u64>(), right in any::<u64>()) {
        macro_rules! count {
            ($ty:ty) => {{
                let a = <$ty>::new(left);
                let b = <$ty>::new(right);
                prop_assert_eq!(a.checked_add(b).map(<$ty>::get), left.checked_add(right));
                prop_assert_eq!(a.checked_sub(b).map(<$ty>::get), left.checked_sub(right));
                prop_assert_eq!(a.checked_mul(right).map(<$ty>::get), left.checked_mul(right));
                round_trip(&a);
            }};
        }
        count!(TokenCount);
        count!(ByteCount);
        count!(ExecutionCount);
        let a = Money::new("USD".parse().unwrap(), left);
        let b = Money::new("USD".parse().unwrap(), right);
        prop_assert_eq!(a.checked_add(b).ok().map(Money::micro_units), left.checked_add(right));
        prop_assert_eq!(a.checked_sub(b).ok().map(Money::micro_units), left.checked_sub(right));
        prop_assert_eq!(a.checked_mul(right).ok().map(Money::micro_units), left.checked_mul(right));
        prop_assert!(a.checked_add(Money::new("EUR".parse().unwrap(), right)).is_err());
        prop_assert!(a.checked_sub(Money::new("EUR".parse().unwrap(), right)).is_err());
        round_trip(&a);
    }

    #[test]
    fn currency_constructor_and_serde_enforce_uppercase(bytes in prop_oneof![
        any::<[u8; 3]>(),
        (b'A'..=b'Z', b'A'..=b'Z', b'A'..=b'Z').prop_map(|(a, b, c)| [a, b, c]),
    ]) {
        let valid = bytes.iter().all(u8::is_ascii_uppercase);
        prop_assert_eq!(CurrencyCode::new(bytes).is_ok(), valid);
        if let Ok(value) = std::str::from_utf8(&bytes) {
            prop_assert_eq!(value.parse::<CurrencyCode>().is_ok(), valid);
            prop_assert_eq!(serde_json::from_value::<CurrencyCode>(json!(value)).is_ok(), valid);
        }
        if valid { round_trip(&CurrencyCode::new(bytes).unwrap()); }
    }

    #[test]
    fn schema_identity_is_canonical_while_issuer_identity_is_exact(path in "[a-z0-9_-]{1,510}") {
        let value = format!("https://issuer.example/{path}");
        let valid = value.len() <= SchemaId::MAX_LEN;
        prop_assert_eq!(value.parse::<SchemaId>().is_ok(), valid);
        prop_assert_eq!(IssuerId::new(value.clone()).is_ok(), valid);
        if valid {
            round_trip(&value.parse::<SchemaId>().unwrap());
            let normalized = IssuerId::new(value.clone()).unwrap();
            let exact = IssuerId::new(value.replace("issuer.example", "ISSUER.example")).unwrap();
            prop_assert_ne!(&normalized, &exact);
            round_trip(&exact);
            for invalid in [value.replace("issuer.example", "ISSUER.example"), format!("{value}?"), format!("{value}#"), value.replace("https://", "https://user@"), value.replace("https://", "http://")] {
                prop_assert!(invalid.parse::<SchemaId>().is_err());
                prop_assert!(serde_json::from_value::<SchemaId>(json!(invalid)).is_err());
            }
        }
    }

    #[test]
    fn delegated_scopes_match_three_party_intersection(a in any::<u64>(), b in any::<u64>(), c in any::<u64>()) {
        let caller = scope_set(a);
        let grant = scope_set(b);
        let policy = scope_set(c);
        let effective = caller.intersection(&grant).intersection(&policy);
        prop_assert_eq!(&effective, &scope_set(a & b & c));
        prop_assert_eq!(&effective, &caller.intersection(&grant.intersection(&policy)));
        prop_assert!(effective.is_subset(&caller));
        prop_assert!(effective.is_subset(&grant));
        prop_assert!(effective.is_subset(&policy));
        round_trip(&effective);
    }

    #[test]
    fn canonical_unicode_keys_use_utf16_order_and_stable_bytes(
        values in collection::btree_map(
            collection::vec(any::<char>(), 0..12).prop_map(|value| value.into_iter().collect::<String>()),
            collection::vec(any::<char>(), 0..24).prop_map(|value| value.into_iter().collect::<String>()),
            0..20,
        )
    ) {
        let value = serde_json::to_value(&values).unwrap();
        let bounded = BoundedJson::try_from_value(value).unwrap();
        let canonical = CanonicalJson::new(&bounded).unwrap();
        let mut keys = values.keys().collect::<Vec<_>>();
        keys.sort_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
        let fields = keys.into_iter().map(|key| format!("{}:{}", serde_json::to_string(key).unwrap(), serde_json::to_string(&values[key]).unwrap())).collect::<Vec<_>>();
        let expected = format!("{{{}}}", fields.join(","));
        prop_assert_eq!(canonical.as_str(), expected.as_str());
        prop_assert_eq!(canonical.digest(), Digest::sha256(expected.as_bytes()));
        prop_assert_eq!(CanonicalJson::new(&BoundedJson::from_slice(canonical.as_bytes()).unwrap()).unwrap(), canonical);
    }

    #[test]
    fn extension_profiles_enforce_exact_map_bytes_key_bytes_and_entry_count(
        names in collection::btree_set("[a-z][a-z0-9]{0,12}", 1..12),
        text in collection::vec(any::<char>(), 0..64).prop_map(|value| value.into_iter().collect::<String>()),
    ) {
        let entries = names.iter().map(|name| (ExtensionKey::new(format!("com.example.{name}")).unwrap(), ExtensionValue::opaque(BoundedJson::try_from_value(json!(&text)).unwrap()))).collect::<Vec<_>>();
        let extensions = Extensions::try_new(entries.clone()).unwrap();
        let bytes = serde_json::to_vec(&extensions).unwrap().len();
        let key_bytes = entries.iter().map(|(key, _)| key.as_str().len()).max().unwrap();
        let limits = ExtensionLimits::try_new(entries.len(), bytes, key_bytes, JsonLimits::DEFAULT).unwrap();
        prop_assert_eq!(extensions.compact_bytes(), bytes);
        prop_assert_eq!(Extensions::try_new_with_limits(entries.clone(), limits).unwrap(), extensions.clone());
        for (count, total, key) in [(entries.len(), bytes - 1, key_bytes), (entries.len(), bytes, key_bytes - 1)] {
            let narrowed = ExtensionLimits::try_new(count, total, key, JsonLimits::DEFAULT).unwrap();
            prop_assert!(Extensions::try_new_with_limits(entries.clone(), narrowed).is_err());
            prop_assert!(extensions.clone().try_restrict(narrowed).is_err());
        }
        if entries.len() > 1 {
            let narrowed = ExtensionLimits::try_new(entries.len() - 1, bytes, key_bytes, JsonLimits::DEFAULT).unwrap();
            prop_assert!(Extensions::try_new_with_limits(entries.clone(), narrowed).is_err());
        }
        let mut duplicated = entries;
        duplicated.push(duplicated[0].clone());
        prop_assert!(Extensions::try_new(duplicated).is_err());
        round_trip(&extensions);
    }
}

#[test]
fn unicode_order_vector_differs_from_rust_string_order() {
    let keys = BTreeSet::from(["\u{1f600}", "\u{e000}"]);
    assert_eq!(keys.first(), Some(&"\u{e000}"));
    let canonical = CanonicalJson::new(
        &BoundedJson::try_from_value(json!({"\u{e000}": 1, "\u{1f600}": 2})).unwrap(),
    )
    .unwrap();
    assert_eq!(canonical.as_str(), "{\"😀\":2,\"\u{e000}\":1}");
}
