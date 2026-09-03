// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0
//! Artifact storage locator validation contracts.

use stateknot_store_postgres::{ArtifactStorageLocator, StoreError};

#[test]
fn artifact_storage_locator_is_portable_and_debug_redacted() {
    let locator = ArtifactStorageLocator::new(
        "primary-artifacts-v1",
        "objects/ab/cd/content.bin",
        Some("provider-version-1".to_string()),
        Some("\"opaque-etag\"".to_string()),
    )
    .unwrap();
    assert_eq!(locator.storage_namespace(), "primary-artifacts-v1");
    assert_eq!(locator.object_key(), "objects/ab/cd/content.bin");
    assert_eq!(locator.object_version(), Some("provider-version-1"));
    assert_eq!(locator.object_etag(), Some("\"opaque-etag\""));
    let debug = format!("{locator:?}");
    assert!(!debug.contains("objects/"));
    assert!(!debug.contains("opaque-etag"));
    assert!(debug.contains("<redacted>"));

    for key in ["", "/absolute", "trailing/", "a//b", "a/../b", "a\\b"] {
        assert!(matches!(
            ArtifactStorageLocator::new("primary-artifacts-v1", key, None, None),
            Err(StoreError::InvalidArtifactStorageLocator)
        ));
    }
    assert!(matches!(
        ArtifactStorageLocator::new("../bucket", "safe/key", None, None),
        Err(StoreError::InvalidArtifactStorageLocator)
    ));
}
