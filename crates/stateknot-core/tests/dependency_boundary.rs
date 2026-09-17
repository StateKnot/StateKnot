// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Locks the reviewed runtime-neutral direct dependency boundary for core.

use std::{collections::BTreeSet, path::Path, process::Command};

use serde::Deserialize;

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    dependencies: Vec<Dependency>,
}

#[derive(Deserialize)]
struct Dependency {
    name: String,
    kind: Option<String>,
    target: Option<String>,
    rename: Option<String>,
}

#[test]
fn core_direct_dependencies_match_the_reviewed_runtime_neutral_boundary() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--locked", "--no-deps"])
        .current_dir(workspace)
        .output()
        .expect("cargo metadata must start");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: Metadata =
        serde_json::from_slice(&output.stdout).expect("cargo metadata must return valid JSON");
    let package = metadata
        .packages
        .iter()
        .find(|package| package.name == "stateknot-core")
        .expect("workspace metadata must contain stateknot-core");

    let actual = package
        .dependencies
        .iter()
        .map(|dependency| {
            assert!(
                dependency.target.is_none(),
                "core dependency {} is target-conditional and escaped the reviewed boundary",
                dependency.name
            );
            assert!(
                dependency.rename.is_none(),
                "core dependency {} is renamed and escaped the reviewed boundary",
                dependency.name
            );
            (
                dependency.kind.as_deref().unwrap_or("normal").to_owned(),
                dependency.name.clone(),
            )
        })
        .collect::<BTreeSet<_>>();

    let expected = [
        ("dev", "proptest"),
        ("normal", "chrono"),
        ("normal", "fluent-uri"),
        ("normal", "futures-core"),
        ("normal", "language-tags"),
        ("normal", "mime"),
        ("normal", "schemars"),
        ("normal", "serde"),
        ("normal", "serde_json"),
        ("normal", "serde_json_canonicalizer"),
        ("normal", "sha2"),
        ("normal", "thiserror"),
        ("normal", "uuid"),
    ]
    .into_iter()
    .map(|(kind, name)| (kind.to_owned(), name.to_owned()))
    .collect::<BTreeSet<_>>();

    assert_eq!(
        actual, expected,
        "stateknot-core dependency changes require an explicit runtime-neutrality review"
    );
}
