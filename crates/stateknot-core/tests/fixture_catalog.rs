// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Closed inventory and semantic integrity root for compatibility fixtures.

use std::{
    collections::BTreeSet,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use stateknot_core::{BoundedJson, CanonicalJson, Digest, JsonLimits};

const CATALOG_FILE: &str = "catalog-v1.json";
const CATALOG_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/catalog/1.0.0";
const FIXTURE_SCHEMA_PREFIX: &str = "https://stateknot.github.io/schema/test-fixture/";
const ROOT_DOMAIN: &[u8] = b"stateknot.core.compatibility-fixture-catalog.v1\0";
const MAX_FIXTURES: usize = 256;
const MAX_FIXTURE_BYTES: u64 = 512 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Catalog {
    schema: String,
    version: u16,
    entries: Vec<CatalogEntry>,
    root_digest: Digest,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct CatalogEntry {
    path: String,
    schema: String,
    content_digest: Digest,
}

#[derive(Serialize)]
struct RootPreimage<'a> {
    schema: &'a str,
    version: u16,
    entries: &'a [CatalogEntry],
}

fn fixture_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn fixture_paths() -> Vec<String> {
    let mut paths = fs::read_dir(fixture_directory())
        .expect("fixture directory must be readable")
        .map(|entry| entry.expect("fixture directory entry must be readable"))
        .filter_map(|entry| {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                return None;
            }
            assert!(
                entry
                    .file_type()
                    .expect("fixture type must be readable")
                    .is_file(),
                "JSON fixture entry {} must be a regular file",
                path.display()
            );
            Some(
                entry
                    .file_name()
                    .into_string()
                    .expect("fixture name must be UTF-8"),
            )
        })
        .filter(|path| path != CATALOG_FILE)
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn fixture_bytes(path: &Path) -> Vec<u8> {
    let metadata = fs::metadata(path).expect("fixture metadata must be readable");
    assert!(
        metadata.len() <= MAX_FIXTURE_BYTES,
        "fixture {} exceeds the reviewable byte ceiling",
        path.display()
    );
    let bytes = fs::read(path).expect("fixture bytes must be readable");
    assert!(
        bytes.ends_with(b"\n") && !bytes.ends_with(b"\n\n") && !bytes.ends_with(b"\r\n"),
        "fixture {} must end with one newline",
        path.display()
    );
    bytes
}

fn bounded_document(path: &Path) -> BoundedJson {
    let bytes = fixture_bytes(path);
    BoundedJson::from_slice_with_limits(&bytes, JsonLimits::MAXIMUM).unwrap_or_else(|error| {
        panic!(
            "fixture {} is not strict bounded JSON: {error}",
            path.display()
        )
    })
}

fn load_catalog() -> Catalog {
    let path = fixture_directory().join(CATALOG_FILE);
    let document = bounded_document(&path);
    serde_json::from_value(document.as_value().clone())
        .unwrap_or_else(|error| panic!("fixture catalog has an invalid closed shape: {error}"))
}

fn build_catalog() -> Catalog {
    let directory = fixture_directory();
    let entries = fixture_paths()
        .into_iter()
        .map(|path| {
            let absolute_path = directory.join(&path);
            let bytes = fixture_bytes(&absolute_path);
            let document = BoundedJson::from_slice_with_limits(&bytes, JsonLimits::MAXIMUM)
                .unwrap_or_else(|error| {
                    panic!(
                        "fixture {} is not strict bounded JSON: {error}",
                        absolute_path.display()
                    )
                });
            let schema = document
                .as_value()
                .as_object()
                .and_then(|value| value.get("schema"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| panic!("fixture {path} must expose a top-level string schema"));
            CatalogEntry {
                path,
                schema: schema.to_owned(),
                content_digest: Digest::sha256(&bytes),
            }
        })
        .collect::<Vec<_>>();
    let root_digest = catalog_root(&entries);
    Catalog {
        schema: CATALOG_SCHEMA.to_owned(),
        version: 1,
        entries,
        root_digest,
    }
}

fn catalog_root(entries: &[CatalogEntry]) -> Digest {
    let value = serde_json::to_value(RootPreimage {
        schema: CATALOG_SCHEMA,
        version: 1,
        entries,
    })
    .expect("catalog root preimage must serialize");
    let bounded = BoundedJson::try_from_value_with_limits(value, JsonLimits::MAXIMUM)
        .expect("catalog root preimage must remain bounded");
    let canonical = CanonicalJson::new(&bounded).expect("catalog root preimage must canonicalize");
    let mut preimage = Vec::with_capacity(ROOT_DOMAIN.len() + canonical.as_bytes().len());
    preimage.extend_from_slice(ROOT_DOMAIN);
    preimage.extend_from_slice(canonical.as_bytes());
    Digest::sha256(&preimage)
}

fn validate_catalog(catalog: &Catalog) -> Result<(), &'static str> {
    if catalog.schema != CATALOG_SCHEMA || catalog.version != 1 {
        return Err("unsupported catalog schema or version");
    }
    if catalog.entries.is_empty() || catalog.entries.len() > MAX_FIXTURES {
        return Err("catalog size is outside the closed bound");
    }

    let mut paths = BTreeSet::new();
    let mut schemas = BTreeSet::new();
    let mut previous = None;
    for entry in &catalog.entries {
        if entry.path.len() > 128
            || !entry.path.starts_with("core-")
            || Path::new(&entry.path).extension() != Some(OsStr::new("json"))
            || !entry.path.is_ascii()
            || entry.path.contains('/')
            || entry.path.contains('\\')
            || entry.path.contains("..")
        {
            return Err("catalog contains an unsafe fixture path");
        }
        let Some(stem) = entry.path.strip_suffix(".json") else {
            return Err("catalog contains an unsafe fixture path");
        };
        let Some((family, major)) = stem.rsplit_once("-v") else {
            return Err("fixture path has no canonical version suffix");
        };
        if major.is_empty()
            || major.starts_with('0')
            || !major.bytes().all(|value| value.is_ascii_digit())
        {
            return Err("fixture path has no canonical version suffix");
        }
        let expected_schema = format!("{FIXTURE_SCHEMA_PREFIX}{family}/{major}.0.0");
        if entry.schema != expected_schema {
            return Err("catalog contains an invalid fixture schema identity");
        }
        if previous.is_some_and(|value: &String| value >= &entry.path) {
            return Err("catalog entries are not in strict path order");
        }
        if !paths.insert(&entry.path) || !schemas.insert(&entry.schema) {
            return Err("catalog path or schema identity is duplicated");
        }
        previous = Some(&entry.path);
    }
    if catalog.root_digest != catalog_root(&catalog.entries) {
        return Err("catalog root digest does not bind the exact entry set");
    }
    Ok(())
}

fn rust_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("source directory must be readable") {
        let entry = entry.expect("source entry must be readable");
        let path = entry.path();
        if entry
            .file_type()
            .expect("source type must be readable")
            .is_dir()
        {
            rust_sources(&path, sources);
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs")
            && path.file_name().and_then(|value| value.to_str()) != Some("fixture_catalog.rs")
        {
            sources.push(path);
        }
    }
}

fn compile_time_includes_fixture(source: &str, fixture_path: &str) -> bool {
    source.match_indices(fixture_path).any(|(offset, _)| {
        source[..offset]
            .rfind("include_str!(")
            .is_some_and(|macro_offset| offset - macro_offset <= 128)
    })
}

#[test]
fn compatibility_fixture_catalog_is_closed_and_integrity_bound() {
    let committed = load_catalog();
    let rebuilt = build_catalog();
    if committed != rebuilt {
        let replacement =
            serde_json::to_string_pretty(&rebuilt).expect("rebuilt catalog must serialize");
        panic!("fixture catalog is stale; replace it with:\n{replacement}\n");
    }
    validate_catalog(&committed).expect("committed fixture catalog must be valid");
}

#[test]
fn every_catalogued_fixture_is_exercised_by_rust_compatibility_tests() {
    let catalog = load_catalog();
    validate_catalog(&catalog).expect("committed fixture catalog must be valid");

    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut sources = Vec::new();
    rust_sources(&manifest.join("src"), &mut sources);
    rust_sources(&manifest.join("tests"), &mut sources);
    let sources = sources
        .into_iter()
        .map(|path| fs::read_to_string(&path).expect("Rust source must be readable"))
        .collect::<Vec<_>>();

    for entry in catalog.entries {
        let include_path = format!("fixtures/{}", entry.path);
        assert!(
            sources
                .iter()
                .any(|source| compile_time_includes_fixture(source, &include_path)),
            "fixture {} is integrity-pinned but has no executable Rust compatibility test",
            entry.path
        );
    }
}

#[test]
fn catalog_validation_rejects_reordering_schema_substitution_and_path_escape() {
    let catalog = build_catalog();
    validate_catalog(&catalog).unwrap();

    let mut reordered = catalog.clone();
    reordered.entries.swap(0, 1);
    reordered.root_digest = catalog_root(&reordered.entries);
    assert_eq!(
        validate_catalog(&reordered),
        Err("catalog entries are not in strict path order")
    );

    let mut substituted = catalog.clone();
    substituted.entries[1].schema = substituted.entries[0].schema.clone();
    substituted.root_digest = catalog_root(&substituted.entries);
    assert_eq!(
        validate_catalog(&substituted),
        Err("catalog contains an invalid fixture schema identity")
    );

    let mut escaped = catalog;
    escaped.entries[0].path = "../core-fixture.json".to_owned();
    escaped.root_digest = catalog_root(&escaped.entries);
    assert_eq!(
        validate_catalog(&escaped),
        Err("catalog contains an unsafe fixture path")
    );
}
