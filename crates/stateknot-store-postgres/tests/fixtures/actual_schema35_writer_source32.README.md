# Actual schema35 writer fixture reconstructed as schema32

`actual_schema35_writer_source32.sql` and `.json` are one immutable pair.
They were produced by the real schema35+A Store/Core writer, whose production
inputs match commit `83559f7651f48d65755c1fc5fd8a9ba82e721193` exactly.
The Core/Store source, all migrations, workspace manifest, lockfile and toolchain
were checked against that commit (137 files, no differences). The original
`tests/postgres/frame_closures.rs` also matches that commit byte for byte.
Only the additional test-only exporter reproduced below was appended.

This is **schema35 writer output plus isolated schema32 source reconstruction**.
It is not output from a historical schema32 executable and does not qualify a
released N−1/N−2 upgrade. The old writer executes the original real Wait and
Return calls, saves their immutable expected facts, runs the original
`revert_graph_frame_closures.sql`, asserts ledger 32 and the current Store's
`IncompatibleSchema`, and stops before upgrade. No V3 bytes are changed into V2.
The producer has a V2 Wait and V1 terminal barrier, no V3 graph barrier facts.

The retained PG16.15 original data-only dump has 361875 bytes and SHA256
`2921a7cd08e9458a7bcbed9bf6225a7e2d0f48d5230d6ada35a28258b2dbc990`.
The checked-in SQL has 361725 bytes and SHA256
`a08558e789498fde599645a70d336bf28946ca7e809566f79aa0d37dd1d55a26`.
Only the two psql-client `\restrict` / `\unrestrict` lines were removed for
`sqlx` raw SQL execution. Every other byte, including all INSERT statements,
canonical bytea values, original timestamps, 32 ledger rows and the paired
restore trigger directives, is unchanged. The metadata JSON SHA256 is
`35b90c759260f7b1fe72be1cd16c03de008a5e4bb0ef89103d928b5cd1fa4aaf`.

The dump has 83 INSERT statements. Its 348 original bytea literals contain
151642 raw bytes; the retained evidence also stores their fixed offsets,
lengths and individual SHA256 values. The PG16 pack SHA256 is
`8ef1099756e94d7767286794d604314a6af38cc2bd752a94eb520490c716b0e1`.
An independent PG17.11 producer also passed and retained its own original dump.
Both new reader-major regressions deliberately load this same PG16 snapshot.

The JSON stores `producer_schema`, `reconstructed_source_schema`, fixture DB
name, both real Run fences/namespaces, original full saved checkpoint/event/
direct usage, actual returned digest/event/result, the original 1..32 migration
checksums, both complete 14-table `facts` vectors and original Wait registration
rows. These values were captured by the old business calls, not synthesized
from the new reader's output. New readers still authenticate actual stored
rows, canonical bytes, physical components and ownership normally.

## Regenerate from the actual old writer

Use a separate source directory and an isolated PostgreSQL fixture database.
Check `df` first. Copy source only; never copy `target` or other generated
folders. The normal default stack, runtime options, assertions and all migration
guards must remain unchanged. In a shell, set `writer_source`, `fixture_export`
and `fixture_readme` to absolute task paths; `fixture_readme` is this document.
Set `STATEKNOT_TEST_DATABASE_URL` to your isolated base DB and set
`STATEKNOT_REQUIRE_POSTGRES_TESTS=1`.

1. Extract the old production source with
   `git archive 83559f7651f48d65755c1fc5fd8a9ba82e721193 | tar -x -C "$writer_source"`.
2. Extract the `diff` block below verbatim into a temporary patch and apply it
   with `patch -d "$writer_source" -p1 < exporter.patch`. This only adds an
   exporter test; it does not replace the original test or production code.
3. Set `STATEKNOT_TEST_SOURCE32_EXPORT_DIR="$fixture_export"`. Strictly build
   `cargo test -p stateknot-store-postgres --test postgres --all-features --locked
   --no-run` in the old source, using a task cache and
   `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
   RUSTFLAGS='-D warnings' RUSTDOCFLAGS='-D warnings'` in that command's env.
4. Run the saved binary with exact test
   `frame_closures::actual_schema35_writer_exports_original_wait_return_source32_fixture
   --exact --nocapture --test-threads=1`. Read `$fixture_export/metadata.json` for
   its retained nested fixture `database`. Verify ledger 1..32/all success, 50
   schema tables and scoped barrier versions 1 and 2 only.
5. Use the actual server's `pg_dump -U postgres --dbname "$fixture_database"
   --data-only --inserts --column-inserts --no-owner --no-privileges
   --disable-triggers > data.original.sql`. The flag emits bounded restore
   commands; it does not disable the live producer DB's triggers.
6. Preserve the raw dump and metadata JSON together. For the SQL fixture only,
   remove lines starting with `\restrict ` or `\unrestrict `, preserving every
   other byte. Compute raw/normalized/metadata SHA256 manifests. Random IDs and
   actual database event times mean a new generation gets new hashes; never
   overwrite an old artifact under its old hash. Review a regenerated SQL/JSON
   pair together and re-run the complete upgrade regression on both majors.

The new regression first creates an empty schema36 DB and uses both original
source-reconstruction scripts unchanged to reach32 while no V3 facts exist.
It restores this exact snapshot and ledger in one transaction. The dump's
paired DISABLE/ENABLE TRIGGER ALL directives are confined to that fixture import;
the test asserts every original schema32 trigger state matches afterwards and
closes that connection to discard dump-local session settings. MIGRATOR then
upgrades32→36, preserves all original checksum/row/Wait/Return assertions and
executes real cancellation/whole closure. The final original refusal/rollback
assertion remains, with an extra check that the V3 guard predicate is false so
it cannot mask the whole-closure refusal. No production guard is weakened.

## Exact test-only producer addition

```diff
--- a/crates/stateknot-store-postgres/tests/postgres/frame_closures.rs
+++ b/crates/stateknot-store-postgres/tests/postgres/frame_closures.rs
@@ -1762,3 +1762,136 @@
     );
     store.close().await;
 }
+
+#[tokio::test]
+#[allow(clippy::too_many_lines)]
+async fn actual_schema35_writer_exports_original_wait_return_source32_fixture() {
+    let _guard = DATABASE_TEST_MUTEX.lock().await;
+    let Some(shared) = test_store().await else {
+        return;
+    };
+    let options = shared.options().clone();
+    shared.close().await;
+    let original = std::env::var(DATABASE_URL_ENV).unwrap();
+    let name = format!(
+        "stateknot_frame_close_upgrade_{}",
+        RunId::generate().to_string().replace('-', "")
+    );
+    let admin = PgPoolOptions::new()
+        .max_connections(1)
+        .connect(&database_url_with_name(&original, "postgres"))
+        .await
+        .unwrap();
+    query(&format!("CREATE DATABASE {name}"))
+        .execute(&admin)
+        .await
+        .unwrap();
+    let url = database_url_with_name(&original, &name);
+    PostgresStore::migrate_database(&url, options.clone())
+        .await
+        .unwrap();
+    let store = PostgresStore::connect(&url, options.clone()).await.unwrap();
+    let waits = NodeWaits::try_new([NodeWait::timer(
+        TimerId::generate(),
+        RunTimerKind::Sleep,
+        Timestamp::from_unix_micros(2_000_000_000_000_000).unwrap(),
+    )])
+    .unwrap();
+    let (_, entry, graph, fence, plan, observed, revision) = Box::pin(
+        super::frame_waits::prepared(&store, "frame-close-upgrade-wait", waits),
+    )
+    .await;
+    let saved = Box::pin(super::frame_waits::suspend(
+        &store,
+        plan,
+        &fence,
+        observed,
+        entry.direct_usage_after().unwrap(),
+        revision,
+        &graph,
+    ))
+    .await;
+    let (_, prior_entry, prior_graph, prior_fence, prepared, prior_barrier) = Box::pin(
+        super::frame_returns::finished(&store, "frame-close-upgrade-return"),
+    )
+    .await;
+    let stateknot_store_postgres::GraphFrameReturnCommitOutcome::Committed(returned) =
+        Box::pin(store.return_graph_frame(
+            prepared,
+            EventId::generate(),
+            prior_fence.clone(),
+            prior_barrier.event().head(),
+            prior_barrier.direct_usage_after().unwrap(),
+            &AcceptGraphSchemas,
+            &IntegrationGraphReducer {
+                reference: prior_graph.reducer().clone(),
+            },
+        ))
+        .await
+        .unwrap()
+    else {
+        panic!("fresh upgrade return");
+    };
+    let fixture = PgPoolOptions::new()
+        .max_connections(1)
+        .connect(&url)
+        .await
+        .unwrap();
+    let checksums: Vec<(i64, Vec<u8>)> = query_as(
+        "SELECT version,checksum FROM public._sqlx_migrations WHERE version<=32 ORDER BY version",
+    )
+    .fetch_all(&fixture)
+    .await
+    .unwrap();
+    let before = facts(&fixture, &fence).await;
+    let prior = facts(&fixture, &prior_fence).await;
+    let waits_before: String = query_scalar("SELECT jsonb_agg(to_jsonb(w) ORDER BY wait_id)::text FROM stateknot.run_wait_registrations w WHERE tenant_id=$1 AND run_id=$2")
+        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&fixture).await.unwrap();
+    store.close().await;
+    // Source reconstruction is distinct from retained N-1/N-2 executable qualification.
+    sqlx_core::raw_sql::raw_sql(include_str!("../fixtures/revert_graph_frame_closures.sql"))
+        .execute(&fixture)
+        .await
+        .unwrap();
+    assert_eq!(
+        query_scalar::<_, i64>("SELECT max(version) FROM public._sqlx_migrations")
+            .fetch_one(&fixture)
+            .await
+            .unwrap(),
+        32
+    );
+    assert!(matches!(
+        PostgresStore::connect(&url, options.clone()).await,
+        Err(StoreError::IncompatibleSchema)
+    ));
+    let export =
+        std::path::PathBuf::from(std::env::var("STATEKNOT_TEST_SOURCE32_EXPORT_DIR").unwrap());
+    assert!(export.is_absolute());
+    std::fs::create_dir_all(&export).unwrap();
+    let metadata = serde_json::json!({
+        "database": name,
+        "producer_schema": 35,
+        "reconstructed_source_schema": 32,
+        "fence": fence,
+        "prior_fence": prior_fence,
+        "entry_namespace": entry.entry().checkpoint().frame().namespace(),
+        "prior_entry_namespace": prior_entry.entry().checkpoint().frame().namespace(),
+        "saved_checkpoint": saved.checkpoint(),
+        "saved_event": saved.event(),
+        "saved_direct_usage": saved.direct_usage_after().unwrap(),
+        "returned_digest": returned.digest(),
+        "returned_event": returned.event(),
+        "returned_result": returned.result(),
+        "checksums": checksums,
+        "before": before,
+        "prior": prior,
+        "waits_before": waits_before
+    });
+    std::fs::write(
+        export.join("metadata.json"),
+        serde_json::to_vec_pretty(&metadata).unwrap(),
+    )
+    .unwrap();
+    fixture.close().await;
+    admin.close().await;
+}
```
