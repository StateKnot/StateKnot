from pathlib import Path
import hashlib,json,shutil,difflib,tarfile
root=Path('/Users/jiawy/Documents/RustAgentFramework');base=root/'.local-verification/2026-10-15';integration=base/'elapsed36-attempt-proof-integration';parent=base/'elapsed36-attempt-proof-wait-oracle-source';source=base/'elapsed36-attempt-proof-test-fixes-source';out=integration/'source32-real-old-producer-v1';out.mkdir(exist_ok=False);source.mkdir(exist_ok=False)
m=json.loads((integration/'wait-oracle-v2/source-manifest.json').read_text());assert len(m['files'])==919
for row in m['files']:
 p=parent/row['path'];assert hashlib.sha256(p.read_bytes()).hexdigest()==row['sha256'];q=source/row['path'];q.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,q)
export=integration/'source32-old35-producer/native-pg16-v1/export';assets=source/'crates/stateknot-store-postgres/tests/fixtures';original=(export/'data.original.sql').read_bytes();removed=[x for x in original.splitlines(keepends=True)if x.startswith((b'\\restrict ',b'\\unrestrict '))];assert len(removed)==2
normalized=b''.join(x for x in original.splitlines(keepends=True)if x not in removed);(assets/'actual_schema35_writer_source32.sql').write_bytes(normalized);(assets/'actual_schema35_writer_source32.json').write_bytes((export/'metadata.json').read_bytes())
path='crates/stateknot-store-postgres/tests/postgres/frame_closures.rs';p=source/path;body=p.read_text();start=body.index('async fn populated_schema_32_upgrade_keeps_waits_returns_and_checksums_before_whole_closure() {');end=body.index('\n#[tokio::test]',start);old=body[start:end];(out/'original-test.rs.txt').write_text(old)
a=old.index('    let store = PostgresStore::connect(&url, options.clone()).await.unwrap();');b=old.index('    // Source reconstruction is distinct',a)
newprefix='''    // The current writer emits original V3 Wait facts that cannot be represented
    // in schema32. Reconstruct an empty schema32 first, then restore the exact
    // rows emitted by the actual frozen schema35 writer; never relabel new bytes.
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    assert_eq!(
        query_scalar::<_, i64>("SELECT count(*) FROM stateknot.runs")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        0
    );
'''
new=old[:a]+newprefix+old[b:]
a=new.index('    assert!(matches!(\n        PostgresStore::connect(&url, options.clone()).await,');b=new.index('    PostgresStore::migrate_database(&url, options.clone())',a)
load='''    assert!(matches!(
        PostgresStore::connect(&url, options.clone()).await,
        Err(StoreError::IncompatibleSchema)
    ));
    let trigger_states: Vec<(String, String, String)> = query_as(
        "SELECT c.relname::text,t.tgname::text,t.tgenabled::text FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('stateknot','public') ORDER BY n.nspname,c.relname,t.tgname",
    )
    .fetch_all(&fixture)
    .await
    .unwrap();
    let mut restore = fixture.begin().await.unwrap();
    query("DELETE FROM public._sqlx_migrations")
        .execute(&mut *restore)
        .await
        .unwrap();
    // pg_dump's bounded fixture restore disables and re-enables every trigger
    // around the original INSERT statements. All bytea/canonical values and all
    // 32 ledger rows are unchanged; no production writer or guard is modified.
    sqlx_core::raw_sql::raw_sql(include_str!("../fixtures/actual_schema35_writer_source32.sql"))
        .execute(&mut *restore)
        .await
        .unwrap();
    restore.commit().await.unwrap();
    assert_eq!(
        trigger_states,
        query_as::<_, (String, String, String)>(
            "SELECT c.relname::text,t.tgname::text,t.tgenabled::text FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('stateknot','public') ORDER BY n.nspname,c.relname,t.tgname"
        )
        .fetch_all(&fixture)
        .await
        .unwrap()
    );
    // Discard pg_dump's connection-local settings before ordinary Store reads.
    fixture.close().await;
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let expected: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/actual_schema35_writer_source32.json"
    ))
    .unwrap();
    assert_eq!(expected["producer_schema"], 35);
    assert_eq!(expected["reconstructed_source_schema"], 32);
    let fence: RunFence = serde_json::from_value(expected["fence"].clone()).unwrap();
    let prior_fence: RunFence = serde_json::from_value(expected["prior_fence"].clone()).unwrap();
    let prior_namespace: GraphNamespace =
        serde_json::from_value(expected["prior_entry_namespace"].clone()).unwrap();
    let saved_checkpoint: stateknot_core::GraphFrameCheckpoint =
        serde_json::from_value(expected["saved_checkpoint"].clone()).unwrap();
    let saved_event: stateknot_core::JournalEvent =
        serde_json::from_value(expected["saved_event"].clone()).unwrap();
    let saved_usage: BudgetUsage =
        serde_json::from_value(expected["saved_direct_usage"].clone()).unwrap();
    let returned_digest: Digest =
        serde_json::from_value(expected["returned_digest"].clone()).unwrap();
    let checksums: Vec<(i64, Vec<u8>)> =
        serde_json::from_value(expected["checksums"].clone()).unwrap();
    let before: Vec<String> = serde_json::from_value(expected["before"].clone()).unwrap();
    let prior: Vec<String> = serde_json::from_value(expected["prior"].clone()).unwrap();
    let waits_before: String = serde_json::from_value(expected["waits_before"].clone()).unwrap();
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM public._sqlx_migrations")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        32
    );
'''
new=new[:a]+load+new[b:]
new=new.replace('prior_entry.entry().checkpoint().frame().namespace()','&prior_namespace').replace('returned.digest()','returned_digest').replace('saved.checkpoint()','&saved_checkpoint').replace('saved.event()','saved_event').replace('saved.direct_usage_after().unwrap()','saved_usage')
# Add the original compound barrier read as an independent, fully decoded proof.
a=new.index('    upgraded\n        .append_control_plane_abandon_waits(')
extra='''    let saved = Box::pin(upgraded.load_graph_frame_barrier(
        fence.tenant_id(),
        fence.run_id(),
        saved_checkpoint.frame().namespace(),
        saved_checkpoint.checkpoint().parent().unwrap().checkpoint_id(),
    ))
    .await
    .unwrap()
    .unwrap();
    assert_eq!(saved.checkpoint(), &saved_checkpoint);
    assert_eq!(saved.event(), &saved_event);
    assert_eq!(saved.direct_usage_after().unwrap(), saved_usage);
'''
new=new[:a]+extra+new[a:]
# Preserve the original combined rollback refusal and prove its first guard is
# false: rejection must come from the retained whole-closure guard, not V3 data.
a=new.index('    let mut tx = fixture.begin().await.unwrap();')
extra='''    assert_eq!(
        query_scalar::<_, i64>("SELECT (SELECT count(*) FROM stateknot.graph_wait_conditions WHERE profile<>'legacy_registration') + (SELECT count(*) FROM stateknot.root_graph_barriers WHERE convert_from(bundle_bytes,'UTF8')::jsonb->>'version'='3') + (SELECT count(*) FROM stateknot.graph_frame_barriers WHERE convert_from(barrier_bytes,'UTF8')::jsonb->>'version'='3')")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        0
    );
'''
new=new[:a]+extra+new[a:]
p.write_text(body[:start]+new+body[end:]);(out/'candidate-unformatted.patch').write_text(''.join(difflib.unified_diff((parent/path).read_text().splitlines(True),p.read_text().splitlines(True),fromfile='a/'+path,tofile='b/'+path)))
proof={'parent_source':str(parent),'parent_file_count':919,'parent_tree_sha256':m['source_tree_sha256'],'new_source':str(source),'original_dump':str(export/'data.original.sql'),'original_dump_sha256':hashlib.sha256(original).hexdigest(),'normalized_sql_sha256':hashlib.sha256(normalized).hexdigest(),'normalized_sql_bytes':len(normalized),'normalization':'Only the two psql client restrict/unrestrict lines removed. Every other byte including INSERT statements, canonical bytea, timestamps, ledger and paired restore trigger directives unchanged.','removed_client_lines_count':len(removed),'fixture_origin':'Actual schema35+A writer; original test business prefix; original source32 reconstruction after original revert guard. Not an actual historical schema32 binary or released N-1/N-2 qualification.','chosen_fixture':'PG16 actual producer snapshot is identical across the two new reader-major tests; independent PG17 producer export retained separately.','production_files_changed':[],'original_revert_guards_changed':False}
(out/'copy-and-normalization-proof.json').write_text(json.dumps(proof,indent=2)+'\n');(out/'prepare-script.py').write_bytes(Path(__file__).read_bytes());print(json.dumps(proof,indent=2))
