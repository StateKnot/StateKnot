// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Physical base-backup/WAL recovery drill for one consumed child Join.

use super::backup_restore::{prepare_consumed_join, replay_consumed_join, snapshot};
use super::*;
use sqlx_core::{query::query, query_scalar::query_scalar};
use std::{
    process::{Command, Output, Stdio},
    thread,
};

const REQUIRE_ENV: &str = "STATEKNOT_REQUIRE_PITR_TESTS";
const IMAGE_ENV: &str = "STATEKNOT_PITR_POSTGRES_IMAGE";
const RESTORE_POINT: &str = "stateknot_child_join_before_late_write";
const PASSWORD: &str = "stateknot_pitr_disposable_password";

struct DockerResources {
    containers: Vec<String>,
    volumes: Vec<String>,
}

impl DockerResources {
    fn new() -> Self {
        Self {
            containers: Vec::new(),
            volumes: Vec::new(),
        }
    }

    fn volume(&mut self, name: String) -> String {
        docker_ok(&["volume", "create", &name], Duration::from_secs(30));
        self.volumes.push(name.clone());
        name
    }

    fn container(&mut self, name: String) -> String {
        self.containers.push(name.clone());
        name
    }

    fn stop(&mut self, name: &str) {
        docker_ok(&["stop", "--time", "10", name], Duration::from_secs(30));
        self.containers.retain(|candidate| candidate != name);
    }
}

impl Drop for DockerResources {
    fn drop(&mut self) {
        for name in &self.containers {
            let _ = docker_output(&["stop", "--time", "2", name], Duration::from_secs(10));
        }
        for name in &self.volumes {
            let _ = docker_output(&["volume", "rm", name], Duration::from_secs(10));
        }
    }
}

fn docker_output(args: &[&str], timeout: Duration) -> Output {
    let mut process = Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("PITR qualification requires Docker");
    let started = Instant::now();
    loop {
        if process.try_wait().unwrap().is_some() {
            return process.wait_with_output().unwrap();
        }
        if started.elapsed() > timeout {
            process.kill().unwrap();
            let output = process.wait_with_output().unwrap();
            panic!(
                "Docker command timed out after {timeout:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn docker_ok(args: &[&str], timeout: Duration) -> String {
    let output = docker_output(args, timeout);
    assert!(
        output.status.success(),
        "Docker operation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn selected_image() -> Option<String> {
    let image = match std::env::var(IMAGE_ENV) {
        Ok(image) => image,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_ENV).is_none() => {
            return None;
        }
        Err(error) => panic!("mandatory pinned PITR image is missing: {error}"),
    };
    let Some(digest) = image.strip_prefix("postgres@sha256:") else {
        panic!("PITR qualification requires a digest-pinned PostgreSQL image");
    };
    assert!(
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid PITR image digest"
    );
    Some(image)
}

fn mapped_port(container: &str) -> u16 {
    let binding = docker_ok(&["port", container, "5432/tcp"], Duration::from_secs(10));
    let (address, port) = binding
        .rsplit_once(':')
        .expect("Docker must publish a port");
    assert_eq!(
        address, "127.0.0.1",
        "PITR PostgreSQL must be loopback-only"
    );
    port.parse::<u16>()
        .expect("Docker must publish a valid port")
}

async fn ready_database(url: &str) -> sqlx_postgres::PgPool {
    let started = Instant::now();
    loop {
        if let Ok(pool) = sqlx_postgres::PgPool::connect(url).await {
            if let Ok(recovering) = query_scalar::<_, bool>("SELECT pg_is_in_recovery()")
                .fetch_one(&pool)
                .await
            {
                if !recovering {
                    return pool;
                }
            }
            pool.close().await;
        }
        assert!(
            started.elapsed() < Duration::from_secs(90),
            "PostgreSQL did not become primary"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn consumed_child_join_survives_named_point_in_time_recovery() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(image) = selected_image() else {
        return;
    };
    let identity = uuid::Uuid::now_v7().simple().to_string();
    let mut resources = DockerResources::new();
    let source_name = format!("stateknot-pitr-source-{identity}");
    let target_name = format!("stateknot-pitr-target-{identity}");
    let archive = resources.volume(format!("stateknot-pitr-wal-{identity}"));
    let backup = resources.volume(format!("stateknot-pitr-base-{identity}"));
    let target_data = resources.volume(format!("stateknot-pitr-target-data-{identity}"));
    let archive_mount = format!("{archive}:/archive");
    let archive_read_mount = format!("{archive}:/archive:ro");
    let backup_mount = format!("{backup}:/backup");
    let backup_read_mount = format!("{backup}:/backup:ro");
    let target_mount = format!("{target_data}:/restore");
    let postgres_mount = format!("{target_data}:/var/lib/postgresql/data");
    // Named volumes are created root-owned. Give only the disposable Postgres
    // process access to the backup and WAL archive directories.
    docker_ok(
        &[
            "run",
            "--rm",
            "--user",
            "root",
            "--volume",
            &archive_mount,
            "--volume",
            &backup_mount,
            "--entrypoint",
            "sh",
            &image,
            "-ec",
            "chown postgres:postgres /archive /backup && chmod 700 /archive /backup",
        ],
        Duration::from_secs(30),
    );
    docker_ok(
        &[
            "run",
            "--detach",
            "--rm",
            "--name",
            &source_name,
            "--env",
            &format!("POSTGRES_PASSWORD={PASSWORD}"),
            "--env",
            "POSTGRES_DB=stateknot_test",
            "--publish",
            "127.0.0.1::5432",
            "--volume",
            &archive_mount,
            "--volume",
            &backup_mount,
            &image,
            "-c",
            "archive_mode=on",
            "-c",
            "wal_level=replica",
            "-c",
            "max_wal_senders=5",
            "-c",
            "archive_command=test ! -f /archive/%f && cp %p /archive/%f",
        ],
        Duration::from_secs(30),
    );
    resources.container(source_name.clone());
    let source_url = format!(
        "postgres://postgres:{PASSWORD}@127.0.0.1:{}/stateknot_test",
        mapped_port(&source_name)
    );
    let source_pool = ready_database(&source_url).await;
    let version: String = query_scalar("SHOW server_version")
        .fetch_one(&source_pool)
        .await
        .unwrap();
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, 8)
        .with_acquire_timeout(Duration::from_secs(30))
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
        .with_lease_timing(Duration::from_secs(5 * 60), Duration::from_secs(5 * 60));
    PostgresStore::migrate_database(&source_url, options.clone())
        .await
        .unwrap();
    let source = PostgresStore::connect(&source_url, options.clone())
        .await
        .unwrap();
    let (key, attempt_id) =
        Box::pin(prepare_consumed_join(&source, "physical-pitr-child-join")).await;
    let before = Box::pin(snapshot(&source, &key, attempt_id)).await;
    source.close().await;

    docker_ok(
        &[
            "exec",
            "--user",
            "postgres",
            &source_name,
            "pg_basebackup",
            "--host=/var/run/postgresql",
            "--username=postgres",
            "--pgdata=/backup/base",
            "--format=plain",
            "--wal-method=stream",
            "--checkpoint=fast",
        ],
        Duration::from_secs(120),
    );
    docker_ok(
        &["exec", &source_name, "pg_verifybackup", "/backup/base"],
        Duration::from_secs(60),
    );
    let manifest = docker_ok(
        &[
            "exec",
            &source_name,
            "sha256sum",
            "/backup/base/backup_manifest",
        ],
        Duration::from_secs(10),
    );
    let manifest_sha256 = manifest
        .split_whitespace()
        .next()
        .expect("base backup manifest digest missing")
        .to_owned();
    assert!(
        manifest_sha256.len() == 64 && manifest_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "base backup manifest digest is malformed"
    );
    query_scalar::<_, String>(&format!(
        "SELECT pg_create_restore_point('{RESTORE_POINT}')::text"
    ))
    .fetch_one(&source_pool)
    .await
    .unwrap();
    query("CREATE TABLE public.stateknot_pitr_late_marker (id bigint PRIMARY KEY)")
        .execute(&source_pool)
        .await
        .unwrap();
    query("INSERT INTO public.stateknot_pitr_late_marker (id) VALUES (1)")
        .execute(&source_pool)
        .await
        .unwrap();
    let late_count: i64 = query_scalar("SELECT count(*) FROM public.stateknot_pitr_late_marker")
        .fetch_one(&source_pool)
        .await
        .unwrap();
    assert_eq!(late_count, 1);
    let segment: String = query_scalar("SELECT pg_walfile_name(pg_current_wal_lsn())")
        .fetch_one(&source_pool)
        .await
        .unwrap();
    assert!(segment.len() == 24 && segment.bytes().all(|byte| byte.is_ascii_hexdigit()));
    query_scalar::<_, String>("SELECT pg_switch_wal()::text")
        .fetch_one(&source_pool)
        .await
        .unwrap();
    let archived_segment = format!("/archive/{segment}");
    let archive_start = Instant::now();
    loop {
        if docker_output(
            &["exec", &source_name, "test", "-s", &archived_segment],
            Duration::from_secs(10),
        )
        .status
        .success()
        {
            break;
        }
        assert!(
            archive_start.elapsed() < Duration::from_secs(45),
            "the post-target WAL segment was not archived"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    source_pool.close().await;
    resources.stop(&source_name);

    docker_ok(
        &[
            "run",
            "--rm",
            "--user",
            "root",
            "--volume",
            &backup_read_mount,
            "--volume",
            &target_mount,
            "--entrypoint",
            "sh",
            &image,
            "-ec",
            "cp -a /backup/base/. /restore/ && chown -R postgres:postgres /restore && chmod 700 /restore && printf '%s\\n' \"restore_command = 'cp /archive/%f %p'\" \"recovery_target_name = 'stateknot_child_join_before_late_write'\" \"recovery_target_action = 'promote'\" >> /restore/postgresql.auto.conf && touch /restore/recovery.signal",
        ],
        Duration::from_secs(60),
    );
    docker_ok(
        &[
            "run",
            "--detach",
            "--rm",
            "--name",
            &target_name,
            "--env",
            &format!("POSTGRES_PASSWORD={PASSWORD}"),
            "--publish",
            "127.0.0.1::5432",
            "--volume",
            &postgres_mount,
            "--volume",
            &archive_read_mount,
            &image,
        ],
        Duration::from_secs(30),
    );
    resources.container(target_name.clone());
    let target_url = format!(
        "postgres://postgres:{PASSWORD}@127.0.0.1:{}/stateknot_test",
        mapped_port(&target_name)
    );
    let target_pool = ready_database(&target_url).await;
    let restored_late: Option<String> =
        query_scalar("SELECT to_regclass('public.stateknot_pitr_late_marker')::text")
            .fetch_one(&target_pool)
            .await
            .unwrap();
    assert!(
        restored_late.is_none(),
        "PITR replayed the post-target write"
    );
    let restored = PostgresStore::connect(&target_url, options).await.unwrap();
    let after = Box::pin(snapshot(&restored, &key, attempt_id)).await;
    assert_eq!(
        after, before,
        "PITR changed the committed durable Join state"
    );
    assert!(matches!(
        restored
            .load_run(&tenant("unrelated-pitr-tenant"), key.parent_run_id())
            .await,
        Err(StoreError::RunNotFound)
    ));
    Box::pin(replay_consumed_join(&restored, &key)).await;
    restored.close().await;
    target_pool.close().await;
    println!(
        "\nSTATEKNOT_CHILD_JOIN_PITR_EVIDENCE={}",
        json!({
            "profile": "child-join-named-pitr-v1",
            "base_backup_verified": true,
            "manifest_sha256": manifest_sha256,
            "wal_segment_archived": segment,
            "named_restore_point": RESTORE_POINT,
            "post_target_write_excluded": true,
            "exact_durable_snapshot_verified": true,
            "noninitial_replay_verified": true,
            "cross_tenant_refusal_verified": true,
            "invariants": "passed",
            "postgres": version,
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH
        })
    );
}
