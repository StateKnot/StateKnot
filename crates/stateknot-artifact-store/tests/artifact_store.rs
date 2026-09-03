// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Object/registry idempotency and resolver-integrity contracts.

use std::{
    fmt::Write as _,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use stateknot_artifact_store::{
    AllowArtifactRead, ArtifactReadAuthorizationError, ArtifactReadAuthorizationRequest,
    ArtifactReadAuthorizer, ArtifactRegistry, ArtifactStoreErrorKind, ArtifactStoreOptions,
    RemoteArtifactOrigin, StateKnotArtifactStore,
};
use stateknot_core::{
    AgentResultProvenance, ArtifactIdentity, AttemptId, BoundedJson, BoxFuture, ByteCount,
    CapabilityIdentity, CapabilityName, CapabilityReference, Digest, EventId, InvocationId,
    IssuerId, JournalAppend, JournalEventIntent, JournalEventKind, JournalExpectation,
    JournalPayload, PrincipalIdentity, RunId, SchemaId, SchemaReference, SubjectId, TenantId,
    ThreadId, Timestamp, Version,
};
use stateknot_integrations::{A2aArtifactIngestionRequest, A2aArtifactSource, A2aPart};
use stateknot_store_postgres::{
    ArtifactRegistration, ArtifactRegistrationOutcome, PostgresStore, PostgresStoreOptions,
    PostgresTransportSecurity, RunProjection, StoreError, StoredArtifact,
};

const DATABASE_URL_ENV: &str = "STATEKNOT_TEST_DATABASE_URL";
const REQUIRE_DATABASE_ENV: &str = "STATEKNOT_REQUIRE_POSTGRES_TESTS";
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

#[derive(Default)]
struct MemoryRegistry {
    values: Mutex<Vec<StoredArtifact>>,
    load_calls: AtomicUsize,
}

impl ArtifactRegistry for MemoryRegistry {
    fn health_check(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }

    fn register(
        &self,
        registration: ArtifactRegistration,
    ) -> BoxFuture<'_, Result<ArtifactRegistrationOutcome, StoreError>> {
        Box::pin(async move {
            let mut values = self.values.lock().unwrap();
            if let Some(existing) = values.iter().find(|existing| {
                existing.artifact().identity() == registration.artifact().identity()
                    || existing.registration().registration_key() == registration.registration_key()
                    || existing.locator() == registration.locator()
            }) {
                return if existing.registration() == &registration {
                    Ok(ArtifactRegistrationOutcome::Idempotent(existing.clone()))
                } else {
                    Err(StoreError::ArtifactRegistrationConflict)
                };
            }
            let stored = StoredArtifact::new(
                registration,
                Timestamp::from_unix_micros(1_900_000_000_000_000).unwrap(),
            );
            values.push(stored.clone());
            Ok(ArtifactRegistrationOutcome::Registered(stored))
        })
    }

    fn load(
        &self,
        identity: ArtifactIdentity,
    ) -> BoxFuture<'_, Result<StoredArtifact, StoreError>> {
        Box::pin(async move {
            self.load_calls.fetch_add(1, Ordering::Relaxed);
            self.values
                .lock()
                .unwrap()
                .iter()
                .find(|stored| stored.artifact().identity() == &identity)
                .cloned()
                .ok_or(StoreError::ArtifactNotFound)
        })
    }
}

struct DenyArtifactRead;

impl ArtifactReadAuthorizer for DenyArtifactRead {
    fn authorize(
        &self,
        _request: &ArtifactReadAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), ArtifactReadAuthorizationError>> {
        Box::pin(async { Err(ArtifactReadAuthorizationError::Denied) })
    }
}

#[derive(Default)]
struct CountingArtifactRead {
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl ArtifactReadAuthorizer for CountingArtifactRead {
    fn authorize(
        &self,
        _request: &ArtifactReadAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), ArtifactReadAuthorizationError>> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(25)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

struct ServedResponse {
    origin: String,
    url: String,
    task: JoinHandle<()>,
}

async fn serve_response(
    status: &'static str,
    content_type: Option<&'static str>,
    extra_headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
    advertised_length: Option<usize>,
) -> ServedResponse {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{address}");
    let url = format!("{origin}/artifact");
    let mut head = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
    if let Some(content_type) = content_type {
        write!(head, "Content-Type: {content_type}\r\n").unwrap();
    }
    for (name, value) in extra_headers {
        write!(head, "{name}: {value}\r\n").unwrap();
    }
    write!(
        head,
        "Content-Length: {}\r\n\r\n",
        advertised_length.unwrap_or(body.len())
    )
    .unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 4096];
        let _ = socket.read(&mut request).await.unwrap();
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
        socket.shutdown().await.unwrap();
    });
    ServedResponse { origin, url, task }
}

fn principal() -> PrincipalIdentity {
    PrincipalIdentity::new(
        "https://issuer.example.com/stateknot"
            .parse::<IssuerId>()
            .unwrap(),
        "artifact-test".parse::<SubjectId>().unwrap(),
    )
}

fn tool() -> CapabilityIdentity {
    CapabilityIdentity::new(
        principal(),
        CapabilityReference::new(
            CapabilityName::new("remote-research-agent").unwrap(),
            Version::new(1, 0, 0),
        ),
    )
}

#[allow(clippy::too_many_arguments)]
fn request(
    tenant_id: TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    event_id: EventId,
    task_id: &str,
    content: &str,
) -> A2aArtifactIngestionRequest {
    request_with_part(
        tenant_id,
        run_id,
        invocation_id,
        attempt_id,
        event_id,
        task_id,
        A2aPart::text(content)
            .unwrap()
            .with_media_type("text/plain;charset=utf-8")
            .unwrap(),
        ByteCount::new(1024 * 1024),
    )
}

#[allow(clippy::too_many_arguments)]
fn request_with_part(
    tenant_id: TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    event_id: EventId,
    task_id: &str,
    part: A2aPart,
    maximum_bytes: ByteCount,
) -> A2aArtifactIngestionRequest {
    A2aArtifactIngestionRequest::new(
        tenant_id,
        run_id,
        invocation_id,
        attempt_id,
        event_id,
        tool(),
        A2aArtifactSource::task_artifact(task_id, "artifact-1", 0, 0).unwrap(),
        Some("answer.txt".to_string()),
        Some("Durable A2A result".to_string()),
        part,
        maximum_bytes,
    )
    .unwrap()
}

fn remote_options(origins: impl IntoIterator<Item = RemoteArtifactOrigin>) -> ArtifactStoreOptions {
    ArtifactStoreOptions::default()
        .with_remote_origins(origins)
        .unwrap()
        .with_timeouts(Duration::from_secs(5), Duration::from_secs(5))
        .unwrap()
        .with_limits(
            10 * 1024 * 1024,
            10 * 1024 * 1024,
            ArtifactStoreOptions::MIN_MULTIPART_CHUNK_BYTES,
            2,
        )
        .unwrap()
}

#[tokio::test]
async fn exact_retry_reuses_final_object_and_resolver_detects_tampering() {
    let objects = Arc::new(InMemory::new());
    let registry = Arc::new(MemoryRegistry::default());
    let store = StateKnotArtifactStore::initialize(
        objects.clone(),
        registry.clone(),
        Arc::new(AllowArtifactRead),
        "unit-artifacts-v1",
        ArtifactStoreOptions::default(),
    )
    .await
    .unwrap();
    let tenant_id = TenantId::new("tenant-a").unwrap();
    let run_id = RunId::generate();
    let invocation_id = InvocationId::generate();
    let attempt_id = AttemptId::generate();
    let event_id = EventId::generate();

    let first = store
        .ingest_a2a(request(
            tenant_id.clone(),
            run_id,
            invocation_id,
            attempt_id,
            event_id,
            "task-1",
            "durable answer",
        ))
        .await
        .unwrap();
    let retry = store
        .ingest_a2a(request(
            tenant_id,
            run_id,
            invocation_id,
            attempt_id,
            event_id,
            "task-1",
            "durable answer",
        ))
        .await
        .unwrap();
    assert_eq!(retry, first);
    assert_eq!(registry.values.lock().unwrap().len(), 1);
    let object_count = objects
        .list(None)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .count();
    assert_eq!(
        object_count, 1,
        "staging and startup probes must be removed"
    );

    let resolved = store
        .resolve(principal(), &first, ByteCount::new(1024))
        .await
        .unwrap();
    assert_eq!(resolved.bytes().as_ref(), b"durable answer");

    let changed = store
        .ingest_a2a(request(
            first.identity().tenant_id().clone(),
            run_id,
            invocation_id,
            attempt_id,
            event_id,
            "task-1",
            "substituted answer",
        ))
        .await
        .unwrap_err();
    assert_eq!(changed.kind(), ArtifactStoreErrorKind::Integrity);

    let object_key = registry.values.lock().unwrap()[0]
        .locator()
        .object_key()
        .to_string();
    objects
        .put(&Path::parse(object_key).unwrap(), "tampered".into())
        .await
        .unwrap();
    let corrupted = store
        .resolve(principal(), &first, ByteCount::new(1024))
        .await
        .unwrap_err();
    assert_eq!(corrupted.kind(), ArtifactStoreErrorKind::Integrity);

    let debug = format!("{store:?}");
    assert!(!debug.contains("unit-artifacts-v1"));
    assert!(!debug.contains("stateknot/artifacts"));
}

#[tokio::test]
async fn authorization_denial_happens_before_registry_lookup() {
    let objects = Arc::new(InMemory::new());
    let registry = Arc::new(MemoryRegistry::default());
    let writer = StateKnotArtifactStore::initialize(
        objects.clone(),
        registry.clone(),
        Arc::new(AllowArtifactRead),
        "authorization-order-v1",
        ArtifactStoreOptions::default(),
    )
    .await
    .unwrap();
    let artifact = writer
        .ingest_a2a(request(
            TenantId::new("tenant-auth").unwrap(),
            RunId::generate(),
            InvocationId::generate(),
            AttemptId::generate(),
            EventId::generate(),
            "task-auth",
            "secret result",
        ))
        .await
        .unwrap();
    assert_eq!(registry.load_calls.load(Ordering::Relaxed), 0);

    let reader = StateKnotArtifactStore::initialize(
        objects,
        registry.clone(),
        Arc::new(DenyArtifactRead),
        "authorization-order-v1",
        ArtifactStoreOptions::default(),
    )
    .await
    .unwrap();
    let denied = reader
        .resolve(principal(), &artifact, ByteCount::new(1024))
        .await
        .unwrap_err();
    assert_eq!(denied.kind(), ArtifactStoreErrorKind::AuthorizationDenied);
    assert_eq!(registry.load_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn process_local_admission_bounds_complete_artifact_operations() {
    assert!(
        ArtifactStoreOptions::default()
            .with_concurrency_limit(0)
            .is_err()
    );
    assert!(
        ArtifactStoreOptions::default()
            .with_concurrency_limit(ArtifactStoreOptions::HARD_MAXIMUM_CONCURRENT_OPERATIONS + 1,)
            .is_err()
    );

    let objects = Arc::new(InMemory::new());
    let registry = Arc::new(MemoryRegistry::default());
    let authorizer = Arc::new(CountingArtifactRead::default());
    let store = Arc::new(
        StateKnotArtifactStore::initialize(
            objects,
            registry,
            authorizer.clone(),
            "bounded-concurrency-v1",
            ArtifactStoreOptions::default()
                .with_concurrency_limit(1)
                .unwrap(),
        )
        .await
        .unwrap(),
    );
    let artifact = store
        .ingest_a2a(request(
            TenantId::new("tenant-concurrency").unwrap(),
            RunId::generate(),
            InvocationId::generate(),
            AttemptId::generate(),
            EventId::generate(),
            "task-concurrency",
            "bounded result",
        ))
        .await
        .unwrap();

    let first = store.resolve(principal(), &artifact, ByteCount::new(1024));
    let second = store.resolve(principal(), &artifact, ByteCount::new(1024));
    let (first, second) = tokio::join!(first, second);
    first.unwrap();
    second.unwrap();
    assert_eq!(authorizer.peak.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_presentation_is_rejected_before_remote_fetch_or_object_write() {
    let objects = Arc::new(InMemory::new());
    let registry = Arc::new(MemoryRegistry::default());
    let origin = RemoteArtifactOrigin::loopback_http("http://127.0.0.1:9").unwrap();
    let store = StateKnotArtifactStore::initialize(
        objects.clone(),
        registry.clone(),
        Arc::new(AllowArtifactRead),
        "presentation-validation-v1",
        remote_options([origin]),
    )
    .await
    .unwrap();
    let request = A2aArtifactIngestionRequest::new(
        TenantId::new("tenant-presentation").unwrap(),
        RunId::generate(),
        InvocationId::generate(),
        AttemptId::generate(),
        EventId::generate(),
        tool(),
        A2aArtifactSource::task_artifact("task-presentation", "artifact-1", 0, 0).unwrap(),
        Some("../escape".to_string()),
        None,
        A2aPart::url("http://127.0.0.1:9/artifact").unwrap(),
        ByteCount::new(1024),
    )
    .unwrap();

    let error = store.ingest_a2a(request).await.unwrap_err();
    assert_eq!(error.kind(), ArtifactStoreErrorKind::InvalidContent);
    assert!(registry.values.lock().unwrap().is_empty());
    assert_eq!(
        objects
            .list(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .count(),
        0
    );
}

#[tokio::test]
async fn approved_remote_url_streams_through_multipart_and_resolves_exact_bytes() {
    let body_length = ArtifactStoreOptions::MIN_MULTIPART_CHUNK_BYTES + 4096;
    let body = vec![0x5a; body_length];
    let expected_digest = stateknot_core::Digest::sha256(&body);
    let response = serve_response(
        "200 OK",
        Some("application/octet-stream"),
        Vec::new(),
        body,
        None,
    )
    .await;
    let origin = RemoteArtifactOrigin::loopback_http(&response.origin).unwrap();
    let objects = Arc::new(InMemory::new());
    let registry = Arc::new(MemoryRegistry::default());
    let store = StateKnotArtifactStore::initialize(
        objects.clone(),
        registry,
        Arc::new(AllowArtifactRead),
        "remote-multipart-v1",
        remote_options([origin]),
    )
    .await
    .unwrap();
    let artifact = store
        .ingest_a2a(request_with_part(
            TenantId::new("tenant-remote").unwrap(),
            RunId::generate(),
            InvocationId::generate(),
            AttemptId::generate(),
            EventId::generate(),
            "task-large-url",
            A2aPart::url(response.url)
                .unwrap()
                .with_media_type("application/octet-stream")
                .unwrap(),
            ByteCount::new(u64::try_from(body_length).unwrap()),
        ))
        .await
        .unwrap();
    response.task.await.unwrap();
    assert_eq!(
        artifact.representation().byte_length().get(),
        u64::try_from(body_length).unwrap()
    );
    assert_eq!(artifact.representation().digest(), expected_digest);
    let resolved = store
        .resolve(
            principal(),
            &artifact,
            ByteCount::new(u64::try_from(body_length).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(resolved.bytes().len(), body_length);
    assert!(resolved.bytes().iter().all(|byte| *byte == 0x5a));
    assert_eq!(
        objects
            .list(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .count(),
        1
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn remote_url_policy_rejects_unapproved_redirect_media_encoding_and_size() {
    let media = serve_response(
        "200 OK",
        Some("application/json"),
        Vec::new(),
        b"{}".to_vec(),
        None,
    )
    .await;
    let redirect = serve_response(
        "302 Found",
        None,
        vec![("Location", "http://127.0.0.1:9/private".to_string())],
        Vec::new(),
        None,
    )
    .await;
    let oversized = serve_response(
        "200 OK",
        Some("application/octet-stream"),
        Vec::new(),
        Vec::new(),
        Some(2048),
    )
    .await;
    let encoded = serve_response(
        "200 OK",
        Some("text/plain;charset=utf-8"),
        vec![("Content-Encoding", "gzip".to_string())],
        b"not-compressed".to_vec(),
        None,
    )
    .await;
    let malformed_json = serve_response(
        "200 OK",
        Some("application/json"),
        Vec::new(),
        b"{not-json".to_vec(),
        None,
    )
    .await;
    let charset_mismatch = serve_response(
        "200 OK",
        Some("text/plain;charset=iso-8859-1"),
        Vec::new(),
        b"ascii-but-ambiguous".to_vec(),
        None,
    )
    .await;
    let origins = [
        &media,
        &redirect,
        &oversized,
        &encoded,
        &malformed_json,
        &charset_mismatch,
    ]
    .map(|response| RemoteArtifactOrigin::loopback_http(&response.origin).unwrap());
    let objects = Arc::new(InMemory::new());
    let registry = Arc::new(MemoryRegistry::default());
    let store = StateKnotArtifactStore::initialize(
        objects.clone(),
        registry.clone(),
        Arc::new(AllowArtifactRead),
        "remote-policy-v1",
        remote_options(origins),
    )
    .await
    .unwrap();
    let tenant_id = TenantId::new("tenant-policy").unwrap();
    let run_id = RunId::generate();
    let invocation_id = InvocationId::generate();
    let attempt_id = AttemptId::generate();
    let event_id = EventId::generate();

    let unapproved = store
        .ingest_a2a(request_with_part(
            tenant_id.clone(),
            run_id,
            invocation_id,
            attempt_id,
            event_id,
            "task-unapproved",
            A2aPart::url("http://127.0.0.1:9/private").unwrap(),
            ByteCount::new(1024),
        ))
        .await
        .unwrap_err();
    assert_eq!(unapproved.kind(), ArtifactStoreErrorKind::PolicyDenied);

    let media_mismatch = store
        .ingest_a2a(request_with_part(
            tenant_id.clone(),
            run_id,
            invocation_id,
            attempt_id,
            event_id,
            "task-media",
            A2aPart::url(media.url)
                .unwrap()
                .with_media_type("text/plain")
                .unwrap(),
            ByteCount::new(1024),
        ))
        .await
        .unwrap_err();
    assert_eq!(media_mismatch.kind(), ArtifactStoreErrorKind::Integrity);

    let rejected_redirect = store
        .ingest_a2a(request_with_part(
            tenant_id.clone(),
            run_id,
            invocation_id,
            attempt_id,
            event_id,
            "task-redirect",
            A2aPart::url(redirect.url).unwrap(),
            ByteCount::new(1024),
        ))
        .await
        .unwrap_err();
    assert_eq!(
        rejected_redirect.kind(),
        ArtifactStoreErrorKind::PolicyDenied
    );

    let rejected_size = store
        .ingest_a2a(request_with_part(
            tenant_id.clone(),
            run_id,
            invocation_id,
            attempt_id,
            event_id,
            "task-size",
            A2aPart::url(oversized.url).unwrap(),
            ByteCount::new(1024),
        ))
        .await
        .unwrap_err();
    assert_eq!(rejected_size.kind(), ArtifactStoreErrorKind::InvalidContent);

    let rejected_encoding = store
        .ingest_a2a(request_with_part(
            tenant_id,
            run_id,
            invocation_id,
            attempt_id,
            event_id,
            "task-encoding",
            A2aPart::url(encoded.url).unwrap(),
            ByteCount::new(1024),
        ))
        .await
        .unwrap_err();
    assert_eq!(
        rejected_encoding.kind(),
        ArtifactStoreErrorKind::InvalidContent
    );

    let rejected_json = store
        .ingest_a2a(request_with_part(
            TenantId::new("tenant-policy").unwrap(),
            run_id,
            invocation_id,
            attempt_id,
            event_id,
            "task-json",
            A2aPart::url(malformed_json.url)
                .unwrap()
                .with_media_type("application/json")
                .unwrap(),
            ByteCount::new(1024),
        ))
        .await
        .unwrap_err();
    assert_eq!(rejected_json.kind(), ArtifactStoreErrorKind::InvalidContent);

    let rejected_charset = store
        .ingest_a2a(request_with_part(
            TenantId::new("tenant-policy").unwrap(),
            run_id,
            invocation_id,
            attempt_id,
            event_id,
            "task-charset",
            A2aPart::url(charset_mismatch.url)
                .unwrap()
                .with_media_type("text/plain;charset=utf-8")
                .unwrap(),
            ByteCount::new(1024),
        ))
        .await
        .unwrap_err();
    assert_eq!(rejected_charset.kind(), ArtifactStoreErrorKind::Integrity);

    media.task.await.unwrap();
    redirect.task.await.unwrap();
    oversized.task.await.unwrap();
    encoded.task.await.unwrap();
    malformed_json.task.await.unwrap();
    charset_mismatch.task.await.unwrap();
    assert!(registry.values.lock().unwrap().is_empty());
    assert_eq!(
        objects
            .list(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .count(),
        0
    );
}

#[tokio::test]
async fn object_persistence_and_real_postgres_registration_form_one_verified_boundary() {
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let postgres_options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled);
    PostgresStore::migrate_database(&database_url, postgres_options.clone())
        .await
        .unwrap();
    let postgres = PostgresStore::connect(&database_url, postgres_options)
        .await
        .unwrap();
    let tenant_id = TenantId::new(format!("artifact-vertical-{}", RunId::generate())).unwrap();
    let run_id = RunId::generate();
    let run_invocation_id = InvocationId::generate();
    postgres
        .admit_run(AgentResultProvenance::new(
            tenant_id.clone(),
            run_id,
            ThreadId::generate(),
            run_invocation_id,
            tool(),
        ))
        .await
        .unwrap();
    let event_id = EventId::generate();
    let payload = JournalPayload::new(
        SchemaReference::new(
            "https://stateknot.github.io/schema/artifact-ingestion-test/1.0.0"
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"artifact ingestion integration schema v1"),
        ),
        JournalEventKind::new("artifact-ingestion-test").unwrap(),
        BoundedJson::try_from_value(serde_json::json!({"phase": "executing"})).unwrap(),
    )
    .unwrap();
    let event =
        JournalEventIntent::control_plane(tenant_id.clone(), run_id, event_id, payload).unwrap();
    postgres
        .append_control_plane(
            JournalAppend::new(JournalExpectation::empty(), event).unwrap(),
            RunProjection::unchanged(),
        )
        .await
        .unwrap();

    let objects = Arc::new(InMemory::new());
    let store = StateKnotArtifactStore::initialize(
        objects,
        Arc::new(postgres.clone()),
        Arc::new(AllowArtifactRead),
        "postgres-vertical-v1",
        ArtifactStoreOptions::default(),
    )
    .await
    .unwrap();
    let artifact = store
        .ingest_a2a(request(
            tenant_id,
            run_id,
            InvocationId::generate(),
            AttemptId::generate(),
            event_id,
            "task-postgres",
            "registered object bytes",
        ))
        .await
        .unwrap();
    let persisted = postgres.load_artifact(artifact.identity()).await.unwrap();
    assert_eq!(persisted.artifact(), &artifact);
    let resolved = store
        .resolve(principal(), &artifact, ByteCount::new(1024))
        .await
        .unwrap();
    assert_eq!(resolved.bytes().as_ref(), b"registered object bytes");
    postgres.close().await;
}
