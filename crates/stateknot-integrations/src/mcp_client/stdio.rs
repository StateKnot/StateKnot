// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Experimental owned Linux MCP session.

mod owner;
mod wire;

use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsString,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
    time::Duration,
};

use serde::Serialize;
use serde_json::{Map, Value};
use stateknot_core::{BoundedJson, Digest, ModelSchemaRegistry, SchemaReference};
use thiserror::Error;
use tokio::{
    io::AsyncReadExt,
    process::ChildStderr,
    sync::Mutex,
    task::JoinHandle,
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use super::{
    MCP_PROTOCOL_VERSION_2026_07_28, McpClientIdentity, McpTool, McpToolCall, McpToolCallResponse,
    McpToolCatalog, NEXT_CLIENT_BINDING_ID, allocate_monotonic_id, parse_complete_tool_result,
    parse_discovery, parse_tool_page,
};
use crate::mcp_tool_descriptor_digest;
use owner::Owner;
use wire::{Wire, WireError};

const STARTUP: Duration = Duration::from_secs(10);
const CALL: Duration = Duration::from_secs(10);
const CLEANUP: Duration = Duration::from_secs(15);
const MAX_TOOLS: usize = 1024;
const MAX_PAGES: usize = 16;
const MAX_PINS: usize = 64;
const MAX_STDERR: usize = 256 * 1024;
const MAX_PRELUDE: usize = 32 * 1024;

/// Whether a Tool request could have reached the application pipe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpStdioSendPhase {
    /// No request byte could have been written.
    NotSent,
    /// A write was attempted. The external effect may be unknown.
    MayHaveBeenSent,
}

/// Sanitized classification for the experimental stdio session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpStdioFailure {
    /// Operator configuration was rejected.
    Configuration,
    /// The owner could not start or continue its generation.
    Owner,
    /// Discovery did not match the reviewed server or Tool pins.
    Admission,
    /// The call was refused before dispatch.
    Input,
    /// Another call currently holds the one dispatch slot.
    Busy,
    /// The session is no longer accepting work.
    Closed,
    /// The absolute operation deadline elapsed.
    Deadline,
    /// A configured transport bound was reached.
    Limit,
    /// The server broke the declared protocol.
    Protocol,
    /// The application pipe failed.
    Transport,
    /// The server returned a complete JSON-RPC error.
    Remote,
    /// Cleanup has yet to be verified.
    CleanupPending,
}

/// A payload-free error with an independent Tool send phase.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("MCP stdio {failure:?} ({phase:?})")]
pub struct McpStdioError {
    failure: McpStdioFailure,
    phase: McpStdioSendPhase,
}

impl McpStdioError {
    const fn new(failure: McpStdioFailure, sent: bool) -> Self {
        Self {
            failure,
            phase: if sent {
                McpStdioSendPhase::MayHaveBeenSent
            } else {
                McpStdioSendPhase::NotSent
            },
        }
    }

    /// Returns the sanitized failure class.
    pub const fn failure(self) -> McpStdioFailure {
        self.failure
    }

    /// Returns whether the Tool request may have been sent.
    pub const fn send_phase(self) -> McpStdioSendPhase {
        self.phase
    }
}

type Result<T> = std::result::Result<T, McpStdioError>;

fn failure(kind: McpStdioFailure) -> McpStdioError {
    McpStdioError::new(kind, false)
}

fn wire_failure(error: WireError) -> McpStdioFailure {
    match error {
        WireError::Io => McpStdioFailure::Transport,
        WireError::Limit => McpStdioFailure::Limit,
        WireError::Protocol => McpStdioFailure::Protocol,
        WireError::Remote => McpStdioFailure::Remote,
        WireError::Closed => McpStdioFailure::Closed,
        WireError::Deadline => McpStdioFailure::Deadline,
    }
}

/// Operator-installed Linux launcher and one pre-provisioned generation.
pub struct McpStdioOwnerProfile {
    helper: PathBuf,
    control: PathBuf,
    recovery: PathBuf,
    generation: String,
}

impl McpStdioOwnerProfile {
    /// Selects a fixed owner binary and the root-owned control directory for a fresh generation.
    ///
    /// The controller provisions the owner configuration and recovery directory first.
    pub fn new(helper: PathBuf, control: PathBuf, generation: String) -> Result<Self> {
        if !absolute_path(&helper)
            || !absolute_path(&control)
            || control.as_os_str().as_bytes().len() > 90
            || generation.len() != 32
            || !generation
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(failure(McpStdioFailure::Configuration));
        }
        let recovery = Path::new("/var/lib/stateknot/mcp-stdio").join(&generation);
        Ok(Self {
            helper,
            control,
            recovery,
            generation,
        })
    }
}

/// One bounded literal Unix launch request. Its bytes are wiped when dropped.
pub struct McpStdioCommand(Zeroizing<Vec<u8>>);

impl McpStdioCommand {
    /// Uses the given absolute executable and working directory, literal arguments and environment.
    pub fn new(
        executable: &Path,
        cwd: &Path,
        arguments: &[OsString],
        environment: &BTreeMap<OsString, OsString>,
    ) -> Result<Self> {
        #[derive(Serialize)]
        struct Entry<'a> {
            name: &'a [u8],
            value: &'a [u8],
        }
        #[derive(Serialize)]
        struct Launch<'a> {
            executable: &'a [u8],
            cwd: &'a [u8],
            arguments: Vec<&'a [u8]>,
            environment: Vec<Entry<'a>>,
        }
        if !absolute_path(executable)
            || !absolute_path(cwd)
            || arguments.len() > 128
            || environment.len() > 128
        {
            return Err(failure(McpStdioFailure::Configuration));
        }
        let mut bytes = 0usize;
        let mut add = |value: &[u8]| {
            bytes = bytes.saturating_add(value.len());
            !value.contains(&0) && bytes <= 8192
        };
        let executable = executable.as_os_str().as_bytes();
        let cwd = cwd.as_os_str().as_bytes();
        if !add(executable)
            || !add(cwd)
            || !arguments.iter().all(|arg| add(arg.as_bytes()))
            || !environment.iter().all(|(key, value)| {
                let name = key.as_bytes();
                !name.is_empty()
                    && name.len() <= 128
                    && !name.contains(&b'=')
                    && add(name)
                    && add(value.as_bytes())
            })
        {
            return Err(failure(McpStdioFailure::Configuration));
        }
        let launch = Launch {
            executable,
            cwd,
            arguments: arguments.iter().map(|arg| arg.as_bytes()).collect(),
            environment: environment
                .iter()
                .map(|(key, value)| Entry {
                    name: key.as_bytes(),
                    value: value.as_bytes(),
                })
                .collect(),
        };
        let mut frame = Zeroizing::new(vec![0; 4]);
        serde_json::to_writer(&mut *frame, &launch)
            .map_err(|_| failure(McpStdioFailure::Configuration))?;
        let length =
            u32::try_from(frame.len() - 4).map_err(|_| failure(McpStdioFailure::Configuration))?;
        if frame.len() - 4 > MAX_PRELUDE {
            return Err(failure(McpStdioFailure::Configuration));
        }
        frame[..4].copy_from_slice(&length.to_be_bytes());
        Ok(Self(frame))
    }
}

fn absolute_path(path: &Path) -> bool {
    let bytes = path.as_os_str().as_bytes();
    path.is_absolute() && bytes.len() <= 4096 && !bytes.contains(&0)
}

/// A complete reviewed Tool descriptor and exact local schema references.
pub struct McpStdioToolPin {
    name: String,
    digest: Digest,
    input: SchemaReference,
    output: Option<SchemaReference>,
}

impl McpStdioToolPin {
    /// Pins a Tool. Passing no output schema admits only a descriptor without one.
    pub fn new(
        name: String,
        digest: Digest,
        input: SchemaReference,
        output: Option<SchemaReference>,
    ) -> Result<Self> {
        if name.is_empty() || name.len() > super::MAX_TOOL_NAME_BYTES {
            return Err(failure(McpStdioFailure::Configuration));
        }
        Ok(Self {
            name,
            digest,
            input,
            output,
        })
    }
}

/// A single-session configuration with an explicit local Tool allowlist.
pub struct McpStdioConfig {
    profile: McpStdioOwnerProfile,
    command: McpStdioCommand,
    identity: McpClientIdentity,
    server: McpClientIdentity,
    pins: Vec<McpStdioToolPin>,
    schemas: Arc<dyn ModelSchemaRegistry>,
}

impl McpStdioConfig {
    /// Constructs one unstarted binding. The expected server identity is a protocol assertion.
    pub fn new(
        profile: McpStdioOwnerProfile,
        command: McpStdioCommand,
        identity: McpClientIdentity,
        server: McpClientIdentity,
        pins: Vec<McpStdioToolPin>,
        schemas: Arc<dyn ModelSchemaRegistry>,
    ) -> Result<Self> {
        let mut names = HashSet::new();
        if pins.is_empty()
            || pins.len() > MAX_PINS
            || !pins.iter().all(|pin| {
                names.insert(pin.name.as_str())
                    && pinned_bytes(schemas.as_ref(), &pin.input).is_some()
                    && pin
                        .output
                        .as_ref()
                        .is_none_or(|output| pinned_bytes(schemas.as_ref(), output).is_some())
            })
        {
            return Err(failure(McpStdioFailure::Configuration));
        }
        Ok(Self {
            profile,
            command,
            identity,
            server,
            pins,
            schemas,
        })
    }
}

fn pinned_bytes<'a>(
    schemas: &'a dyn ModelSchemaRegistry,
    schema: &SchemaReference,
) -> Option<&'a [u8]> {
    let bytes = schemas.canonical_schema_bytes(schema)?;
    (bytes.len() <= 256 * 1024 && Digest::sha256(bytes) == schema.digest()).then_some(bytes)
}

/// Owns a single experimental Linux process generation.
pub struct McpStdioSession {
    config: Option<McpStdioConfig>,
    owner: Option<Owner>,
    wire: Arc<Mutex<Option<Wire>>>,
    stop: CancellationToken,
    client: Option<McpStdioClient>,
    stderr: Option<JoinHandle<bool>>,
    stdout: Option<JoinHandle<bool>>,
    clean: bool,
    pipe_failed: bool,
}

impl McpStdioSession {
    /// Creates a configured session. The owner is launched by the first poll of start.
    pub fn new(config: McpStdioConfig) -> Self {
        Self {
            config: Some(config),
            owner: None,
            wire: Arc::new(Mutex::new(None)),
            stop: CancellationToken::new(),
            client: None,
            stderr: None,
            stdout: None,
            clean: false,
            pipe_failed: false,
        }
    }

    /// Launches, discovers the reviewed server, and admits its pinned Tools atomically.
    pub async fn start(&mut self) -> Result<()> {
        if self.stop.is_cancelled() {
            return Err(failure(McpStdioFailure::Closed));
        }
        let config = self
            .config
            .take()
            .ok_or_else(|| failure(McpStdioFailure::Closed))?;
        let mut cancel = StopOnDrop(Some(self.stop.clone()));
        let deadline = Instant::now() + STARTUP;
        let profile = &config.profile;
        let (owner, stdin, stdout, stderr) = Owner::spawn(
            &profile.helper,
            &profile.control,
            &profile.recovery,
            &profile.generation,
            self.stop.clone(),
        )
        .map_err(|_| failure(McpStdioFailure::Owner))?;
        self.owner = Some(owner);
        self.stderr = Some(tokio::spawn(drain_stderr(stderr, self.stop.clone())));
        let mut pipe = self
            .wire
            .try_lock()
            .map_err(|_| failure(McpStdioFailure::Busy))?;
        let wire = pipe.insert(Wire::new(stdin, stdout));
        let owner = self
            .owner
            .as_mut()
            .ok_or_else(|| failure(McpStdioFailure::Owner))?;
        let discovery = timeout_at(deadline, async {
            owner
                .ready()
                .await
                .map_err(|_| failure(McpStdioFailure::Owner))?;
            owner.go().map_err(|_| failure(McpStdioFailure::Owner))?;
            wire.prelude(&config.command.0)
                .await
                .map_err(|err| failure(wire_failure(err)))?;
            admit(wire, &config, deadline, &self.stop).await
        });
        let catalog = tokio::select! {
            biased;
            () = self.stop.cancelled() => return Err(failure(McpStdioFailure::Closed)),
            result = discovery => result.map_err(|_| failure(McpStdioFailure::Deadline))??,
        };
        if self.stop.is_cancelled() || Instant::now() >= deadline {
            return Err(failure(McpStdioFailure::Closed));
        }
        drop(pipe);
        self.client = Some(McpStdioClient(Arc::new(Client {
            identity: config.identity,
            catalog,
            pins: config.pins,
            schemas: config.schemas,
            wire: Arc::downgrade(&self.wire),
            stop: self.stop.clone(),
        })));
        cancel.0 = None;
        Ok(())
    }

    /// Returns a cloneable Tool interface only while the generation admits work.
    pub fn client(&self) -> Option<McpStdioClient> {
        if self.stop.is_cancelled() {
            None
        } else {
            self.client.clone()
        }
    }

    /// Synchronously closes admission and requests independent owner shutdown.
    pub fn begin_shutdown(&self) {
        self.stop.cancel();
    }

    /// Requests shutdown and waits for verified generation retirement and pipe EOF.
    pub async fn shutdown(&mut self) -> Result<()> {
        self.begin_shutdown();
        if self.config.take().is_some() {
            self.clean = true;
            return Ok(());
        }
        self.wait().await
    }

    /// Observes retirement without initiating shutdown. A dropped wait retains ownership.
    pub async fn wait(&mut self) -> Result<()> {
        if self.clean {
            return Ok(());
        }
        let owner = self.owner.as_mut().ok_or_else(|| {
            failure(if self.config.is_some() {
                McpStdioFailure::Closed
            } else {
                McpStdioFailure::CleanupPending
            })
        })?;
        self.stop.cancelled().await;
        let end = Instant::now() + CLEANUP;
        if self.stdout.is_none() {
            let mut pipe = timeout_at(end, self.wire.lock())
                .await
                .map_err(|_| failure(McpStdioFailure::CleanupPending))?;
            if let Some(wire) = pipe.take() {
                self.stdout = Some(tokio::spawn(async move {
                    Box::pin(wire.finish()).await.is_ok()
                }));
            }
        }
        let pending = || failure(McpStdioFailure::CleanupPending);
        timeout_at(end, owner.wait())
            .await
            .map_err(|_| pending())?
            .map_err(|_| pending())?;
        self.pipe_failed |= !await_eof(end, &mut self.stdout).await?;
        self.pipe_failed |= !await_eof(end, &mut self.stderr).await?;
        if self.pipe_failed {
            return Err(pending());
        }
        self.clean = true;
        Ok(())
    }
}

async fn await_eof(end: Instant, task: &mut Option<JoinHandle<bool>>) -> Result<bool> {
    let pending = || failure(McpStdioFailure::CleanupPending);
    let Some(drain) = task else { return Ok(true) };
    let completed = timeout_at(end, drain).await.map_err(|_| pending())?;
    // JoinHandle is consumed once it has returned, even if a subsequent wait
    // for the other pipe times out.
    *task = None;
    Ok(matches!(completed, Ok(true)))
}

impl Drop for McpStdioSession {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

struct StopOnDrop(Option<CancellationToken>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        if let Some(stop) = &self.0 {
            stop.cancel();
        }
    }
}

async fn drain_stderr(mut stderr: ChildStderr, stop: CancellationToken) -> bool {
    let mut bytes = [0; 4096];
    let mut count = 0usize;
    loop {
        match stderr.read(&mut bytes).await {
            Ok(0) => return true,
            Ok(read) => {
                count = count.saturating_add(read);
                if count > MAX_STDERR {
                    stop.cancel();
                }
            }
            Err(_) => {
                stop.cancel();
                return false;
            }
        }
    }
}

async fn admit(
    wire: &mut Wire,
    config: &McpStdioConfig,
    deadline: Instant,
    stop: &CancellationToken,
) -> Result<McpToolCatalog> {
    let refuse = || failure(McpStdioFailure::Admission);
    let binding = allocate_monotonic_id(&NEXT_CLIENT_BINDING_ID).ok_or_else(refuse)?;
    let (response, _) = wire
        .rpc(
            &config.identity,
            "server/discover",
            Map::new(),
            &mut false,
            deadline,
            stop,
        )
        .await
        .map_err(|err| failure(wire_failure(err)))?;
    let server = parse_discovery(&response).map_err(|_| refuse())?;
    if !server.supports_tools()
        || server.name() != Some(config.server.name())
        || server.version() != Some(config.server.version())
        || !server
            .supported_versions()
            .any(|version| version == MCP_PROTOCOL_VERSION_2026_07_28)
    {
        return Err(refuse());
    }
    let mut tools = Vec::new();
    let mut seen_names = HashSet::new();
    let mut cursors = HashSet::new();
    let mut params = Map::new();
    let mut total = 0;
    for _ in 0..MAX_PAGES {
        let (response, _) = wire
            .rpc(
                &config.identity,
                "tools/list",
                params,
                &mut false,
                deadline,
                stop,
            )
            .await
            .map_err(|err| failure(wire_failure(err)))?;
        let raw = response
            .get("tools")
            .and_then(Value::as_array)
            .ok_or_else(refuse)?;
        total += raw.len();
        if total > MAX_TOOLS {
            return Err(failure(McpStdioFailure::Limit));
        }
        for descriptor in raw {
            let name = descriptor.get("name").and_then(Value::as_str);
            if let Some(index) = config
                .pins
                .iter()
                .position(|pin| Some(pin.name.as_str()) == name)
                && !seen_names.insert(index)
            {
                return Err(refuse());
            }
        }
        let page = parse_tool_page(&response, binding).map_err(|_| refuse())?;
        for tool in page.tools {
            if let Some(pin) = config.pins.iter().find(|pin| pin.name == tool.name()) {
                verify_pin(&tool, pin, config.schemas.as_ref())?;
                tools.push(tool);
            }
        }
        let Some(cursor) = page.next_cursor else {
            return if tools.len() == config.pins.len() {
                Ok(McpToolCatalog {
                    tools,
                    rejected: Vec::new(),
                })
            } else {
                Err(refuse())
            };
        };
        if !cursors.insert(cursor.clone()) {
            return Err(refuse());
        }
        params = Map::from_iter([("cursor".to_owned(), Value::String(cursor.into_string()))]);
    }
    Err(failure(McpStdioFailure::Limit))
}

fn verify_pin(
    tool: &McpTool,
    pin: &McpStdioToolPin,
    schemas: &dyn ModelSchemaRegistry,
) -> Result<()> {
    let matches_schema = |value: &Value, schema: &SchemaReference| {
        let Some(local) = pinned_bytes(schemas, schema) else {
            return false;
        };
        serde_json_canonicalizer::to_vec(value).is_ok_and(|remote| remote == local)
    };
    let output_matches = match (tool.output_schema(), pin.output.as_ref()) {
        (None, None) => true,
        (Some(value), Some(schema)) => matches_schema(value, schema),
        _ => false,
    };
    if crate::mcp::forbidden_approved_tool_extension(tool.raw())
        || mcp_tool_descriptor_digest(tool.raw()).ok() != Some(pin.digest)
        || !matches_schema(tool.input_schema(), &pin.input)
        || !output_matches
    {
        return Err(failure(McpStdioFailure::Admission));
    }
    Ok(())
}

struct Client {
    identity: McpClientIdentity,
    catalog: McpToolCatalog,
    pins: Vec<McpStdioToolPin>,
    schemas: Arc<dyn ModelSchemaRegistry>,
    wire: Weak<Mutex<Option<Wire>>>,
    stop: CancellationToken,
}

/// Cloneable access to one admitted generation. Dropping the Session closes every clone.
#[derive(Clone)]
pub struct McpStdioClient(Arc<Client>);

impl McpStdioClient {
    /// Returns the atomically admitted Tool catalog.
    pub fn catalog(&self) -> &McpToolCatalog {
        &self.0.catalog
    }

    /// Dispatches one Tool call. A busy caller is refused before dispatch.
    pub async fn call_tool(&self, tool: &McpTool, arguments: Value) -> Result<McpToolCallResponse> {
        let end = Instant::now() + CALL;
        let input_error = || failure(McpStdioFailure::Input);
        let inner = &self.0;
        if inner.stop.is_cancelled() {
            return Err(failure(McpStdioFailure::Closed));
        }
        let expected = inner.catalog.find(tool.name()).ok_or_else(input_error)?;
        if expected.binding_id != tool.binding_id {
            return Err(input_error());
        }
        let pin = inner
            .pins
            .iter()
            .find(|pin| pin.name == tool.name())
            .ok_or_else(input_error)?;
        let shared = inner
            .wire
            .upgrade()
            .ok_or_else(|| failure(McpStdioFailure::Closed))?;
        let mut held = shared
            .try_lock()
            .map_err(|_| failure(McpStdioFailure::Busy))?;
        let wire = held
            .as_mut()
            .ok_or_else(|| failure(McpStdioFailure::Closed))?;
        let arguments = BoundedJson::try_from_value(arguments).map_err(|_| input_error())?;
        if !arguments.as_value().is_object()
            || inner.schemas.validate(&pin.input, &arguments).is_err()
        {
            return Err(input_error());
        }
        if Instant::now() >= end {
            return Err(failure(McpStdioFailure::Deadline));
        }
        let mut guard = StopOnDrop(Some(inner.stop.clone()));
        let mut sent = false;
        let mut params = Map::new();
        params.insert("name".into(), Value::String(tool.name().to_owned()));
        params.insert("arguments".into(), arguments.into_value());
        let result = tokio::select! {
            biased;
            () = inner.stop.cancelled() => Err(McpStdioFailure::Closed),
            result = timeout_at(end, wire.rpc(&inner.identity, "tools/call", params, &mut sent, end, &inner.stop)) => {
                result.map_err(|_| McpStdioFailure::Deadline).and_then(|value| value.map_err(wire_failure))
            }
        };
        if matches!(result, Err(McpStdioFailure::Remote)) {
            guard.0 = None;
        }
        let (response, notifications) = result.map_err(|kind| McpStdioError::new(kind, sent))?;
        let protocol = || McpStdioError::new(McpStdioFailure::Protocol, sent);
        let outcome = parse_complete_tool_result(response).map_err(|_| protocol())?;
        if !outcome.is_error()
            && let Some(schema) = &pin.output
        {
            let output = outcome.structured_content().cloned().ok_or_else(protocol)?;
            let output = BoundedJson::try_from_value(output).map_err(|_| protocol())?;
            inner
                .schemas
                .validate(schema, &output)
                .map_err(|_| protocol())?;
        }
        if Instant::now() >= end {
            return Err(McpStdioError::new(McpStdioFailure::Deadline, sent));
        }
        guard.0 = None;
        Ok(McpToolCallResponse {
            outcome: McpToolCall::Complete(outcome),
            notifications,
        })
    }
}
