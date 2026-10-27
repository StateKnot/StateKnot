// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Test-only, one-session, loopback, plaintext `PostgreSQL` 3.0 fault proxy.
//! Pins sqlx 0.8.6's failure-close Parse -> simple Query(COMMIT) exchange.
//! Never log frames: startup/authentication/bind messages can contain secrets.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use sqlx_postgres::PgConnectOptions;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};

const MAX_FRAME: u32 = 4 * 1024 * 1024;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TransactionTarget {
    FailureClose,
    FailureCloseCompletion,
    ChildAdmission,
    ChildCancellationDelivery,
    ChildSettlement,
    ChildJoinRegistration,
    ChildJoinPublication,
    ChildJoinConsumption,
    AgentDeadlineCancellation,
    GraphNodeStart,
    GraphFrameClosure,
    GraphFrameReturn,
    GraphFrameBarrier,
    LeaseRenewal,
    LeaseRelease,
    RunQuarantine,
    GraphFrameCancellationConfirmation,
    ToolInvocationOutcome,
}

impl TransactionTarget {
    const fn parse_prefix(self) -> &'static [u8] {
        match self {
            Self::ToolInvocationOutcome => {
                b"\nUPDATE stateknot.tool_invocations AS current_invocation\n"
            }
            Self::FailureClose => b"INSERT INTO stateknot.run_failure_closes ",
            Self::FailureCloseCompletion => b"\nUPDATE stateknot.runs\nSET journal_sequence = $3,",
            Self::ChildAdmission => b"INSERT INTO stateknot.child_run_ownership ",
            Self::ChildCancellationDelivery => {
                b"INSERT INTO stateknot.child_run_cancellation_receipts "
            }
            Self::ChildSettlement => b"INSERT INTO stateknot.child_run_settlements ",
            Self::ChildJoinRegistration => b"INSERT INTO stateknot.child_run_joins ",
            Self::ChildJoinPublication => b"INSERT INTO stateknot.child_run_join_bindings ",
            Self::ChildJoinConsumption => b"\nINSERT INTO stateknot.pending_node_results (",
            Self::AgentDeadlineCancellation => b"SELECT agent_deadline_at FROM stateknot.runs ",
            Self::GraphNodeStart => b"\nINSERT INTO stateknot.node_attempts (",
            Self::GraphFrameClosure => b"INSERT INTO stateknot.graph_frame_closures(",
            Self::GraphFrameReturn => b"INSERT INTO stateknot.graph_frame_returns(",
            Self::GraphFrameBarrier => b"INSERT INTO stateknot.graph_frame_barriers (",
            Self::LeaseRenewal => b"\nUPDATE stateknot.runs\nSET lease_renewed_at = $5,",
            Self::LeaseRelease => b"\nUPDATE stateknot.runs\nSET lease_attempt_id = NULL,",
            Self::RunQuarantine => b"\nINSERT INTO stateknot.run_quarantines (",
            Self::GraphFrameCancellationConfirmation => {
                b"
UPDATE stateknot.runs
SET journal_sequence = $3,
    journal_event_id = $4,
    journal_recorded_at = $5,
    journal_digest = $6,
    lifecycle_bytes = $7,
    lifecycle_revision = $8::numeric,
    lifecycle_status = $9,
"
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Cut {
    BeforeCommit,
    CommitResponse,
}

pub(super) struct CommitProxy {
    port: u16,
    backend_pid: Arc<AtomicI32>,
    reached: mpsc::Receiver<Cut>,
    task: JoinHandle<io::Result<()>>,
    resume: Option<tokio::sync::oneshot::Sender<()>>,
    messages_while_ack_held: Arc<AtomicUsize>,
    outcome_phase: Option<tokio::sync::oneshot::Sender<()>>,
}

impl CommitProxy {
    pub(super) async fn start(
        options: &PgConnectOptions,
        cut: Cut,
        target: TransactionTarget,
    ) -> Self {
        Self::start_after_matching_commits(options, cut, target, 0).await
    }

    // Skipped targets require statement_cache_capacity(0) on the test-only
    // connection so each target transaction has its own actual Parse message.
    pub(super) async fn start_after_matching_commits(
        options: &PgConnectOptions,
        cut: Cut,
        target: TransactionTarget,
        skip: u8,
    ) -> Self {
        Self::start_cut(options, cut, target, skip, None, None).await
    }

    // A real executor call distinguishes execution maintenance from any
    // startup/preparation renewal under the same original lease duration.
    pub(super) async fn start_after_node_launch(
        options: &PgConnectOptions,
        cut: Cut,
        target: TransactionTarget,
        calls: Arc<AtomicUsize>,
    ) -> Self {
        Self::start_cut(options, cut, target, 0, Some(calls), None).await
    }

    // The writer uses this one session only for commit_tool_terminal after its
    // direct Store has completed prepare/start/provider dispatch. The controller
    // snapshots all committed tenant facts before releasing the matching Parse.
    pub(super) async fn start_paused_tool_outcome(options: &PgConnectOptions, cut: Cut) -> Self {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut proxy = Self::start_cut(
            options,
            cut,
            TransactionTarget::ToolInvocationOutcome,
            0,
            None,
            Some(receiver),
        )
        .await;
        proxy.outcome_phase = Some(sender);
        proxy
    }

    pub(super) fn arm_tool_outcome(&mut self) {
        self.outcome_phase
            .take()
            .expect("arm the isolated outcome phase once")
            .send(())
            .expect("outcome proxy must remain live");
    }

    #[allow(clippy::too_many_lines)]
    async fn start_cut(
        options: &PgConnectOptions,
        cut: Cut,
        target: TransactionTarget,
        mut skip: u8,
        launched: Option<Arc<AtomicUsize>>,
        mut outcome_phase: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> Self {
        let backend = loopback_target(options);
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let backend_pid = Arc::new(AtomicI32::new(0));
        let pid = backend_pid.clone();
        let messages_while_ack_held = Arc::new(AtomicUsize::new(0));
        let held_messages = messages_while_ack_held.clone();
        let (sender, reached) = mpsc::channel(1);
        let (resume, resumed) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut client, _) = listener.accept().await?;
            drop(listener); // Exactly one session; the worker explicitly has a 1-connection pool.
            let mut server = TcpStream::connect(backend).await?;
            client.set_nodelay(true)?;
            server.set_nodelay(true)?;
            let length = client.read_u32().await?;
            if !(8..=65_536).contains(&length) {
                return Err(invalid("unsupported startup length"));
            }
            let mut startup = vec![0; (length - 4) as usize];
            client.read_exact(&mut startup).await?;
            if startup[..4] != 196_608_u32.to_be_bytes() {
                return Err(invalid(
                    "only plaintext PostgreSQL protocol 3.0 is supported",
                ));
            }
            server.write_u32(length).await?;
            server.write_all(&startup).await?;
            let (mut frontend_read, mut frontend_write) = client.into_split();
            let (mut backend_read, mut backend_write) = server.into_split();
            let hold_response = AtomicBool::new(false);
            let cut_once = AtomicBool::new(false);
            // Separate directional futures preserve partial read_exact state. We
            // never cancel and resume a partially read frame inside a select loop.
            let frontend = async {
                let mut armed = false;
                loop {
                    let frame = read_frame(&mut frontend_read).await?;
                    if hold_response.load(Ordering::SeqCst) {
                        held_messages.fetch_add(1, Ordering::SeqCst);
                    }
                    if frame.tag == b'P' {
                        let query = parse_query(&frame.body)?;
                        let matched = query.starts_with(target.parse_prefix());
                        armed |= matched;
                        if matched && let Some(phase) = outcome_phase.take() {
                            phase
                                .await
                                .map_err(|_| invalid("controller gone before outcome phase"))?;
                        }
                    }
                    if armed && frame.tag == b'Q' && frame.body == b"COMMIT\0" {
                        armed = false;
                        if skip > 0 {
                            skip -= 1;
                        } else if launched
                            .as_ref()
                            .is_none_or(|calls| calls.load(Ordering::SeqCst) > 0)
                            && !cut_once.swap(true, Ordering::SeqCst)
                        {
                            if cut == Cut::BeforeCommit {
                                sender
                                    .send(cut)
                                    .await
                                    .map_err(|_| invalid("controller gone"))?;
                                return std::future::pending::<io::Result<()>>().await;
                            }
                            hold_response.store(true, Ordering::SeqCst);
                        }
                    }
                    write_frame(&mut backend_write, &frame).await?;
                }
            };
            let backend = async {
                let mut resumed = Some(resumed);
                loop {
                    let frame = read_frame(&mut backend_read).await?;
                    if frame.tag == b'K' {
                        if frame.body.len() != 8 {
                            return Err::<(), _>(invalid("unexpected backend key shape"));
                        }
                        pid.store(
                            i32::from_be_bytes(frame.body[..4].try_into().unwrap()),
                            Ordering::SeqCst,
                        );
                    }
                    if hold_response.load(Ordering::SeqCst) {
                        if frame.tag != b'C' || frame.body != b"COMMIT\0" {
                            return Err(invalid("expected COMMIT CommandComplete"));
                        }
                        let ready = read_frame(&mut backend_read).await?;
                        if ready.tag != b'Z' || ready.body != b"I" {
                            return Err(invalid("expected idle ReadyForQuery after COMMIT"));
                        }
                        // Neither response frame is forwarded. An independent
                        // database reader additionally proves the commit is durable.
                        sender
                            .send(cut)
                            .await
                            .map_err(|_| invalid("controller gone"))?;
                        resumed
                            .take()
                            .ok_or_else(|| invalid("ACK already resumed"))?
                            .await
                            .map_err(|_| invalid("controller gone before ACK"))?;
                        hold_response.store(false, Ordering::SeqCst);
                        write_frame(&mut frontend_write, &frame).await?;
                        write_frame(&mut frontend_write, &ready).await?;
                        continue;
                    }
                    write_frame(&mut frontend_write, &frame).await?;
                }
            };
            tokio::try_join!(frontend, backend)?;
            Ok(())
        });
        Self {
            port,
            backend_pid,
            reached,
            task,
            resume: Some(resume),
            messages_while_ack_held,
            outcome_phase: None,
        }
    }

    pub(super) const fn port(&self) -> u16 {
        self.port
    }

    pub(super) fn backend_pid(&self) -> i32 {
        let pid = self.backend_pid.load(Ordering::SeqCst);
        assert!(pid > 0, "must observe the actual session PID");
        pid
    }

    pub(super) async fn wait_cut(&mut self, expected: Cut) {
        tokio::time::timeout(Duration::from_secs(90), async {
            tokio::select! {
                result = self.reached.recv() => assert_eq!(result, Some(expected)),
                result = &mut self.task => panic!("proxy ended before cut: {result:?}"),
            }
        })
        .await
        .expect("exact commit cut was not reached");
    }

    pub(super) fn resume_ack(&mut self) {
        self.resume
            .take()
            .expect("resume ACK once")
            .send(())
            .unwrap();
    }

    pub(super) fn messages_while_ack_held(&self) -> usize {
        self.messages_while_ack_held.load(Ordering::SeqCst)
    }

    pub(super) async fn stop(&mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}

impl Drop for CommitProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) fn loopback_target(options: &PgConnectOptions) -> SocketAddr {
    let ip: IpAddr = if options.get_host() == "localhost" {
        Ipv4Addr::LOCALHOST.into()
    } else {
        options
            .get_host()
            .parse()
            .expect("fault proxy requires literal loopback address")
    };
    assert!(
        ip.is_loopback(),
        "fault proxy must never target a remote database"
    );
    assert!(options.get_socket().is_none(), "fault proxy requires TCP");
    SocketAddr::new(ip, options.get_port())
}

struct Frame {
    tag: u8,
    body: Vec<u8>,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Frame> {
    let tag = reader.read_u8().await?;
    let length = reader.read_u32().await?;
    if !(4..=MAX_FRAME).contains(&length) {
        return Err(invalid("protocol frame outside test bound"));
    }
    let mut body = vec![0; (length - 4) as usize];
    reader.read_exact(&mut body).await?;
    Ok(Frame { tag, body })
}

async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, frame: &Frame) -> io::Result<()> {
    writer.write_u8(frame.tag).await?;
    writer
        .write_u32(u32::try_from(frame.body.len() + 4).unwrap())
        .await?;
    writer.write_all(&frame.body).await
}

fn parse_query(body: &[u8]) -> io::Result<&[u8]> {
    let mut fields = body.splitn(3, |byte| *byte == 0);
    fields
        .next()
        .ok_or_else(|| invalid("missing statement name"))?;
    let query = fields
        .next()
        .ok_or_else(|| invalid("missing Parse query"))?;
    let params = fields
        .next()
        .ok_or_else(|| invalid("unterminated Parse query"))?;
    if params.len() < 2 {
        return Err(invalid("missing Parse parameter count"));
    }
    Ok(query)
}

#[tokio::test]
async fn commit_proxy_rejects_truncated_oversized_and_malformed_frames() {
    for bytes in [
        b"Q\0\0\0\x03".as_slice(),
        b"Q\x7f\xff\xff\xff",
        b"Q\0\0\0\x05",
    ] {
        assert!(read_frame(&mut &bytes[..]).await.is_err());
    }
    assert!(parse_query(b"unterminated").is_err());
    assert!(parse_query(b"name\0query\0").is_err());
    assert_eq!(parse_query(b"name\0SELECT 1\0\0\0").unwrap(), b"SELECT 1");
    for target in [
        TransactionTarget::FailureClose,
        TransactionTarget::FailureCloseCompletion,
        TransactionTarget::ChildAdmission,
        TransactionTarget::ChildCancellationDelivery,
        TransactionTarget::ChildSettlement,
        TransactionTarget::ChildJoinRegistration,
        TransactionTarget::ChildJoinPublication,
        TransactionTarget::ChildJoinConsumption,
        TransactionTarget::AgentDeadlineCancellation,
        TransactionTarget::GraphFrameCancellationConfirmation,
        TransactionTarget::ToolInvocationOutcome,
    ] {
        assert!(!target.parse_prefix().is_empty());
    }
    let remote = PgConnectOptions::new().host("192.0.2.1");
    assert!(std::panic::catch_unwind(|| loopback_target(&remote)).is_err());
}
