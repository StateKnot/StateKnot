// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Owned test subprocesses, never production workers or database server processes.

use std::{
    io::{BufRead, BufReader, Read, Write},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc::{self, Receiver, TryRecvError},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use serde_json::{Value, json};

pub(super) const INPUT_ENV: &str = "STATEKNOT_TEST_PROCESS_INPUT";
const TOKEN_ENV: &str = "STATEKNOT_TEST_PROCESS_TOKEN";
const MARKER: &str = "STATEKNOT_PROCESS_READY=";
const MAX_OUTPUT: u64 = 64 * 1024;
const READY_TIMEOUT: Duration = Duration::from_secs(90);
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) struct TestProcess {
    child: Child,
    ready: Receiver<Result<Value, String>>,
    reader: Option<JoinHandle<()>>,
    token: String,
}

impl TestProcess {
    pub(super) fn spawn(test: &str, input: &Value) -> Self {
        let token = uuid::Uuid::now_v7().to_string();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture", "--test-threads=1"])
            // No process-global environment mutation; database credentials stay off argv.
            .env(INPUT_ENV, serde_json::to_string(input).unwrap())
            .env(TOKEN_ENV, &token)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn owned qualification process");
        let stdout = child.stdout.take().unwrap();
        let (sender, ready) = mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            // Bound the *whole* pre-readiness output, not just each line. EOF, a wrong
            // test filter, a crash or malformed output must never count as readiness.
            let mut output = BufReader::new(stdout.take(MAX_OUTPUT));
            let mut line = String::new();
            let result = loop {
                line.clear();
                match output.read_line(&mut line) {
                    Ok(0) => {
                        break Err("process ended or exceeded output bound before ready".into());
                    }
                    Ok(_) => {
                        if let Some(message) = line.trim_end().strip_prefix(MARKER) {
                            break serde_json::from_str(message)
                                .map_err(|_| "invalid process readiness message".into());
                        }
                    }
                    Err(_) => break Err("cannot read process readiness".into()),
                }
            };
            let _ = sender.send(result);
            // Drain bounded remaining output so a resumed worker may exit through
            // libtest normally. No second readiness message is accepted.
            let _ = std::io::copy(&mut output, &mut std::io::sink());
        });
        Self {
            child,
            ready,
            reader: Some(reader),
            token,
        }
    }

    pub(super) async fn ready(&mut self) -> Result<Value, String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            match self.ready.try_recv() {
                Ok(Ok(message)) if message["token"].as_str() == Some(self.token.as_str()) => {
                    if self.child.try_wait().unwrap().is_some() {
                        return Err("process exited instead of remaining at kill point".into());
                    }
                    return Ok(message["data"].clone());
                }
                Ok(Ok(_)) => return Err("process readiness identity mismatch".into()),
                Ok(Err(error)) => return Err(error),
                Err(TryRecvError::Disconnected) => return Err("readiness reader stopped".into()),
                Err(TryRecvError::Empty) => {}
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for durable process kill point".into());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub(super) async fn kill_and_reap(&mut self) -> ExitStatus {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "worker died before kill"
        );
        self.child
            .kill()
            .expect("force-kill only our owned child PID");
        let deadline = Instant::now() + EXIT_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(!status.success(), "kill must not be graceful completion");
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    assert_eq!(status.signal(), Some(9), "require real SIGKILL");
                }
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "killed child was not reaped in time"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub(super) fn resume(&mut self) {
        let pipe = self.child.stdin.as_mut().expect("parent liveness pipe");
        pipe.write_all(b"r").unwrap();
        pipe.flush().unwrap();
    }

    pub(super) async fn wait_success(&mut self) {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "resumed worker failed: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "resumed worker did not finish");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

impl Drop for TestProcess {
    fn drop(&mut self) {
        // Also reap on an assertion failure / readiness timeout. The reader cannot
        // outlive a pipe writer: this test child never spawns descendant processes.
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

pub(super) fn publish_ready(data: &Value) {
    let token = std::env::var(TOKEN_ENV).expect("worker requires a parent rendezvous token");
    println!("\n{MARKER}{}", json!({"token": token, "data": data}));
    std::io::stdout().flush().unwrap();
}

pub(super) async fn ready_and_park(data: Value) {
    publish_ready(&data);
    // Keep the caller's store alive. No cooperative cancellation, pool.close(),
    // task abortion, Drop, process exit or in-memory recovery substitutes for kill.
    std::future::pending::<()>().await;
}

pub(super) fn watch_parent() {
    drop(parent_control());
}

pub(super) fn parent_control() -> tokio::sync::oneshot::Receiver<()> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    // The parent's otherwise-unused pipe is a liveness handle. If the controller
    // itself crashes, do not leave a parked worker behind. Exit 24 is cleanup,
    // never successful qualification (which independently requires SIGKILL).
    std::thread::spawn(move || {
        let mut sender = Some(sender);
        loop {
            let mut byte = [0_u8; 1];
            match std::io::stdin().read(&mut byte) {
                Ok(1) if byte[0] == b'r' && sender.is_some() => {
                    if sender.take().unwrap().send(()).is_err() {
                        std::process::exit(24);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                _ => std::process::exit(24),
            }
        }
    });
    receiver
}

#[tokio::test]
async fn resume_worker() {
    if std::env::var_os(INPUT_ENV).is_none() {
        return;
    }
    let resume = parent_control();
    publish_ready(&Value::Null);
    resume.await.unwrap();
}

#[tokio::test]
async fn process_harness_resumes_and_observes_asserting_worker_exit() {
    let mut process = TestProcess::spawn("process_harness::resume_worker", &Value::Null);
    process.ready().await.unwrap();
    process.resume();
    process.wait_success().await;
}

#[tokio::test]
async fn parent_watch_worker() {
    if std::env::var_os(INPUT_ENV).is_none() {
        return;
    }
    watch_parent();
    ready_and_park(Value::Null).await;
}

#[tokio::test]
async fn process_harness_exits_when_controller_liveness_pipe_closes() {
    let mut process = TestProcess::spawn("process_harness::parent_watch_worker", &Value::Null);
    process.ready().await.unwrap();
    drop(process.child.stdin.take());
    let deadline = Instant::now() + EXIT_TIMEOUT;
    loop {
        if let Some(status) = process.child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(24));
            break;
        }
        assert!(Instant::now() < deadline, "orphan watchdog failed");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn process_harness_rejects_successful_exit_without_a_kill_point() {
    let mut process = TestProcess::spawn("no_such_qualification_worker", &Value::Null);
    assert!(process.ready().await.is_err());
}
