// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) const INNER: &str = "application-owned-inner-state";
pub(super) const KEY: [u8; 32] = [71; 32]; // Test-only deterministic codec key.
pub(super) const SCOPES: [&str; 2] = ["bound:use", "bound:extra"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Mapping {
    pub tenant: TenantId,
    pub effective: PrincipalIdentity,
    pub generation: u64,
    pub scopes: Vec<String>,
}

pub(super) fn identity(subject: &str) -> PrincipalIdentity {
    PrincipalIdentity::new(
        "https://issuer.example.test/bound-state".parse().unwrap(),
        subject.parse().unwrap(),
    )
}

impl Mapping {
    pub(super) fn new(tenant: TenantId) -> Self {
        Self {
            tenant,
            effective: identity("local-operator"),
            generation: 1,
            scopes: SCOPES.into_iter().map(str::to_owned).collect(),
        }
    }
    pub(super) fn principal(&self) -> McpServerPrincipal {
        McpServerPrincipal::from_host_binding(
            McpServerHostBinding::new(
                "bound-state-fixture",
                identity("same-external-subject"),
                self.tenant.clone(),
                self.effective.clone(),
                NonZeroU64::new(self.generation).unwrap(),
            )
            .unwrap(),
            self.scopes.clone(),
        )
        .unwrap()
    }
}

#[derive(Default)]
pub(super) struct Gate {
    pub armed: AtomicBool,
    pub entered: Notify,
    pub release: Notify,
}
impl Gate {
    pub(super) fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
    pub(super) async fn hold_once(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            let released = self.release.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            self.entered.notify_one();
            released.await;
        }
    }
}

#[derive(Clone)]
pub(super) struct Host(pub Arc<HostState>);
pub(super) struct HostState {
    pub mapping: RwLock<Mapping>,
    pub legacy_principal: AtomicBool,
    pub business_calls: AtomicUsize,
    pub completions: AtomicUsize,
    pub wire_states: Mutex<Vec<Option<String>>>,
    pub inner_states: Mutex<Vec<Option<String>>>,
    pub bindings: Mutex<Vec<Vec<u8>>>,
    pub business_facts: Mutex<Vec<Value>>,
    pub admission_gate: Gate,
    pub policy_gate: Gate,
    pub inner: Option<String>,
    pub durable: Option<durable::Ledger>,
}
impl Host {
    pub(super) fn new() -> Self {
        Self::with_ledger(
            Mapping::new(TenantId::new("bound-network-a").unwrap()),
            None,
        )
    }
    pub(super) fn with_ledger(mapping: Mapping, ledger: Option<durable::Ledger>) -> Self {
        Self(Arc::new(HostState {
            mapping: RwLock::new(mapping),
            legacy_principal: AtomicBool::new(false),
            business_calls: AtomicUsize::new(0),
            completions: AtomicUsize::new(0),
            wire_states: Mutex::new(Vec::new()),
            inner_states: Mutex::new(Vec::new()),
            bindings: Mutex::new(Vec::new()),
            business_facts: Mutex::new(Vec::new()),
            admission_gate: Gate::default(),
            policy_gate: Gate::default(),
            inner: Some(INNER.to_owned()),
            durable: ledger,
        }))
    }
    pub(super) fn current(&self) -> Mapping {
        self.0.mapping.read().unwrap().clone()
    }
    pub(super) fn set(&self, mapping: Mapping) {
        *self.0.mapping.write().unwrap() = mapping;
    }
    pub(super) fn count(&self) -> usize {
        self.0.business_calls.load(Ordering::SeqCst)
    }
    fn is_current(&self, principal: &McpServerPrincipal) -> bool {
        // An explicit legacy fixture policy remains usable without the new profile.
        self.0.legacy_principal.load(Ordering::SeqCst) || principal == &self.current().principal()
    }
    async fn policy(&self, principal: &McpServerPrincipal, state: Option<&str>) -> bool {
        self.0
            .wire_states
            .lock()
            .unwrap()
            .push(state.map(str::to_owned));
        self.0.policy_gate.hold_once().await;
        self.is_current(principal)
    }
    async fn business(
        &self,
        principal: &McpServerPrincipal,
        state: Option<&str>,
        responses: &BTreeMap<String, Value>,
        ad: &[u8],
    ) -> bool {
        self.0.business_calls.fetch_add(1, Ordering::SeqCst);
        self.0
            .inner_states
            .lock()
            .unwrap()
            .push(state.map(str::to_owned));
        self.0.bindings.lock().unwrap().push(ad.to_vec());
        if responses.is_empty() {
            return false;
        }
        assert_eq!(
            state,
            self.0.inner.as_deref(),
            "business sees original inner state"
        );
        if let Some(ledger) = &self.0.durable {
            ledger
                .append(principal.host_binding().unwrap().tenant_id())
                .await;
        }
        self.0.completions.fetch_add(1, Ordering::SeqCst);
        true
    }
    fn challenge(&self) -> McpServerInputRequired {
        let request = McpServerInputRequest::try_from_protocol_json(json!({
            "method":"roots/list", "params":{}
        }))
        .unwrap();
        McpServerInputRequired::new(
            BTreeMap::from([("approval".to_owned(), request)]),
            self.0.inner.clone(),
        )
        .unwrap()
    }
}
impl McpServerAuthenticator for Host {
    fn authenticate(
        &self,
        request: McpServerAuthenticationRequest,
    ) -> BoxFuture<'_, Result<McpServerPrincipal, McpServerAuthenticationError>> {
        Box::pin(async move {
            if request.credential().expose_secret() != "host-token" {
                return Err(McpServerAuthenticationError::InvalidCredential);
            }
            if self.0.legacy_principal.load(Ordering::SeqCst) {
                return Ok(
                    McpServerPrincipal::new("hb1:legacy-text-is-not-a-binding", SCOPES).unwrap(),
                );
            }
            Ok(self.current().principal())
        })
    }
}
impl McpServerAdmissionControl for Host {
    fn admit(
        &self,
        request: McpServerAdmissionRequest,
    ) -> BoxFuture<'_, Result<(), McpServerAdmissionError>> {
        Box::pin(async move {
            self.0.admission_gate.hold_once().await;
            if self.is_current(request.principal()) {
                Ok(())
            } else {
                Err(McpServerAdmissionError::Forbidden)
            }
        })
    }
}
impl McpServerToolAuthorization for Host {
    fn authorize(
        &self,
        request: McpServerToolAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerToolAuthorizationError>> {
        Box::pin(async move {
            if self
                .policy(request.principal(), request.call().request_state())
                .await
            {
                Ok(())
            } else {
                Err(McpServerToolAuthorizationError::Forbidden)
            }
        })
    }
}
impl McpServerResourceAuthorization for Host {
    fn authorize(
        &self,
        request: McpServerResourceAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerResourceAuthorizationError>> {
        Box::pin(async move {
            if self
                .policy(request.principal(), request.request().request_state())
                .await
            {
                Ok(())
            } else {
                Err(McpServerResourceAuthorizationError::Forbidden)
            }
        })
    }
}
impl McpServerPromptAuthorization for Host {
    fn authorize(
        &self,
        request: McpServerPromptAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerPromptAuthorizationError>> {
        Box::pin(async move {
            if self
                .policy(request.principal(), request.request().request_state())
                .await
            {
                Ok(())
            } else {
                Err(McpServerPromptAuthorizationError::Forbidden)
            }
        })
    }
}
impl McpServerSkillAuthorization for Host {
    fn authorize(
        &self,
        request: McpServerSkillAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerSkillAuthorizationError>> {
        Box::pin(async move {
            if self.is_current(request.principal()) {
                Ok(())
            } else {
                Err(McpServerSkillAuthorizationError::Forbidden)
            }
        })
    }
}
impl McpServerCompletionProvider for Host {
    fn complete(
        &self,
        request: McpServerCompletionRequest,
    ) -> BoxFuture<'_, Result<McpServerCompletionResult, McpServerCompletionError>> {
        Box::pin(async move {
            self.0.policy_gate.hold_once().await;
            if !self.is_current(request.principal()) {
                return Err(McpServerCompletionError::NotFoundOrForbidden);
            }
            self.0.business_calls.fetch_add(1, Ordering::SeqCst);
            Ok(McpServerCompletionResult::new(["approved"], Some(1), false).unwrap())
        })
    }
}
impl McpServerToolHandler for Host {
    fn call(
        &self,
        call: McpServerToolCall,
        context: McpServerToolContext,
    ) -> BoxFuture<'_, Result<McpServerToolOutcome, McpServerToolHandlerError>> {
        Box::pin(async move {
            self.0.business_facts.lock().unwrap().push(json!({"operation":call.name(),"arguments":call.arguments(),"responses":call.input_responses()}));
            if !self
                .business(
                    context.principal(),
                    call.request_state(),
                    call.input_responses(),
                    context.request_state_binding(),
                )
                .await
            {
                return Ok(McpServerToolOutcome::InputRequired(self.challenge()));
            }
            Ok(
                McpServerToolResult::success([McpServerContent::text("committed").unwrap()])
                    .unwrap()
                    .into(),
            )
        })
    }
}
impl McpServerResourceReader for Host {
    fn read(
        &self,
        request: McpServerResourceRead,
        context: McpServerResourceContext,
    ) -> BoxFuture<'_, Result<McpServerResourceOutcome, McpServerResourceReaderError>> {
        Box::pin(async move {
            self.0.business_facts.lock().unwrap().push(json!({"operation":request.uri(),"arguments":null,"responses":request.input_responses()}));
            if !self
                .business(
                    context.principal(),
                    request.request_state(),
                    request.input_responses(),
                    context.request_state_binding(),
                )
                .await
            {
                return Ok(McpServerResourceOutcome::InputRequired(self.challenge()));
            }
            // Public contents are an explicit application decision, independent of Private catalogs.
            Ok(McpServerResourceResult::new(
                [
                    McpServerResourceContent::text(request.uri(), Some("text/plain"), "committed")
                        .unwrap(),
                ],
                Duration::from_secs(1),
                McpServerCacheScope::Public,
            )
            .unwrap()
            .into())
        })
    }
}
impl McpServerPromptRenderer for Host {
    fn render(
        &self,
        request: McpServerPromptRender,
        context: McpServerPromptContext,
    ) -> BoxFuture<'_, Result<McpServerPromptOutcome, McpServerPromptRendererError>> {
        Box::pin(async move {
            self.0.business_facts.lock().unwrap().push(json!({"operation":request.name(),"arguments":request.arguments(),"responses":request.input_responses()}));
            if !self
                .business(
                    context.principal(),
                    request.request_state(),
                    request.input_responses(),
                    context.request_state_binding(),
                )
                .await
            {
                return Ok(McpServerPromptOutcome::InputRequired(self.challenge()));
            }
            Ok(McpServerPromptResult::new([McpServerPromptMessage::new(
                McpServerPromptRole::User,
                McpServerContent::text("committed").unwrap(),
            )])
            .unwrap()
            .into())
        })
    }
}

pub(super) fn options(scope: McpServerCacheScope) -> McpServerApplicationOptions {
    McpServerApplicationOptions::new(
        "bound-state-test",
        "1.0.0",
        1,
        Duration::from_secs(30),
        scope,
    )
    .unwrap()
}
pub(super) fn codec() -> McpServerRequestStateCodec {
    McpServerRequestStateCodec::new(KEY).unwrap()
}
pub(super) fn profile() -> McpServerBoundStateProfile {
    McpServerBoundStateProfile::new(codec(), Duration::from_secs(30)).unwrap()
}
pub(super) fn tools(host: &Host, scoped: bool) -> McpServerToolRegistry {
    let mut b = McpServerToolRegistryBuilder::default();
    for name in ["bound_a", "bound_b"] {
        b.register(
            McpServerToolDefinition::new(name, json!({"type":"object"})).unwrap(),
            host.clone(),
        )
        .unwrap();
    }
    if scoped {
        b.register(
            McpServerToolDefinition::new("bound_scoped", json!({"type":"object"}))
                .unwrap()
                .with_required_scopes(["bound:extra"])
                .unwrap(),
            host.clone(),
        )
        .unwrap();
    }
    b.build().unwrap()
}
pub(super) fn resources(scoped: bool) -> McpServerResourceCatalog {
    let mut b = McpServerResourceCatalogBuilder::default();
    for name in ["a", "b"] {
        b.register_resource(
            McpServerResourceDefinition::new(
                format!("test://bound/{name}"),
                format!("resource_{name}"),
            )
            .unwrap(),
        )
        .unwrap();
        b.register_template(
            McpServerResourceTemplateDefinition::new(
                format!("test://bound/{name}/{{id}}"),
                format!("template_{name}"),
            )
            .unwrap(),
        )
        .unwrap();
    }
    if scoped {
        b.register_resource(
            McpServerResourceDefinition::new("test://bound/scoped", "scoped")
                .unwrap()
                .with_required_scopes(["bound:extra"])
                .unwrap(),
        )
        .unwrap();
        b.register_template(
            McpServerResourceTemplateDefinition::new("test://bound/scoped/{id}", "scoped")
                .unwrap()
                .with_required_scopes(["bound:extra"])
                .unwrap(),
        )
        .unwrap();
    }
    b.build().unwrap()
}
pub(super) fn prompts(scoped: bool) -> McpServerPromptCatalog {
    let mut b = McpServerPromptCatalogBuilder::default();
    for name in ["bound_a", "bound_b"] {
        b.register(
            McpServerPromptDefinition::new(name)
                .unwrap()
                .with_arguments([McpServerPromptArgument::new("value").unwrap()])
                .unwrap(),
        )
        .unwrap();
    }
    if scoped {
        b.register(
            McpServerPromptDefinition::new("bound_scoped")
                .unwrap()
                .with_required_scopes(["bound:extra"])
                .unwrap(),
        )
        .unwrap();
    }
    b.build().unwrap()
}
pub(super) fn skills(scoped: bool) -> McpServerSkillCatalog {
    let mut b = McpServerSkillCatalogBuilder::default();
    for name in ["bound-a", "bound-b", "bound-scoped"] {
        if name == "bound-scoped" && !scoped {
            continue;
        }
        let doc = format!("---\nname: {name}\ndescription: Bound state fixture.\n---\n\n# Bound\n");
        let mut def = McpServerSkillDefinition::new(
            format!("skill://{name}/SKILL.md"),
            [McpServerSkillFile::text("SKILL.md", "text/markdown", doc).unwrap()],
        )
        .unwrap();
        if name == "bound-scoped" {
            def = def.with_required_scopes(["bound:extra"]).unwrap();
        }
        b.register(def).unwrap();
    }
    b.build().unwrap()
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Mount {
    Composite,
    Tool,
    Resource,
    Prompt,
    Skill,
}
impl Mount {
    pub(super) const CONSUMERS: [Self; 3] = [Self::Tool, Self::Resource, Self::Prompt];
    pub(super) fn operation(self) -> (&'static str, Value) {
        match self {
            Self::Tool => (
                "tools/call",
                json!({"name":"bound_a","arguments":{"value":"same"}}),
            ),
            Self::Resource => ("resources/read", json!({"uri":"test://bound/a"})),
            Self::Prompt => (
                "prompts/get",
                json!({"name":"bound_a","arguments":{"value":"same"}}),
            ),
            _ => panic!("not an MRTR consumer"),
        }
    }
}

pub(super) struct Server {
    pub url: String,
    pub host: Host,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.host.0.admission_gate.release.notify_waiters();
        self.host.0.policy_gate.release.notify_waiters();
        self.task.abort();
    }
}
impl Server {
    pub(super) async fn tool_handle(host: Host, handler: McpServerToolService) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let http = McpServerHttpService::with_admission_control(
            handler,
            McpServerHttpOptions::loopback(address.port()).unwrap(),
            McpServerAuthentication::bearer(
                host.clone(),
                McpServerBearerChallenge::new("bound-state", None::<String>).unwrap(),
            ),
            Arc::new(host.clone()),
        )
        .unwrap();
        let app = Router::new().fallback_service(http);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url: format!("http://{address}/mcp"),
            host,
            task,
        }
    }
    pub(super) async fn start(
        host: Host,
        mount: Mount,
        strict: bool,
        shared: bool,
        early: bool,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        macro_rules! router {
            ($handler:expr) => {{
                let http = McpServerHttpService::with_admission_control(
                    $handler,
                    McpServerHttpOptions::loopback(address.port()).unwrap(),
                    McpServerAuthentication::bearer(
                        host.clone(),
                        McpServerBearerChallenge::new("bound-state", None::<String>).unwrap(),
                    ),
                    Arc::new(host.clone()),
                )
                .unwrap();
                Router::new().fallback_service(http)
            }};
        }
        let opts = options(McpServerCacheScope::Private);
        let app = match mount {
            Mount::Composite => {
                let mut b = McpServerApplicationBuilder::new(opts);
                if strict && early {
                    b = b.with_bound_state(profile()).unwrap();
                }
                b = b
                    .with_tools(tools(&host, true), host.clone())
                    .unwrap()
                    .with_resources(resources(true), host.clone(), host.clone())
                    .unwrap()
                    .with_prompts(prompts(true), host.clone(), host.clone())
                    .unwrap()
                    .with_skills(skills(true), host.clone())
                    .unwrap()
                    .with_completion_provider(host.clone())
                    .unwrap();
                if strict && !early {
                    b = b.with_bound_state(profile()).unwrap();
                }
                router!(b.build().unwrap())
            }
            Mount::Tool => {
                let mut s = if shared {
                    McpServerToolService::with_shared_authorization(
                        tools(&host, true),
                        opts,
                        Arc::new(host.clone()),
                    )
                    .unwrap()
                } else {
                    McpServerToolService::new(tools(&host, true), opts, host.clone()).unwrap()
                };
                if strict {
                    s = s.with_bound_state(profile()).unwrap();
                }
                router!(s.clone())
            }
            Mount::Resource => {
                let mut s = if shared {
                    McpServerResourceService::with_shared(
                        resources(true),
                        opts,
                        Arc::new(host.clone()),
                        Arc::new(host.clone()),
                    )
                    .unwrap()
                } else {
                    McpServerResourceService::new(resources(true), opts, host.clone(), host.clone())
                        .unwrap()
                };
                if strict {
                    s = s.with_bound_state(profile()).unwrap();
                }
                router!(s.clone())
            }
            Mount::Prompt => {
                let mut s = if shared {
                    McpServerPromptService::with_shared(
                        prompts(true),
                        opts,
                        Arc::new(host.clone()),
                        Arc::new(host.clone()),
                    )
                    .unwrap()
                } else {
                    McpServerPromptService::new(prompts(true), opts, host.clone(), host.clone())
                        .unwrap()
                };
                if early {
                    s = s.with_shared_completion_provider(Arc::new(host.clone()));
                }
                if strict {
                    s = s.with_bound_state(profile()).unwrap();
                }
                if !early {
                    s = s.with_completion_provider(host.clone());
                }
                router!(s.clone())
            }
            Mount::Skill => {
                let mut s = McpServerSkillService::new(skills(true), opts, host.clone()).unwrap();
                if strict {
                    s = s.with_bound_state(profile()).unwrap();
                }
                router!(s.clone())
            }
        };
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url: format!("http://{address}/mcp"),
            host,
            task,
        }
    }
    pub(super) async fn invoke(&self, method: &str, mut params: Value) -> Value {
        params.as_object_mut().unwrap().insert("_meta".into(), json!({
            "io.modelcontextprotocol/protocolVersion":"2026-07-28",
            "io.modelcontextprotocol/clientInfo":{"name":"bound-state-test","version":"1"},
            "io.modelcontextprotocol/clientCapabilities":{"roots":{"listChanged":false},"extensions":{(MCP_SKILLS_EXTENSION_ID):{}}}
        }));
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let mut request = client
            .post(&self.url)
            .bearer_auth("host-token")
            .header("MCP-Protocol-Version", "2026-07-28")
            .header("MCP-Method", method)
            .header("Accept", "application/json, text/event-stream");
        if let Some(name) = params
            .get("name")
            .or_else(|| params.get("uri"))
            .and_then(Value::as_str)
        {
            request = request.header("MCP-Name", name);
        }
        let response = request
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        let Ok(mut decoded) = serde_json::from_str::<Value>(&body) else {
            assert!(
                !status.is_success(),
                "unexpected successful {method} response: {body}"
            );
            return json!({"error":{"http_status":status.as_u16(),"fixture_body":body}});
        };
        assert_eq!(decoded.get("jsonrpc"), Some(&json!("2.0")), "{body}");
        if !status.is_success() {
            assert!(decoded.get("error").is_some_and(Value::is_object), "{body}");
        }
        // Protocol refusals use HTTP 400 and still carry a JSON-RPC envelope.
        // Keep the real transport status alongside the untouched protocol error.
        decoded["_fixture_http_status"] = json!(status.as_u16());
        decoded
    }
}

pub(super) fn state(response: &Value) -> String {
    assert_eq!(
        response.pointer("/result/resultType"),
        Some(&json!("input_required")),
        "{response}"
    );
    response
        .pointer("/result/requestState")
        .and_then(Value::as_str)
        .unwrap()
        .to_owned()
}
pub(super) fn continued(mut params: Value, state: &str) -> Value {
    params["requestState"] = json!(state);
    params["inputResponses"] = json!({"approval":{"roots":[]}});
    params
}
pub(super) fn invalid(response: &Value) {
    assert_eq!(
        response.get("_fixture_http_status"),
        Some(&json!(400)),
        "{response}"
    );
    assert_eq!(
        response.pointer("/error/code"),
        Some(&json!(-32602)),
        "{response}"
    );
    assert_eq!(
        response.pointer("/error/message"),
        Some(&json!("Invalid MCP request state")),
        "{response}"
    );
    assert!(
        response.pointer("/error/data").is_none(),
        "error must not reflect state: {response}"
    );
}
pub(super) fn complete(response: &Value) {
    assert!(response.get("error").is_none(), "{response}");
    assert_ne!(
        response.pointer("/result/resultType"),
        Some(&json!("input_required")),
        "{response}"
    );
    assert!(response.get("result").is_some(), "{response}");
    assert_ne!(
        response.pointer("/result/isError"),
        Some(&json!(true)),
        "{response}"
    );
}
