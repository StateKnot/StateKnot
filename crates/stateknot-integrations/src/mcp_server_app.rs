// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Composite StateKnot-owned MCP 2026-07-28 server application.

use std::{borrow::Cow, fmt, sync::Arc};

use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CompleteRequestParams, CompleteResult,
        DiscoverResult, ErrorCode, GetPromptRequestParams, GetPromptResponse, Implementation,
        ListPromptsResult, ListResourceTemplatesResult, ListResourcesResult, ListToolsResult,
        PaginatedRequestParams, PromptsCapability, ProtocolVersion, ReadResourceRequestParams,
        ReadResourceResponse, ResourcesCapability, ServerCapabilities, ServerInfo, Tool,
        ToolsCapability,
    },
    service::RequestContext,
};
use thiserror::Error;

use crate::{
    McpServerApplicationOptions, McpServerCompletionProvider, McpServerPromptAuthorization,
    McpServerPromptCatalog, McpServerPromptRenderer, McpServerPromptService,
    McpServerPromptServiceBuildError, McpServerResourceAuthorization, McpServerResourceCatalog,
    McpServerResourceReader, McpServerResourceService, McpServerResourceServiceBuildError,
    McpServerToolAuthorization, McpServerToolRegistry, McpServerToolService,
    McpServerToolServiceBuildError,
};

/// Startup-only builder for a composite MCP application.
pub struct McpServerApplicationBuilder {
    options: McpServerApplicationOptions,
    tools: Option<McpServerToolService>,
    resources: Option<McpServerResourceService>,
    prompts: Option<McpServerPromptService>,
    completion: Option<Arc<dyn McpServerCompletionProvider>>,
}

impl McpServerApplicationBuilder {
    /// Creates a builder with one shared identity, pagination, and cache policy.
    #[must_use]
    pub fn new(options: McpServerApplicationOptions) -> Self {
        Self {
            options,
            tools: None,
            resources: None,
            prompts: None,
            completion: None,
        }
    }

    /// Adds the immutable Tool registry and decoded authorization policy.
    pub fn with_tools<A>(
        mut self,
        registry: McpServerToolRegistry,
        authorization: A,
    ) -> Result<Self, McpServerApplicationBuildError>
    where
        A: McpServerToolAuthorization,
    {
        if self.tools.is_some() {
            return Err(McpServerApplicationBuildError::DuplicateTools);
        }
        self.tools = Some(
            McpServerToolService::new(registry, self.options.clone(), authorization)
                .map_err(McpServerApplicationBuildError::ToolService)?,
        );
        Ok(self)
    }

    /// Adds immutable Resource metadata, a reader, and decoded authorization.
    pub fn with_resources<R, A>(
        mut self,
        catalog: McpServerResourceCatalog,
        reader: R,
        authorization: A,
    ) -> Result<Self, McpServerApplicationBuildError>
    where
        R: McpServerResourceReader,
        A: McpServerResourceAuthorization,
    {
        if self.resources.is_some() {
            return Err(McpServerApplicationBuildError::DuplicateResources);
        }
        self.resources = Some(
            McpServerResourceService::new(catalog, self.options.clone(), reader, authorization)
                .map_err(McpServerApplicationBuildError::ResourceService)?,
        );
        Ok(self)
    }

    /// Adds immutable Prompt metadata, a renderer, and decoded authorization.
    pub fn with_prompts<R, A>(
        mut self,
        catalog: McpServerPromptCatalog,
        renderer: R,
        authorization: A,
    ) -> Result<Self, McpServerApplicationBuildError>
    where
        R: McpServerPromptRenderer,
        A: McpServerPromptAuthorization,
    {
        if self.prompts.is_some() {
            return Err(McpServerApplicationBuildError::DuplicatePrompts);
        }
        self.prompts = Some(
            McpServerPromptService::new(catalog, self.options.clone(), renderer, authorization)
                .map_err(McpServerApplicationBuildError::PromptService)?,
        );
        Ok(self)
    }

    /// Adds a Prompt/Resource Completion provider.
    pub fn with_completion_provider<C>(
        mut self,
        provider: C,
    ) -> Result<Self, McpServerApplicationBuildError>
    where
        C: McpServerCompletionProvider,
    {
        if self.completion.is_some() {
            return Err(McpServerApplicationBuildError::DuplicateCompletion);
        }
        self.completion = Some(Arc::new(provider));
        Ok(self)
    }

    /// Freezes a non-empty application.
    pub fn build(mut self) -> Result<McpServerApplication, McpServerApplicationBuildError> {
        if self.tools.is_none() && self.resources.is_none() && self.prompts.is_none() {
            return Err(McpServerApplicationBuildError::Empty);
        }
        if let Some(completion) = self.completion {
            let prompts = self
                .prompts
                .take()
                .ok_or(McpServerApplicationBuildError::CompletionRequiresPrompts)?;
            self.prompts = Some(prompts.with_shared_completion_provider(completion));
        }
        Ok(McpServerApplication {
            options: self.options,
            tools: self.tools,
            resources: self.resources,
            prompts: self.prompts,
        })
    }
}

impl fmt::Debug for McpServerApplicationBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerApplicationBuilder")
            .field("options", &self.options)
            .field("tools", &self.tools.is_some())
            .field("resources", &self.resources.is_some())
            .field("prompts", &self.prompts.is_some())
            .field("completion", &self.completion.is_some())
            .finish_non_exhaustive()
    }
}

/// Composite application construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerApplicationBuildError {
    /// At least one server capability must be configured.
    #[error("MCP server application has no capabilities")]
    Empty,
    /// Tool surface was configured twice.
    #[error("MCP tool surface is already configured")]
    DuplicateTools,
    /// Resource surface was configured twice.
    #[error("MCP resource surface is already configured")]
    DuplicateResources,
    /// Prompt surface was configured twice.
    #[error("MCP prompt surface is already configured")]
    DuplicatePrompts,
    /// Completion was configured twice.
    #[error("MCP completion surface is already configured")]
    DuplicateCompletion,
    /// This profile binds Completion through the Prompt service.
    #[error("MCP completion requires a configured prompt surface")]
    CompletionRequiresPrompts,
    /// Tool service policy was inconsistent.
    #[error("invalid MCP tool service: {0}")]
    ToolService(McpServerToolServiceBuildError),
    /// Resource service policy was inconsistent.
    #[error("invalid MCP resource service: {0}")]
    ResourceService(McpServerResourceServiceBuildError),
    /// Prompt service policy was inconsistent.
    #[error("invalid MCP prompt service: {0}")]
    PromptService(McpServerPromptServiceBuildError),
}

/// Cloneable composite Tools, Resources, Prompts, and Completion handler.
#[derive(Clone)]
pub struct McpServerApplication {
    options: McpServerApplicationOptions,
    tools: Option<McpServerToolService>,
    resources: Option<McpServerResourceService>,
    prompts: Option<McpServerPromptService>,
}

impl McpServerApplication {
    /// Returns whether the Tool surface is configured.
    #[must_use]
    pub const fn has_tools(&self) -> bool {
        self.tools.is_some()
    }

    /// Returns whether the Resource surface is configured.
    #[must_use]
    pub const fn has_resources(&self) -> bool {
        self.resources.is_some()
    }

    /// Returns whether the Prompt surface is configured.
    #[must_use]
    pub const fn has_prompts(&self) -> bool {
        self.prompts.is_some()
    }

    /// Returns whether Completion is advertised.
    #[must_use]
    pub fn has_completion(&self) -> bool {
        self.prompts
            .as_ref()
            .is_some_and(|service| service.get_info().capabilities.completions.is_some())
    }

    fn capabilities(&self) -> ServerCapabilities {
        let mut capabilities = ServerCapabilities::default();
        if self.tools.is_some() {
            capabilities.tools = Some(ToolsCapability::default());
        }
        if self.resources.is_some() {
            capabilities.resources = Some(ResourcesCapability::default());
        }
        if self.prompts.is_some() {
            capabilities.prompts = Some(PromptsCapability::default());
        }
        if self.has_completion() {
            capabilities.completions = Some(serde_json::Map::new());
        }
        capabilities
    }
}

impl fmt::Debug for McpServerApplication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerApplication")
            .field("options", &self.options)
            .field("tools", &self.has_tools())
            .field("resources", &self.has_resources())
            .field("prompts", &self.has_prompts())
            .field("completion", &self.has_completion())
            .finish_non_exhaustive()
    }
}

impl ServerHandler for McpServerApplication {
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }

    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::new(self.capabilities()).with_server_info(Implementation::new(
            self.options.server_name.to_string(),
            self.options.server_version.to_string(),
        ));
        if let Some(instructions) = &self.options.instructions {
            info = info.with_instructions(instructions.to_string());
        }
        info
    }

    async fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> Result<DiscoverResult, ErrorData> {
        Ok(
            DiscoverResult::from_server_info(vec![ProtocolVersion::V_2026_07_28], self.get_info())
                .with_ttl_ms(self.options.cache_ttl_ms)
                .with_cache_scope(self.options.cache_scope.into()),
        )
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tools
            .as_ref()
            .and_then(|service| ServerHandler::get_tool(service, name))
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let Some(service) = &self.tools else {
            return Err(method_not_found("tools/list"));
        };
        ServerHandler::list_tools(service, request, context).await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let Some(service) = &self.tools else {
            return Err(method_not_found("tools/call"));
        };
        ServerHandler::call_tool(service, request, context).await
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let Some(service) = &self.resources else {
            return Err(method_not_found("resources/list"));
        };
        ServerHandler::list_resources(service, request, context).await
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        let Some(service) = &self.resources else {
            return Err(method_not_found("resources/templates/list"));
        };
        ServerHandler::list_resource_templates(service, request, context).await
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let Some(service) = &self.resources else {
            return Err(method_not_found("resources/read"));
        };
        ServerHandler::read_resource(service, request, context).await
    }

    async fn list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        let Some(service) = &self.prompts else {
            return Err(method_not_found("prompts/list"));
        };
        ServerHandler::list_prompts(service, request, context).await
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        let Some(service) = &self.prompts else {
            return Err(method_not_found("prompts/get"));
        };
        ServerHandler::get_prompt(service, request, context).await
    }

    async fn complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, ErrorData> {
        let Some(service) = &self.prompts else {
            return Err(method_not_found("completion/complete"));
        };
        ServerHandler::complete(service, request, context).await
    }
}

fn method_not_found(method: &'static str) -> ErrorData {
    ErrorData::new(ErrorCode::METHOD_NOT_FOUND, method, None)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use http::{Request, StatusCode};
    use http_body_util::{BodyExt as _, Full};
    use serde_json::{Value, json};
    use tower_service::Service as _;

    use super::*;
    use crate::{
        AllowMcpServerPromptAuthorization, AllowMcpServerResourceAuthorization,
        AllowMcpServerToolAuthorization, McpServerAuthentication, McpServerCacheScope,
        McpServerContent, McpServerHttpOptions, McpServerHttpService,
        McpServerPromptCatalogBuilder, McpServerPromptContext, McpServerPromptDefinition,
        McpServerPromptMessage, McpServerPromptOutcome, McpServerPromptRender,
        McpServerPromptRendererError, McpServerPromptResult, McpServerPromptRole,
        McpServerResourceCatalogBuilder, McpServerResourceContent, McpServerResourceContext,
        McpServerResourceDefinition, McpServerResourceOutcome, McpServerResourceRead,
        McpServerResourceReaderError, McpServerResourceResult, McpServerToolCall,
        McpServerToolContext, McpServerToolDefinition, McpServerToolHandlerError,
        McpServerToolOutcome, McpServerToolRegistryBuilder, McpServerToolResult,
    };

    #[derive(Clone, Copy)]
    struct TestTool;

    impl crate::McpServerToolHandler for TestTool {
        fn call(
            &self,
            _call: McpServerToolCall,
            _context: McpServerToolContext,
        ) -> stateknot_core::BoxFuture<'_, Result<McpServerToolOutcome, McpServerToolHandlerError>>
        {
            Box::pin(async {
                let content = McpServerContent::text("tool").unwrap();
                Ok(McpServerToolResult::success([content]).unwrap().into())
            })
        }
    }

    #[derive(Clone, Copy)]
    struct TestReader;

    impl McpServerResourceReader for TestReader {
        fn read(
            &self,
            request: McpServerResourceRead,
            _context: McpServerResourceContext,
        ) -> stateknot_core::BoxFuture<
            '_,
            Result<McpServerResourceOutcome, McpServerResourceReaderError>,
        > {
            let uri = request.uri().to_owned();
            Box::pin(async move {
                let content =
                    McpServerResourceContent::text(uri, Some("text/plain"), "resource").unwrap();
                Ok(McpServerResourceResult::new(
                    [content],
                    Duration::ZERO,
                    McpServerCacheScope::Private,
                )
                .unwrap()
                .into())
            })
        }
    }

    #[derive(Clone, Copy)]
    struct TestRenderer;

    impl McpServerPromptRenderer for TestRenderer {
        fn render(
            &self,
            _request: McpServerPromptRender,
            _context: McpServerPromptContext,
        ) -> stateknot_core::BoxFuture<
            '_,
            Result<McpServerPromptOutcome, McpServerPromptRendererError>,
        > {
            Box::pin(async {
                let content = McpServerContent::text("prompt").unwrap();
                Ok(McpServerPromptResult::new([McpServerPromptMessage::new(
                    McpServerPromptRole::User,
                    content,
                )])
                .unwrap()
                .into())
            })
        }
    }

    fn application() -> McpServerHttpService<McpServerApplication> {
        let mut tools = McpServerToolRegistryBuilder::default();
        tools
            .register(
                McpServerToolDefinition::new(
                    "test_tool",
                    json!({ "type": "object", "additionalProperties": false }),
                )
                .unwrap(),
                TestTool,
            )
            .unwrap();
        let mut resources = McpServerResourceCatalogBuilder::default();
        resources
            .register_resource(McpServerResourceDefinition::new("test://one", "One").unwrap())
            .unwrap();
        let mut prompts = McpServerPromptCatalogBuilder::default();
        prompts
            .register(McpServerPromptDefinition::new("test_prompt").unwrap())
            .unwrap();
        let options = McpServerApplicationOptions::new(
            "stateknot-composite-test",
            "0.0.0",
            32,
            Duration::from_secs(60),
            McpServerCacheScope::Private,
        )
        .unwrap();
        let application = McpServerApplicationBuilder::new(options)
            .with_tools(tools.build().unwrap(), AllowMcpServerToolAuthorization)
            .unwrap()
            .with_resources(
                resources.build().unwrap(),
                TestReader,
                AllowMcpServerResourceAuthorization,
            )
            .unwrap()
            .with_prompts(
                prompts.build().unwrap(),
                TestRenderer,
                AllowMcpServerPromptAuthorization,
            )
            .unwrap()
            .build()
            .unwrap();
        McpServerHttpService::new(
            application,
            McpServerHttpOptions::loopback(32127).unwrap(),
            McpServerAuthentication::anonymous_loopback(),
        )
        .unwrap()
    }

    fn request(method: &str, mut params: Value) -> Request<Full<Bytes>> {
        params.as_object_mut().unwrap().insert(
            "_meta".to_owned(),
            json!({
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": { "name": "test", "version": "0" },
                "io.modelcontextprotocol/clientCapabilities": {}
            }),
        );
        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
        }))
        .unwrap();
        Request::builder()
            .method("POST")
            .uri("http://127.0.0.1:32127/mcp")
            .header("host", "127.0.0.1:32127")
            .header("origin", "http://127.0.0.1:32127")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", method)
            .body(Full::new(Bytes::from(body)))
            .unwrap()
    }

    #[tokio::test]
    async fn composite_discovery_advertises_only_configured_surfaces() {
        let mut service = application();
        let response = service
            .call(request("server/discover", json!({})))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value.pointer("/result/capabilities/tools").is_some());
        assert!(value.pointer("/result/capabilities/resources").is_some());
        assert!(value.pointer("/result/capabilities/prompts").is_some());
        assert!(value.pointer("/result/capabilities/completions").is_none());
    }
}
