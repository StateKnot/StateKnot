// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Private request-handoff and extractor compatibility controls. These are
//! in-process observations, paired with the public TCP initial-policy family.

use super::{Body, DefaultBodyLimit, Request, Router, canonicalize_request_payload};
use axum::{Json, extract::FromRequest as _, response::IntoResponse as _};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode, Version};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tower_service::Service as _;

const PUSH: &str = "/a2a/rest/tasks/task-a/pushNotificationConfigs";

struct Opaque {
    instance: usize,
    clones: Arc<AtomicUsize>,
}

impl Clone for Opaque {
    fn clone(&self) -> Self {
        Self {
            instance: self.clones.fetch_add(1, Ordering::SeqCst) + 1,
            clones: self.clones.clone(),
        }
    }
}

fn push_request(body: Body, length: usize) -> Request {
    Request::builder()
        .method(Method::POST)
        .uri(format!("{PUSH}?opaque=query%20value"))
        .version(Version::HTTP_2)
        .header("Content-Type", "application/a2a+json")
        .header("Content-Length", length)
        .header("x-opaque", "first")
        .body(body)
        .unwrap()
}

async fn call(mut router: Router, request: Request) -> http::Response<Body> {
    std::future::poll_fn(|cx| {
        <Router as tower_service::Service<Request>>::poll_ready(&mut router, cx)
    })
    .await
    .unwrap();
    router.call(request).await.unwrap()
}

#[tokio::test]
async fn push_handoff_keeps_original_parts_instance_headers_and_large_raw_bytes() {
    let maximum = 2 * 1024 * 1024 + 4096;
    let mut bytes =
        b"{ \"url\" : \"https://callback.example/notify\", \"id\" : \"cfg-a\" }\n".to_vec();
    bytes.resize(maximum, b' ');
    let expected = Arc::new(bytes);
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/a2a+json"),
    );
    headers.insert(
        "content-length",
        HeaderValue::from_str(&maximum.to_string()).unwrap(),
    );
    headers.append("x-opaque", HeaderValue::from_static("first"));
    headers.append("x-opaque", HeaderValue::from_static("second"));
    let clones = Arc::new(AtomicUsize::new(0));
    let captured = expected.clone();
    let expected_headers = headers.clone();
    let route = Router::new()
        .fallback(move |request: Request| {
            let expected = captured.clone();
            let expected_headers = expected_headers.clone();
            async move {
                let request =
                    canonicalize_request_payload(request, "/a2a/rest", "/a2a/jsonrpc", maximum)
                        .await
                        .unwrap();
                assert_eq!(request.method(), Method::POST);
                assert_eq!(
                    request.uri().to_string(),
                    format!("{PUSH}?opaque=query%20value")
                );
                assert_eq!(request.version(), Version::HTTP_2);
                assert_eq!(request.headers(), &expected_headers);
                // Cloning extractor input is allowed; forwarding the cloned
                // extension would change instance and fail this distinct oracle.
                assert_eq!(request.extensions().get::<Opaque>().unwrap().instance, 0);
                assert_eq!(
                    request.extensions().get::<u128>(),
                    Some(&0xfeed_dead_beef_u128)
                );
                assert_eq!(
                    request
                        .into_body()
                        .collect()
                        .await
                        .unwrap()
                        .to_bytes()
                        .as_ref(),
                    expected.as_slice()
                );
                StatusCode::NO_CONTENT
            }
        })
        .layer(DefaultBodyLimit::max(maximum));
    let mut request = push_request(Body::from(expected.as_ref().clone()), maximum);
    *request.headers_mut() = headers;
    request.extensions_mut().insert(Opaque {
        instance: 0,
        clones: clones.clone(),
    });
    request.extensions_mut().insert(0xfeed_dead_beef_u128);
    assert_eq!(call(route, request).await.status(), StatusCode::NO_CONTENT);
    // The original extension instance is the handoff contract. An extractor
    // may preserve it without cloning; cloning is only a test instrument.
}

fn io_error_body() -> Body {
    Body::from_stream(futures_util::stream::iter([Err::<Bytes, _>(
        std::io::Error::other("controlled body IO"),
    )]))
}

async fn rejection(original: bool, body: Body, maximum: usize) -> (StatusCode, HeaderMap, Bytes) {
    let router = Router::new()
        .fallback(move |request: Request| async move {
            if original {
                match Json::<Value>::from_request(request, &()).await {
                    Ok(_) => StatusCode::NO_CONTENT.into_response(),
                    Err(error) => error.into_response(),
                }
            } else {
                match canonicalize_request_payload(request, "/a2a/rest", "/a2a/jsonrpc", maximum)
                    .await
                {
                    Ok(_) => StatusCode::NO_CONTENT.into_response(),
                    Err(response) => response,
                }
            }
        })
        .layer(DefaultBodyLimit::max(maximum));
    let response = call(router, push_request(body, 1)).await;
    let (parts, body) = response.into_parts();
    (
        parts.status,
        parts.headers,
        body.collect().await.unwrap().to_bytes(),
    )
}

#[tokio::test]
async fn early_push_bytes_rejections_equal_original_json_extractor_in_full() {
    let original = rejection(true, io_error_body(), 2048).await;
    let early = rejection(false, io_error_body(), 2048).await;
    assert_eq!(early, original);
    assert_eq!(early.0, StatusCode::BAD_REQUEST);
    assert_eq!(
        early.2.as_ref(),
        b"Failed to buffer the request body: controlled body IO"
    );
    let original = rejection(true, Body::from(vec![b' '; 2049]), 2048).await;
    let early = rejection(false, Body::from(vec![b' '; 2049]), 2048).await;
    assert_eq!(early, original);
    assert_eq!(early.0, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        early.2.as_ref(),
        b"Failed to buffer the request body: length limit exceeded"
    );
}

async fn normalize(path: &str, bytes: &[u8]) -> (HeaderMap, Bytes) {
    let request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("Content-Type", "application/json")
        .header("Content-Length", bytes.len())
        .header("x-opaque", "preserved")
        .body(Body::from(bytes.to_vec()))
        .unwrap();
    let normalized = canonicalize_request_payload(request, "/a2a/rest", "/a2a/jsonrpc", 2048)
        .await
        .unwrap();
    let (parts, body) = normalized.into_parts();
    (parts.headers, body.collect().await.unwrap().to_bytes())
}

#[tokio::test]
async fn original_rest_send_and_rpc_canonicalization_and_content_length_removal_remain() {
    let raw=b"{ \"message\" : { \"messageId\" : \"fixture\", \"role\" : \"ROLE_USER\", \"parts\" : [{\"text\":\"payload\"}] } }";
    for path in ["/a2a/rest/message:send", "/a2a/rest/message:stream"] {
        let (headers, bytes) = normalize(path, raw).await;
        assert_eq!(headers["x-opaque"], "preserved");
        assert!(!headers.contains_key("Content-Length"));
        let actual: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(actual, serde_json::from_slice::<Value>(raw).unwrap());
        assert_ne!(bytes.as_ref(), raw);
    }
    let (headers,bytes)=normalize("/a2a/jsonrpc",b"{ \"jsonrpc\":\"2.0\", \"id\":42, \"method\":\"GetTask\", \"params\":{\"id\":\"task-a\"} }").await;
    assert!(!headers.contains_key("Content-Length"));
    assert_eq!(headers["x-opaque"], "preserved");
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap(),
        json!({"jsonrpc":"2.0","id":42,"method":"GetTask","params":{"id":"task-a"}})
    );
}
