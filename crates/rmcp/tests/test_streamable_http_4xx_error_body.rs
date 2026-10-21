#![cfg(all(
    feature = "transport-streamable-http-client",
    feature = "transport-streamable-http-client-reqwest",
    not(feature = "local")
))]

use std::{collections::HashMap, sync::Arc};

use rmcp::{
    model::{
        ClientJsonRpcMessage, ClientRequest, DiscoverRequest, DiscoverRequestParams, PingRequest,
        RequestId,
    },
    transport::streamable_http_client::{
        StreamableHttpClient, StreamableHttpError, StreamableHttpPostResponse,
    },
};

/// Spin up a minimal axum server that always responds with the given status,
/// content-type, and body — no MCP logic involved.
async fn spawn_mock_server(
    status: u16,
    content_type: &'static str,
    body: &'static str,
    challenge: Option<&'static str>,
) -> String {
    use axum::{Router, body::Body, http::Response, routing::post};

    let router = Router::new().route(
        "/mcp",
        post(move || async move {
            let mut builder = Response::builder()
                .status(status)
                .header("content-type", content_type);
            if let Some(challenge) = challenge {
                builder = builder.header("www-authenticate", challenge);
            }
            builder.body(Body::from(body)).unwrap()
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    format!("http://{addr}/mcp")
}

fn ping_message() -> ClientJsonRpcMessage {
    ClientJsonRpcMessage::request(
        ClientRequest::PingRequest(PingRequest::default()),
        RequestId::Number(1),
    )
}

fn discover_message() -> ClientJsonRpcMessage {
    ClientJsonRpcMessage::request(
        ClientRequest::DiscoverRequest(DiscoverRequest::new(DiscoverRequestParams {})),
        RequestId::Number(1),
    )
}

#[tokio::test]
async fn bare_401_preserves_status() {
    for message in [ping_message(), discover_message()] {
        for body in [
            "",
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Unauthorized"}}"#,
        ] {
            let url = spawn_mock_server(401, "application/json", body, None).await;
            let result = reqwest::Client::new()
                .post_message(
                    Arc::from(url.as_str()),
                    message.clone(),
                    None,
                    None,
                    HashMap::new(),
                )
                .await;
            match result {
                Err(StreamableHttpError::HttpStatus(error)) => {
                    assert_eq!(error.status, 401);
                    assert!(error.retry_after.is_none());
                }
                other => panic!("expected HTTP status 401, got: {other:?}"),
            }
        }
    }
}

#[tokio::test]
async fn preserves_non_authentication_statuses() {
    let mut failures = Vec::new();
    for status in [400, 403, 429, 503] {
        for content_type in ["text/plain", "application/json"] {
            let url = spawn_mock_server(status, content_type, "Rejected", None).await;
            let result = reqwest::Client::builder()
                .retry(reqwest::retry::never())
                .build()
                .unwrap()
                .post_message(
                    Arc::from(url.as_str()),
                    ping_message(),
                    None,
                    None,
                    HashMap::new(),
                )
                .await;
            match result {
                Err(StreamableHttpError::HttpStatus(error)) => {
                    assert_eq!(error.status, status);
                }
                other => failures.push(format!("{status}/{content_type}: {other:?}")),
            }
        }
    }
    assert!(failures.is_empty(), "HTTP status was lost: {failures:?}");
}

#[tokio::test]
async fn post_challenges_keep_authentication_errors() {
    for (status, challenge) in [
        (401, "Bearer realm=\"mcp\""),
        (
            403,
            "Bearer error=\"insufficient_scope\", scope=\"mcp:read\"",
        ),
    ] {
        let url = spawn_mock_server(status, "application/json", "", Some(challenge)).await;
        let result = reqwest::Client::new()
            .post_message(
                Arc::from(url.as_str()),
                ping_message(),
                None,
                None,
                HashMap::new(),
            )
            .await;
        match result {
            Err(StreamableHttpError::AuthRequired(error)) if status == 401 => {
                assert_eq!(error.www_authenticate_header, challenge);
            }
            Err(StreamableHttpError::InsufficientScope(error)) if status == 403 => {
                assert_eq!(error.www_authenticate_header, challenge);
                assert_eq!(error.required_scope.as_deref(), Some("mcp:read"));
            }
            other => panic!("expected HTTP {status} authentication challenge, got: {other:?}"),
        }
    }
}

/// HTTP 4xx with Content-Type: application/json and a valid JSON-RPC error body
/// must be surfaced as `StreamableHttpPostResponse::Json`, not swallowed as a
/// transport error.
#[tokio::test]
async fn http_4xx_json_rpc_error_body_is_surfaced_as_json_response() {
    let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Invalid Request"}}"#;
    let url = spawn_mock_server(400, "application/json", body, None).await;

    let client = reqwest::Client::new();
    let result = client
        .post_message(
            Arc::from(url.as_str()),
            ping_message(),
            None,
            None,
            HashMap::new(),
        )
        .await;

    match result {
        Ok(StreamableHttpPostResponse::Json(msg, _)) => {
            let json = serde_json::to_value(&msg).unwrap();
            assert_eq!(json["error"]["code"], -32600);
            assert_eq!(json["error"]["message"], "Invalid Request");
        }
        other => panic!("expected Json response, got: {other:?}"),
    }
}

/// An unstructured error body retains the actual HTTP status.
#[tokio::test]
async fn plain_errors_preserve_status() {
    let url = spawn_mock_server(400, "text/plain", "Bad Request", None).await;

    let client = reqwest::Client::new();
    let result = client
        .post_message(
            Arc::from(url.as_str()),
            ping_message(),
            None,
            None,
            HashMap::new(),
        )
        .await;

    match result {
        Err(StreamableHttpError::HttpStatus(error)) => assert_eq!(error.status, 400),
        other => panic!("expected HTTP status 400, got: {other:?}"),
    }
}

/// Invalid JSON-RPC error bodies retain their actual HTTP status.
#[tokio::test]
async fn malformed_errors_preserve_status() {
    let url = spawn_mock_server(400, "application/json", r#"{"error":"not jsonrpc"}"#, None).await;

    let client = reqwest::Client::new();
    let result = client
        .post_message(
            Arc::from(url.as_str()),
            ping_message(),
            None,
            None,
            HashMap::new(),
        )
        .await;

    match result {
        Err(StreamableHttpError::HttpStatus(error)) => assert_eq!(error.status, 400),
        other => panic!("expected HTTP status 400, got: {other:?}"),
    }
}

/// A discovery request attached to an existing session must retain the
/// transport error path; legacy fallback only applies to sessionless probes.
#[tokio::test]
async fn sessionful_discover_4xx_does_not_trigger_legacy_fallback() {
    let url = spawn_mock_server(422, "text/plain", "Session-bound rejection", None).await;

    let client = reqwest::Client::new();
    let result = client
        .post_message(
            Arc::from(url.as_str()),
            discover_message(),
            Some(Arc::from("session-1")),
            None,
            HashMap::new(),
        )
        .await;

    match result {
        Err(StreamableHttpError::HttpStatus(error)) => assert_eq!(error.status, 422),
        other => panic!("expected HTTP status 422, got: {other:?}"),
    }
}
