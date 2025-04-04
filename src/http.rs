// Copyright (C) 2025, Vivoh, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, is permitted only by Vivoh, Inc by License.
//
//

use std::{convert::Infallible, net::SocketAddr, sync::Arc};

// External Crates
use bytes::Bytes;
use serde::Serialize;
use tracing::{debug, error, info};

// HTTP and Hyper Related
use http::{
    header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE},
    Request, Response, StatusCode,
};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http2::Builder as Http2Builder;
use hyper_util::rt::{TokioExecutor, TokioIo};

// TLS and Rustls
use rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

// Tokio and Utilities
use tokio::{net::TcpListener, sync::broadcast};

// Static Files
const INDEX_HTML: &[u8] = include_bytes!("../static/index.html");
const ADMIN_HTML: &[u8] = include_bytes!("../static/admin.html");
const HLS_JS: &[u8] = include_bytes!("../static/hls.js");
const VIVOH_PNG: &[u8] = include_bytes!("../static/vivoh.png");

#[derive(Serialize)]
struct ControlData {
    connections: usize,
}

impl ControlData {
    fn new(connections: usize) -> Self {
        Self { connections }
    }
}

async fn http_service(
    sender: broadcast::Sender<Bytes>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    debug!(method = %req.method(), path = %req.uri().path(), "Received request");

    let response = match req.uri().path() {
        "/" | "/index.html" => serve_static(INDEX_HTML, "text/html"),
        "/admin" => serve_static(ADMIN_HTML, "text/html"),
        "/hls.js" => serve_static(HLS_JS, "application/javascript"),
        "/vivoh.png" => serve_static(VIVOH_PNG, "image/png"),
        "/control" => {
            let control_data = ControlData::new(sender.receiver_count());
            let json = Bytes::from(serde_json::to_string(&control_data).unwrap());
            Response::builder()
                .header(CONTENT_TYPE, "application/json")
                .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                .status(StatusCode::OK)
                .body(Full::from(json))
                .unwrap()
        }
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .body(Full::from(Bytes::from("404 Not Found")))
            .unwrap(),
    };

    Ok(response)
}

fn serve_static(content: &'static [u8], content_type: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .header(CONTENT_TYPE, content_type)
        .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .status(StatusCode::OK)
        .body(Full::from(Bytes::from(content)))
        .unwrap()
}

pub struct H2Server {
    addr: SocketAddr,
    listener: TcpListener,
    acceptor: TlsAcceptor,
    builder: Http2Builder<TokioExecutor>,
    sender: broadcast::Sender<Bytes>,
}

impl H2Server {
    pub async fn new(
        addr: SocketAddr,
        mut tls_config: ServerConfig,
        sender: broadcast::Sender<Bytes>,
    ) -> anyhow::Result<Self> {
        tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        let mut builder = Http2Builder::new(TokioExecutor::new());
        builder.max_concurrent_streams(256);

        Ok(Self {
            addr,
            listener: TcpListener::bind(addr).await?,
            acceptor: TlsAcceptor::from(Arc::new(tls_config)),
            builder,
            sender,
        })
    }

    pub async fn run(self) {
        debug!(addr = %self.addr, "HTTP/2 server running");

        while let Ok((tcp_stream, peer_addr)) = self.listener.accept().await {
            let sender = self.sender.clone();
            let acceptor = self.acceptor.clone();
            let builder = self.builder.clone();

            tokio::spawn(async move {
                info!(%peer_addr, "New HTTP/2 connection");

                let tls_stream = match acceptor.accept(tcp_stream).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        error!(%peer_addr, %error, "❌ TLS handshake failed");
                        return;
                    }
                };

                let conn = builder.serve_connection(
                    TokioIo::new(tls_stream),
                    hyper::service::service_fn(|req| http_service(sender.clone(), req)),
                );

                if let Err(error) = conn.await {
                    error!(%peer_addr, %error, "❌ HTTP/2 connection error");
                }
            });
        }
    }
}
