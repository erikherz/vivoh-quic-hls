// Copyright (C) 2025, Vivoh, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, is permitted only by Vivoh, Inc by License.
//

// ─── Standard Library ───────────────────────────────────────────────────────────
use std::io::ErrorKind;
use std::sync::Arc;
use std::path::PathBuf;
use std::net::SocketAddr;
use std::time::Duration;

// ─── External Crates ────────────────────────────────────────────────────────────
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, error, info, warn};
use web_transport_quinn::Session;
use clap::Parser;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pki_types::pem::PemObject;
use bytes::Bytes;
use quinn;

// ─── Internal Crate ─────────────────────────────────────────────────────────────
use vivoh_quic_hls::{
    VqdError,
    WebTransportMediaPacket,
    ConnectionRole,
    serialize_media_packet,
    ChannelManager,
    ChannelInfo,
};

// Import H2Server from your http.rs module
mod http;
use http::H2Server;

const CHANNEL_NAME: &str = "/live";

#[derive(Parser, Debug, Clone)]
#[command(name = "vivoh-quic-hls-server", about = "WebTransport hls server")]
pub struct Args {
    #[arg(short, long, default_value = "[::]:443")]
    pub addr: SocketAddr,

    #[arg(long)]
    pub cert: PathBuf,

    #[arg(long)]
    pub key: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    
    // Initialize crypto provider
    if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
        warn!("Crypto provider initialization warning (may already be installed): {:?}", e);
    }

    // Parse CLI args
    let args = Args::parse();

    // Create channel manager with a live channel
    let channel_manager = Arc::new(Mutex::new(ChannelManager::new()));
    {
        let mut manager = channel_manager.lock().await;
        manager.channels.insert("/live".to_string(), ChannelInfo::new("/live".to_string()));
    }    

    // Load certificates and private key
    let certs = CertificateDer::pem_file_iter(&args.cert)?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(&args.key)?;

    // Create a broadcast channel for the H2Server
    let (sender, _) = broadcast::channel::<Bytes>(16384);

    // Create rustls config for HTTP/2 server
    let mut h2_rustls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs.clone(), key.clone_key())?;

    // Configure max_early_data_size for QUIC
    h2_rustls_config.max_early_data_size = u32::MAX;

    // Start HTTP/2 server
    info!("Starting HTTP/2 server on {}", args.addr);
    let h2_server = tokio::spawn(
        H2Server::new(args.addr, h2_rustls_config, sender)
            .await?
            .run(),
    );

    // Create rustls config for WebTransport server with proper ALPN settings
    let mut wt_rustls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    wt_rustls_config.alpn_protocols = vec![b"h3".to_vec()];
    debug!("Server ALPN: {:?}", wt_rustls_config.alpn_protocols);

    // Start WebTransport server
    info!("Starting WebTransport server on {}", args.addr);
    let quic_config = quinn::crypto::rustls::QuicServerConfig::try_from(wt_rustls_config)
        .map_err(|e| anyhow::anyhow!("QUIC config error: {e}"))?;
    let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_config));
    let endpoint = quinn::Endpoint::server(server_config, args.addr)?;
    let server = web_transport_quinn::Server::new(endpoint);
    
    // Spawn heartbeat task
    let heartbeat_handle = tokio::spawn(async {
        loop {
            info!("Server alive, waiting for sessions...");
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    // Wait for servers to complete
    tokio::select! {
        result = h2_server => match result {
            Ok(()) => {
                info!("HTTP/2 server task ended");
                Ok(())
            },
            Err(error) => {
                error!(%error, "Failed to join HTTP/2 server task");
                Err(anyhow::anyhow!("Failed to join HTTP/2 server task"))
            }
        },
        result = run_accept_loop(server, channel_manager) => {
            info!("WebTransport server accept loop ended");
            result.map_err(|e| anyhow::anyhow!("Server error: {e}"))
        },
        _ = heartbeat_handle => {
            info!("Heartbeat task ended");
            Ok(())
        },
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown requested via Ctrl+C");
            Ok(())
        }
    }
}

async fn handle_publisher(
    session: Session,
    conn_id: String,
    manager: Arc<Mutex<ChannelManager>>,
) {
    info!("Handling publisher stream for {}", conn_id);

    loop {
        match session.accept_uni().await {
            Ok(mut stream) => {
                let manager = manager.clone();
                let _conn_id = conn_id.clone();

                tokio::spawn(async move {
                    // Limit to 2MB per WMP to avoid memory abuse
                    let read_result = stream.read_to_end(2_000_000).await;

                    match read_result {
                        Ok(buf) => {
                            match WebTransportMediaPacket::parse(&buf) {
                                Ok(packet) => {
                                    debug!(
                                        "Parsed WMP #{} ({}ms, video: {} bytes)",
                                        packet.packet_id,
                                        packet.duration,
                                        packet.video_data.len()
                                    );

                                    let mut mgr = manager.lock().await;
                                    if let Some(channel) = mgr.channels.get_mut(CHANNEL_NAME) {
                                        // Add to buffer
                                        channel.buffer.push_back(packet.clone());

                                        // Trim oldest if over capacity
                                        while channel.buffer.len() > 32 {
                                            channel.buffer.pop_front();
                                        }

                                        // Broadcast serialized version to players
                                        let data = crate::serialize_media_packet(&packet);
                                        let _ = channel.broadcast.send(data);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse WMP from publisher: {e}");
                                }
                            }
                        }
                        Err(e) => {
                            error!("Publisher stream read error: {e}");
                        }
                    }
                });
            }
            Err(e) => {
                error!("Publisher session error: {e}");
                break;
            }
        }
    }
}


async fn handle_player(
    session: Session,
    conn_id: String,
    manager: Arc<Mutex<ChannelManager>>,
) {
    info!("Handling player stream for {}", conn_id);

    let (initial_packets, mut rx) = {
        let mgr = manager.lock().await;
        if let Some(channel) = mgr.channels.get(CHANNEL_NAME) {
            let buffered = channel
                .buffer
                .iter()
                .cloned()
                .map(|wmp| crate::serialize_media_packet(&wmp))
                .collect::<Vec<_>>();

            (buffered, channel.broadcast.subscribe())
        } else {
            error!("Channel {} not found", CHANNEL_NAME);
            return;
        }
    };

    // Send initial buffered packets
    for data in initial_packets {
        if let Err(e) = send_stream(&session, data).await {
            error!("Failed to send buffered WMP to player: {e}");
            return;
        }
    }

    // Listen for new packets
    while let Ok(data) = rx.recv().await {
        if let Err(e) = send_stream(&session, data).await {
            error!("Failed to send live WMP to player: {e}");
            break;
        }
    }
}

async fn send_stream(session: &Session, data: bytes::Bytes) -> Result<(), std::io::Error> {

    let mut stream = session.open_uni().await.map_err(|e| {
        error!("Failed to open uni stream: {e}");
        std::io::Error::new(std::io::ErrorKind::Other, "stream open failed")
    })?;

    stream.write_all(&data).await.map_err(|e| {
        std::io::Error::new(ErrorKind::Other, format!("WebTransport write failed: {e}"))
    })?;    

    Ok(())
}

async fn run_accept_loop(
    mut server: web_transport_quinn::Server,
    manager: Arc<Mutex<ChannelManager>>,
) -> Result<(), VqdError> {
    let mut next_id = 0;
    info!("WebTransport accept loop started");

    while let Some(req) = server.accept().await {
        let path = req.url().path().to_string();
        let conn_id = format!("conn-{}", next_id);
        next_id += 1;

        let is_publisher = path.ends_with("/pub");
        let role = if is_publisher {
            ConnectionRole::ActivePublisher
        } else {
            ConnectionRole::Player
        };

        info!(
            "Accepted session on path: {}, role: {:?}, conn_id: {}",
            path, role, conn_id
        );

        let session = match req.ok().await {
            Ok(sess) => sess,
            Err(e) => {
                error!("Failed to establish session for {}: {}", conn_id, e);
                continue;
            }
        };

        let manager = manager.clone();
        tokio::spawn(async move {
            {
                let mut mgr = manager.lock().await;

                mgr.channels
                    .entry("/live".to_string())
                    .or_insert_with(|| vivoh_quic_hls::ChannelInfo::new("/live".to_string()));

                mgr.set_connection_role(&conn_id, role.clone());

                if role == ConnectionRole::ActivePublisher {
                    if let Some(channel) = mgr.channels.get_mut("/live") {
                        channel.set_active_publisher(conn_id.clone());
                    }
                }
            }

            info!("WebTransport session established for {}", conn_id);

            if role == ConnectionRole::ActivePublisher {
                handle_publisher(session, conn_id.clone(), manager.clone()).await;
            } else {
                handle_player(session, conn_id.clone(), manager.clone()).await;
            }

            info!("Connection {} ended, cleaning up", conn_id);
            manager.lock().await.remove_connection(&conn_id);
        });
    }

    Ok(())
}