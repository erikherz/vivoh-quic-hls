// Copyright (C) 2025, Vivoh, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, is permitted only by Vivoh, Inc by License.
//

// ─── Standard Library ───────────────────────────────────────────────────────────
use std::collections::{HashMap, VecDeque};

// ─── External Crates ────────────────────────────────────────────────────────────
use bytes::Bytes;
use quick_xml::de;
use tokio::sync::broadcast;
use tracing::info;

pub const BROADCAST_CHANNEL_SIZE: usize = 16384;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionRole {
    ActivePublisher,
    Player,
}

#[derive(Debug, Clone)]
pub struct WebTransportMediaPacket {
    pub packet_id: u32,
    pub timestamp: u64,
    pub duration: u32,
    pub segment_duration: f32,  // Duration in seconds, typically 1.0
    pub av_data: Bytes,
}

impl WebTransportMediaPacket {
    pub fn parse(buf: &[u8]) -> Result<Self, crate::VqdError> {
        use bytes::{Buf, Bytes};

        let mut cursor = std::io::Cursor::new(buf);

        if cursor.remaining() < 16 {
            return Err(crate::VqdError::Other("WMP too short".into()));
        }

        let packet_id = cursor.get_u32();
        let timestamp = cursor.get_u64();
        let duration = cursor.get_u32();
        let segment_duration = cursor.get_f32();

        // Get the length of av_data field
        let av_data_len = cursor.get_u32() as usize;

        if cursor.remaining() < av_data_len {
            return Err(crate::VqdError::Other("WMP truncated".into()));
        }
        
        let mut av_buffer = vec![0u8; av_data_len];
        cursor.copy_to_slice(&mut av_buffer);
        let av_data = Bytes::from(av_buffer);

        Ok(Self {
            packet_id,
            timestamp,
            duration,
            segment_duration,
            av_data,
        })
    }
}

// Shared error type for both publisher and server
use thiserror::Error;

#[derive(Error, Debug)]
pub enum VqdError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("XML parsing error: {0}")]
    Xml(#[from] de::DeError),

    #[error("Missing file: {0}")]
    MissingFile(String),

    #[error("Other error: {0}")]
    Other(String),
}

impl From<rustls::Error> for VqdError {
    fn from(e: rustls::Error) -> Self {
        VqdError::Other(format!("TLS error: {}", e))
    }
}

impl From<url::ParseError> for VqdError {
    fn from(err: url::ParseError) -> Self {
        VqdError::Other(err.to_string())
    }
}

impl From<std::net::AddrParseError> for VqdError {
    fn from(err: std::net::AddrParseError) -> Self {
        VqdError::Other(err.to_string())
    }
}

impl From<web_transport_quinn::ClientError> for VqdError {
    fn from(err: web_transport_quinn::ClientError) -> Self {
        VqdError::Other(err.to_string())
    }
}

pub fn serialize_media_packet(packet: &WebTransportMediaPacket) -> bytes::Bytes {
    use bytes::{BytesMut, BufMut};

    let mut buf = BytesMut::new();

    buf.put_u32(packet.packet_id);
    buf.put_u64(packet.timestamp);
    buf.put_u32(packet.duration);
    buf.put_f32(packet.segment_duration);

    // Put the length of av_data
    buf.put_u32(packet.av_data.len() as u32);
    
    // Put the actual av_data
    buf.put_slice(&packet.av_data);

    buf.freeze()
}

pub struct ChannelInfo {
    pub path: String,
    pub broadcast: broadcast::Sender<Bytes>,
    pub active_publisher: Option<String>,   
    pub last_activity: std::time::Instant,
    pub buffer: VecDeque<WebTransportMediaPacket>,
}

impl ChannelInfo {
    pub fn new(path: String) -> Self {
        let (broadcast, _) = broadcast::channel(8);
        ChannelInfo {
            path,
            active_publisher: None,
            broadcast,
            last_activity: std::time::Instant::now(),
            buffer: VecDeque::new(),
        }
    }

    pub fn has_active_publisher(&self) -> bool {
        self.active_publisher.is_some()
    }

    pub fn set_active_publisher(&mut self, connection_id: String) {
        info!(
            "Setting active publisher for channel {}: {}",
            self.path, connection_id
        );
        self.active_publisher = Some(connection_id);
        self.last_activity = std::time::Instant::now();
    }

    pub fn remove_publisher(&mut self, connection_id: &str) -> bool {
        // Check if this was the active publisher
        if self
            .active_publisher
            .as_ref()
            .map_or(false, |id| id == connection_id)
        {
            info!(
                "Removing active publisher {} from channel {}",
                connection_id, self.path
            );
            self.active_publisher = None;
            true
        } else {
            false
        }
    }

    pub fn get_connection_role(&self, connection_id: &str) -> ConnectionRole {
        // Check if this is the active publisher
        if self
            .active_publisher
            .as_ref()
            .map_or(false, |id| id == connection_id)
        {
            return ConnectionRole::ActivePublisher;
        }

        // Otherwise, it's a player
        ConnectionRole::Player
    }
}

pub struct ChannelManager {
    pub channels: HashMap<String, ChannelInfo>,
    pub connection_roles: HashMap<String, (String, ConnectionRole)>, // Maps connection_id -> (path, role)
    pub last_cleanup: std::time::Instant,
}

impl ChannelManager {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            connection_roles: HashMap::new(),
            last_cleanup: std::time::Instant::now(),
        }
    }

    pub fn register_connection(&mut self, connection_id: &str) {
        self.connection_roles.insert(
            connection_id.to_string(),
            ("/live".to_string(), ConnectionRole::Player),
        );
    
        info!(
            "Registered new connection {} for path /live with player role",
            connection_id
        );
    }   
    

    pub fn get_connection_role(&self, connection_id: &str) -> Option<(String, ConnectionRole)> {
        self.connection_roles.get(connection_id).cloned()
    }

    pub fn set_connection_role(&mut self, connection_id: &str, role: ConnectionRole) {
        info!(
            "Setting connection {} role to {:?} for path /live",
            connection_id, role
        );
    
        self.connection_roles
            .insert(connection_id.to_string(), ("/live".to_string(), role));
    }    

    pub fn is_active_publisher(&self, connection_id: &str) -> bool {
        if let Some(channel) = self.channels.get("/live") {
            if let Some(active_pub_id) = &channel.active_publisher {
                return active_pub_id == connection_id;
            }
        }
        false
    }    

    pub fn remove_connection(&mut self, connection_id: &str) -> Option<(String, ConnectionRole)> {
        // Remove and get the connection info
        let connection_info = self.connection_roles.remove(connection_id)?;
        let (_path, role) = &connection_info;
    
        // If this was the active publisher, clear it from /live
        if *role == ConnectionRole::ActivePublisher {
            if let Some(channel) = self.channels.get_mut("/live") {
                channel.remove_publisher(connection_id);
            }
        }
    
        Some(connection_info)
    }    

    pub fn log_channel_stats(&self) {
        if let Some(channel) = self.channels.get("/live") {
            info!(
                "Channel /live: {} receivers, active_pub: {}",
                channel.broadcast.receiver_count(),
                channel.active_publisher.is_some()
            );
        }
    }
    
}

pub mod tls {
    use std::sync::Arc;
    use tracing::debug;
    use std::{fs::File, io::BufReader};
    use quinn::ServerConfig;
    use rustls::ServerConfig as RustlsServerConfig;
    use rustls_pemfile::{certs, pkcs8_private_keys};
    use crate::VqdError;
    use quinn::crypto::rustls::QuicServerConfig;
    use rustls_pki_types::{CertificateDer as Certificate, PrivateKeyDer as PrivateKey, PrivatePkcs8KeyDer};


    pub fn build_server_config(cert_path: &str, key_path: &str) -> Result<ServerConfig, VqdError> {
        // Read certificate chain
        let cert_file = &mut BufReader::new(File::open(cert_path)?);
        let cert_chain = certs(cert_file)?
            .into_iter()
            .map(Certificate::from)
            .collect::<Vec<_>>();
    
        // Read private key
        let key_file = &mut BufReader::new(File::open(key_path)?);
        let mut keys = pkcs8_private_keys(key_file)?;
        if keys.is_empty() {
            return Err(VqdError::Other("No private keys found".into()));
        }

        let key_der = PrivatePkcs8KeyDer::from(keys.remove(0));
        let private_key = PrivateKey::from(key_der);

        // Build rustls config
        let mut rustls_config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)?;
    
        rustls_config.alpn_protocols = vec![b"h3".to_vec()];
        debug!("Server ALPN: {:?}", rustls_config.alpn_protocols);
    
        // Convert into Quinn-compatible config
        let quic_config = QuicServerConfig::try_from(rustls_config)
            .map_err(|e| VqdError::Other(format!("QUIC config error: {e}")))?;

        Ok(ServerConfig::with_crypto(Arc::new(quic_config)))
    }
}

// Re-export for shared use
pub use bytes;
pub use anyhow;
pub use VqdError as Error;