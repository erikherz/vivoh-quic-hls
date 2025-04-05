// Copyright (C) 2025, Vivoh, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, is permitted only by Vivoh, Inc by License.
//

// ─── Standard Library ───────────────────────────────────────────────────────────
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::Duration;
use std::collections::{VecDeque, HashSet};
use std::fs;
use std::path::PathBuf;

// ─── External Crates ────────────────────────────────────────────────────────────
use quinn::{ClientConfig as QuinnClientConfig, Endpoint};
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info, warn};
use url::Url;
use web_transport_quinn::{Client, Session};
use clap::Parser;
use bytes::Bytes;
use regex::Regex;

// ─── Internal Crate ─────────────────────────────────────────────────────────────
use vivoh_quic_hls::{
    VqdError,
    WebTransportMediaPacket,
    serialize_media_packet,
};

const CONNECTION_RETRY_MAX: u32 = 5;
const CONNECTION_RETRY_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Parser, Clone)]
pub struct Args {
    /// Input folder containing HLS segments and playlists
    #[arg(short, long, conflicts_with = "pipe")]
    pub input: Option<PathBuf>,

    /// Read HLS segments from stdin pipe
    #[arg(long, conflicts_with = "input")]
    pub pipe: bool,

    /// Server URL to connect via WebTransport (e.g. https://your.domain.com)
    #[arg(short, long)]
    pub server: String,
}

#[tokio::main]
async fn main() -> Result<(), VqdError> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    // Validate input options
    if !args.pipe && args.input.is_none() {
        return Err(VqdError::Other(
            "Either --input or --pipe must be specified".to_string(),
        ));
    }

    if args.pipe {
        info!("Reading HLS data from stdin pipe");
    } else if let Some(input_dir) = &args.input {
        // Make sure the input directory exists
        if !input_dir.exists() || !input_dir.is_dir() {
            return Err(VqdError::Other(format!(
                "Input directory {} does not exist or is not a directory",
                input_dir.display()
            )));
        }
        info!("Starting publisher with input directory: {}", input_dir.display());
    }
    
    info!("Server URL: {}", args.server);

    // Initialize the crypto provider
    if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
        warn!("Crypto provider already installed or failed to install: {:?}", e);
    }

    // Create a channel for media packets with increased capacity for better buffering
    let (tx, _) = broadcast::channel(64);
    let connection_ready = Arc::new(AtomicBool::new(false));

    // Initialize the client with a clone of the sender
    let client = WebTransportClient::new(
        args.server.clone(),
        tx.clone(),
        connection_ready.clone(),
    )?;

    // Initialize the appropriate reader based on input type
    let reader_handle = if args.pipe {
        // Initialize the pipe reader
        let pipe_reader = PipeReader::new().await?;
        
        // Create a reader task that waits for the connection to be ready
        let connection_ready_for_reader = connection_ready.clone();
        let tx_for_reader = tx.clone();
        
        tokio::spawn(async move {
            // Wait for connection to be ready before starting to read
            info!("Pipe reader waiting for WebTransport connection to be established...");
            while !connection_ready_for_reader.load(Ordering::Relaxed) {
                sleep(Duration::from_millis(100)).await;
            }
            info!("Connection ready, starting to read piped HLS data");
            
            if let Err(e) = pipe_reader.start_reading(tx_for_reader).await {
                error!("Pipe reader error: {}", e);
            }
        })
    } else {
        // Initialize the hls reader with the directory input
        let hls_reader = HlsReader::new(args.input.unwrap().clone()).await?;
        
        // Create a reader task that waits for the connection to be ready
        let connection_ready_for_reader = connection_ready.clone();
        let tx_for_reader = tx.clone();
        
        tokio::spawn(async move {
            // Wait for connection to be ready before starting to read
            info!("HLS reader waiting for WebTransport connection to be established...");
            while !connection_ready_for_reader.load(Ordering::Relaxed) {
                sleep(Duration::from_millis(100)).await;
            }
            info!("Connection ready, starting to read HLS data");
            
            if let Err(e) = hls_reader.start_reading(tx_for_reader).await {
                error!("HLS reader error: {}", e);
            }
        })
    };

    // Initialize the client with a clone of the sender
    let client_handle = tokio::spawn(async move {
        if let Err(e) = client.run().await {
            error!("WebTransport client error: {}", e);
        }
    });

    // Wait for both tasks to complete
    tokio::select! {
        _ = client_handle => info!("WebTransport client task ended"),
        _ = reader_handle => info!("Reader task ended"),
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down");
        }
    }

    Ok(())
}

pub struct WebTransportClient {
    server_url: String,
    media_channel: broadcast::Sender<WebTransportMediaPacket>,
    connection_ready: Arc<AtomicBool>,
}

impl WebTransportClient {
    pub fn new(
        server_url: String,
        media_channel: broadcast::Sender<WebTransportMediaPacket>,
        connection_ready: Arc<AtomicBool>,
    ) -> Result<Self, VqdError> {
        Ok(Self {
            server_url,
            media_channel,
            connection_ready,
        })
    }

    pub async fn run(&self) -> Result<(), VqdError> {
        info!("Starting WebTransport client to publish to {}", self.server_url);

        if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
            warn!("Crypto provider already installed or failed to install: {:?}", e);
        }

        
        let mut retry_count = 0;

        loop {
            info!("Connecting to WebTransport server (attempt {})", retry_count + 1);

            match self.connect_and_publish().await {
                Ok(_) => {
                    info!("WebTransport client completed successfully");
                    break;
                }
                Err(error) => {
                    error!("WebTransport client error: {}", error);
                    self.connection_ready.store(false, Ordering::Relaxed);
                    retry_count += 1;

                    if retry_count >= CONNECTION_RETRY_MAX {
                        return Err(VqdError::Other(format!("Failed to connect after {} attempts", retry_count)));
                    }

                    info!("Retrying in {} seconds...", CONNECTION_RETRY_INTERVAL.as_secs());
                    tokio::time::sleep(CONNECTION_RETRY_INTERVAL).await;
                }
            }
        }

        Ok(())
    }

    fn build_client_config(&self) -> Result<QuinnClientConfig, VqdError> {
        let mut roots = RootCertStore::empty();
        for cert in load_native_certs().unwrap_or_default() {
            let _ = roots.add(cert);
        }

        let mut client_crypto = RustlsClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        client_crypto.alpn_protocols = vec![b"h3".to_vec()];

        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)
            .map_err(|e| VqdError::Other(format!("Failed to create crypto config: {}", e)))?;

        Ok(QuinnClientConfig::new(Arc::new(crypto)))
    }

    async fn connect_and_publish(&self) -> Result<(), VqdError> {
        self.connection_ready.store(false, Ordering::SeqCst);

        let mut url = Url::parse(&self.server_url)?;
        if !url.path().ends_with("/pub") {
            let mut path = url.path().to_string();
            if path.ends_with('/') {
                path.push_str("pub");
            } else {
                path.push_str("/pub");
            }
            url.set_path(&path);
        }

        info!("Final publisher URL: {}", url);

        let client_config = self.build_client_config()?;
        let mut endpoint = Endpoint::client("[::]:0".parse()?)?;
        endpoint.set_default_client_config(client_config.clone());

        let wt_client = Client::new(endpoint, client_config);
        info!("Connecting to WebTransport server at {}", url);

        let session: Session = match wt_client.connect(&url).await {
            Ok(s) => {
                info!("✅ WebTransport session established.");
                s
            }
            Err(e) => {
                error!("❌ Failed to connect: {}", e);
                return Err(e.into());
            }
        };
        
        self.connection_ready.store(true, Ordering::SeqCst);
        info!("Publisher connection ready");

        let mut receiver = self.media_channel.subscribe();

        while let Ok(media_packet) = receiver.recv().await {
            if !self.connection_ready.load(Ordering::SeqCst) {
                break;
            }

            let data = serialize_media_packet(&media_packet);

            match session.open_uni().await {
                Ok(mut stream) => {
                    if let Err(e) = stream.write_all(data.as_ref()).await {
                        error!("❌ Failed to send media packet: {}", e);
                    } else {
                        debug!("📤 Sent media packet #{} ({} bytes)", media_packet.packet_id, data.len());
                    }
                }
                Err(e) => {
                    error!("❌ Failed to open unidirectional stream: {}", e);
                    break;
                }
            }
        }

        session.close(0u32.into(), b"done");
        Ok(())
    }
}

fn read_file(path: PathBuf) -> Result<Bytes, VqdError> {
    match fs::read(path.clone()) {
        Ok(data) => Ok(Bytes::from(data)),
        Err(e) => {
            error!("Failed to read file {}: {}", path.display(), e);
            Err(VqdError::Io(e))
        }
    }
}

// Function to format a timestamp (in seconds) to HH:MM:SS.mmm format
fn format_timestamp(seconds: f64) -> String {
    let total_millis = (seconds * 1000.0).round() as u64;
    let h = total_millis / 3_600_000;
    let m = (total_millis % 3_600_000) / 60_000;
    let s = (total_millis % 60_000) / 1000;
    let ms = total_millis % 1000;
    format!("{:02}:{:02}:{:02}.{:03}", h, m, s, ms)
}

struct HlsReader {
    input_dir: PathBuf,
    packet_id: u32,
    seen_segments: HashSet<u32>,     
    seen_queue: VecDeque<u32>,       
    current_timestamp: f64,         
}

impl HlsReader {
    async fn new(input_dir: PathBuf) -> Result<Self, VqdError> {
        Ok(Self {
            input_dir,
            packet_id: 0,
            seen_segments: HashSet::new(),
            seen_queue: VecDeque::new(),
            current_timestamp: 0.0,
        })
    }

    async fn start_reading(mut self, tx: broadcast::Sender<WebTransportMediaPacket>) -> Result<(), VqdError> {
        const MAX_SEEN: usize = 10;
        
        loop {
            if tx.receiver_count() == 0 {
                warn!("No receivers listening, waiting...");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            
            // Now we don't need to ensure initialization segments are loaded
            // Scan for TS segments
            let mut segment_files = Vec::new();
            if let Ok(entries) = fs::read_dir(&self.input_dir) {
                for entry in entries.filter_map(Result::ok) {
                    let path = entry.path();
                    if path.extension().map_or(false, |ext| ext == "ts") {
                        segment_files.push(path);
                    }
                }
            }
            
            // Sort segments by name to process in order
            segment_files.sort();
            
            if segment_files.is_empty() {
                warn!("No TS segments found, waiting...");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            
            // Process each segment that we haven't seen yet
            for path in segment_files {
                let filename = path.file_name().unwrap_or_default().to_string_lossy().to_string();
                
                // Try to extract sequence number from filename
                let sequence_number = if let Some(captures) = Regex::new(r"(\d+)\.ts")
                    .ok()
                    .and_then(|re| re.captures(&filename)) {
                    if let Some(num_str) = captures.get(1) {
                        num_str.as_str().parse::<u32>().unwrap_or(0)
                    } else {
                        0
                    }
                } else {
                    0
                };
                
                if self.seen_segments.contains(&sequence_number) {
                    continue;
                }
                
                // Add to tracking collections
                self.seen_queue.push_back(sequence_number);
                self.seen_segments.insert(sequence_number);
                
                // Maintain a reasonable collection size
                if self.seen_queue.len() > MAX_SEEN {
                    if let Some(old) = self.seen_queue.pop_front() {
                        self.seen_segments.remove(&old);
                    }
                }
                
                // Read the segment data
                let segment_data = match read_file(path.clone()) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to read TS segment: {}", e);
                        continue;
                    }
                };
                
                // Try to find the segment duration from the playlist - using fixed 2.0 as default
                let duration = 2.0; // Default duration in seconds
                
                let timestamp = (self.current_timestamp * 30_000.0) as u64;
                let duration_ticks = (duration * 30_000.0) as u32;
                
                let wmp = WebTransportMediaPacket {
                    packet_id: self.packet_id,
                    timestamp,
                    duration: duration_ticks,
                    segment_duration: duration as f32,
                    av_data: segment_data,
                };
                
                info!("WMP Packet Summary:");
                info!("  packet_id        --> {}", wmp.packet_id);
                info!("  timestamp        --> {} (raw: {})", 
                      format_timestamp(self.current_timestamp), wmp.timestamp);
                info!("  duration         --> {} ticks", wmp.duration);
                info!("  segment_duration --> {} seconds", wmp.segment_duration);
                info!("  av_data          --> {} bytes", wmp.av_data.len());
                
                // Send the packet to the channel
                if let Err(e) = tx.send(wmp) {
                    error!("Failed to send WMP to channel: {}", e);
                }
                
                self.current_timestamp += duration;
                self.packet_id += 1;
            }
            
            // Sleep to avoid consuming too much CPU
            sleep(Duration::from_secs(1)).await;
        }
    }
}

struct PipeReader {
    packet_id: u32,
    current_timestamp: f64,
    seen_segments: HashSet<u32>,
    seen_queue: VecDeque<u32>,
}

impl PipeReader {
    async fn new() -> Result<Self, VqdError> {
        Ok(Self {
            packet_id: 0,
            current_timestamp: 0.0,
            seen_segments: HashSet::new(),
            seen_queue: VecDeque::new(),
        })
    }

    async fn start_reading(mut self, tx: broadcast::Sender<WebTransportMediaPacket>) -> Result<(), VqdError> {
        const MAX_SEEN: usize = 10;
        
        // Create a buffered reader for stdin
        let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
        
        info!("Started pipe reader, waiting for data on stdin...");
        
        loop {
            if tx.receiver_count() == 0 {
                warn!("No receivers listening, waiting...");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            
            // Read a TS packet from stdin
            // You would define a protocol for how data is read from stdin
            // This is a simplified example
            let mut header_buf = [0u8; 8];
            match stdin.read_exact(&mut header_buf).await {
                Ok(_) => {
                    // Continue processing - got header
                },
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        // End of input
                        warn!("End of stdin input reached");
                        break;
                    } else {
                        error!("Error reading from stdin: {}", e);
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
            
            // Check header magic
            if &header_buf != b"TSPACKET" {
                error!("Invalid packet header: expected 'TSPACKET'");
                continue;
            }
            
            // Read packet size (4 bytes)
            let mut size_buf = [0u8; 4];
            if let Err(e) = stdin.read_exact(&mut size_buf).await {
                error!("Failed to read packet size: {}", e);
                continue;
            }
            
            let packet_size = u32::from_be_bytes(size_buf) as usize;
            
            // Read the packet data
            let mut packet_data = vec![0u8; packet_size];
            if let Err(e) = stdin.read_exact(&mut packet_data).await {
                error!("Failed to read packet data: {}", e);
                continue;
            }
            
            // Generate a sequence number based on packet_id
            let sequence_number = self.packet_id;
            
            if self.seen_segments.contains(&sequence_number) {
                continue;
            }
            
            // Add to tracking collections
            self.seen_queue.push_back(sequence_number);
            self.seen_segments.insert(sequence_number);
            
            // Maintain a reasonable collection size
            if self.seen_queue.len() > MAX_SEEN {
                if let Some(old) = self.seen_queue.pop_front() {
                    self.seen_segments.remove(&old);
                }
            }
            
            // Use fixed duration as an example
            let duration = 2.0; // Default duration in seconds
            
            let timestamp = (self.current_timestamp * 30_000.0) as u64;
            let duration_ticks = (duration * 30_000.0) as u32;
            
            let segment_data = Bytes::from(packet_data);
            
            let wmp = WebTransportMediaPacket {
                packet_id: self.packet_id,
                timestamp,
                duration: duration_ticks,
                segment_duration: duration as f32,
                av_data: segment_data,
            };
            
            info!("WMP Packet Summary:");
            info!("  packet_id        --> {}", wmp.packet_id);
            info!("  timestamp        --> {} (raw: {})", 
                  format_timestamp(self.current_timestamp), wmp.timestamp);
            info!("  duration         --> {} ticks", wmp.duration);
            info!("  segment_duration --> {} seconds", wmp.segment_duration);
            info!("  av_data          --> {} bytes", wmp.av_data.len());
            
            // Send the packet to the channel
            if let Err(e) = tx.send(wmp) {
                error!("Failed to send WMP to channel: {}", e);
            }
            
            self.current_timestamp += duration;
            self.packet_id += 1;
        }
        
        Ok(())
    }
}