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
use std::io::Read;

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

// Function to parse segment duration from an HLS playlist line
fn parse_duration(line: &str) -> Option<f64> {
    let re = Regex::new(r"#EXTINF:(\d+\.\d+),").ok()?;
    re.captures(line).and_then(|cap| {
        cap.get(1).and_then(|m| m.as_str().parse::<f64>().ok())
    })
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
    audio_init: Option<Bytes>,
    video_init: Option<Bytes>,
    packet_id: u32,
    seen_segments: HashSet<(String, u32)>, // (variant, sequence)
    seen_queue: VecDeque<(String, u32)>,   // (variant, sequence) 
    current_timestamp: f64,               // Current timestamp in seconds
}

impl HlsReader {
    async fn new(input_dir: PathBuf) -> Result<Self, VqdError> {
        Ok(Self {
            input_dir,
            audio_init: None,
            video_init: None,
            packet_id: 0,
            seen_segments: HashSet::new(),
            seen_queue: VecDeque::new(),
            current_timestamp: 0.0,
        })
    }

    async fn ensure_init_segments_loaded(&mut self) -> Result<bool, VqdError> {
        // For HLS with fMP4, initialization segments are specified by the EXT-X-MAP tag
        // We'll first try to find and parse the master playlist
        let master_playlist_path = self.find_master_playlist()?;
        
        // If we already have the init segments, no need to continue
        if self.audio_init.is_some() && self.video_init.is_some() {
            return Ok(true);
        }
        
        // Try to find the variant playlists from the master playlist
        let master_playlist_content = match read_file(master_playlist_path) {
            Ok(data) => {
                match std::str::from_utf8(&data) {
                    Ok(content) => content.to_string(),
                    Err(e) => {
                        error!("Failed to decode master playlist as UTF-8: {}", e);
                        return Ok(false);
                    }
                }
            },
            Err(e) => {
                error!("Failed to read master playlist: {}", e);
                return Ok(false);
            }
        };
        
        let variant_playlists = self.parse_master_playlist(&master_playlist_content)?;
        if variant_playlists.is_empty() {
            warn!("No variant playlists found in master playlist");
            return Ok(false);
        }
        
        // Look for initialization segments in variant playlists
        let mut audio_init_found = false;
        let mut video_init_found = false;
        
        for variant in &variant_playlists {
            let is_audio = variant.to_lowercase().contains("audio");
            let variant_path = self.input_dir.join(variant);
            
            if !variant_path.exists() {
                continue;
            }
            
            let variant_content = match read_file(variant_path) {
                Ok(data) => {
                    match std::str::from_utf8(&data) {
                        Ok(content) => content.to_string(),
                        Err(e) => {
                            error!("Failed to decode variant playlist as UTF-8: {}", e);
                            continue;
                        }
                    }
                },
                Err(e) => {
                    error!("Failed to read variant playlist: {}", e);
                    continue;
                }
            };
            
            // Search for EXT-X-MAP tag which specifies the initialization segment
            if let Some(map_uri) = self.extract_map_uri_from_playlist(&variant_content) {
                info!("Found EXT-X-MAP tag: {}", map_uri);
                
                // Extract byterange if present
                let byterange = self.extract_map_byterange_from_playlist(&variant_content);
                
                // Resolve the URI relative to the input directory
                let init_path = self.input_dir.join(&map_uri);
                
                if init_path.exists() {
                    match read_file(init_path) {
                        Ok(data) => {
                            let init_data = if let Some((offset, length)) = byterange {
                                // Apply byterange if specified
                                if offset + length <= data.len() {
                                    Bytes::copy_from_slice(&data[offset..offset+length])
                                } else {
                                    error!("Byterange ({}, {}) exceeds init segment size {}", 
                                          offset, length, data.len());
                                    continue;
                                }
                            } else {
                                data
                            };
                            
                            if is_audio {
                                self.audio_init = Some(init_data.clone());
                                audio_init_found = true;
                                info!("Found audio initialization segment: {} bytes", init_data.len());
                            } else {
                                self.video_init = Some(init_data.clone());
                                video_init_found = true;
                                info!("Found video initialization segment: {} bytes", init_data.len());
                            }
                        },
                        Err(e) => {
                            error!("Failed to read initialization segment: {}", e);
                            continue;
                        }
                    }
                } else {
                    warn!("Initialization segment not found: {}", init_path.display());
                }
            }
        }
        
        // If we didn't find the initialization segments, we have a few fallbacks
        
        // 1. Try some common init segment filenames
        if !audio_init_found {
            let common_audio_inits = [
                "init-audio.mp4",
                "audio/init.mp4",
                "audio_init.mp4",
                "init-1.mp4"
            ];
            
            for name in &common_audio_inits {
                let path = self.input_dir.join(name);
                if path.exists() {
                    match read_file(path) {
                        Ok(data) => {
                            self.audio_init = Some(data.clone());
                            audio_init_found = true;
                            info!("Found audio initialization segment at {}: {} bytes", name, data.len());
                            break;
                        },
                        Err(e) => {
                            error!("Failed to read audio init file {}: {}", name, e);
                        }
                    }
                }
            }
        }
        
        if !video_init_found {
            let common_video_inits = [
                "init-video.mp4",
                "video/init.mp4",
                "video_init.mp4",
                "init-0.mp4"
            ];
            
            for name in &common_video_inits {
                let path = self.input_dir.join(name);
                if path.exists() {
                    match read_file(path) {
                        Ok(data) => {
                            self.video_init = Some(data.clone());
                            video_init_found = true;
                            info!("Found video initialization segment at {}: {} bytes", name, data.len());
                            break;
                        },
                        Err(e) => {
                            error!("Failed to read video init file {}: {}", name, e);
                        }
                    }
                }
            }
        }
        
        // 2. If we still don't have init segments, try to extract them from the first segments
        if !audio_init_found || !video_init_found {
            warn!("Some initialization segments not found. Will attempt to extract from first segments.");
            if let Err(e) = self.extract_init_from_segments().await {
                error!("Failed to extract initialization segments from media segments: {}", e);
                return Ok(false);
            }
        }
        
        // Check if we have what we need
        Ok(self.audio_init.is_some() && self.video_init.is_some())
    }
    
    fn extract_map_uri_from_playlist(&self, playlist: &str) -> Option<String> {
        // Search for EXT-X-MAP tag
        for line in playlist.lines() {
            let line = line.trim();
            if line.starts_with("#EXT-X-MAP:") {
                // Parse the URI attribute
                if let Some(uri_start) = line.find("URI=\"") {
                    let uri_content_start = uri_start + 5; // Length of URI="
                    if let Some(uri_end) = line[uri_content_start..].find('\"') {
                        return Some(line[uri_content_start..uri_content_start + uri_end].to_string());
                    }
                }
            }
        }
        None
    }
    
    fn extract_map_byterange_from_playlist(&self, playlist: &str) -> Option<(usize, usize)> {
        // Search for EXT-X-MAP tag with BYTERANGE attribute
        for line in playlist.lines() {
            let line = line.trim();
            if line.starts_with("#EXT-X-MAP:") && line.contains("BYTERANGE") {
                // Parse the BYTERANGE attribute
                if let Some(byterange_start) = line.find("BYTERANGE=\"") {
                    let byterange_content_start = byterange_start + 10; // Length of BYTERANGE="
                    if let Some(byterange_end) = line[byterange_content_start..].find('\"') {
                        let byterange = &line[byterange_content_start..byterange_content_start + byterange_end];
                        
                        // BYTERANGE format is "length[@offset]"
                        if let Some(at_pos) = byterange.find('@') {
                            let length_str = &byterange[0..at_pos];
                            let offset_str = &byterange[at_pos+1..];
                            
                            if let (Ok(length), Ok(offset)) = (length_str.parse::<usize>(), offset_str.parse::<usize>()) {
                                return Some((offset, length));
                            }
                        } else {
                            // If no offset is specified, offset is 0
                            if let Ok(length) = byterange.parse::<usize>() {
                                return Some((0, length));
                            }
                        }
                    }
                }
            }
        }
        None
    }
    
    async fn extract_init_from_segments(&mut self) -> Result<bool, VqdError> {
        // This function extracts initialization data from media segments for HLS fMP4
        // For each segment, we look for the 'ftyp' and 'moov' boxes that form the init section
        
        // First, properly find and parse all playlists
        let master_playlist_path = self.find_master_playlist()?;
        
        // Read the master playlist
        let master_content = match read_file(master_playlist_path.clone()) {
            Ok(data) => {
                let content_str = match std::str::from_utf8(&data) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to decode master playlist as UTF-8: {}", e);
                        return Err(VqdError::Other(format!("Failed to decode master playlist as UTF-8: {}", e)));
                    }
                };
                
                content_str.to_string()
            },
            Err(e) => {
                error!("Failed to read master playlist: {}", e);
                return Err(VqdError::Other(format!("Failed to read master playlist: {}", e)));
            }
        };
        
        // Parse the master playlist to find variant playlists
        let variant_playlists = self.parse_master_playlist(&master_content)?;
        
        if variant_playlists.is_empty() {
            return Err(VqdError::Other("No variant playlists found in master playlist".to_string()));
        }
        
        // Track if we've extracted initialization data
        let mut audio_init_extracted = false;
        let mut video_init_extracted = false;
        
        // First try to find segments through the variant playlists
        for variant in &variant_playlists {
            let is_audio = variant.to_lowercase().contains("audio");
            let variant_path = self.input_dir.join(variant);
            
            if !variant_path.exists() {
                continue;
            }
            
            info!("Examining variant playlist: {}", variant);
            
            // Read the variant playlist
            let variant_content = match read_file(variant_path) {
                Ok(data) => {
                    let content_str = match std::str::from_utf8(&data) {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to decode variant playlist as UTF-8: {}", e);
                            continue;
                        }
                    };
                    
                    content_str.to_string()
                },
                Err(e) => {
                    error!("Failed to read variant playlist: {}", e);
                    continue;
                }
            };
            
            // Parse the variant playlist to find the first segment
            let segments = match self.parse_variant_playlist(variant, &variant_content) {
                Ok(segs) => segs,
                Err(e) => {
                    error!("Failed to parse variant playlist: {}", e);
                    continue;
                }
            };
            
            if segments.is_empty() {
                debug!("No segments found in variant playlist: {}", variant);
                continue;
            }
            
            // Get the first segment from this variant
            let (_, first_segment, _) = &segments[0];
            let segment_path = self.input_dir.join(first_segment);
            
            info!("Attempting to extract initialization data from {}", segment_path.display());
            
            if segment_path.exists() {
                match read_file(segment_path.clone()) {
                    Ok(data) => {
                        // Try to extract initialization data
                        if let Some(init_data) = self.extract_mp4_init_section(&data) {
                            if is_audio && !audio_init_extracted {
                                self.audio_init = Some(init_data.clone());
                                audio_init_extracted = true;
                                info!("Successfully extracted audio initialization data: {} bytes", init_data.len());
                            } else if !is_audio && !video_init_extracted {
                                self.video_init = Some(init_data.clone());
                                video_init_extracted = true;
                                info!("Successfully extracted video initialization data: {} bytes", init_data.len());
                            }
                        } else {
                            debug!("Failed to extract initialization data from {}", segment_path.display());
                        }
                    },
                    Err(e) => {
                        error!("Failed to read segment file: {}", e);
                    }
                }
            }
        }
        
        // If we couldn't find segments through playlists, try a direct search in the directory
        if !audio_init_extracted || !video_init_extracted {
            debug!("Fallback: searching directory for media segments");
            
            // Look for segments with common naming patterns
            let segment_patterns = [
                ("*.m4s", 10),    // Common fMP4 extension with limit of 10 files
                ("*.mp4", 10),    // Standard MP4 extension
                ("segment*.m4s", 5),
                ("chunk*.m4s", 5),
                ("media*.m4s", 5),
                ("*.ts", 10)      // Also check for TS segments as a last resort
            ];
            
            for (pattern, limit) in &segment_patterns {
                if audio_init_extracted && video_init_extracted {
                    break;
                }
                
                let glob_pattern = format!("{}/{}", self.input_dir.display(), pattern);
                
                match glob::glob(&glob_pattern) {
                    Ok(paths) => {
                        let mut count = 0;
                        
                        for entry in paths {
                            if count >= *limit {
                                break;
                            }
                            
                            if let Ok(path) = entry {
                                let filename = path.file_name().unwrap_or_default().to_string_lossy();
                                let is_audio = filename.to_lowercase().contains("audio");
                                
                                if (is_audio && audio_init_extracted) || (!is_audio && video_init_extracted) {
                                    continue;
                                }
                                
                                match read_file(path.clone()) {
                                    Ok(data) => {
                                        // For MP4/m4s files, try to extract initialization section
                                        if path.extension().map_or(false, |ext| ext == "m4s" || ext == "mp4") {
                                            if let Some(init_data) = self.extract_mp4_init_section(&data) {
                                                if is_audio && !audio_init_extracted {
                                                    self.audio_init = Some(init_data.clone());
                                                    audio_init_extracted = true;
                                                    info!("Successfully extracted audio initialization data from {}: {} bytes", 
                                                        path.display(), init_data.len());
                                                } else if !is_audio && !video_init_extracted {
                                                    self.video_init = Some(init_data.clone());
                                                    video_init_extracted = true;
                                                    info!("Successfully extracted video initialization data from {}: {} bytes", 
                                                        path.display(), init_data.len());
                                                }
                                            }
                                        }
                                        // For TS files, try to extract PAT/PMT
                                        else if path.extension().map_or(false, |ext| ext == "ts") {
                                            if let Some(init_data) = self.extract_ts_init_section(&data) {
                                                if is_audio && !audio_init_extracted {
                                                    self.audio_init = Some(init_data.clone());
                                                    audio_init_extracted = true;
                                                    info!("Successfully extracted audio TS initialization data from {}: {} bytes", 
                                                        path.display(), init_data.len());
                                                } else if !is_audio && !video_init_extracted {
                                                    self.video_init = Some(init_data.clone());
                                                    video_init_extracted = true;
                                                    info!("Successfully extracted video TS initialization data from {}: {} bytes", 
                                                        path.display(), init_data.len());
                                                }
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        error!("Failed to read segment file {}: {}", path.display(), e);
                                    }
                                }
                                
                                count += 1;
                            }
                        }
                    },
                    Err(e) => {
                        error!("Failed to glob pattern {}: {}", glob_pattern, e);
                    }
                }
            }
        }
        
        // If we still couldn't extract initialization data, create empty placeholders and warn
        if !audio_init_extracted {
            warn!("Could not extract audio initialization data, using empty placeholder");
            self.audio_init = Some(Bytes::from(Vec::new()));
        }
        
        if !video_init_extracted {
            warn!("Could not extract video initialization data, using empty placeholder");
            self.video_init = Some(Bytes::from(Vec::new()));
        }
        
        Ok(true)
    }
    
    fn extract_mp4_init_section(&self, data: &Bytes) -> Option<Bytes> {
        // Extract the initialization section from an fMP4 segment
        // According to the HLS spec, this includes the 'ftyp' and 'moov' boxes
        
        let mut pos = 0;
        let mut ftyp_pos = None;
        let mut moov_end = None;
        
        // Parse MP4 box structure
        while pos + 8 <= data.len() {
            let size = u32::from_be_bytes([data[pos], data[pos+1], data[pos+2], data[pos+3]]) as usize;
            if size == 0 || size < 8 || pos + size > data.len() {
                // Invalid box size
                break;
            }
            
            let box_type = &data[pos+4..pos+8];
            
            // Look for ftyp box (should be first)
            if box_type == b"ftyp" && ftyp_pos.is_none() {
                ftyp_pos = Some(pos);
            }
            // Look for moov box
            else if box_type == b"moov" {
                moov_end = Some(pos + size);
                // The moov box should come after ftyp, so we've found what we need
                break;
            }
            // If we encounter a moof box before moov, this is not an initialization segment
            else if box_type == b"moof" || box_type == b"mdat" {
                // These boxes shouldn't appear in an initialization segment
                if ftyp_pos.is_some() {
                    debug!("Found moof/mdat box before moov, this appears to be a media segment, not an init segment");
                }
                break;
            }
            
            pos += size;
        }
        
        // If we found both 'ftyp' and 'moov', extract the initialization section
        if let (Some(start), Some(end)) = (ftyp_pos, moov_end) {
            if end <= data.len() {
                debug!("Extracted MP4 initialization section: {} bytes", end - start);
                return Some(Bytes::copy_from_slice(&data[start..end]));
            }
        } else if let Some(_start) = ftyp_pos {
            debug!("Found ftyp but no moov box");
        }
        
        None
    }
    
    fn extract_ts_init_section(&self, data: &Bytes) -> Option<Bytes> {
        // For HLS with Transport Streams, the initialization section consists of
        // a Program Association Table (PAT) followed by a Program Map Table (PMT)
        
        // This is a simplified implementation
        // A proper implementation would parse the TS packet structure and extract the PAT/PMT
        
        const TS_PACKET_SIZE: usize = 188;
        
        // Check if we have at least two TS packets
        if data.len() < TS_PACKET_SIZE * 2 {
            return None;
        }
        
        // Verify this is a valid TS packet (starts with sync byte 0x47)
        if data[0] != 0x47 || data[TS_PACKET_SIZE] != 0x47 {
            return None;
        }
        
        // Extract the first two packets as initialization data
        // In a real implementation, you'd verify these are PAT and PMT packets
        Some(Bytes::copy_from_slice(&data[0..TS_PACKET_SIZE*2]))
    }

    async fn start_reading(mut self, tx: broadcast::Sender<WebTransportMediaPacket>) -> Result<(), VqdError> {
        const MAX_SEEN: usize = 10;
        
        loop {
            if tx.receiver_count() == 0 {
                warn!("No receivers listening, waiting...");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            
            // Ensure init segments are loaded
            if !self.ensure_init_segments_loaded().await? {
                warn!("Waiting for initialization segments...");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            
            // Find available segment numbers for HLS
            // For HLS files, they typically follow a pattern like:
            // "chunk-0-00001.m4s" (video segment 1)
            // "chunk-1-00001.m4s" (audio segment 1)
            let mut segment_numbers = Vec::new();
            
            // Let's scan the directory for segment files
            if let Ok(entries) = fs::read_dir(&self.input_dir) {
                let mut audio_segments = HashSet::new();
                let mut video_segments = HashSet::new();
                
                for entry in entries.filter_map(Result::ok) {
                    let filename = entry.file_name().to_string_lossy().to_string();
                    
                    // Check if this is an audio segment (assuming chunk-1-XXXXX.m4s pattern)
                    if let Some(captures) = Regex::new(r"chunk-1-(\d+)\.m4s")
                        .ok()
                        .and_then(|re| re.captures(&filename)) {
                        if let Some(num_str) = captures.get(1) {
                            if let Ok(num) = num_str.as_str().parse::<u32>() {
                                audio_segments.insert(num);
                            }
                        }
                    }
                    
                    // Check if this is a video segment (assuming chunk-0-XXXXX.m4s pattern)
                    if let Some(captures) = Regex::new(r"chunk-0-(\d+)\.m4s")
                        .ok()
                        .and_then(|re| re.captures(&filename)) {
                        if let Some(num_str) = captures.get(1) {
                            if let Ok(num) = num_str.as_str().parse::<u32>() {
                                video_segments.insert(num);
                            }
                        }
                    }
                }
                
                // Find segments that have both audio and video
                for num in audio_segments.iter() {
                    if video_segments.contains(num) {
                        segment_numbers.push(*num);
                    }
                }
                
                // Sort segment numbers for consistent processing
                segment_numbers.sort();
            }
            
            if segment_numbers.is_empty() {
                warn!("No common audio/video segments found, waiting...");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            
            // Process each segment number that we haven't seen yet
            for &number in &segment_numbers {
                if self.seen_segments.contains(&("default".to_string(), number)) {
                    continue;
                }
                
                // Add to tracking collections
                self.seen_queue.push_back(("default".to_string(), number));
                self.seen_segments.insert(("default".to_string(), number));
                
                // Maintain a reasonable collection size
                if self.seen_queue.len() > MAX_SEEN {
                    if let Some(old) = self.seen_queue.pop_front() {
                        self.seen_segments.remove(&old);
                    }
                }
                
                // Get the audio and video segment files
                let audio_path = self.input_dir.join(format!("chunk-1-{:05}.m4s", number));
                let video_path = self.input_dir.join(format!("chunk-0-{:05}.m4s", number));
                
                if !audio_path.exists() || !video_path.exists() {
                    warn!("Incomplete segment pair for segment {}: audio exists: {}, video exists: {}", 
                        number, audio_path.exists(), video_path.exists());
                    continue;
                }
                
                // Read the segment data
                let audio_data = match read_file(audio_path) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to read audio segment: {}", e);
                        continue;
                    }
                };
                
                let video_data = match read_file(video_path) {
                    Ok(data) => data,
                    Err(e) => {
                        error!("Failed to read video segment: {}", e);
                        continue;
                    }
                };
                
                // For simplicity, we'll use a fixed duration
                // A better approach would be to extract duration from the segment or playlist
                let duration = 2.0; // Default duration in seconds
                
                // Calculate timestamp and duration
                let timestamp = (self.current_timestamp * 30_000.0) as u64;
                let duration_ticks = (duration * 30_000.0) as u32;
                
                let wmp = WebTransportMediaPacket {
                    packet_id: self.packet_id,
                    timestamp,
                    duration: duration_ticks,
                    segment_duration: duration as f32,
                    audio_init: self.audio_init.clone().unwrap(),
                    video_init: self.video_init.clone().unwrap(),
                    audio_data,
                    video_data,
                };
                
                info!("WMP Packet Summary:");
                info!("  packet_id        --> {}", wmp.packet_id);
                info!("  timestamp        --> {} (raw: {})", 
                      format_timestamp(self.current_timestamp), wmp.timestamp);
                info!("  duration         --> {} ticks", wmp.duration);
                info!("  segment_duration --> {} seconds", wmp.segment_duration);
                info!("  audio_init       --> {} bytes", wmp.audio_init.len());
                info!("  video_init       --> {} bytes", wmp.video_init.len());
                info!("  audio_data       --> {} bytes", wmp.audio_data.len());
                info!("  video_data       --> {} bytes", wmp.video_data.len());
                
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

    fn find_master_playlist(&self) -> Result<PathBuf, VqdError> {
        // Common names for master playlists
        let possible_names = [
            "master.m3u8",
            "playlist.m3u8",
            "index.m3u8",
            "stream.m3u8",
        ];
        
        for name in &possible_names {
            let path = self.input_dir.join(name);
            if path.exists() {
                return Ok(path);
            }
        }
        
        // If no standard name is found, look for any .m3u8 file
        for entry in fs::read_dir(&self.input_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "m3u8") {
                // Read a bit of the file to see if it looks like a master playlist
                let mut file = fs::File::open(&path)?;
                let mut buffer = [0; 100]; // Read first 100 bytes
                let n = file.read(&mut buffer)?;
                let content = String::from_utf8_lossy(&buffer[..n]);
                
                // Check if it contains variant streams (#EXT-X-STREAM-INF)
                if content.contains("#EXT-X-STREAM-INF") {
                    return Ok(path);
                }
            }
        }
        
        Err(VqdError::Other("No master playlist found".to_string()))
    }
    
    fn parse_master_playlist(&self, playlist: &str) -> Result<Vec<String>, VqdError> {
        let mut variants = Vec::new();
        
        for line in playlist.lines() {
            if line.starts_with('#') {
                continue;  // Skip comments and tags for now
            }
            
            if !line.is_empty() && !line.starts_with('#') {
                // This is a URI to a variant playlist
                variants.push(line.trim().to_string());
            }
        }
        
        Ok(variants)
    }
    
    fn parse_variant_playlist(&self, _variant: &str, playlist: &str) -> Result<Vec<(u32, String, f64)>, VqdError> {
        let mut segments = Vec::new();
        let mut current_duration = 0.0;
        let mut media_sequence = 0;
        let mut in_segment = false;

        // First, check if there's a #EXT-X-MEDIA-SEQUENCE tag to get the starting sequence number
        for line in playlist.lines() {
            let line = line.trim();
            if line.starts_with("#EXT-X-MEDIA-SEQUENCE:") {
                if let Some(seq_str) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
                    if let Ok(seq) = seq_str.trim().parse::<u32>() {
                        media_sequence = seq;
                        break;
                    }
                }
            }
        }
        
        let mut sequence_number = media_sequence;
        
        // Now parse the segments
        for line in playlist.lines() {
            let line = line.trim();
            
            // Skip empty lines
            if line.is_empty() {
                continue;
            }
            
            // Parse duration from EXTINF line
            if line.starts_with("#EXTINF:") {
                if let Some(dur) = parse_duration(line) {
                    current_duration = dur;
                    in_segment = true;
                }
            }
            // Skip other tags
            else if line.starts_with('#') {
                continue;
            }
            // This is a segment URI
            else if in_segment {
                let mut segment_file = line.to_string();
                
                // If segment contains a slash, get just the filename
                if let Some(pos) = segment_file.rfind('/') {
                    segment_file = segment_file[pos + 1..].to_string();
                }
                
                segments.push((sequence_number, segment_file.clone(), current_duration));
                sequence_number += 1;
                in_segment = false;
            }
        }
        
        Ok(segments)
    }
}

struct PipeReader {
    audio_init: Option<Bytes>,
    video_init: Option<Bytes>,
    packet_id: u32,
    current_timestamp: f64,
    seen_segments: HashSet<u32>,
    seen_queue: VecDeque<u32>,
}

impl PipeReader {
    async fn new() -> Result<Self, VqdError> {
        Ok(Self {
            audio_init: None,
            video_init: None,
            packet_id: 0,
            current_timestamp: 0.0,
            seen_segments: HashSet::new(),
            seen_queue: VecDeque::new(),
        })
    }

    async fn start_reading(mut self, tx: broadcast::Sender<WebTransportMediaPacket>) -> Result<(), VqdError> {
        const MAX_SEEN: usize = 10;
        let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
        
        info!("Starting pipe reader, waiting for data on stdin...");
        
        // First, try to read initialization segments
        while self.audio_init.is_none() || self.video_init.is_none() {
            match self.read_init_segments_from_stdin(&mut stdin).await {
                Ok((audio_init, video_init)) => {
                    info!("Successfully read initialization segments from stdin");
                    info!("Audio init: {} bytes, Video init: {} bytes", 
                           audio_init.len(), video_init.len());
                    self.audio_init = Some(audio_init);
                    self.video_init = Some(video_init);
                    break;
                },
                Err(e) => {
                    warn!("Waiting for initialization segments: {}", e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        
        // Main loop for reading media segments
        let mut audio_data: Option<Bytes> = None;
        let mut video_data: Option<Bytes> = None;
        let mut sequence_counter: u32 = 0;
        
        loop {
            if tx.receiver_count() == 0 {
                warn!("No receivers listening, waiting...");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            
            // Try to read a media segment
            match self.read_media_segment_from_stdin(&mut stdin).await {
                Ok((segment_type, segment_data, duration)) => {
                    // Store the segment data based on type
                    if segment_type == "audio" {
                        audio_data = Some(segment_data);
                    } else {
                        video_data = Some(segment_data);
                    }
                    
                    // If we have both audio and video data, create and send a media packet
                    if let (Some(audio), Some(video)) = (&audio_data, &video_data) {
                        sequence_counter += 1;
                        
                        // Only process segments we haven't seen before
                        if self.seen_segments.contains(&sequence_counter) {
                            continue;
                        }
                        
                        // Add to tracking collections
                        self.seen_queue.push_back(sequence_counter);
                        self.seen_segments.insert(sequence_counter);
                        
                        // Maintain a reasonable collection size
                        if self.seen_queue.len() > MAX_SEEN {
                            if let Some(old) = self.seen_queue.pop_front() {
                                self.seen_segments.remove(&old);
                            }
                        }
                        
                        // Calculate timestamp and duration
                        let timestamp = (self.current_timestamp * 30_000.0) as u64;
                        let duration_ticks = (duration * 30_000.0) as u32;
                        
                        let wmp = WebTransportMediaPacket {
                            packet_id: self.packet_id,
                            timestamp,
                            duration: duration_ticks,
                            segment_duration: duration as f32,
                            audio_init: self.audio_init.clone().unwrap(),
                            video_init: self.video_init.clone().unwrap(),
                            audio_data: audio.clone(),
                            video_data: video.clone(),
                        };
                        
                        info!("WMP Packet Summary:");
                        info!("  packet_id        --> {}", wmp.packet_id);
                        info!("  timestamp        --> {} (raw: {})", 
                              format_timestamp(self.current_timestamp), wmp.timestamp);
                        info!("  duration         --> {} ticks", wmp.duration);
                        info!("  segment_duration --> {} seconds", wmp.segment_duration);
                        info!("  audio_init       --> {} bytes", wmp.audio_init.len());
                        info!("  video_init       --> {} bytes", wmp.video_init.len());
                        info!("  audio_data       --> {} bytes", wmp.audio_data.len());
                        info!("  video_data       --> {} bytes", wmp.video_data.len());
                        
                        // Send the packet to the channel
                        if let Err(e) = tx.send(wmp) {
                            error!("Failed to send WMP to channel: {}", e);
                        }
                        
                        self.current_timestamp += duration as f64;
                        self.packet_id += 1;
                        
                        // Clear the buffers for the next pair of segments
                        audio_data = None;
                        video_data = None;
                    }
                },
                Err(e) => {
                    error!("Error reading media segment: {}", e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
            
            // Brief sleep to avoid consuming too much CPU
            sleep(Duration::from_millis(10)).await;
        }
    }

    // Helper method to read initialization segments from stdin
    async fn read_init_segments_from_stdin(
        &self, 
        reader: &mut tokio::io::BufReader<tokio::io::Stdin>
    ) -> Result<(Bytes, Bytes), VqdError> {
        // For pipe input, we need a protocol to distinguish between different data types
        // This could be a simple header or marker bytes
        
        // Read a marker or header that indicates init segments are coming
        let mut header = [0u8; 8];
        reader.read_exact(&mut header).await?;
        
        // Validate the marker/header - this is an example, adapt to your protocol
        if &header != b"INIT_SEG" {
            return Err(VqdError::Other("Invalid initialization segment marker".to_string()));
        }
        
        // Read audio init segment size (4 bytes)
        let mut size_buf = [0u8; 4];
        reader.read_exact(&mut size_buf).await?;
        let audio_init_size = u32::from_be_bytes(size_buf) as usize;
        
        // Read audio init segment data
        let mut audio_init_data = vec![0u8; audio_init_size];
        reader.read_exact(&mut audio_init_data).await?;
        
        // Read video init segment size (4 bytes)
        reader.read_exact(&mut size_buf).await?;
        let video_init_size = u32::from_be_bytes(size_buf) as usize;
        
        // Read video init segment data
        let mut video_init_data = vec![0u8; video_init_size];
        reader.read_exact(&mut video_init_data).await?;
        
        Ok((Bytes::from(audio_init_data), Bytes::from(video_init_data)))
    }
    
    // Helper method to read a media segment from stdin
    async fn read_media_segment_from_stdin(
        &self,
        reader: &mut tokio::io::BufReader<tokio::io::Stdin>
    ) -> Result<(String, Bytes, f64), VqdError> {
        // Read a marker or header that indicates a media segment is coming
        let mut header = [0u8; 8];
        reader.read_exact(&mut header).await?;
        
        // Validate the marker/header - this is an example, adapt to your protocol
        if &header != b"MEDIA_SG" {
            return Err(VqdError::Other("Invalid media segment marker".to_string()));
        }
        
        // Read segment type (1 byte: 0 = video, 1 = audio)
        let mut type_buf = [0u8; 1];
        reader.read_exact(&mut type_buf).await?;
        let segment_type = if type_buf[0] == 0 { "video" } else { "audio" };
        
        // Read segment duration (8 bytes, f64)
        let mut duration_buf = [0u8; 8];
        reader.read_exact(&mut duration_buf).await?;
        let duration = f64::from_be_bytes(duration_buf);
        
        // Read segment size (4 bytes)
        let mut size_buf = [0u8; 4];
        reader.read_exact(&mut size_buf).await?;
        let segment_size = u32::from_be_bytes(size_buf) as usize;
        
        // Read segment data
        let mut segment_data = vec![0u8; segment_size];
        reader.read_exact(&mut segment_data).await?;
        
        Ok((segment_type.to_string(), Bytes::from(segment_data), duration))
    }
}