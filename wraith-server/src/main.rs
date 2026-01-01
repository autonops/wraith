//! Wraith Server - Telemetry ingestion server
//!
//! Architecture:
//! - Axum HTTP server receives events from Wraith clients
//! - Events are published to NATS for buffering
//! - ClickHouse consumer reads from NATS and persists events
//!
//! # Usage
//!
//! ```bash
//! # Start with docker-compose (recommended)
//! docker-compose up
//!
//! # Or run directly (requires NATS and ClickHouse running)
//! cargo run --bin wraith-server
//! ```

mod clickhouse;
mod config;
mod dashboard_routes;
mod models;
mod nats;
mod routes;

use axum::{
    routing::{get, post},
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::clickhouse::ClickHouseConsumer;
use crate::config::Config;
use crate::nats::NatsPublisher;
use crate::routes::{health, ingest_batch, ingest_single, ready, AppState};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Load configuration
    let config = Config::from_env();
    
    // Setup logging
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));
    
    if config.log_json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .init();
    }
    
    info!("Starting Wraith Server v{}", env!("CARGO_PKG_VERSION"));
    info!("Configuration: {:?}", config);
    
    // Connect to NATS
    let nats = NatsPublisher::connect(&config.nats_url, config.nats_subject.clone()).await?;
    
    // Start ClickHouse consumer in background
    let consumer = ClickHouseConsumer::new(&config).await?;
    consumer.init_schema().await?;
    
    // Create ClickHouse client for dashboard queries
    let clickhouse_client = Arc::new(
        ::clickhouse::Client::default()
            .with_url(&config.clickhouse_url)
    );
    
    tokio::spawn(async move {
        if let Err(e) = consumer.run().await {
            tracing::error!("Consumer error: {}", e);
        }
    });
    
    // Create app state
    let state = AppState { nats };
    
    // Build router
    // Build router
let app = Router::new()
    // Health checks
    .route("/health", get(health))
    .route("/ready", get(ready))
    // Event ingestion
    .route("/events", post(ingest_batch))
    .route("/event", post(ingest_single))
    // Apply state to routes that need it
    .with_state(state)
    // Dashboard API (doesn't need AppState, uses its own ClickHouse client)
    .nest("/api/dashboard", dashboard_routes::dashboard_router(clickhouse_client))
    // Middleware (must come last)
    .layer(TraceLayer::new_for_http())
    .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any));
    
    // Start server
    let addr: SocketAddr = config.server_addr().parse()?;
    info!("Listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    
    info!("Server shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, shutting down");
        },
        _ = terminate => {
            info!("Received terminate signal, shutting down");
        },
    }
}
