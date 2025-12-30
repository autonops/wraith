//! ClickHouse consumer - reads from NATS and writes to ClickHouse.

use async_nats::Client;
use clickhouse::Client as ClickHouseClient;
use futures::StreamExt;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::models::StoredEvent;

/// Consumer that reads events from NATS and writes to ClickHouse
pub struct ClickHouseConsumer {
    nats_client: Client,
    clickhouse_client: ClickHouseClient,
    subject: String,
    database: String,
    table: String,
}

impl ClickHouseConsumer {
    /// Create a new consumer
    pub async fn new(config: &Config) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        info!("Connecting to NATS at {}", config.nats_url);
        let nats_client = async_nats::connect(&config.nats_url).await?;
        info!("Connected to NATS");
        
        info!("Connecting to ClickHouse at {}", config.clickhouse_url);
        let clickhouse_client = ClickHouseClient::default()
            .with_url(&config.clickhouse_url)
            .with_database(&config.clickhouse_database);
        info!("Connected to ClickHouse");
        
        Ok(Self {
            nats_client,
            clickhouse_client,
            subject: config.nats_subject.clone(),
            database: config.clickhouse_database.clone(),
            table: config.clickhouse_table.clone(),
        })
    }
    
    /// Initialize the ClickHouse schema
    pub async fn init_schema(&self) -> Result<(), clickhouse::error::Error> {
        info!("Initializing ClickHouse schema");
        
        let create_table = format!(r#"
            CREATE TABLE IF NOT EXISTS {} (
                id String,
                received_at DateTime64(3),
                installation_id String,
                level LowCardinality(String),
                event_type LowCardinality(String),
                tool LowCardinality(String),
                command LowCardinality(String),
                duration_ms UInt64,
                error_type LowCardinality(String),
                tool_version LowCardinality(String),
                python_version LowCardinality(String),
                os LowCardinality(String),
                raw_json String
            ) ENGINE = MergeTree()
            ORDER BY (received_at, installation_id, event_type)
            PARTITION BY toYYYYMM(received_at)
            TTL toDateTime(received_at) + INTERVAL 90 DAY
        "#, self.table);
        
        self.clickhouse_client.query(&create_table).execute().await?;
        info!("ClickHouse schema initialized");
        
        Ok(())
    }
    
    /// Run the consumer loop
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting consumer for subject: {}", self.subject);
        
        let mut subscriber = self.nats_client.subscribe(self.subject.clone()).await?;
        info!("Subscribed to NATS subject: {}", self.subject);
        
        while let Some(message) = subscriber.next().await {
            match serde_json::from_slice::<StoredEvent>(&message.payload) {
                Ok(event) => {
                    if let Err(e) = self.insert_event(&event).await {
                        error!("Failed to insert event {}: {}", event.id, e);
                    } else {
                        debug!("Inserted event {}", event.id);
                    }
                }
                Err(e) => {
                    warn!("Failed to deserialize event: {}", e);
                }
            }
        }
        
        info!("Consumer stopped");
        Ok(())
    }
    
    /// Insert a single event into ClickHouse using raw SQL
    async fn insert_event(&self, event: &StoredEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Format timestamp as milliseconds since epoch
        let received_at_ms = event.received_at.timestamp_millis();
        
        // Escape single quotes in strings
        let escape = |s: &str| s.replace('\'', "''").replace('\\', "\\\\");
        
        let query = format!(
            r#"INSERT INTO {}.{} (
                id, received_at, installation_id, level, event_type,
                tool, command, duration_ms, error_type,
                tool_version, python_version, os, raw_json
            ) VALUES (
                '{}', fromUnixTimestamp64Milli({}), '{}', '{}', '{}',
                '{}', '{}', {}, '{}',
                '{}', '{}', '{}', '{}'
            )"#,
            self.database,
            self.table,
            escape(&event.id),
            received_at_ms,
            escape(&event.installation_id),
            escape(&event.level),
            escape(&event.event_type),
            escape(&event.tool),
            escape(&event.command),
            event.duration_ms,
            escape(&event.error_type),
            escape(&event.tool_version),
            escape(&event.python_version),
            escape(&event.os),
            escape(&event.raw_json),
        );
        
        self.clickhouse_client.query(&query).execute().await?;
        
        Ok(())
    }
}
