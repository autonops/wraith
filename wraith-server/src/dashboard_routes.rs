//! Dashboard API routes for internal telemetry viewing
//!
//! Provides read-only endpoints for the Wraith Dashboard UI.

use axum::{
    extract::State,
    routing::get,
    Json, Router,
};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ============================================================================
// Data Structures
// ============================================================================

#[derive(Serialize)]
pub struct StatsResponse {
    pub total_events: u64,
    pub unique_sessions: u64,
    pub events_today: u64,
    pub events_this_week: u64,
    pub events_this_month: u64,
    pub success_rate: f64,
}

#[derive(Serialize, Deserialize, clickhouse::Row)]
pub struct CommandCount {
    pub tool: String,
    pub command: String,
    pub count: u64,
}

#[derive(Serialize, Deserialize, clickhouse::Row)]
pub struct OsCount {
    pub os: String,
    pub count: u64,
}

#[derive(Serialize, Deserialize, clickhouse::Row)]
pub struct VersionCount {
    pub version: String,
    pub count: u64,
}

#[derive(Serialize, Deserialize, clickhouse::Row)]
pub struct DailyCount {
    pub date: String,
    pub count: u64,
}

#[derive(Serialize, Deserialize, clickhouse::Row)]
pub struct HourlyCount {
    pub hour: u8,
    pub count: u64,
}

#[derive(Serialize, Deserialize, clickhouse::Row)]
pub struct RecentEvent {
    pub received_at: String,
    pub session_id: String,
    pub tool: String,
    pub command: String,
    pub success: bool,
    pub duration_ms: u64,
    pub os: String,
    pub version: String,
}

// Helper struct for single value queries
#[derive(clickhouse::Row, Deserialize)]
struct CountRow {
    count: u64,
}

// ============================================================================
// Router
// ============================================================================

pub fn dashboard_router(client: Arc<Client>) -> Router<()> {
    Router::new()
        .route("/stats", get(get_stats))
        .route("/commands", get(get_commands))
        .route("/os", get(get_os_breakdown))
        .route("/versions", get(get_versions))
        .route("/daily", get(get_daily_activity))
        .route("/hourly", get(get_hourly_activity))
        .route("/events", get(get_recent_events))
        .with_state(client)
}

// ============================================================================
// Handlers
// ============================================================================

async fn get_stats(State(client): State<Arc<Client>>) -> Json<StatsResponse> {
    let total_events = client
        .query("SELECT count() as count FROM wraith.events")
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0);

    let unique_sessions = client
        .query("SELECT uniq(session_id) as count FROM wraith.events")
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0);

    let events_today = client
        .query("SELECT count() as count FROM wraith.events WHERE toDate(received_at) = today()")
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0);

    let events_this_week = client
        .query("SELECT count() as count FROM wraith.events WHERE received_at >= now() - INTERVAL 7 DAY")
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0);

    let events_this_month = client
        .query("SELECT count() as count FROM wraith.events WHERE received_at >= now() - INTERVAL 30 DAY")
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0);

    let success_count = client
        .query("SELECT count() as count FROM wraith.events WHERE success = true")
        .fetch_one::<CountRow>()
        .await
        .map(|r| r.count)
        .unwrap_or(0);

    let success_rate = if total_events > 0 {
        (success_count as f64 / total_events as f64) * 100.0
    } else {
        0.0
    };

    Json(StatsResponse {
        total_events,
        unique_sessions,
        events_today,
        events_this_week,
        events_this_month,
        success_rate,
    })
}

async fn get_commands(State(client): State<Arc<Client>>) -> Json<Vec<CommandCount>> {
    let commands = client
        .query(
            "SELECT tool, command, count() as count 
             FROM wraith.events 
             GROUP BY tool, command 
             ORDER BY count DESC 
             LIMIT 20"
        )
        .fetch_all::<CommandCount>()
        .await
        .unwrap_or_default();

    Json(commands)
}

async fn get_os_breakdown(State(client): State<Arc<Client>>) -> Json<Vec<OsCount>> {
    let os_counts = client
        .query(
            "SELECT os, count() as count 
             FROM wraith.events 
             GROUP BY os 
             ORDER BY count DESC"
        )
        .fetch_all::<OsCount>()
        .await
        .unwrap_or_default();

    Json(os_counts)
}

async fn get_versions(State(client): State<Arc<Client>>) -> Json<Vec<VersionCount>> {
    let versions = client
        .query(
            "SELECT version, count() as count 
             FROM wraith.events 
             GROUP BY version 
             ORDER BY count DESC 
             LIMIT 10"
        )
        .fetch_all::<VersionCount>()
        .await
        .unwrap_or_default();

    Json(versions)
}

async fn get_daily_activity(State(client): State<Arc<Client>>) -> Json<Vec<DailyCount>> {
    let daily = client
        .query(
            "SELECT toString(toDate(received_at)) as date, count() as count 
             FROM wraith.events 
             WHERE received_at >= now() - INTERVAL 30 DAY
             GROUP BY date 
             ORDER BY date"
        )
        .fetch_all::<DailyCount>()
        .await
        .unwrap_or_default();

    Json(daily)
}

async fn get_hourly_activity(State(client): State<Arc<Client>>) -> Json<Vec<HourlyCount>> {
    let hourly = client
        .query(
            "SELECT toHour(received_at) as hour, count() as count 
             FROM wraith.events 
             WHERE received_at >= now() - INTERVAL 7 DAY
             GROUP BY hour 
             ORDER BY hour"
        )
        .fetch_all::<HourlyCount>()
        .await
        .unwrap_or_default();

    Json(hourly)
}

async fn get_recent_events(State(client): State<Arc<Client>>) -> Json<Vec<RecentEvent>> {
    let events = client
        .query(
            "SELECT 
                toString(received_at) as received_at,
                session_id,
                tool,
                command,
                success,
                duration_ms,
                os,
                version
             FROM wraith.events 
             ORDER BY received_at DESC 
             LIMIT 50"
        )
        .fetch_all::<RecentEvent>()
        .await
        .unwrap_or_default();

    Json(events)
}
