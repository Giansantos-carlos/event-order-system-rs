mod db;
mod domain;
mod kafka;
mod outbox;
mod saga;
mod service;

use axum::{routing::get, Router};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let db_host = std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".into());
    let db_port = std::env::var("DB_PORT").unwrap_or_else(|_| "5432".into());
    let db_name = std::env::var("DB_NAME").unwrap_or_else(|_| "inventory_db".into());
    let db_user = std::env::var("DB_USER").unwrap_or_else(|_| "eventsystem".into());
    let db_password = std::env::var("DB_PASSWORD").unwrap_or_else(|_| "eventsystem".into());
    let database_url =
        format!("postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}");

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    sqlx::migrate!().run(&pool).await?;

    let bootstrap_servers =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:19092".into());
    kafka::ensure_topics(&bootstrap_servers).await?;
    let producer = kafka::build_producer(&bootstrap_servers)?;

    let prometheus_handle = metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;

    tokio::spawn(outbox::run_publisher(pool.clone(), producer));
    tokio::spawn(kafka::run_order_events_listener(pool.clone(), bootstrap_servers.clone()));
    tokio::spawn(kafka::run_payment_events_listener(pool.clone(), bootstrap_servers.clone()));

    let app = Router::new()
        .route("/actuator/health", get(|| async { "OK" }))
        .route(
            "/actuator/prometheus",
            get(move || {
                let handle = prometheus_handle.clone();
                async move { handle.render() }
            }),
        );

    let port: u16 = std::env::var("SERVER_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8083);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!("inventory-service listening on :{port}");
    axum::serve(listener, app).await?;

    Ok(())
}
