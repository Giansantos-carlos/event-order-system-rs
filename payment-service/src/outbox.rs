use chrono::Utc;
use event_contracts::EventEnvelope;
use rdkafka::producer::{FutureProducer, FutureRecord};
use sqlx::PgPool;
use std::time::Duration;

use crate::db;

pub async fn run_publisher(pool: PgPool, producer: FutureProducer) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        ticker.tick().await;
        if let Err(e) = publish_pending(&pool, &producer).await {
            tracing::error!("outbox publish cycle failed: {e:?}");
        }
    }
}

async fn publish_pending(pool: &PgPool, producer: &FutureProducer) -> anyhow::Result<()> {
    let pending = db::fetch_unpublished_outbox(pool, 100).await?;
    for event in pending {
        let envelope = EventEnvelope {
            event_id: event.id,
            event_type: event.event_type.clone(),
            aggregate_id: event.aggregate_id.clone(),
            occurred_at: Utc::now(),
            payload: event.payload.clone(),
        };

        let json = match serde_json::to_string(&envelope) {
            Ok(json) => json,
            Err(e) => {
                tracing::error!(event_id = %event.id, "failed to serialize outbox envelope: {e:?}");
                continue;
            }
        };

        let send_result = producer
            .send(
                FutureRecord::to(&event.topic).payload(&json).key(&event.aggregate_id),
                Duration::from_secs(5),
            )
            .await;

        match send_result {
            Ok(_) => {
                if let Err(e) = db::mark_outbox_published(pool, event.id).await {
                    tracing::error!(event_id = %event.id, "failed to mark outbox event published: {e:?}");
                }
            }
            Err((e, _)) => {
                tracing::error!(event_id = %event.id, "failed to publish outbox event, will retry next poll: {e:?}");
            }
        }
    }
    Ok(())
}
