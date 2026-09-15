use event_contracts::{topics, EventEnvelope};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::RDKafkaErrorCode,
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use sqlx::PgPool;
use std::{future::Future, time::Duration};

use crate::saga;

const MAX_RETRIES: u32 = 5;

/// Creates every topic this service either publishes or subscribes to, including the `.DLT`
/// dead-letter topics `consume_loop` routes to. Idempotent — "already exists" is not an error —
/// so it's safe to call regardless of which service happens to start first.
pub async fn ensure_topics(bootstrap_servers: &str) -> anyhow::Result<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let names = [
        topics::ORDER_EVENTS,
        topics::PAYMENT_EVENTS,
        topics::INVENTORY_EVENTS,
        "order-events.DLT",
        "payment-events.DLT",
        "inventory-events.DLT",
    ];
    let new_topics: Vec<NewTopic> = names
        .iter()
        .map(|name| NewTopic::new(name, 3, TopicReplication::Fixed(1)))
        .collect();

    let results = admin.create_topics(&new_topics, &AdminOptions::new()).await?;
    for result in results {
        if let Err((name, err)) = result {
            if !matches!(err, RDKafkaErrorCode::TopicAlreadyExists) {
                tracing::warn!("failed to create topic {name}: {err:?}");
            }
        }
    }
    Ok(())
}

pub fn build_producer(bootstrap_servers: &str) -> anyhow::Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .set("acks", "all")
        .create()?;
    Ok(producer)
}

fn build_consumer(bootstrap_servers: &str, group_id: &str) -> anyhow::Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    Ok(consumer)
}

/// Retries a failing event with exponential backoff (1s -> 15s cap), then routes it to a
/// `<topic>.DLT` topic so one poison message can't block the partition forever.
pub async fn consume_loop<F, Fut>(
    bootstrap_servers: &str,
    topic: &str,
    group_id: &str,
    handler: F,
) -> anyhow::Result<()>
where
    F: Fn(EventEnvelope) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let consumer = build_consumer(bootstrap_servers, group_id)?;
    consumer.subscribe(&[topic])?;
    let dlq_producer = build_producer(bootstrap_servers)?;
    let dlq_topic = format!("{topic}.DLT");

    loop {
        let message = match consumer.recv().await {
            Ok(message) => message,
            Err(e) => {
                tracing::error!("kafka recv error on {topic}: {e:?}");
                continue;
            }
        };

        let Some(payload) = message.payload() else {
            let _ = consumer.commit_message(&message, CommitMode::Async);
            continue;
        };

        match serde_json::from_slice::<EventEnvelope>(payload) {
            Ok(envelope) => {
                let mut attempt = 0u32;
                let mut delay = Duration::from_secs(1);
                loop {
                    match handler(envelope.clone()).await {
                        Ok(()) => break,
                        Err(e) => {
                            attempt += 1;
                            if attempt >= MAX_RETRIES {
                                tracing::error!(
                                    event_id = %envelope.event_id,
                                    "giving up after {attempt} attempts: {e:?}, routing to {dlq_topic}"
                                );
                                if let Ok(json) = serde_json::to_string(&envelope) {
                                    let _ = dlq_producer
                                        .send(
                                            FutureRecord::to(&dlq_topic)
                                                .payload(&json)
                                                .key(&envelope.aggregate_id),
                                            Duration::from_secs(5),
                                        )
                                        .await;
                                }
                                break;
                            }
                            tracing::warn!(
                                event_id = %envelope.event_id,
                                "attempt {attempt} failed: {e:?}, retrying in {delay:?}"
                            );
                            tokio::time::sleep(delay).await;
                            delay = std::cmp::min(delay * 2, Duration::from_secs(15));
                        }
                    }
                }
            }
            Err(e) => tracing::error!("failed to deserialize event envelope on {topic}: {e:?}"),
        }

        if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
            tracing::error!("failed to commit offset on {topic}: {e:?}");
        }
    }
}

pub async fn run_order_events_listener(pool: PgPool, bootstrap_servers: String) {
    let handler = move |envelope: EventEnvelope| {
        let pool = pool.clone();
        async move { saga::handle_order_event(&pool, envelope).await }
    };
    if let Err(e) = consume_loop(&bootstrap_servers, topics::ORDER_EVENTS, "inventory-service", handler).await {
        tracing::error!("order-events listener crashed: {e:?}");
    }
}

pub async fn run_payment_events_listener(pool: PgPool, bootstrap_servers: String) {
    let handler = move |envelope: EventEnvelope| {
        let pool = pool.clone();
        async move { saga::handle_payment_event(&pool, envelope).await }
    };
    if let Err(e) = consume_loop(&bootstrap_servers, topics::PAYMENT_EVENTS, "inventory-service", handler).await
    {
        tracing::error!("payment-events listener crashed: {e:?}");
    }
}
