from confluent_kafka import Producer
from producer.config import KAFKA_BOOTSTRAP, KAFKA_TOPIC_BACKFILL
import logging
from datetime import datetime, timezone
import json

log = logging.getLogger(__name__)

def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "acks":               "all",
        "retries":            5,
        "retry.backoff.ms":   300,
        "linger.ms":          50,          # micro-batch for throughput
        "batch.size":         131072,      # 128 KB
    })

def delivery_report(err, msg):
    if err:
        log.error("Kafka delivery failed for %s: %s", msg.key(), err)

def publish_offer(producer: Producer, offer: dict):
    event = {
        "event_type": "INSERT",
        "payload": offer,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    producer.produce(
        KAFKA_TOPIC_BACKFILL,
        key = offer.get("id","").encode(),
        value = json.dumps(event).encode(),
        callback = delivery_report,
    )
    # Non-blocking; flush happens in batches / at the end
    producer.poll(0)