import logging
from datetime import datetime, timezone
from producer.auth import TokenManager
from producer.backfill import build_redis, fetch_offers_for_rome
from producer.producer import build_producer, publish_event
from redis import Redis
from confluent_kafka import Producer
from producer.config import KAFKA_TOPIC_CDC, ROME_CODES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

def assert_backfill_ran(r: Redis) -> str:
    completed_at = r.get("backfill_completed_at")
    if not completed_at:
        raise RuntimeError("Backfill has never completed. Please run the backfill first.")
    log.info(f"Backfill completed at {completed_at}. Proceeding with CDC.")
    return completed_at

def fetch_all_current_offers(token_mgr: TokenManager) -> dict[str, dict]:
    """Return {offer_id: full_offer_dict} for every offer across all ROME codes.
    Duplicates (same offer under two ROME codes) keep the latest dateActualisation.
    """
    current: dict[str, dict] = {}
    for rome_code in ROME_CODES:
        log.info("Fetching offers for ROME %s", rome_code)
        offers = fetch_offers_for_rome(rome_code, token_mgr)
        for offer in offers:
            oid = offer.get("id")
            if not oid:
                continue
            ts = offer.get("dateActualisation", "")
            stored_ts = current[oid].get("dateActualisation", "") if oid in current else ""
            if oid not in current or ts > stored_ts:
                current[oid] = offer        # store full offer dict
        log.info("  ROME %s → %d offers", rome_code, len(offers))
    log.info("Total unique offers across all ROME codes: %d", len(current))
    return current

def load_redis_state(r: Redis) -> tuple[set[str], dict[str, str]]:
    stored_ids = r.smembers("active_offer_ids")
    stored_timestamps = r.hgetall("offer_timestamps")
    log.info(f"Loaded {len(stored_ids)} stored offer IDs and {len(stored_timestamps)} stored offer timestamps from Redis")
    return stored_ids, stored_timestamps

def compute_diff(current: dict[str, dict],
    stored_ids: set[str], stored_ts: dict[str, str]) -> tuple[set[str], set[str], set[str]]:
    """
    Returns (new_ids, expired_ids, updated_ids).

    new_ids     - in API, not in Redis           → INSERT
    expired_ids - in Redis, not in API           → EXPIRE
    updated_ids - in both, but timestamp changed → UPDATE
    """
    current_ids = set(current.keys())

    new_ids = current_ids - stored_ids
    expired_ids = stored_ids - current_ids
    common_ids = current_ids & stored_ids

    updated_ids = {
        oid for oid in common_ids
        if current[oid].get("dateActualisation", "") != stored_ts.get(oid, "")
    }

    log.info(
        "Diff → INSERT: %d | UPDATE: %d | EXPIRE: %d",
        len(new_ids), len(updated_ids), len(expired_ids),
    )

    return new_ids, expired_ids, updated_ids

def publish_events(producer: Producer, current: dict[str, dict], new_ids: set[str], expired_ids: set[str], updated_ids: set[str]):
    for oid in new_ids:
        publish_event(producer, KAFKA_TOPIC_CDC, "INSERT", oid, current[oid])   # full offer
    for oid in updated_ids:
        publish_event(producer, KAFKA_TOPIC_CDC, "UPDATE", oid, current[oid])   # full offer
    for oid in expired_ids:
        publish_event(producer, KAFKA_TOPIC_CDC, "EXPIRE", oid, None)           # id only

    log.info("All CDC events queued to Kafka. Flushing …")
    producer.flush(timeout=60)
    log.info("Kafka flush complete.")

def commit_to_redis(r: Redis, current: dict[str, dict], new_ids: set[str], expired_ids: set[str], updated_ids: set[str]):
    """
    Single pipeline (no MULTI/EXEC overhead for non-conflicting writes):
      SADD   new offer IDs into active_offer_ids
      SREM   expired offer IDs from active_offer_ids
      HSET   new + updated timestamps
      HDEL   expired timestamps
    """
    if not (new_ids or expired_ids or updated_ids):
        log.info("No changes to commit to Redis.")
        return
    CHUNK_SIZE = 500
    def _chunk(iterable):
        items = list(iterable)
        for i in range(0, len(items), CHUNK_SIZE):
            yield items[i:i + CHUNK_SIZE]
    pipe = r.pipeline(transaction=False)
    
    for chunk in _chunk(new_ids):
        pipe.sadd("active_offer_ids", *chunk)
    for chunk in _chunk(expired_ids):
        pipe.srem("active_offer_ids", *chunk)
    changed_ts = {oid: current[oid].get("dateActualisation", "") for oid in (updated_ids | new_ids)}
    for chunk in _chunk(changed_ts.items()):
        pipe.hset("offer_timestamps", mapping=dict(chunk))
    for chunk in _chunk(expired_ids):
        pipe.hdel("offer_timestamps", *chunk)
    pipe.execute()
    log.info("Redis committed: +%d SADD, -%d SREM, %d HSET, %d HDEL",
             len(new_ids), len(expired_ids), len(changed_ts), len(expired_ids))

def main():
    r = build_redis()
    producer = build_producer()
    token_mgr = TokenManager()

    assert_backfill_ran(r)

    current = fetch_all_current_offers(token_mgr)

    stored_ids, stored_ts = load_redis_state(r)
    new_ids, expired_ids, updated_ids = compute_diff(current, stored_ids, stored_ts)

    publish_events(producer, current, new_ids, expired_ids, updated_ids)

    commit_to_redis(r, current, new_ids, expired_ids, updated_ids)

    total_events = len(new_ids) + len(updated_ids) + len(expired_ids)
    ran_at = datetime.now(timezone.utc).isoformat()

    print("\n" + "═" * 45)
    print("  CDC SYNC METRICS")
    print("═" * 45)
    print(f"  INSERT   {len(new_ids):>8}")
    print(f"  UPDATE   {len(updated_ids):>8}")
    print(f"  EXPIRE   {len(expired_ids):>8}")
    print("─" * 45)
    print(f"  TOTAL    {total_events:>8}")
    print("═" * 45)
    print(f"  Synced at: {ran_at}")
 
    if total_events == 0:
        print("  ✓ State is already in sync — no changes detected.")

if __name__ == "__main__":
    main()