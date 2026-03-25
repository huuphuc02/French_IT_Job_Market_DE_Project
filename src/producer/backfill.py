from producer.auth import TokenManager
from producer.config import PAGE_SIZE, FT_OFFERS_URL, REQUEST_DELAY, REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD, ROME_CODES
import requests
import logging
import time
import redis
from producer.producer import build_producer, publish_offer
from datetime import datetime, timezone

log = logging.getLogger(__name__)

def _parse_total(content_range: str) -> int:
    """Parse total from 'offres 0-149/1423'."""
    try:
        return int(content_range.split("/")[1])
    except (IndexError, ValueError):
        return None

def fetch_offers_for_rome(rome_code: str, token_mgr: TokenManager) -> list[dict]:
    """Return all offers for a given ROME code."""
    all_offers: list[dict] = []
    start = 0
    while True:
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        params = {
            "codeROME": rome_code,
            "range": f"{start}-{start + PAGE_SIZE - 1}",
        }

        resp = requests.get(FT_OFFERS_URL, headers=headers, params=params, timeout=30)
        if resp.status_code == 204: # no more offers
            break
        if resp.status_code == 206 or resp.status_code == 200:
            data = resp.json()
            offers = data.get("resultats", [])
            all_offers.extend(offers)
            log.info("  ROME %s | fetched %d offers (start=%d, total so far=%d)",
                     rome_code, len(offers), start, len(all_offers))

            # Content range header tell us the total
            content_range = resp.headers.get("Content-Range", "")
            total = _parse_total(content_range)

            if total is not None and len(all_offers) >= total:
                break
            start += PAGE_SIZE
            time.sleep(REQUEST_DELAY)
        else:
            resp.raise_for_status()

    return all_offers

def build_redis() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
    )

def index_offer(r: redis.Redis, pipe: redis.client.Pipeline, offer: dict) -> None:
    """Queue SADD + HSET commands into an existing pipeline."""
    offer_id          = offer.get("id")
    date_actualisation = offer.get("dateActualisation", "")
 
    if not offer_id:
        return
 
    pipe.sadd("active_offer_ids",    offer_id)
    pipe.hset("offer_timestamps",    offer_id, date_actualisation)

def main():
    token_mgr = TokenManager()
    r = build_redis()
    producer = build_producer()

    summary: dict[str, int] = {}
    for rome_code in ROME_CODES:
        log.info("Processing ROME code %s", rome_code)
        offers = fetch_offers_for_rome(rome_code, token_mgr)
        summary[rome_code] = len(offers)

        # Pipeline Redis writes in chunks of 500 to avoid huge transactions
        CHUNK_SIZE = 500
        for chunk_start in range(0, len(offers), CHUNK_SIZE):
            chunk = offers[chunk_start:chunk_start + CHUNK_SIZE]
            pipe = r.pipeline(transaction=False)

            for offer in chunk:
                publish_offer(producer, offer)
                index_offer(r, pipe, offer)
            pipe.execute()
        log.info("  → %d offers published for %s", len(offers), rome_code)
        # Flush remaining Kafka messages
    log.info("Flushing Kafka producer …")
    producer.flush(timeout=60)
 
    # Mark backfill complete
    completed_at = datetime.now(timezone.utc).isoformat()
    r.set("backfill_completed_at", completed_at)
    log.info("Backfill marked complete at %s", completed_at)
 
    # ── Summary ──────────────────────────────
    total = sum(summary.values())
    print("\n" + "═" * 45)
    print("  BACKFILL SUMMARY")
    print("═" * 45)
    for code, count in summary.items():
        print(f"  {code}   {count:>6} offers")
    print("─" * 45)
    print(f"  TOTAL      {total:>6} offers")
    print("═" * 45)
    print(f"  Completed at: {completed_at}")

if __name__ == "__main__":
    main()
