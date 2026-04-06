"""
dag_cdc_ingest.py
=================
Runs every 10 minutes to:
  1. Check backfill has completed (guard — fails fast if not)
  2. Run the CDC sync script (src/producer/cdc.py)
     — fetch all ROME codes, compute INSERT/UPDATE/EXPIRE diff vs Redis,
       publish events to it-job-offers-cdc, commit new state to Redis
  3. Log current Redis state (active offer count, last sync time)

  Environment variables consumed by the producer:
    KAFKA_BOOTSTRAP  → kafka:9092
    REDIS_HOST       → redis
    REDIS_PORT       → 6379
    FT_CLIENT_ID     → from .env / Airflow Variables
    FT_CLIENT_SECRET → from .env / Airflow Variables

  max_active_runs=1 ensures that if a CDC run is still in flight when the
  next 10-minute tick fires, the new run queues instead of running in parallel
  (which would cause a race condition on the Redis state store).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

SRC_PATH = "/opt/airflow/src"


def _check_backfill_completed(**context) -> None:
    """
    Fail the task (and the DAG run) if the backfill has never completed.
    This prevents the CDC from running against an empty Redis state, which
    would falsely expire every offer on the next cycle.
    """
    import redis as redis_lib
    import os

    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", "6379"))

    r = redis_lib.Redis(host=host, port=port, db=0, decode_responses=True)

    completed_at = r.get("backfill_completed_at")
    if not completed_at:
        raise RuntimeError(
            "backfill_completed_at is not set in Redis. "
            "Run dag_backfill_initial first before starting CDC ingestion."
        )

    active_count = r.scard("active_offer_ids")
    log.info(
        "Backfill confirmed (completed_at=%s). "
        "Redis currently holds %d active offer IDs. Proceeding with CDC.",
        completed_at,
        active_count,
    )


def _log_cdc_metrics(**context) -> None:
    """
    Read the current Redis state post-sync and log a summary banner.
    We cannot pull INSERT/UPDATE/EXPIRE counts directly here (the script
    prints them to stdout which Airflow captures in the BashOperator log),
    so we report the resulting Redis state as a health check.
    """
    import redis as redis_lib
    import os
    from datetime import datetime, timezone

    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", "6379"))

    r = redis_lib.Redis(host=host, port=port, db=0, decode_responses=True)

    active_count  = r.scard("active_offer_ids")
    ts_count      = r.hlen("offer_timestamps")
    completed_at  = r.get("backfill_completed_at")
    checked_at    = datetime.now(timezone.utc).isoformat()

    banner = (
        "\n" + "═" * 50 + "\n"
        "  CDC INGEST DAG — CYCLE COMPLETE\n" +
        "═" * 50 + "\n"
        f"  Checked at              : {checked_at}\n"
        f"  Backfill completed at   : {completed_at}\n"
        f"  active_offer_ids (Redis): {active_count}\n"
        f"  offer_timestamps (Redis): {ts_count}\n" +
        "  (INSERT/UPDATE/EXPIRE counts are in the run_cdc_sync task log)\n" +
        "═" * 50
    )
    log.info(banner)


with DAG(
    dag_id="dag_cdc_ingest",
    description="CDC poll every 10 min: diff API vs Redis, publish INSERT/UPDATE/EXPIRE to Kafka.",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,   # never run two CDC cycles in parallel (Redis race condition)
    tags=["ingestion", "cdc", "streaming"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "email_on_failure": False,
        # CDC fetch + publish should complete well within 8 minutes,
        # leaving 2 minutes of headroom before the next trigger.
        "execution_timeout": timedelta(minutes=8),
    },
) as dag:

    # ── Task 1: Guard — fail immediately if backfill never ran ───────────────
    check_backfill_completed = PythonOperator(
        task_id="check_backfill_completed",
        python_callable=_check_backfill_completed,
        doc_md=(
            "Verify `backfill_completed_at` exists in Redis.\n"
            "Raises RuntimeError (fails the DAG run) if missing.\n"
            "This prevents the CDC from treating every offer as EXPIRED "
            "on its first cycle against an empty state store."
        ),
    )

    # ── Task 2: Run the CDC sync inside the Airflow worker ───────────────────
    #
    # cdc.py performs the full cycle:
    #   fetch all ROME codes → compute diff vs Redis → publish to Kafka
    #   → commit updated state to Redis → print INSERT/UPDATE/EXPIRE summary
    #
    # Stdout from the script is captured verbatim into this task's log,
    # so the CDC summary table is visible in the Airflow UI.
    run_cdc_sync = BashOperator(
        task_id="run_cdc_sync",
        bash_command="python -m producer.cdc",
        env={
            "PYTHONPATH": SRC_PATH,
            "KAFKA_BOOTSTRAP": "kafka:9092",
            "REDIS_HOST": "redis",
            "REDIS_PORT": "6379",
        },
        append_env=True,
        cwd=SRC_PATH,
        doc_md=(
            "Run `python -m producer.cdc` inside the Airflow worker.\n\n"
            "The script:\n"
            "- Fetches all ROME codes (fully paginated)\n"
            "- Diffs against Redis: `current_ids - stored_ids` → INSERT, etc.\n"
            "- Publishes INSERT/UPDATE/EXPIRE events to `it-job-offers-cdc`\n"
            "- Atomically updates `active_offer_ids` + `offer_timestamps` in Redis\n"
            "- Prints a CDC metrics summary table to stdout\n"
        ),
    )

    # ── Task 3: Log Redis state after sync ───────────────────────────────────
    log_metrics = PythonOperator(
        task_id="log_metrics",
        python_callable=_log_cdc_metrics,
        doc_md=(
            "Read the post-sync Redis state and log a health-check banner.\n"
            "The per-event-type counts (INSERT/UPDATE/EXPIRE) are in the "
            "`run_cdc_sync` task log."
        ),
    )

    check_backfill_completed >> run_cdc_sync >> log_metrics
