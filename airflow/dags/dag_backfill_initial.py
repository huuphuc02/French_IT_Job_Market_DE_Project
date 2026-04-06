"""
dag_backfill_initial.py
=======================
Runs ONCE to:
  1. Check Redis is empty (guard against accidental re-runs)
  2. Run the full backfill script (src/producer/backfill.py)
     — paginate all ROME codes, publish INSERT events to Kafka,
       seed Redis state store
  3. Verify Redis was seeded (active_offer_ids exists and is non-empty)
  4. Log backfill metrics from Redis

  Environment variables consumed by the producer:
    KAFKA_BOOTSTRAP  → kafka:9092
    REDIS_HOST       → redis
    REDIS_PORT       → 6379
    FT_CLIENT_ID     → from .env
    FT_CLIENT_SECRET → from .env
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

log = logging.getLogger(__name__)

SRC_PATH = "/opt/airflow/src"
PYTHON_ENV = f"PYTHONPATH={SRC_PATH}"

def _check_redis_is_empty(**context) -> bool:
    """
    ShortCircuitOperator callable.
    Returns True  → pipeline continues (Redis is clean, safe to backfill).
    Returns False → pipeline short-circuits (backfill already ran, skip all).

    We use ShortCircuit so that accidental DAG re-triggers are a no-op,
    not a double-write.
    """
    import redis as redis_lib
    import os

    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", "6379"))

    r = redis_lib.Redis(host=host, port=port, db=0, decode_responses=True)

    backfill_completed_at = r.get("backfill_completed_at")
    active_count = r.scard("active_offer_ids")

    if backfill_completed_at:
        log.warning(
            "Backfill already completed at %s (%d IDs in Redis). "
            "Short-circuiting — nothing to do.",
            backfill_completed_at,
            active_count,
        )
        return False

    log.info("Redis is clean (backfill_completed_at not set). Proceeding with backfill.")
    return True


def _verify_redis_seeded(**context) -> None:
    """
    Confirm that backfill.py actually wrote state to Redis.
    Raises ValueError if the state is missing — which will fail the task
    and alert the operator.
    """
    import redis as redis_lib
    import os

    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", "6379"))

    r = redis_lib.Redis(host=host, port=port, db=0, decode_responses=True)

    completed_at = r.get("backfill_completed_at")
    active_count = r.scard("active_offer_ids")
    ts_count = r.hlen("offer_timestamps")

    if not completed_at:
        raise ValueError(
            "backfill_completed_at key missing from Redis — backfill may have failed."
        )
    if active_count == 0:
        raise ValueError(
            "active_offer_ids is empty after backfill — no offers were seeded."
        )

    log.info(
        "Redis state verified: completed_at=%s | active_offer_ids=%d | offer_timestamps=%d",
        completed_at,
        active_count,
        ts_count,
    )

    # Push metrics to XCom so log_metrics task can read them
    context["ti"].xcom_push(key="completed_at", value=completed_at)
    context["ti"].xcom_push(key="active_count", value=active_count)
    context["ti"].xcom_push(key="ts_count", value=ts_count)


def _log_metrics(**context) -> None:
    """Print a final summary banner to the Airflow task log."""
    ti = context["ti"]
    completed_at = ti.xcom_pull(task_ids="verify_redis_seeded", key="completed_at")
    active_count = ti.xcom_pull(task_ids="verify_redis_seeded", key="active_count")
    ts_count = ti.xcom_pull(task_ids="verify_redis_seeded", key="ts_count")

    banner = (
        "\n" + "═" * 50 + "\n"
        "  BACKFILL DAG — COMPLETED\n" +
        "═" * 50 + "\n"
        f"  Completed at   : {completed_at}\n"
        f"  Active offer IDs in Redis : {active_count}\n"
        f"  Timestamps stored         : {ts_count}\n" +
        "═" * 50
    )
    log.info(banner)


with DAG(
    dag_id="dag_backfill_initial",
    description="One-time full backfill: fetch all ROME codes, publish to Kafka, seed Redis.",
    schedule_interval="@once",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ingestion", "backfill", "one-time"],
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay_seconds": 60,
        "email_on_failure": False,
    },
) as dag:

    # ── Task 1: Guard — skip everything if backfill already ran ──────────────
    check_redis_is_empty = ShortCircuitOperator(
        task_id="check_redis_is_empty",
        python_callable=_check_redis_is_empty,
        doc_md=(
            "Check that `backfill_completed_at` is NOT set in Redis.\n"
            "If it is, short-circuit the pipeline (no-op). Safe re-trigger guard."
        ),
    )

    # ── Task 2: Run the backfill script inside the Airflow worker ────────────
    run_backfill = BashOperator(
        task_id="run_backfill",
        bash_command=(
            f"{PYTHON_ENV} "
            "python -m producer.backfill"
        ),
        env={
            # Inherit the container environment (Kafka, Redis, API creds).
            # BashOperator starts a subprocess; we explicitly forward the keys
            # the producer needs so they are always present.
            "PYTHONPATH": SRC_PATH,
            "KAFKA_BOOTSTRAP": "kafka:9092",
            "REDIS_HOST": "redis",
            "REDIS_PORT": "6379",
            # API credentials — Airflow picks these up from docker-compose .env
            "FT_CLIENT_ID": "{{ var.value.get('FT_CLIENT_ID', '') or '' }}",
            "FT_CLIENT_SECRET": "{{ var.value.get('FT_CLIENT_SECRET', '') or '' }}",
        },
        append_env=True,       # merge with the container's existing env
        cwd=SRC_PATH,
        execution_timeout=None,  # let Airflow's global task timeout govern
        doc_md=(
            "Run `python -m producer.backfill` inside the Airflow worker.\n\n"
            "The script:\n"
            "- Paginates all ROME codes (150 offers/page)\n"
            "- Publishes INSERT events to `it-job-offers-backfill`\n"
            "- Seeds `active_offer_ids` + `offer_timestamps` in Redis\n"
            "- Sets `backfill_completed_at` when done\n"
        ),
    )

    # ── Task 3: Confirm Redis was seeded correctly ───────────────────────────
    verify_redis_seeded = PythonOperator(
        task_id="verify_redis_seeded",
        python_callable=_verify_redis_seeded,
        doc_md="Assert Redis contains non-zero active_offer_ids and backfill_completed_at.",
    )

    # ── Task 4: Log final metrics banner ────────────────────────────────────
    log_metrics = PythonOperator(
        task_id="log_metrics",
        python_callable=_log_metrics,
        doc_md="Print backfill summary to the Airflow task log.",
    )

    # ── Pipeline: linear, guard short-circuits on re-run ────────────────────
    check_redis_is_empty >> run_backfill >> verify_redis_seeded >> log_metrics
