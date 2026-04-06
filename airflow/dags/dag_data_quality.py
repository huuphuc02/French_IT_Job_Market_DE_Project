"""
dag_data_quality.py
===================
Runs daily to check the health of the pipeline's output tables.

Checks performed:
  1. volume_check      — stg_job_offers row count is above expected floor
  2. null_rate_check   — critical columns in fact_job_offers have < 5% NULLs
  3. duplicate_check   — fact_job_offers has no duplicate offer_ids
  4. expire_rate_check — EXPIRE event rate is not anomalously high (< 50% of active)
  5. log_dq_summary    — log all results; raise if any check failed

All checks are soft-fail by default: each check pushes a PASS/FAIL flag to XCom.
The final `log_dq_summary` task reads all flags and raises an AirflowException
if any check failed, which marks the DAG run as failed and can page on-call.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

BQ_PROJECT = "basic-decoder-415403"
BQ_DATASET = "french_it_jobs"

# Thresholds
MIN_STAGING_ROWS    = 100       # expect at least 100 offers in staging
MAX_NULL_RATE       = 0.05      # critical columns should be < 5% NULL
MAX_EXPIRE_RATE     = 0.50      # EXPIRE events should be < 50% of active count


def _get_bq_client():
    from google.cloud import bigquery
    import os
    return bigquery.Client(project=os.getenv("BQ_PROJECT", BQ_PROJECT))


def _volume_check(**context) -> None:
    """stg_job_offers must have at least MIN_STAGING_ROWS rows."""
    client = _get_bq_client()
    result = client.query(
        f"SELECT COUNT(*) AS n FROM `{BQ_PROJECT}.{BQ_DATASET}.stg_job_offers`"
    ).result()
    count = next(result).n

    passed = count >= MIN_STAGING_ROWS
    context["ti"].xcom_push(key="volume_check", value={
        "passed": passed,
        "row_count": count,
        "threshold": MIN_STAGING_ROWS,
        "message": (
            f"OK — {count} rows in stg_job_offers."
            if passed
            else f"FAIL — only {count} rows in stg_job_offers (min: {MIN_STAGING_ROWS})."
        ),
    })
    log.info("volume_check: count=%d passed=%s", count, passed)


def _null_rate_check(**context) -> None:
    """
    Critical columns in fact_job_offers must have < MAX_NULL_RATE fraction of NULLs.
    Checks: offer_id, rome_code, contract_type, date_created.
    """
    client = _get_bq_client()
    columns = ["offer_id", "rome_key", "contract_type", "date_created"]

    null_exprs = ", ".join(
        f"COUNTIF({col} IS NULL) / COUNT(*) AS null_rate_{col}"
        for col in columns
    )
    result = client.query(
        f"SELECT {null_exprs} FROM `{BQ_PROJECT}.{BQ_DATASET}.fact_job_offers`"
    ).result()
    row = next(result)

    failures = {}
    for col in columns:
        rate = getattr(row, f"null_rate_{col}", 0.0)
        if rate > MAX_NULL_RATE:
            failures[col] = rate

    passed = len(failures) == 0
    context["ti"].xcom_push(key="null_rate_check", value={
        "passed": passed,
        "failures": failures,
        "message": (
            f"OK — all critical columns below {MAX_NULL_RATE*100:.0f}% NULL rate."
            if passed
            else f"FAIL — columns exceed NULL threshold: {failures}"
        ),
    })
    log.info("null_rate_check: passed=%s failures=%s", passed, failures)


def _duplicate_check(**context) -> None:
    """fact_job_offers must have no duplicate offer_ids (PK integrity)."""
    client = _get_bq_client()
    result = client.query(f"""
        SELECT COUNT(*) AS duplicate_count
        FROM (
            SELECT offer_id, COUNT(*) AS cnt
            FROM `{BQ_PROJECT}.{BQ_DATASET}.fact_job_offers`
            GROUP BY offer_id
            HAVING cnt > 1
        )
    """).result()
    dup_count = next(result).duplicate_count

    passed = dup_count == 0
    context["ti"].xcom_push(key="duplicate_check", value={
        "passed": passed,
        "duplicate_offer_ids": dup_count,
        "message": (
            "OK — no duplicate offer_ids in fact_job_offers."
            if passed
            else f"FAIL — {dup_count} offer_ids appear more than once in fact_job_offers."
        ),
    })
    log.info("duplicate_check: dup_count=%d passed=%s", dup_count, passed)


def _expire_rate_check(**context) -> None:
    """
    EXPIRE rate anomaly guard.
    If more than MAX_EXPIRE_RATE of the current Redis active count suddenly
    appears expired in BigQuery, something is wrong (e.g. a full Redis flush
    caused every offer to be re-inserted as INSERT, making old rows look expired).
    Uses fact_job_offers (is_active=FALSE vs total).
    """
    client = _get_bq_client()
    result = client.query(f"""
        SELECT
            COUNTIF(NOT is_active) AS expired_count,
            COUNT(*) AS total_count,
            SAFE_DIVIDE(COUNTIF(NOT is_active), COUNT(*)) AS expire_rate
        FROM `{BQ_PROJECT}.{BQ_DATASET}.fact_job_offers`
    """).result()
    row = next(result)

    expire_rate   = row.expire_rate or 0.0
    expired_count = row.expired_count
    total_count   = row.total_count

    passed = expire_rate <= MAX_EXPIRE_RATE
    context["ti"].xcom_push(key="expire_rate_check", value={
        "passed": passed,
        "expire_rate": round(expire_rate, 4),
        "expired_count": expired_count,
        "total_count": total_count,
        "message": (
            f"OK — expire rate {expire_rate*100:.1f}% is within normal range."
            if passed
            else (
                f"FAIL — expire rate {expire_rate*100:.1f}% exceeds threshold "
                f"({MAX_EXPIRE_RATE*100:.0f}%). "
                f"({expired_count} expired / {total_count} total). "
                "Possible Redis state corruption or pipeline restart."
            )
        ),
    })
    log.info(
        "expire_rate_check: rate=%.2f%% expired=%d total=%d passed=%s",
        expire_rate * 100, expired_count, total_count, passed,
    )


def _log_dq_summary(**context) -> None:
    """
    Collect all check results via XCom, log a full report, and raise
    AirflowException if any check failed (marks the DAG run as failed).
    """
    ti = context["ti"]
    check_keys = [
        ("volume_check",      "volume_check"),
        ("null_rate_check",   "null_rate_check"),
        ("duplicate_check",   "duplicate_check"),
        ("expire_rate_check", "expire_rate_check"),
    ]

    results = {}
    for task_id, key in check_keys:
        results[task_id] = ti.xcom_pull(task_ids=task_id, key=key) or {}

    # Build report lines
    lines = []
    failed = []
    for name, r in results.items():
        icon = "✓" if r.get("passed") else "✗"
        lines.append(f"  {icon} {name:<25} {r.get('message', 'no result')}")
        if not r.get("passed"):
            failed.append(name)

    banner = (
        "\n" + "═" * 60 + "\n"
        "  DAILY DATA QUALITY REPORT\n" +
        "═" * 60 + "\n" +
        "\n".join(lines) + "\n" +
        "─" * 60 + "\n"
    )

    if failed:
        banner += f"  RESULT: FAILED ({', '.join(failed)})\n" + "═" * 60
        log.error(banner)
        raise AirflowException(
            f"Data quality checks FAILED: {', '.join(failed)}. "
            "See task log for details."
        )

    banner += "  RESULT: ALL CHECKS PASSED\n" + "═" * 60
    log.info(banner)


with DAG(
    dag_id="dag_data_quality",
    description="Daily data quality checks: volume, nulls, duplicates, expire anomaly.",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["quality", "monitoring", "daily"],
    default_args={
        "owner": "data-engineering",
        "retries": 0,       
        "email_on_failure": False,
        "execution_timeout": timedelta(minutes=15),
    },
) as dag:

    volume_check = PythonOperator(
        task_id="volume_check",
        python_callable=_volume_check,
        doc_md=f"Check stg_job_offers has ≥ {MIN_STAGING_ROWS} rows.",
    )

    null_rate_check = PythonOperator(
        task_id="null_rate_check",
        python_callable=_null_rate_check,
        doc_md=f"Check critical fact_job_offers columns are < {MAX_NULL_RATE*100:.0f}% NULL.",
    )

    duplicate_check = PythonOperator(
        task_id="duplicate_check",
        python_callable=_duplicate_check,
        doc_md="Check no duplicate offer_ids exist in fact_job_offers.",
    )

    expire_rate_check = PythonOperator(
        task_id="expire_rate_check",
        python_callable=_expire_rate_check,
        doc_md=f"Check EXPIRE rate is < {MAX_EXPIRE_RATE*100:.0f}% of total offers.",
    )

    log_dq_summary = PythonOperator(
        task_id="log_dq_summary",
        python_callable=_log_dq_summary,
        trigger_rule="all_done",   # run even if upstream tasks fail
        doc_md=(
            "Collect all check results, log a full report, and raise "
            "AirflowException if any check failed."
        ),
    )

    # Checks run in parallel; summary waits for all of them
    [volume_check, null_rate_check, duplicate_check, expire_rate_check] >> log_dq_summary
