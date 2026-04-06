"""
dag_spark_streaming.py
======================
Manages the Spark Structured Streaming job lifecycle.

The Spark job is long-running (it never terminates on its own — it calls
query.awaitTermination()). Airflow is NOT the right tool to run it directly
as a task because tasks are expected to complete.

Strategy: use BashOperator to submit the job via spark-submit inside the
existing `spark` container using `docker exec`, then monitor it via a sensor.

Alternatively: use a simplified approach where Airflow checks if the job
is alive and restarts it if not (watchdog pattern).

This DAG implements the WATCHDOG pattern:
  1. check_spark_alive  — check if a spark-submit PID file exists and the
                          process is still running inside the spark container
  2. start_spark        — if not alive, spark-submit the streaming job
                          (non-blocking: redirects stdout to a log file)
  3. wait_for_startup   — TimeDeltaSensor: give Spark 60s to start consuming

Runs every 5 minutes so a crashed Spark job is restarted within 5 minutes.

NOTE: This approach requires the Docker socket to be mounted into Airflow,
OR the spark-submit command is run directly inside the Airflow worker if
the Spark binaries are available there. 

For simplicity in this setup, we exec into the spark container via
`docker exec` from BashOperator (requires /var/run/docker.sock mounted).

docker-compose.yaml changes needed:
  airflow-common volumes:
    - /var/run/docker.sock:/var/run/docker.sock

  airflow-common environment:
    SPARK_CONTAINER_NAME: french_it_job_market_de_project-spark-1
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.time_delta import TimeDeltaSensor

log = logging.getLogger(__name__)

# Name of the Spark container as created by docker compose
# (project name is the directory name, lower-cased, with hyphens)
SPARK_CONTAINER = "french_it_job_market_de_project-spark-1"
PID_FILE        = "/tmp/spark_streaming.pid"
SPARK_LOG       = "/tmp/spark_streaming.log"

# spark-submit command executed inside the spark container
SPARK_SUBMIT_CMD = """ \
/opt/spark/bin/spark-submit \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --driver-class-path /opt/spark/extra-jars/guava-32.1.3-jre.jar:\
/opt/spark/extra-jars/gcs-connector-hadoop3-2.2.22-shaded.jar:\
/opt/spark/extra-jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
  --conf spark.driver.extraClassPath=/opt/spark/extra-jars/* \
  --conf spark.executor.extraClassPath=/opt/spark/extra-jars/* \
  /opt/spark/work-dir/src/streaming/spark_streaming_job.py \
"""


def _is_spark_alive(**context) -> bool:
    """
    Returns True  → Spark is alive, ShortCircuit skips start_spark.
    Returns False → Spark is NOT alive, pipeline continues to start_spark.
    """
    import subprocess
    # Check if the PID file exists in the container and the process is alive
    result = subprocess.run(
        [
            "docker", "exec", SPARK_CONTAINER,
            "bash", "-c",
            f"[ -f {PID_FILE} ] && kill -0 $(cat {PID_FILE}) 2>/dev/null && echo ALIVE || echo DEAD",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    status = result.stdout.strip()
    log.info("Spark process status: %s", status)

    if status == "ALIVE":
        log.info("Spark streaming job is already running — nothing to do.")
        return False   # short-circuit: skip start_spark + sensor

    log.info("Spark streaming job is NOT running — will start it.")
    return True        # continue to start_spark


def _check_spark_started(**context) -> None:
    """After the sensor wait, verify the PID file was created."""
    import subprocess
    result = subprocess.run(
        [
            "docker", "exec", SPARK_CONTAINER,
            "bash", "-c", f"cat {PID_FILE}",
        ],
        capture_output=True, text=True, timeout=10,
    )
    pid = result.stdout.strip()
    if not pid:
        raise RuntimeError(
            f"Spark streaming job did not write a PID file to {PID_FILE}. "
            f"Check {SPARK_LOG} inside the spark container for errors."
        )
    log.info("Spark streaming job started with PID %s", pid)


with DAG(
    dag_id="dag_spark_streaming",
    description=(
        "Watchdog: checks if the Spark Structured Streaming job is alive; "
        "restarts it if not. Runs every 5 minutes."
    ),
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "streaming", "watchdog"],
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "email_on_failure": False,
        "execution_timeout": timedelta(minutes=4),
    },
) as dag:

    # ── Task 1: Check if Spark is alive ──────────────────────────────────────
    check_spark_alive = ShortCircuitOperator(
        task_id="check_spark_alive",
        python_callable=_is_spark_alive,
        doc_md=(
            "Check if the spark-submit process is running in the spark container.\n"
            "Short-circuits (skips remaining tasks) if Spark is already alive."
        ),
    )

    # ── Task 2: Start the Spark streaming job (non-blocking) ─────────────────
    # We redirect stdout/stderr to a log file and write the PID so we can
    # check liveness in subsequent DAG runs.
    start_spark = BashOperator(
        task_id="start_spark",
        bash_command=(
            f"docker exec -d {SPARK_CONTAINER} bash -c \""
            f"nohup {SPARK_SUBMIT_CMD.strip()} "
            f"> {SPARK_LOG} 2>&1 & echo \\$! > {PID_FILE}\""
        ),
        append_env=True,
        doc_md=(
            "Submit the Spark Structured Streaming job inside the spark container.\n"
            "Non-blocking: job runs in the background. PID written to "
            f"`{PID_FILE}` inside the container."
        ),
    )

    # ── Task 3: Wait 60 seconds for Spark to initialise ──────────────────────
    wait_for_startup = TimeDeltaSensor(
        task_id="wait_for_startup",
        delta=timedelta(seconds=60),
        doc_md="Give Spark 60 seconds to start consuming before verifying the PID.",
    )

    # ── Task 4: Confirm PID file exists ──────────────────────────────────────
    verify_started = PythonOperator(
        task_id="verify_started",
        python_callable=_check_spark_started,
        doc_md=f"Verify the PID file was created at {PID_FILE} inside the spark container.",
    )

    check_spark_alive >> start_spark >> wait_for_startup >> verify_started
