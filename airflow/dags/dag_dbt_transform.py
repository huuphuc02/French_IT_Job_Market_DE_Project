"""
dag_dbt_transform.py
====================
Runs hourly to transform BigQuery staging tables into the dimensional model
and reporting marts via dbt.

Pipeline order (respects dependency graph):
  1. check_new_data    — fail fast if staging tables have zero rows
  2. dbt_seed          — load dept_region.csv reference seed
  3. dbt_staging       — stg_job_offers_clean, stg_job_skills_clean, stg_offer_expirations_clean
  4. dbt_intermediate  — int_latest_offer_record, int_offer_expiration_latest,
                         int_offer_lifecycle, int_active_offers
  5. dbt_marts         — dim_*, fact_job_offers, fact_job_skills
  6. dbt_reporting     — marts/reporting/* (mart_salary_analysis, mart_skill_demand, etc.)
  7. dbt_test          — run all schema tests
  8. log_run_summary   — log row counts from key tables

dbt is run via BashOperator inside the Airflow worker.
dbt Cloud project: french_it_job_de_project
dbt profile:       french_it_job_de_project  (reads ~/.dbt/profiles.yml inside container)

The dbt project is NOT mounted into the Airflow container by default.
Two options:
  A) Mount ./dbt into the Airflow container and run dbt locally.
  B) Call the dbt Cloud API (if using dbt Cloud).

This DAG uses Option A (local dbt run):
  Add to docker-compose.yaml airflow volumes:
    - ./dbt:/opt/airflow/dbt
  And to _PIP_ADDITIONAL_REQUIREMENTS:
    dbt-bigquery
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

DBT_DIR   = "/opt/airflow/dbt"
DBT_CMD   = f"dbt --no-write-json"
DBT_FLAGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"


def _check_new_data(**context) -> None:
    """
    Fail quickly if the staging source tables are completely empty.
    An empty stg_job_offers means Spark hasn't written anything yet —
    running dbt on empty tables would produce empty marts (no error, but wasted work).
    """
    from google.cloud import bigquery
    import os

    project = os.getenv("BQ_PROJECT", "basic-decoder-415403")
    dataset = os.getenv("BQ_DATASET", "french_it_jobs")
    client  = bigquery.Client(project=project)

    query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{project}.{dataset}.stg_job_offers`
    """
    result    = client.query(query).result()
    row_count = next(result).row_count

    if row_count == 0:
        raise ValueError(
            f"`{dataset}.stg_job_offers` has 0 rows. "
            "Ensure the Spark streaming job has run and written data before triggering dbt."
        )
    log.info("stg_job_offers has %d rows — proceeding with dbt run.", row_count)
    context["ti"].xcom_push(key="staging_row_count", value=row_count)


def _log_run_summary(**context) -> None:
    """Query row counts from the key output tables and log a summary banner."""
    from google.cloud import bigquery
    import os

    project = os.getenv("BQ_PROJECT", "basic-decoder-415403")
    dataset = os.getenv("BQ_DATASET", "french_it_jobs")
    client  = bigquery.Client(project=project)

    tables = [
        "fact_job_offers",
        "fact_job_skills",
        "mart_salary_analysis",
        "mart_skill_demand",
        "mart_geographical_overview",
        "mart_offer_lifecycle",
        "mart_contract_type_distribution",
    ]

    rows = {}
    for table in tables:
        try:
            result    = client.query(
                f"SELECT COUNT(*) AS n FROM `{project}.{dataset}.{table}`"
            ).result()
            rows[table] = next(result).n
        except Exception as exc:
            rows[table] = f"ERROR: {exc}"

    lines = [f"  {t:<40} {str(rows[t]):>10}" for t in tables]
    banner = (
        "\n" + "═" * 55 + "\n"
        "  DBT TRANSFORM DAG — COMPLETED\n" +
        "═" * 55 + "\n" +
        "\n".join(lines) + "\n" +
        "═" * 55
    )
    log.info(banner)


with DAG(
    dag_id="dag_dbt_transform",
    description="Hourly dbt run: staging → intermediate → marts → reporting → tests.",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["transform", "dbt", "hourly"],
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "execution_timeout": timedelta(minutes=30),
    },
) as dag:

    check_new_data = PythonOperator(
        task_id="check_new_data",
        python_callable=_check_new_data,
        doc_md="Fail early if stg_job_offers is empty — nothing to transform yet.",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_CMD} deps {DBT_FLAGS}",
        env={"DBT_PROJECT_DIR": DBT_DIR},
        append_env=True,
        doc_md="Install dbt packages from packages.yml",
    )

    # Seeds must run first: dim_location depends on the dept_region reference seed.
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_CMD} seed {DBT_FLAGS}",
        env={"DBT_PROJECT_DIR": DBT_DIR},
        append_env=True,
        doc_md="Load `dept_region.csv` reference data into BigQuery.",
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS} --select staging",
        env={"DBT_PROJECT_DIR": DBT_DIR},
        append_env=True,
        doc_md=(
            "Run staging models:\n"
            "- stg_job_offers_clean\n"
            "- stg_job_skills_clean\n"
            "- stg_offer_expirations_clean\n"
        ),
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS} --select intermediate",
        env={"DBT_PROJECT_DIR": DBT_DIR},
        append_env=True,
        doc_md=(
            "Run intermediate models:\n"
            "- int_latest_offer_record\n"
            "- int_offer_expiration_latest\n"
            "- int_offer_lifecycle\n"
            "- int_active_offers\n"
        ),
    )

    # Run dim_* and fact_* but EXCLUDE the reporting sub-folder (separate task below)
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=(
            f"{DBT_CMD} run {DBT_FLAGS} "
            "--select marts --exclude marts.reporting"
        ),
        env={"DBT_PROJECT_DIR": DBT_DIR},
        append_env=True,
        doc_md=(
            "Run mart models (Star Schema):\n"
            "- dim_date, dim_location, dim_company, dim_rome\n"
            "- fact_job_offers, fact_job_skills\n"
        ),
    )

    dbt_reporting = BashOperator(
        task_id="dbt_reporting",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS} --select marts.reporting",
        env={"DBT_PROJECT_DIR": DBT_DIR},
        append_env=True,
        doc_md=(
            "Run reporting marts (serve Looker Studio):\n"
            "- mart_salary_analysis\n"
            "- mart_skill_demand\n"
            "- mart_geographical_overview\n"
            "- mart_offer_lifecycle\n"
            "- mart_contract_type_distribution\n"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test {DBT_FLAGS}",
        env={"DBT_PROJECT_DIR": DBT_DIR},
        append_env=True,
        doc_md="Run all dbt schema tests (not_null, unique, accepted_values, relationships).",
    )

    log_run_summary = PythonOperator(
        task_id="log_run_summary",
        python_callable=_log_run_summary,
        doc_md="Query row counts from key output tables and log a summary banner.",
    )

    (
        check_new_data
        >> dbt_deps
        >> dbt_seed
        >> dbt_staging
        >> dbt_intermediate
        >> dbt_marts
        >> dbt_reporting
        >> dbt_test
        >> log_run_summary
    )
