# French IT Job Market Data Pipeline

A personal data engineering project that collects IT job offers in France from the [France Travail API](https://francetravail.io/data/api/offres-emploi), processes them with Kafka and Spark, models them in BigQuery with dbt, and visualises them in a [Looker Studio dashboard](https://datastudio.google.com/reporting/259bf2a2-5ba2-4e3b-b1dd-62e7d67bae26).

I built it to practice an end-to-end data engineering workflow: ingestion, change data capture, stream processing, warehouse modelling, orchestration and infrastructure as code. It runs on a single machine with Docker Compose and is a learning project. Some of the tools (Kafka, Spark) are more than the data volume strictly needs, and I chose them on purpose to learn how they fit together.

## Architecture

![Architecture Diagram](./tech_stack_architecture.png)

## Ingestion model: micro-batch

The France Travail API has no push or streaming interface, so the pipeline polls it. Airflow triggers a CDC job every 10 minutes, which compares the latest API results with the state stored in Redis and publishes the differences to Kafka. Spark Structured Streaming then reads those Kafka topics.

This means the data is near-real-time at best (10 minute micro-batches). Kafka and Spark do process events as a stream once they are published, but the freshness of the data is limited by the polling interval and the dbt schedule (hourly).

## What the pipeline does

1. **Backfill (once):** fetches all current offers for 12 ROME codes and publishes them to Kafka as `INSERT` events.
2. **CDC polling (every 10 min):** re-fetches offers per ROME code and compares them with Redis. New offers become `INSERT`, changed offers become `UPDATE`, and offers that disappeared from the API become `EXPIRE`.
3. **Stream processing (Spark):** reads the Kafka topics in 30 second micro-batches, enriches each offer (salary normalisation, location parsing, skill extraction), writes raw events to GCS and structured rows to BigQuery staging tables.
4. **Transformation (dbt, hourly):** staging, intermediate, star schema marts and reporting marts.
5. **Data quality (daily):** four custom checks in Airflow, plus dbt tests.

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.12, France Travail REST API |
| Message broker | Apache Kafka (Confluent 7.6, KRaft mode, single broker) |
| CDC state store | Redis 7.2 |
| Stream processing | Apache Spark 3.5 Structured Streaming (local mode) |
| Data lake | Google Cloud Storage |
| Data warehouse | Google BigQuery |
| Transformation | dbt-core with dbt-bigquery |
| Orchestration | Apache Airflow 2.9 |
| Infrastructure | Terraform (GCS bucket and BigQuery dataset only) |
| Visualisation | Looker Studio |
| Containerisation | Docker Compose |

## Project structure

```
.
├── src/
│   ├── producer/
│   │   ├── backfill.py          # Initial historical load to Kafka
│   │   ├── cdc.py               # Incremental CDC sync to Kafka
│   │   ├── ft_client.py         # France Travail API wrapper
│   │   └── config.py            # ROME codes, environment config
│   └── streaming/
│       ├── spark_streaming_job.py   # Spark Structured Streaming entry point
│       ├── skill_extractor.py       # Skill extraction (API data + regex)
│       ├── salary_parser.py         # Salary normalisation to annual EUR
│       ├── location_enricher.py     # Commune / department / region parsing
│       └── schemas.py               # Kafka event schema
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   │       ├── dim_*.sql, fact_*.sql
│   │       ├── schema.yml           # dbt tests
│   │       └── reporting/           # mart_* tables used by Looker Studio
│   ├── seeds/dept_region.csv        # Department to region mapping
│   └── packages.yml
├── airflow/dags/
│   ├── dag_backfill_initial.py      # One-shot historical load
│   ├── dag_cdc_ingest.py            # Every 10 min: CDC poll
│   ├── dag_dbt_transform.py         # Hourly: seed, staging, marts, test
│   └── dag_data_quality.py          # Daily quality checks
├── terraform/
│   ├── main.tf
│   └── variables.tf
├── docker-compose.yaml
├── requirements.txt
└── .env
```

## Data model

**Staging**

| Table | Description |
|---|---|
| `stg_job_offers` | Offers written by Spark from Kafka |
| `stg_job_skills` | Skills extracted per offer |
| `stg_offer_expirations` | EXPIRE events |

**Star schema**

| Table | Grain | Description |
|---|---|---|
| `dim_date` | 1 row per calendar date | Calendar dimension |
| `dim_location` | 1 row per commune and postal code | Location hierarchy |
| `dim_company` | 1 row per company | Employer attributes |
| `dim_rome` | 1 row per ROME code | Occupation classification |
| `fact_job_offers` | 1 row per offer | Offer facts and lifecycle |
| `fact_job_skills` | 1 row per offer and skill | Skill bridge table |

**Reporting marts** (read by Looker Studio)

| Mart | Content |
|---|---|
| `mart_salary_analysis` | Salary distributions by experience, region, ROME code, contract |
| `mart_skill_demand` | Top skills overall, by region, by ROME code, by contract |
| `mart_geographical_overview` | Offer counts by region, department, commune |
| `mart_offer_lifecycle` | How long offers stay online, by ROME code, contract, region |
| `mart_contract_type_distribution` | CDI / CDD / interim split by ROME code |

## Dashboard

[View the dashboard](https://datastudio.google.com/reporting/259bf2a2-5ba2-4e3b-b1dd-62e7d67bae26)

1. **Market overview and geography:** active and total offers, offers by department and region
2. **Skill demand:** top skills (required vs desired), skills by ROME code
3. **Salaries and contracts:** median annual salary by experience group, contract type distribution
4. **Offer lifetime:** how long offers stay online, by ROME code

## Running it locally

### Prerequisites

- Docker and Docker Compose
- A [France Travail API](https://francetravail.io) account (client ID and secret)
- A GCP project with a service account key (BigQuery and GCS permissions)
- Terraform 1.5 or later

### 1. Configure

```bash
git clone <repo-url>
cd French_IT_Job_Market_DE_Project
```

Create a `.env` file:

```bash
FT_CLIENT_ID=your_france_travail_client_id
FT_CLIENT_SECRET=your_france_travail_client_secret
AIRFLOW_UID=50000
```

Place your GCP service account key at `terraform/keys/mycreds.json`.

### 2. Provision GCP resources

```bash
cd terraform
terraform init
terraform apply -var="credentials=./keys/mycreds.json"
cd ..
```

This creates the GCS bucket `french-it-jobs-lake` and the BigQuery dataset `french_it_jobs`.

### 3. Start the services

```bash
docker compose up -d
docker compose ps
```

### 4. Run the backfill

```bash
docker compose --profile backfill up producer
```

Check the Redis state afterwards:

```bash
docker exec french_it_job_market_de_project-redis-1 redis-cli SCARD active_offer_ids
```

### 5. Start Spark Streaming

```bash
docker compose exec \
  -e ENABLE_CLOUD_SINK=true \
  -e GCS_KEY_FILE=/opt/spark/work-dir/keys/mycreds.json \
  spark bash -c "
    PYTHONPATH=/opt/spark/work-dir/src \
    /opt/spark/bin/spark-submit \
      --master 'local[2]' \
      --driver-memory 1g \
      --jars /opt/spark/extra-jars/guava-32.1.3-jre.jar,/opt/spark/extra-jars/gcs-connector-hadoop3-2.2.22-shaded.jar,/opt/spark/extra-jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      /opt/spark/work-dir/src/streaming/spark_streaming_job.py
  "
```

Logs:

```bash
docker exec french_it_job_market_de_project-spark-1 tail -f /tmp/spark_streaming.log
```

### 6. Enable the Airflow DAGs

Open the Airflow UI at http://localhost:8081 (local development credentials are `admin` / `admin`, change them if you expose the service).

| DAG | Schedule | Purpose |
|---|---|---|
| `dag_backfill_initial` | once | Historical load (already done in step 4) |
| `dag_cdc_ingest` | every 10 min | Poll the API and publish CDC events |
| `dag_dbt_transform` | hourly | Run dbt models and tests |
| `dag_data_quality` | daily | Run quality checks |

Trigger the first dbt run manually:

```bash
docker exec french_it_job_market_de_project-airflow-scheduler-1 \
  airflow dags trigger dag_dbt_transform
```

### 7. Connect Looker Studio

Add the `french_it_jobs` BigQuery dataset as a data source and connect the five `mart_*` tables.

## Data quality

**Daily checks in Airflow (`dag_data_quality`)**

| Check | Threshold |
|---|---|
| Volume | `stg_job_offers` has at least 100 rows |
| NULL rate | Less than 5% NULL on critical columns of `fact_job_offers` |
| Duplicates | No duplicate `offer_id` in `fact_job_offers` |
| EXPIRE anomaly | Expired offers below 10% of total (to catch a corrupted Redis state) |

**dbt tests:** `not_null`, `unique`, `accepted_values` and `relationships` on the dimension, fact and mart models.

```bash
docker exec french_it_job_market_de_project-airflow-scheduler-1 \
  bash -c "cd /opt/airflow/dbt && dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
```

## ROME codes covered

| Code | Domain |
|---|---|
| M1801 | Network administrator |
| M1802 | Systems engineer |
| M1803 | IT director (DSI) |
| M1804 | Telecom engineer |
| M1805 | Web developer |
| M1806 | IT consultant |
| M1810 | IT technician |
| M1811 | Data engineer |
| M1812 | Information security manager (RSSI) |
| M1813 | Business software integrator |
| M1405 | Data scientist |
| E1206 | UI / UX designer |

## Environment variables

| Variable | Where set | Description |
|---|---|---|
| `FT_CLIENT_ID` | `.env` | France Travail API client ID |
| `FT_CLIENT_SECRET` | `.env` | France Travail API client secret |
| `AIRFLOW_UID` | `.env` | UID for Airflow containers (use `id -u`) |
| `GOOGLE_APPLICATION_CREDENTIALS` | docker-compose | Path to GCP service account JSON |
| `ENABLE_CLOUD_SINK` | spark-submit env | `true` to write to GCS and BigQuery |
| `KAFKA_BOOTSTRAP` | docker-compose | Kafka broker address |
| `REDIS_HOST` / `REDIS_PORT` | docker-compose | Redis connection |
