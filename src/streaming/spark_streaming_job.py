import datetime
from datetime import timezone
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DoubleType, IntegerType
)
from streaming.schemas import KAFKA_EVENT_SCHEMA
from streaming.skill_extractor import extract_skills
from streaming.salary_parser import parse_salary
from streaming.location_enricher import parse_location
from pyspark.sql.functions import from_json, col

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka:9092")
GCS_BUCKET       = os.getenv("GCS_BUCKET",        "gs://french-it-jobs-lake")
BQ_PROJECT       = os.getenv("BQ_PROJECT",        "basic-decoder-415403")
BQ_DATASET       = os.getenv("BQ_DATASET",        "french_it_jobs")
BQ_TEMP_BUCKET   = os.getenv("BQ_TEMP_BUCKET",    "french-it-jobs-lake")
CHECKPOINT_DIR   = os.getenv("CHECKPOINT_DIR",    "/tmp/spark-checkpoints/main_stream")
GCS_KEY_FILE     = os.getenv("GCS_KEY_FILE",      "./keys/mycreds.json")
ENABLE_CLOUD_SINK = os.getenv("ENABLE_CLOUD_SINK", "").lower() in ("1", "true", "yes")


# ──────────────────────────────────────────────────────────────────────────────
# Explicit schemas — prevents CANNOT_DETERMINE_TYPE when all-None columns appear
# in a batch, and gives BigQuery a stable schema on every write.
# ──────────────────────────────────────────────────────────────────────────────

OFFER_SCHEMA = StructType([
    StructField("offer_id",             StringType(),  True),
    StructField("event_type",           StringType(),  True),
    StructField("ingested_at",          StringType(),  True),
    StructField("is_active",            BooleanType(), True),
    # Core
    StructField("title",                StringType(),  True),
    StructField("date_created",         StringType(),  True),
    StructField("date_updated",         StringType(),  True),
    StructField("description",          StringType(),  True),
    # Location
    StructField("commune_code",         StringType(),  True),
    StructField("commune_name",         StringType(),  True),
    StructField("code_postal",          StringType(),  True),
    StructField("dept_code",            StringType(),  True),
    StructField("latitude",             DoubleType(),  True),
    StructField("longitude",            DoubleType(),  True),
    # Company
    StructField("company_name",         StringType(),  True),
    StructField("company_size",         StringType(),  True),
    StructField("naf_code",             StringType(),  True),
    StructField("sector_code",          StringType(),  True),
    StructField("sector_label",         StringType(),  True),
    # Classification
    StructField("rome_code",            StringType(),  True),
    StructField("rome_label",           StringType(),  True),
    StructField("appellation_label",    StringType(),  True),
    # Contract
    StructField("contract_type",        StringType(),  True),
    StructField("contract_type_label",  StringType(),  True),
    StructField("work_duration",        StringType(),  True),
    StructField("is_alternance",        BooleanType(), True),
    # Experience
    StructField("experience_required",  StringType(),  True),
    StructField("experience_label",     StringType(),  True),
    StructField("qualification_label",  StringType(),  True),
    # Salary
    StructField("salary_text",          StringType(),  True),
    StructField("salary_min_annual",    DoubleType(),  True),
    StructField("salary_max_annual",    DoubleType(),  True),
    StructField("education_level",      StringType(),  True),
    # Other
    StructField("is_accessible_th",     BooleanType(), True),
    StructField("num_positions",        IntegerType(), True),
])

SKILL_SCHEMA = StructType([
    StructField("offer_id",          StringType(), True),
    StructField("skill_label",       StringType(), True),
    StructField("exigence_level",    StringType(), True),
    StructField("extraction_source", StringType(), True),
    StructField("ingested_at",       StringType(), True),
])

EXPIRE_SCHEMA = StructType([
    StructField("offer_id",    StringType(), True),
    StructField("expired_at",  StringType(), True),
    StructField("ingested_at", StringType(), True),
])


# ──────────────────────────────────────────────────────────────────────────────
# Spark session
# ──────────────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    b = SparkSession.builder \
        .appName("french-it-jobs-streaming") \
        .config("spark.sql.streaming.schemaInference", "true")
    if ENABLE_CLOUD_SINK:
        b = b \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCS_KEY_FILE) \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config(
                "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            )
    return b.getOrCreate()


# ──────────────────────────────────────────────────────────────────────────────
# Enrichment
# ──────────────────────────────────────────────────────────────────────────────

def enrich_offer(offer: dict, event_type: str, ingested_at: str) -> dict:
    salary   = parse_salary(offer.get("salaire", {}).get("libelle"))
    location = parse_location(offer.get("lieuTravail", {}))

    return {
        "offer_id":             offer.get("id"),
        "event_type":           event_type,
        "ingested_at":          ingested_at,
        "is_active":            True,
        # Core
        "title":                offer.get("intitule"),
        "date_created":         offer.get("dateCreation"),
        "date_updated":         offer.get("dateActualisation"),
        "description":          offer.get("description"),
        # Location
        "commune_code":         location["commune_code"],
        "commune_name":         location["commune_name"],
        "code_postal":          location["code_postal"],
        "dept_code":            location["dept_code"],
        "latitude":             location["latitude"],
        "longitude":            location["longitude"],
        # Company
        "company_name":         offer.get("entreprise", {}).get("nom"),
        "company_size":         offer.get("trancheEffectifEtab"),
        "naf_code":             offer.get("codeNAF"),
        "sector_code":          offer.get("secteurActivite"),
        "sector_label":         offer.get("secteurActiviteLibelle"),
        # Classification
        "rome_code":            offer.get("romeCode"),
        "rome_label":           offer.get("romeLibelle"),
        "appellation_label":    offer.get("appellationlibelle"),
        # Contract
        "contract_type":        offer.get("typeContrat"),
        "contract_type_label":  offer.get("typeContratLibelle"),
        "work_duration":        offer.get("dureeTravailLibelleConverti"),
        "is_alternance":        offer.get("alternance"),
        # Experience
        "experience_required":  offer.get("experienceExige"),
        "experience_label":     offer.get("experienceLibelle"),
        "qualification_label":  offer.get("qualificationLibelle"),
        # Salary
        "salary_text":          salary.raw or None,
        "salary_min_annual":    salary.annual_min or None,
        "salary_max_annual":    salary.annual_max or None,
        "education_level":      ((offer.get("formations") or [{}])[0]).get("niveauLibelle"),
        # Other
        "is_accessible_th":     offer.get("accessibleTH"),
        "num_positions":        offer.get("nombrePostes"),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Batch processor
# ──────────────────────────────────────────────────────────────────────────────

def process_batch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return

    now       = datetime.datetime.now(timezone.utc).isoformat()
    date_path = datetime.datetime.now(timezone.utc).strftime("year=%Y/month=%m/day=%d")
    spark     = SparkSession.getActiveSession()

    offer_rows, skill_rows, expire_rows = [], [], []

    for row in batch_df.collect():
        event_type = row["event_type"]
        offer_id   = row["offer_id"]

        if event_type in ("INSERT", "UPDATE"):
            if row["offer"] is None:
                print(f"Skipping offer_id={offer_id}: 'offer' field is null (JSON parse failed)")
                continue

            offer_dict = row["offer"].asDict(recursive=True)
            offer_rows.append(enrich_offer(offer_dict, event_type, now))

            skills = extract_skills(offer_dict)
            for skill in skills.all_skills:
                skill_rows.append({
                    "offer_id":          offer_id,
                    "skill_label":       skill,
                    "exigence_level":    "E" if skill in skills.api_skills else "S",
                    "extraction_source": "api_competences" if skill in skills.api_skills else "description_regex",
                    "ingested_at":       now,
                })

        elif event_type == "EXPIRE":
            expire_rows.append({
                "offer_id":   offer_id,
                "expired_at": row["event_timestamp"],
                "ingested_at": now,
            })

    if ENABLE_CLOUD_SINK:
        batch_df.write.mode("append").json(f"{GCS_BUCKET}/raw/{date_path}")

        if offer_rows:
            df = spark.createDataFrame(offer_rows, schema=OFFER_SCHEMA)
            df.write.mode("append").parquet(f"{GCS_BUCKET}/processed/{date_path}/job_offers")
            df.write.format("bigquery") \
                .option("table", f"{BQ_PROJECT}:{BQ_DATASET}.stg_job_offers") \
                .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
                .mode("append").save()

        if skill_rows:
            df = spark.createDataFrame(skill_rows, schema=SKILL_SCHEMA)
            df.write.mode("append").parquet(f"{GCS_BUCKET}/processed/{date_path}/job_skills")
            df.write.format("bigquery") \
                .option("table", f"{BQ_PROJECT}:{BQ_DATASET}.stg_job_skills") \
                .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
                .mode("append").save()

        if expire_rows:
            spark.createDataFrame(expire_rows, schema=EXPIRE_SCHEMA) \
                .write.format("bigquery") \
                .option("table", f"{BQ_PROJECT}:{BQ_DATASET}.stg_offer_expirations") \
                .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
                .mode("append").save()

    else:
        batch_df.write.mode("append").json("output/batch_df")

        if offer_rows:
            spark.createDataFrame(offer_rows, schema=OFFER_SCHEMA) \
                .write.mode("append").format("csv") \
                .option("header", "true") \
                .save("output/offer_rows.csv")

        if expire_rows:
            spark.createDataFrame(expire_rows, schema=EXPIRE_SCHEMA) \
                .write.mode("append").format("csv") \
                .option("header", "true") \
                .save("output/expire_rows.csv")

        if skill_rows:
            spark.createDataFrame(skill_rows, schema=SKILL_SCHEMA) \
                .write.mode("append").format("csv") \
                .option("header", "true") \
                .save("output/skill_rows.csv")

    print(
        f"Batch {batch_id} — "
        f"offers={len(offer_rows)}, "
        f"skills={len(skill_rows)}, "
        f"expires={len(expire_rows)} processed successfully"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "it-job-offers-backfill,it-job-offers-cdc") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    df_parsed = df_raw \
        .select(from_json(col("value").cast("string"), KAFKA_EVENT_SCHEMA).alias("d")) \
        .select("d.*")

    query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()

# docker compose exec \
#   -e ENABLE_CLOUD_SINK=true \
#   -e GCS_KEY_FILE=/opt/spark/work-dir/keys/mycreds.json \
#   spark bash -c "
#     PYTHONPATH=/opt/spark/work-dir/src \
#     /opt/spark/bin/spark-submit \
#       --master 'local[2]' \
#       --driver-memory 1g \
#       --jars /opt/spark/extra-jars/guava-32.1.3-jre.jar,/opt/spark/extra-jars/gcs-connector-hadoop3-2.2.22-shaded.jar,/opt/spark/extra-jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
#       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
#       /opt/spark/work-dir/src/streaming/spark_streaming_job.py
#   "