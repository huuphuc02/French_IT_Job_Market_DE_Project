import datetime
from datetime import timezone
import os
from pyspark.sql import SparkSession
from streaming.schemas import KAFKA_EVENT_SCHEMA
from streaming.skill_extractor import extract_skills
from streaming.salary_parser import parse_salary
from streaming.location_enricher import parse_location
from pyspark.sql.functions import from_json, col

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
GCS_BUCKET = os.getenv("GCS_BUCKET", "gs://french-it-jobs-lake")
BQ_PROJECT = os.getenv("BQ_PROJECT", "basic-decoder-415403")
BQ_DATASET = os.getenv("BQ_DATASET", "french_it_jobs")
BQ_TEMP_BUCKET = os.getenv("BQ_TEMP_BUCKET", "french-it-jobs-lake")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/main_stream")
GCS_KEY_FILE = os.getenv("GCS_KEY_FILE", "./keys/mycreds.json")
# Set ENABLE_CLOUD_SINK=true and pass gcs-connector + spark-bigquery on spark-submit for GCS/BQ.
ENABLE_CLOUD_SINK = os.getenv("ENABLE_CLOUD_SINK", "").lower() in ("1", "true", "yes")


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

def enrich_offer(offer: dict, event_type: str, ingested_at: str) -> dict:
    salary = parse_salary(offer.get("salaire", {}).get("libelle"))
    location = parse_location(offer.get("lieuTravail", {}))

    return {
        "offer_id": offer.get("id"),
        "event_type": event_type,
        "ingested_at": ingested_at,
        "is_active": True,
        # Core
        "title": offer.get("intitule"),
        "date_created": offer.get("dateCreation"),
        "date_updated": offer.get("dateActualisation"),
        "description": offer.get("description"),
        # Location
        "commune_code": location["commune_code"],
        "commune_name": location["commune_name"],
        "code_postal": location["code_postal"],
        "dept_code": location["dept_code"],
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        # Company
        "company_name": offer.get("entreprise", {}).get("nom"),
        "company_size": offer.get("trancheEffectifEtab"),
        "naf_code": offer.get("codeNAF"),
        "sector_code": offer.get("secteurActivite"),
        "sector_label": offer.get("secteurActiviteLibelle"),
        # Classification
        "rome_code": offer.get("romeCode"),
        "rome_label": offer.get("romeLibelle"),
        "appellation_label": offer.get("appellationlibelle"),
        # Contract
        "contract_type": offer.get("typeContrat"),
        "contract_type_label": offer.get("typeContratLibelle"),
        "work_duration": offer.get("dureeTravailLibelleConverti"),
        "is_alternance": offer.get("alternance"),
        # Experience
        "experience_required": offer.get("experienceExige"),
        "experience_label": offer.get("experienceLibelle"),
        "qualification_label": offer.get("qualificationLibelle"),
        # Salary
        "salary_text": salary.raw or None,
        "salary_min_annual": salary.annual_min or None,
        "salary_max_annual": salary.annual_max or None,
        "education_level": ((offer.get("formations") or [{}])[0]).get("niveauLibelle"),
        # Other
        "is_accessible_th": offer.get("accessibleTH"),
        "num_positions": offer.get("nombrePostes"),
    }

def process_batch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return
    
    now = datetime.datetime.now(timezone.utc).isoformat()
    date_path = datetime.datetime.now(timezone.utc).strftime("year=%Y/month=%m/day=%d")
    spark = SparkSession.getActiveSession()

    offer_rows, skill_rows, expire_rows = [], [], []

    for row in batch_df.collect():
        event_type = row['event_type']
        offer_id = row['offer_id']

        if event_type in ('INSERT', 'UPDATE'):
            if row['offer'] is None:
                print(f"Skipping offer_id={offer_id}: 'offer' field is null (JSON parse failed)")
                continue
            # row["offer"] is a Spark Row — convert to plain dict for Python functions
            offer_dict = row['offer'].asDict(recursive=True)
            offer_rows.append(enrich_offer(offer_dict, event_type, now))

            # Skills - one row per skill
            skills = extract_skills(offer_dict)
            for skill in skills.all_skills:
                skill_rows.append({
                    "offer_id": offer_id,
                    "skill_label": skill,
                    "exigence_level": "E" if skill in skills.api_skills else "S",
                    "extraction_source": "api_competences" if skill in skills.api_skills else "description_regex",
                    'ingested_at': now
                })
        elif event_type == 'EXPIRE':
            expire_rows.append({
                "offer_id": offer_id,
                "expired_at": row['event_timestamp'],
                "ingested_at": now
            })
    
    if ENABLE_CLOUD_SINK:
        batch_df.write.mode("append").json(f"{GCS_BUCKET}/raw/{date_path}")
        if offer_rows:
            df = spark.createDataFrame(offer_rows)
            df.write.mode("append").parquet(f"{GCS_BUCKET}/processed/{date_path}/job_offers")
            df.write.format("bigquery") \
                .option("table", f"{BQ_PROJECT}:{BQ_DATASET}.stg_job_offers") \
                .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
                .mode("append").save()
        if skill_rows:
            df = spark.createDataFrame(skill_rows)
            df.write.mode("append").parquet(f"{GCS_BUCKET}/processed/{date_path}/job_skills")
            df.write.format("bigquery") \
                .option("table", f"{BQ_PROJECT}:{BQ_DATASET}.stg_job_skills") \
                .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
                .mode("append").save()
        if expire_rows:
            spark.createDataFrame(expire_rows).write.format("bigquery") \
                .option("table", f"{BQ_PROJECT}:{BQ_DATASET}.stg_offer_expirations") \
                .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
                .mode("append").save()
    else:
        batch_df.write.mode("append").json("output/batch_df")
        if offer_rows:
            spark.createDataFrame(offer_rows).write.mode("append").format("csv").option(
                "header", "true"
            ).save("output/offer_rows.csv")
        if expire_rows:
            spark.createDataFrame(expire_rows).write.mode("append").format("csv").option(
                "header", "true"
            ).save("output/expire_rows.csv")
        if skill_rows:
            spark.createDataFrame(skill_rows).write.mode("append").format("csv").option(
                "header", "true"
            ).save("output/skill_rows.csv")

    print(f"Batch {batch_id} offers={len(offer_rows)}, skills={len(skill_rows)}, expires={len(expire_rows)} processed successfully")

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df_raw = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
        .option('subscribe', 'it-job-offers-backfill,it-job-offers-cdc') \
        .option('startingOffsets', 'earliest') \
        .option('failOnDataLoss', 'false') \
        .load()
    
    df_parsed = df_raw.select(from_json(col('value').cast('string'), KAFKA_EVENT_SCHEMA).alias('d')).select('d.*')

    query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .option('checkpointLocation', CHECKPOINT_DIR) \
        .trigger(processingTime='30 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()