import sys
import os

project_root = os.environ.get("PYTHONPATH", "").split(":")[0]
if not project_root:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import asyncio
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":            "kepha_energy",
    "retries":          3,
    "retry_delay":      300,
    "email_on_failure": False,
}


def ingest(**context):
    from app.ingestion.ingestion_metadata import get_run_metadata
    from app.ingestion.collectapi_client import fetch_all_fuel_prices
    from app.ingestion.electricity_scraper import fetch_electricity_prices
    from app.ingestion.epra_scraper import fetch_kenya_prices, fetch_kenya_usd_prices

    run_meta            = get_run_metadata()
    fuel_records        = fetch_all_fuel_prices(batch_id=run_meta["batch_id"])
    electricity_records = asyncio.run(
        fetch_electricity_prices(batch_id=run_meta["batch_id"])
    )
    kenya_kes_records   = fetch_kenya_prices(batch_id=run_meta["batch_id"])
    kenya_usd_records   = fetch_kenya_usd_prices(batch_id=run_meta["batch_id"])

    all_records = fuel_records + electricity_records
    print(f"Ingested {len(fuel_records)} fuel + {len(electricity_records)} electricity = {len(all_records)} total")
    print(f"Kenya KES records: {len(kenya_kes_records)} | Kenya USD records: {len(kenya_usd_records)}")

    context["ti"].xcom_push(key="run_meta",           value=run_meta)
    context["ti"].xcom_push(key="all_records",        value=all_records)
    context["ti"].xcom_push(key="kenya_kes_records",  value=kenya_kes_records)
    context["ti"].xcom_push(key="kenya_usd_records",  value=kenya_usd_records)


def raw_load(**context):
    from app.loaders.raw_loader import load_raw

    ti          = context["ti"]
    run_meta    = ti.xcom_pull(task_ids="ingest", key="run_meta")
    all_records = ti.xcom_pull(task_ids="ingest", key="all_records")

    result = load_raw(all_records, run_meta)
    print(f"Raw load complete — inserted: {result['inserted']}")


def validate(**context):
    from app.validation.validator import validate as run_validation

    ti          = context["ti"]
    all_records = ti.xcom_pull(task_ids="ingest", key="all_records")
    report      = run_validation(all_records)

    print(f"Validation — total: {report['total']} | valid: {report['valid_count']} | invalid: {report['invalid_count']}")
    if report["invalid_count"] > 0:
        for rule, info in report["failures"].items():
            if info["count"] > 0:
                print(f"  {rule}: {info['count']} failures")

    context["ti"].xcom_push(key="validation_report", value=report)


def transform_and_load(**context):
    from app.transformation.transformer import transform
    from app.loaders.curated_loader import load_curated
    from pymongo import MongoClient
    from config.settings import settings

    ti                = context["ti"]
    report            = ti.xcom_pull(task_ids="validate", key="validation_report")
    run_meta          = ti.xcom_pull(task_ids="ingest",   key="run_meta")
    kenya_kes_records = ti.xcom_pull(task_ids="ingest",   key="kenya_kes_records")
    kenya_usd_records = ti.xcom_pull(task_ids="ingest",   key="kenya_usd_records")

    db = MongoClient(settings.mongo_url)[settings.database_name]

    clean_records = transform(report["valid_records"])
    summary       = load_curated(clean_records, report, run_meta["batch_id"])
    print(f"Global curated load — {summary['upserted_counts']}")

    if kenya_kes_records:
        db["kenya_prices"].insert_many(kenya_kes_records)
        print(f"Kenya KES records → kenya_prices: {len(kenya_kes_records)}")


    if kenya_usd_records:
        clean_kenya_usd = transform(kenya_usd_records)
        for r in clean_kenya_usd:
            db["fuel_prices"].update_one({"_id": r["_id"]}, {"$set": r}, upsert=True)
        print(f"Kenya USD records → fuel_prices: {len(clean_kenya_usd)}")

    print(f"Pipeline run {run_meta['batch_id']} finished successfully")


with DAG(
    dag_id="energy_pipeline",
    description="Daily global energy price ingestion pipeline",
    default_args=default_args,
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["energy", "ingestion"],
) as dag:

    t1_ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
    )

    t2_raw_load = PythonOperator(
        task_id="raw_load",
        python_callable=raw_load,
    )

    t3_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    t4_transform_and_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )

    t1_ingest >> t2_raw_load >> t3_validate >> t4_transform_and_load
