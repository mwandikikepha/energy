
import sys
import os

# I added this when running locally. Has no effect in production.
sys.path.insert(0, os.path.abspath("/home/kepha/energy_platform"))

import asyncio
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    "owner":            "kepha_energy",
    "retries":          3,
    "retry_delay":      300,  
    "email_on_failure": False,
}



def ingest(**context):
  
   # Generate batch metadata, fetch fuel + electricity records,   combine into a single list, push to XCom for next task.
  
   
    from app.ingestion.ingestion_metadata import get_run_metadata
    from app.ingestion.collectapi_client import fetch_all_fuel_prices
    from app.ingestion.electricity_scraper import fetch_electricity_prices

    run_meta            = get_run_metadata()
    fuel_records        = fetch_all_fuel_prices(batch_id=run_meta["batch_id"])
    electricity_records = asyncio.run(
        fetch_electricity_prices(batch_id=run_meta["batch_id"])
    )
    all_records = fuel_records + electricity_records

    print(f"Ingested {len(fuel_records)} fuel + {len(electricity_records)} electricity = {len(all_records)} total")

    context["ti"].xcom_push(key="run_meta",    value=run_meta)
    context["ti"].xcom_push(key="all_records", value=all_records)


def raw_load(**context):
    
   # Pull ingested records from XCom and insert into raw_api_data.
   
    from app.loaders.raw_loader import load_raw

    ti          = context["ti"]
    run_meta    = ti.xcom_pull(task_ids="ingest", key="run_meta")
    all_records = ti.xcom_pull(task_ids="ingest", key="all_records")

    result = load_raw(all_records, run_meta)
    print(f"Raw load complete — inserted: {result['inserted']}")


def validate(**context):
  
    # Pull raw records from XCom, run all quality rules,  push validation report forward.
   
    from app.validation.validator import validate as run_validation

    ti          = context["ti"]
    all_records = ti.xcom_pull(task_ids="ingest", key="all_records")

    report = run_validation(all_records)

    print(f"Validation — total: {report['total']} | valid: {report['valid_count']} | invalid: {report['invalid_count']}")
    if report["invalid_count"] > 0:
        for rule, info in report["failures"].items():
            if info["count"] > 0:
                print(f"  {rule}: {info['count']} failures")

    context["ti"].xcom_push(key="validation_report", value=report)


def transform_and_load(**context):
  
    #Transform valid records and upsert into curated collections.
 
    from app.transformation.transformer import transform
    from app.loaders.curated_loader import load_curated

    ti      = context["ti"]
    report  = ti.xcom_pull(task_ids="validate", key="validation_report")
    run_meta = ti.xcom_pull(task_ids="ingest",   key="run_meta")

    clean_records = transform(report["valid_records"])
    summary       = load_curated(clean_records, report, run_meta["batch_id"])

    print(f"Curated load complete — {summary['upserted_counts']}")
    print(f"Pipeline run {run_meta['batch_id']} finished successfully")



with DAG(
    dag_id="energy_pipeline",
    description="Daily global energy price ingestion pipeline",
    default_args=default_args,
    schedule="0 6 * * *",  # every day at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,                 
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
