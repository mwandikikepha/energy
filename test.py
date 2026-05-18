"""
Full pipeline integration test.
Run from project root:  uv run python test.py
"""
import asyncio
from app.ingestion.ingestion_metadata import get_run_metadata
from app.ingestion.collectapi_client import fetch_all_fuel_prices
from app.ingestion.electricity_scraper import fetch_electricity_prices
from app.ingestion.epra_scraper import fetch_kenya_prices, fetch_kenya_usd_prices
from app.loaders.raw_loader import load_raw
from app.validation.validator import validate
from app.transformation.transformer import transform
from app.loaders.curated_loader import load_curated
from pymongo import MongoClient
from config.settings import settings


async def run():
    print("\n" + "="*55)
    print("  ENERGY PLATFORM — FULL PIPELINE RUN")
    print("="*55)

    # ── Step 1: Generate run metadata ─────────────────────────
    print("\n[1/6] Generating run metadata...")
    run_meta = get_run_metadata()
    print(f"      batch_id   : {run_meta['batch_id']}")
    print(f"      started_at : {run_meta['started_at']}")

    # ── Step 2: Ingest ─────────────────────────────────────────
    print("\n[2/6] Ingesting data...")
    fuel_records        = fetch_all_fuel_prices(batch_id=run_meta["batch_id"])
    electricity_records = await fetch_electricity_prices(batch_id=run_meta["batch_id"])
    kenya_kes_records   = fetch_kenya_prices(batch_id=run_meta["batch_id"])
    kenya_usd_records   = fetch_kenya_usd_prices(batch_id=run_meta["batch_id"])
    all_records         = fuel_records + electricity_records

    print(f"      fuel records        : {len(fuel_records)}")
    print(f"      electricity records : {len(electricity_records)}")
    print(f"      total global        : {len(all_records)}")
    print(f"      kenya KES records   : {len(kenya_kes_records)} (all towns)")
    print(f"      kenya USD records   : {len(kenya_usd_records)} (Nairobi → fuel_prices)")

    # ── Step 3: Raw load ───────────────────────────────────────
    print("\n[3/6] Loading raw records to MongoDB...")
    raw_result = load_raw(all_records, run_meta)
    print(f"      inserted into raw_api_data : {raw_result['inserted']}")

    # ── Step 4: Validate ───────────────────────────────────────
    print("\n[4/6] Validating records...")
    report = validate(all_records)
    print(f"      total    : {report['total']}")
    print(f"      valid    : {report['valid_count']}")
    print(f"      invalid  : {report['invalid_count']}")
    if report["invalid_count"] > 0:
        print("      failures breakdown:")
        for rule, info in report["failures"].items():
            if info["count"] > 0:
                print(f"        {rule}: {info['count']} records")
                for s in info["samples"]:
                    print(f"          └─ {s}")

    # ── Step 5: Transform + curated load ──────────────────────
    print("\n[5/6] Transforming and loading curated records...")
    clean_records = transform(report["valid_records"])
    summary       = load_curated(clean_records, report, run_meta["batch_id"])
    print(f"      upserted counts : {summary['upserted_counts']}")
    print(f"      total valid     : {summary['total_valid']}")
    print(f"      total invalid   : {summary['total_invalid']}")

    # ── Step 6: Kenya EPRA load ────────────────────────────────
    print("\n[6/6] Loading Kenya EPRA records...")
    db = MongoClient(settings.mongo_url)[settings.database_name]

    # KES prices → kenya_prices collection
    db["kenya_prices"].insert_many(kenya_kes_records)
    print(f"      kenya_prices (KES)  : {len(kenya_kes_records)} records inserted")

    # USD prices → fuel_prices collection (overrides CollectAPI Kenya entry)
    clean_kenya_usd = transform(kenya_usd_records)
    for r in clean_kenya_usd:
        db["fuel_prices"].update_one({"_id": r["_id"]}, {"$set": r}, upsert=True)
    print(f"      fuel_prices (USD)   : {len(clean_kenya_usd)} Kenya records upserted")
    print(f"      source              : epra (replaces collectapi for Kenya)")

    print("\n" + "="*55)
    print("  PIPELINE COMPLETE")
    print(f"  batch_id : {run_meta['batch_id']}")
    print("="*55 + "\n")


if __name__ == "__main__":
    asyncio.run(run())
