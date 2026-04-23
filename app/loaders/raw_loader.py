from datetime import datetime, timezone
from pymongo import MongoClient
from config.settings import settings


def get_db():
    client = MongoClient(settings.mongo_url)
    return client[settings.database_name]


def load_raw(records: list[dict], run_metadata: dict) -> dict:
   
    db = get_db()

    # Write the pipeline run document first so it exists even if insert fails
    db["pipeline_runs"].insert_one({
        **run_metadata,
        "total_records_ingested": len(records),
    })

    if not records:
        return {"batch_id": run_metadata["batch_id"], "inserted": 0}

    result = db["raw_api_data"].insert_many(records)

    return {
        "batch_id": run_metadata["batch_id"],
        "inserted": len(result.inserted_ids),
    }