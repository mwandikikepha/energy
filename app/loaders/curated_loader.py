from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from config.settings import settings


PRODUCT_COLLECTION_MAP = {
    "gasoline":    "fuel_prices",
    "diesel":      "fuel_prices",
    "lpg":         "fuel_prices",
    "electricity": "electricity_prices",
}


def get_db():
    client = MongoClient(settings.mongo_url)
    return client[settings.database_name]


def load_curated(transformed_records: list[dict], validation_report: dict, batch_id: str) -> dict:
    """
    Upsert clean transformed records into the correct curated collection.
    Routes fuel products to fuel_prices, electricity to electricity_prices.

    Uses bulk upserts — one operation per collection, not one per record.
    Updates pipeline_runs with final status and counts when done.
    """
    db = get_db()

    # Group records by target collection
    collections: dict[str, list[dict]] = {}
    for record in transformed_records:
        col = PRODUCT_COLLECTION_MAP.get(record["product_type"])
        if col:
            collections.setdefault(col, []).append(record)

    upserted_counts = {}

    for col_name, records in collections.items():
        operations = [
            UpdateOne(
                {"_id": r["_id"]},
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        result = db[col_name].bulk_write(operations)
        upserted_counts[col_name] = result.upserted_count + result.modified_count

    # Save quality check report
    db["data_quality_checks"].insert_one({
        "batch_id":      batch_id,
        "checked_at":    datetime.now(timezone.utc).isoformat(),
        "total":         validation_report["total"],
        "valid_count":   validation_report["valid_count"],
        "invalid_count": validation_report["invalid_count"],
        "failures":      validation_report["failures"],
    })

    # Update pipeline run to completed
    summary = {
        "batch_id":        batch_id,
        "upserted_counts": upserted_counts,
        "total_valid":     validation_report["valid_count"],
        "total_invalid":   validation_report["invalid_count"],
    }

    db["pipeline_runs"].update_one(
        {"batch_id": batch_id},
        {"$set": {
            "status":      "success",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            **summary,
        }},
    )

    return summary