from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, DESCENDING

from config.settings import settings


def get_db():
    client = MongoClient(settings.mongo_url)
    return client[settings.database_name]


def get_latest_prices() -> list[dict]:
    """
    Latest price per country per product across both curated collections.
    Pulls the most recently ingested record for each unique
    country + product_type combination.
    """
    db = get_db()
    results = []

    for collection in ["fuel_prices", "electricity_prices"]:
        pipeline = [
            {"$sort": {"ingestion_timestamp": DESCENDING}},
            {
                "$group": {
                    "_id": {
                        "country":      "$country",
                        "product_type": "$product_type",
                    },
                    "price":               {"$first": "$price"},
                    "currency":            {"$first": "$currency"},
                    "unit":                {"$first": "$unit"},
                    "reporting_date":      {"$first": "$reporting_date"},
                    "ingestion_timestamp": {"$first": "$ingestion_timestamp"},
                    "source":              {"$first": "$source"},
                }
            },
            {
                "$project": {
                    "_id":             0,
                    "country":         "$_id.country",
                    "product_type":    "$_id.product_type",
                    "price":           1,
                    "currency":        1,
                    "unit":            1,
                    "reporting_date":  1,
                    "source":          1,
                }
            },
            {"$sort": {"country": 1}},
        ]
        results.extend(list(db[collection].aggregate(pipeline)))

    return results


def get_price_movers(top_n: int = 5) -> dict:
    """
    Compare the two most recent batches per country per product.
    Returns the top N countries with the biggest price increases
    and top N with the biggest decreases.

    If only one batch exists (first ever run), returns empty lists.
    """
    db = get_db()

    # Get the two most recent distinct reporting dates
    dates = db["fuel_prices"].distinct("reporting_date")
    dates = sorted(dates, reverse=True)

    if len(dates) < 2:
        return {"top_increases": [], "top_decreases": [], "note": "need at least 2 runs for movement data"}

    latest_date   = dates[0]
    previous_date = dates[1]

    movers = []

    for collection in ["fuel_prices", "electricity_prices"]:
        latest   = {
            f"{r['country']}|{r['product_type']}": r["price"]
            for r in db[collection].find({"reporting_date": latest_date})
        }
        previous = {
            f"{r['country']}|{r['product_type']}": r["price"]
            for r in db[collection].find({"reporting_date": previous_date})
        }

        for key, current_price in latest.items():
            if key in previous:
                prev_price = previous[key]
                if prev_price and prev_price != 0:
                    delta      = round(current_price - prev_price, 4)
                    pct_change = round((delta / prev_price) * 100, 2)
                    country, product = key.split("|")
                    movers.append({
                        "country":      country,
                        "product_type": product,
                        "previous":     prev_price,
                        "current":      current_price,
                        "delta":        delta,
                        "pct_change":   pct_change,
                    })

    movers_sorted = sorted(movers, key=lambda x: x["delta"], reverse=True)

    return {
        "top_increases": movers_sorted[:top_n],
        "top_decreases": movers_sorted[-top_n:][::-1],
        "compared_dates": {
            "latest":   latest_date,
            "previous": previous_date,
        }
    }


def get_pipeline_summary() -> dict:
    """
    Latest pipeline run status and data quality summary.
    """
    db = get_db()

    run = db["pipeline_runs"].find_one(
        sort=[("started_at", DESCENDING)]
    )

    quality = db["data_quality_checks"].find_one(
        {"batch_id": run["batch_id"]} if run else {},
        sort=[("checked_at", DESCENDING)]
    )

    return {
        "last_run": {
            "batch_id":    run.get("batch_id")    if run else None,
            "status":      run.get("status")      if run else None,
            "started_at":  run.get("started_at")  if run else None,
            "finished_at": run.get("finished_at") if run else None,
            "total_ingested": run.get("total_records_ingested") if run else 0,
        },
        "quality": {
            "total":         quality.get("total")         if quality else 0,
            "valid_count":   quality.get("valid_count")   if quality else 0,
            "invalid_count": quality.get("invalid_count") if quality else 0,
            "failures":      quality.get("failures")      if quality else {},
        }
    }


def get_price_history(country: str, product: str) -> list[dict]:
    """
    Returns daily price snapshots for a given country + product
    across all available reporting dates. Used for trend display.
    """
    db = get_db()

    collection = "electricity_prices" if product == "electricity" else "fuel_prices"

    records = list(db[collection].find(
        {"country": country, "product_type": product},
        {"_id": 0, "reporting_date": 1, "price": 1}
    ).sort("reporting_date", 1))

    return records


def get_all_dates() -> list[str]:
    """Returns all distinct reporting dates across fuel and electricity."""
    db = get_db()
    dates = set(db["fuel_prices"].distinct("reporting_date"))
    dates.update(db["electricity_prices"].distinct("reporting_date"))
    return sorted(dates)
