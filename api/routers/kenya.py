from fastapi import APIRouter
from pymongo import MongoClient, DESCENDING
from config.settings import settings

router = APIRouter()


def get_db():
    client = MongoClient(settings.mongo_url)
    return client[settings.database_name]


@router.get("/kenya/prices")
def kenya_prices():
    """
    Latest official EPRA pump prices for Kenyan towns.
    Prices in KES per litre. Updated monthly around the 14th/15th.
    """
    db = get_db()
    pipeline = [
        {"$sort": {"ingestion_timestamp": DESCENDING}},
        {
            "$group": {
                "_id": {"town": "$town", "product_type": "$product_type"},
                "price":               {"$first": "$price"},
                "currency":            {"$first": "$currency"},
                "unit":                {"$first": "$unit"},
                "reporting_date":      {"$first": "$reporting_date"},
                "source":              {"$first": "$source"},
            }
        },
        {
            "$project": {
                "_id":          0,
                "town":         "$_id.town",
                "product_type": "$_id.product_type",
                "price":        1,
                "currency":     1,
                "unit":         1,
                "reporting_date": 1,
                "source":       1,
            }
        },
        {"$sort": {"town": 1, "product_type": 1}}
    ]

    records = list(db["kenya_prices"].aggregate(pipeline))
    return {
        "country":  "Kenya",
        "currency": "KES",
        "source":   "EPRA (Energy and Petroleum Regulatory Authority)",
        "total":    len(records),
        "records":  records,
    }
