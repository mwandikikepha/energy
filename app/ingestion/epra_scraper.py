"""
Kenya fuel price scraper — EPRA official data.
Produces two sets of records:
  1. KES prices per town → stored in kenya_prices collection
  2. USD-converted Nairobi prices → merged into fuel_prices collection
     so Kenya appears in the global comparison table correctly
"""
from datetime import datetime, timezone

# Current EPRA cycle: May 15 – June 14, 2026
# Update FALLBACK_PRICES and CYCLE each month when EPRA publishes new prices
# Source: EPRA press release May 14, 2026
CYCLE = "2026-05-15/2026-06-14"

# Official KES prices per town
FALLBACK_PRICES = [
    {"town": "Nairobi",  "product_type": "gasoline", "price_kes": 214.25},
    {"town": "Nairobi",  "product_type": "diesel",   "price_kes": 242.92},
    {"town": "Nairobi",  "product_type": "kerosene", "price_kes": 152.78},
    {"town": "Mombasa",  "product_type": "gasoline", "price_kes": 211.09},
    {"town": "Mombasa",  "product_type": "diesel",   "price_kes": 239.64},
    {"town": "Mombasa",  "product_type": "kerosene", "price_kes": 149.49},
    {"town": "Nakuru",   "product_type": "gasoline", "price_kes": 213.15},
    {"town": "Nakuru",   "product_type": "diesel",   "price_kes": 242.33},
    {"town": "Nakuru",   "product_type": "kerosene", "price_kes": 152.21},
    {"town": "Eldoret",  "product_type": "gasoline", "price_kes": 213.97},
    {"town": "Eldoret",  "product_type": "diesel",   "price_kes": 243.15},
    {"town": "Eldoret",  "product_type": "kerosene", "price_kes": 153.03},
    {"town": "Kisumu",   "product_type": "gasoline", "price_kes": 213.97},
    {"town": "Kisumu",   "product_type": "diesel",   "price_kes": 243.14},
    {"town": "Kisumu",   "product_type": "kerosene", "price_kes": 153.03},
    {"town": "Mandera",  "product_type": "gasoline", "price_kes": 234.90},
    {"town": "Mandera",  "product_type": "diesel",   "price_kes": 265.10},
    {"town": "Mandera",  "product_type": "kerosene", "price_kes": 174.96},
    {"town": "Moyale",   "product_type": "gasoline", "price_kes": 229.10},
    {"town": "Moyale",   "product_type": "diesel",   "price_kes": 258.86},
    {"town": "Embu",     "product_type": "gasoline", "price_kes": 215.69},
    {"town": "Embu",     "product_type": "diesel",   "price_kes": 244.46},
    {"town": "Embu",     "product_type": "kerosene", "price_kes": 154.31},
]


# Current rate used in May 2026 EPRA computation: 130.08
USD_KES_RATE = 130.08


def fetch_kenya_prices(batch_id: str) -> list[dict]:
    """
    Returns official EPRA pump prices for Kenyan towns in KES.
    Stored in the kenya_prices collection.
    """
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()
    reporting_date      = datetime.now(timezone.utc).date().isoformat()

    records = []
    for item in FALLBACK_PRICES:
        records.append({
            "country":             "Kenya",
            "town":                item["town"],
            "product_type":        item["product_type"],
            "price":               item["price_kes"],
            "currency":            "kes",
            "unit":                "per_liter",
            "source":              "epra",
            "epra_cycle":          CYCLE,
            "ingestion_timestamp": ingestion_timestamp,
            "reporting_date":      reporting_date,
            "batch_id":            batch_id,
        })
    return records


def fetch_kenya_usd_prices(batch_id: str) -> list[dict]:
    """
    Returns Nairobi prices converted to USD.
    These get merged into the main fuel_prices collection so Kenya
    appears correctly in global comparisons alongside Tanzania, Uganda etc.
    Only Nairobi prices used — most representative for global comparison.
    """
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()
    reporting_date      = datetime.now(timezone.utc).date().isoformat()

    nairobi = [p for p in FALLBACK_PRICES if p["town"] == "Nairobi"]
    records = []

    for item in nairobi:
        price_usd = round(item["price_kes"] / USD_KES_RATE, 4)
        records.append({
            "country":             "Kenya",
            "product_type":        item["product_type"],
            "price":               price_usd,
            "currency":            "usd",
            "unit":                "per_liter",
            "source":              "epra",
            "epra_cycle":          CYCLE,
            "usd_kes_rate":        USD_KES_RATE,
            "ingestion_timestamp": ingestion_timestamp,
            "reporting_date":      reporting_date,
            "batch_id":            batch_id,
        })
    return records


if __name__ == "__main__":
    kes_records = fetch_kenya_prices(batch_id="test")
    usd_records = fetch_kenya_usd_prices(batch_id="test")

    print(f"KES records (all towns): {len(kes_records)}")
    for r in kes_records:
        print(f"  {r['town']:10} {r['product_type']:10} KES {r['price']}")

    print(f"\nUSD records (Nairobi only, for global table): {len(usd_records)}")
    for r in usd_records:
        print(f"  {r['country']:10} {r['product_type']:10} USD {r['price']} (rate: {r['usd_kes_rate']})")
