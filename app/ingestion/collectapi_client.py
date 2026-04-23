import http.client
import json
from datetime import datetime, timezone

from config.settings import settings


COLLECTAPI_HOST = "api.collectapi.com"

ENDPOINTS = {
    "gasoline": "/gasPrice/otherCountriesGasoline",
    "diesel":   "/gasPrice/otherCountriesDiesel",
    "lpg":      "/gasPrice/otherCountriesLpg",
}

UNITS = {
    "gasoline": "per_liter",
    "diesel":   "per_liter",
    "lpg":      "per_liter",
}

COUNTRY_NAME_MAP = {
    "Dom. Rep.":          "Dominican Republic",
    "N. Maced.":          "North Macedonia",
    "Bosnia & Herz.":     "Bosnia and Herzegovina",
    "Trinidad & Tobago":  "Trinidad and Tobago",
    "C. Afr. Rep.":       "Central African Republic",
    "DR Congo":           "Democratic Republic of the Congo",
    "Burma":              "Myanmar",
    "Ivory Coast":        "Côte d'Ivoire",
    "Swaziland":          "Eswatini",
    "UAE":                "United Arab Emirates",
    "UK":                 "United Kingdom",
    "USA":                "United States",
}


def fetch_product(product: str) -> list[dict]:
    headers = {
        "content-type": "application/json",
        "authorization": f"apikey {settings.collect_api}",
    }

    conn = http.client.HTTPSConnection(COLLECTAPI_HOST, timeout=30)
    conn.request("GET", ENDPOINTS[product], headers=headers)
    res = conn.getresponse()
    raw = res.read().decode("utf-8")
    conn.close()

    if res.status != 200:
        raise RuntimeError(f"CollectAPI {product} returned HTTP {res.status}: {raw}")

    body = json.loads(raw)

    if not body.get("success"):
        raise RuntimeError(f"CollectAPI {product} error: {body.get('message')}")

    return body.get("result", [])


def fetch_all_fuel_prices(batch_id: str) -> list[dict]:
    ingestion_timestamp = datetime.now(timezone.utc).isoformat()
    reporting_date      = datetime.now(timezone.utc).date().isoformat()
    records = []

    for product in ENDPOINTS:
        raw_records = fetch_product(product)

        for r in raw_records:
            country_raw = r.get("country", "").strip()
            country     = COUNTRY_NAME_MAP.get(country_raw, country_raw)

            records.append({
                "country":             country,
                "product_type":        product,
                "price":               r.get("price"),
                "currency":            r.get("currency", "usd"),
                "unit":                UNITS[product],
                "source":              "collectapi",
                "ingestion_timestamp": ingestion_timestamp,
                "reporting_date":      reporting_date,
                "batch_id":            batch_id,
            })

    return records