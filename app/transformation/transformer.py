from datetime import datetime, timezone


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


def make_id(record: dict) -> str:
   
   # Deterministic document ID built from business key fields.  Same country + product + date always produces the same _id,
   # making every curated upsert idempotent — rerunning the pipeline
    #on the same day updates in place rather than creating duplicates.
    
    return f"{record['country']}|{record['product_type']}|{record['reporting_date']}"


def transform(valid_records: list[dict]) -> list[dict]:
  
    # Normalise valid records into the final curated document shape.


    transformed = []

    for record in valid_records:
        country_raw = record.get("country", "").strip()
        country     = COUNTRY_NAME_MAP.get(country_raw, country_raw)

        transformed.append({
            "_id":                 make_id({**record, "country": country}),
            "country":             country,
            "product_type":        record["product_type"],
            "price":               float(record["price"]),
            "currency":            record["currency"],
            "unit":                record["unit"],
            "source":              record["source"],
            "reporting_date":      record["reporting_date"],
            "ingestion_timestamp": record.get("ingestion_timestamp"),
            "batch_id":            record.get("batch_id"),
            "transformed_at":      datetime.now(timezone.utc).isoformat(),
        })

    return transformed
