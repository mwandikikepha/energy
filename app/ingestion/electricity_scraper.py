import re
import httpx
from datetime import datetime, timezone
from bs4 import BeautifulSoup

from config.settings import settings


GPP_URL = "https://www.globalpetrolprices.com/electricity_prices/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
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


async def fetch_electricity_prices(batch_id: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        try:
            response = await client.get(GPP_URL, headers=HEADERS)
            response.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"Failed to reach GlobalPetrolPrices: {e}")

    soup = BeautifulSoup(response.text, "lxml")

    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError("No tables found on GlobalPetrolPrices electricity page.")

    # The data table always has the most rows
    data_table = max(tables, key=lambda t: len(t.find_all("tr")))
    rows = data_table.find_all("tr")

    ingestion_timestamp = datetime.now(timezone.utc).isoformat()
    reporting_date      = datetime.now(timezone.utc).date().isoformat()

    records = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 2:
            continue

        country_raw = cols[0].get_text(strip=True)

        # Skip header bleed-through rows — they're empty, numeric, or flag-only
        if not country_raw or country_raw.isdigit() or len(country_raw) <= 2:
            continue

        country = COUNTRY_NAME_MAP.get(country_raw, country_raw)

        # Price is in the last column; GPP shows USD value there
        price_text = cols[-1].get_text(strip=True)
        match = re.search(r"(\d+\.\d+)", price_text)
        if not match:
            continue

        price = float(match.group(1))

        # Guard: electricity prices are never 0 or implausibly high
        if not (0.001 < price < 100):
            continue

        records.append({
            "country":             country,
            "product_type":        "electricity",
            "price":               price,
            "currency":            "usd",
            "unit":                "per_kwh",
            "source":              "globalpetrolprices",
            "ingestion_timestamp": ingestion_timestamp,
            "reporting_date":      reporting_date,
            "batch_id":            batch_id,
        })

    return records