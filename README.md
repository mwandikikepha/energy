# Fuelwatch Global — Energy Price Data Platform

A production-grade data engineering platform that ingests global fuel and electricity prices from multiple sources, processes them through a quality-controlled pipeline, stores curated data in MongoDB Atlas, automates the whole process using airflow, and serves it through a FastAPI dashboard.

Live at: `https://energy-production-7ace.up.railway.app/`

---

## What it does

Tracks gasoline, diesel, LPG, and electricity prices across 150+ countries. Every day the pipeline runs automatically, fetches the latest prices, validates and transforms them, and writes clean records to MongoDB. The dashboard gives you regional comparisons, country rankings, price trends over time, and a side-by-side country comparison tool.

---

## Architecture

```
CollectAPI (gasoline · diesel · lpg)  ──┐
                                         ├── Ingestion → Validation → Transformation
GlobalPetrolPrices (electricity scrape) ─┘
                                                    │
                                          ┌─────────┴──────────┐
                                          │     MongoDB Atlas   │
                                          │  ┌───────────────┐  │
                                          │  │ raw_api_data  │  │  ← audit trail
                                          │  │ fuel_prices   │  │  ← curated
                                          │  │ elec_prices   │  │  ← curated
                                          │  │ pipeline_runs │  │  ← observability
                                          │  │ quality_checks│  │  ← observability
                                          │  └───────────────┘  │
                                          └─────────┬──────────┘
                                                    │
                                          ┌─────────┴──────────┐
                                          │   FastAPI + Jinja2  │
                                          │  /api/prices        │
                                          │  /api/report/*      │
                                          │  / (dashboard)      │
                                          └────────────────────┘

Scheduling: Apache Airflow DAG (daily 06:00 UTC) — Airflow in production
Local dev:  Apache Airflow DAG (energy_pipeline_dag.py)
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python `http.client`, `httpx`, `BeautifulSoup4` |
| Transformation & validation | Pure Python, Pydantic |
| Storage | MongoDB Atlas, `pymongo` |
| Orchestration (local) | Apache Airflow 2.x |
| Orchestration (production) | Railway Cron Job |
| API | FastAPI, Uvicorn |
| Frontend | Vanilla JS, SVG charts, no framework |
| Config | `pydantic-settings`, `.env` |
| Package management | `uv` |
| Deployment | Docker, Railway |

---

## Data sources

**CollectAPI** — `api.collectapi.com`
- Gasoline: 170 countries
- Diesel: 169 countries
- LPG: 55 countries
- All prices in USD per litre

**GlobalPetrolPrices** — `globalpetrolprices.com/electricity_prices`
- Electricity: ~132 countries
- Scraped from HTML table (updates weekly)
- Prices in USD per kWh

---

## Project structure

```
energy_platform/
│
├── app/
│   ├── ingestion/
│   │   ├── collectapi_client.py     # Fetches fuel prices from CollectAPI
│   │   ├── electricity_scraper.py   # Scrapes electricity prices
│   │   └── ingestion_metadata.py    # Generates batch_id and run metadata
│   │
│   ├── validation/
│   │   ├── rules.py                 # Individual validation rule functions
│   │   └── validator.py             # Runs all rules, returns structured report
│   │
│   ├── transformation/
│   │   └── transformer.py           # Normalises schema, casts types, builds _id
│   │
│   ├── loaders/
│   │   ├── raw_loader.py            # Insert-only writes to raw_api_data
│   │   └── curated_loader.py        # Idempotent upserts to curated collections
│   │
│   └── services/
│       └── reporting.py             # Queries MongoDB for dashboard data
│
├── api/
│   ├── main.py                      # FastAPI app entry point
│   ├── routers/
│   │   ├── prices.py                # GET /api/prices
│   │   └── reports.py               # GET /api/report/*
│   ├── models/
│   │   └── schemas.py               # Pydantic response models
│   └── templates/
│       └── dashboard.html           # Single-page dashboard
│
├── airflow/
│   └── dags/
│       └── energy_pipeline_dag.py   # Local Airflow DAG
│
├── config/
│   └── settings.py                  # Pydantic settings, reads from .env
│
├── tests/
│   ├── test_validator.py
│   ├── test_transformer.py
│   ├── test_loaders.py
│   └── test_routes.py
│
├── test.py                  
├── Dockerfile
├── railway.toml
├── pyproject.toml
```

---

## Pipeline flow

```
1. generate batch_id          ingestion_metadata.py
2. fetch fuel prices          collectapi_client.py       
3. fetch electricity prices   electricity_scraper.py     
4. combine                    526 records total
5. insert raw                 raw_loader.py              → raw_api_data (insert-only)
6. validate                   validator.py               → null / type / negative / product checks
7. transform                  transformer.py             → cast types, normalise names, build _id
8. upsert curated             curated_loader.py          → fuel_prices + electricity_prices
9. write quality report       data_quality_checks
10. close pipeline run        pipeline_runs → status: success
```

Each record carries a `batch_id` through every step. Curated writes use `country|product_type|reporting_date` as a deterministic `_id` — running the pipeline twice on the same day is safe.

---

## API endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Dashboard UI |
| `GET` | `/api/prices` | Latest prices, filter by `?product=` or `?country=` |
| `GET` | `/api/report/movers` | Top price movers between last two runs |
| `GET` | `/api/report/pipeline` | Last pipeline run status and quality summary |
| `GET` | `/api/report/history` | Price history for `?country=&product=` |
| `GET` | `/api/report/dates` | All available reporting dates |
| `GET` | `/docs` | Auto-generated Swagger UI |

---

## Local setup

**Prerequisites:** Python 3.11+, `uv`, MongoDB Atlas account, CollectAPI account

```bash
# Clone and enter
git clone https://github.com/mwandikikepha/energy.git
cd energy

# Install dependencies
uv sync

# Configure environment
cp .env.example .env
# Fill in MONGO_URL, DATABASE_NAME, COLLECT_API in .env

# Run the pipeline manually
uv run python run_pipeline.py

# Start the API
uv run uvicorn api.main:app --reload --port 8000
```

Open `http://localhost:8000`

---

## Running with Docker

```bash
# Build
docker build -t energy-platform .

# Run 
docker run -p 8000:8000 \
  -e MONGO_URL=your_atlas_uri \
  -e DATABASE_NAME=energy_platform \
  -e COLLECT_API=your_token \
  energy
```

---

## Airflow (local development)

The Airflow DAG runs the same pipeline on a schedule locally. Useful for development and testing before going over in production over in production.

```bash
export AIRFLOW_HOME=~/airflow_home
airflow db migrate
airflow standalone
```

Then enable the `energy_pipeline` DAG in the Airflow UI at `http://localhost:8080`.

The DAG runs daily at 06:00 UTC with 3 retries and a 5-minute retry delay.

---

## Deployment (Railway)

Two services, one Dockerfile:

**Web service** — serves the FastAPI dashboard
- Build: Docker
- Start command: `uvicorn api.main:app --host 0.0.0.0 --port 8000`

**Apache Airflow** — runs the pipeline daily
- Build: same Dockerfile
- Start command override: `energy_pipeline_dag.py`
- Schedule: `0 6 * * *`

Both services share the same environment variables pointing to MongoDB Atlas.

---

## Data quality

Four validation rules run on every record before it reaches the curated collections:

| Rule | Check |
|---|---|
| `null_check` | `country`, `price`, `source`, `reporting_date` must be present |
| `price_type` | Price must be castable to float |
| `negative_price` | Price must be ≥ 0 |
| `valid_product` | Product must be one of: gasoline, diesel, lpg, electricity |

Failures are stored in `data_quality_checks` with a breakdown by rule and sample failing records. Invalid records remain in `raw_api_data` for investigation — they never reach curated collections.

---

## MongoDB collections

| Collection | Purpose | Write pattern |
|---|---|---|
| `raw_api_data` | Untouched records exactly as ingested | Insert-only |
| `fuel_prices` | Clean gasoline, diesel, LPG records | Upsert by `_id` |
| `electricity_prices` | Clean electricity records | Upsert by `_id` |
| `pipeline_runs` | One document per run — status, timing, counts | Insert + update |
| `data_quality_checks` | Validation report per run | Insert |

---

## Known limitations

- **CollectAPI LPG coverage** is limited to 55 countries vs 170 for gasoline/diesel
- **Price updates** from both sources are weekly — daily pipeline runs will show no delta until source prices change
- **GlobalPetrolPrices scrape** depends on their HTML structure — a page redesign could break the scraper
- **Currency** — all prices stored as USD. No real-time conversion for local currencies
- **Single region** — deployed on Railway's US infrastructure. No CDN or multi-region setup

---

## Roadmap

- Add Great Expectations for richer data quality contracts
- Currency conversion layer using ECB or Open Exchange Rates API
- Extend LPG coverage by adding a second LPG data source
- Historical backfill endpoint for on-demand reprocessing by `batch_id`
- Anomaly detection on price spikes using a simple z-score model
- CI/CD with GitHub Actions — lint, test, and auto-deploy on push

---

## Author

**Kepha Mwandiki** — Data Engineer & Data Scientist  
GitHub: [@mwandikikepha](https://github.com/mwandikikepha)
