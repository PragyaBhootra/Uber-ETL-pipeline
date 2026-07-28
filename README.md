# Uber Data Analytics | Modern Data Engineering Project

## Introduction

The goal of this project is to perform data analytics on NYC taxi/Uber trip data by building a
star-schema data warehouse: one flat CSV is split into a fact table + 7 dimension tables, then
queried with SQL to surface trip insights (demand patterns, fare behavior, payment mix, data
quality issues).

The pipeline is implemented in three versions, from simplest to closest-to-production:

| Version | Script | Storage | Compute | Warehouse | Cost |
|---|---|---|---|---|---|
| **Local** | `etl_one.py` | CSV on disk | your laptop | DuckDB file (`warehouse.duckdb`) | Free |
| **Free cloud** | `etl_cloud.py` | CSV in this repo | GitHub Actions | MotherDuck (hosted DuckDB) | Free |
| **GCP (original tutorial)** | `mage-files/*.py` | Cloud Storage | Mage on a Compute VM | BigQuery | GCP free tier / paid |

All three run the exact same transform logic (same star schema, same 7 dimensions + fact table).

## Technology Used

- Programming language — Python (pandas)
- Local warehouse — [DuckDB](https://duckdb.org/)
- Hosted warehouse — [MotherDuck](https://motherduck.com/) (free tier, no card required)
- Automation/orchestration — GitHub Actions (free tier)
- Original GCP version — Google Cloud Storage, Compute Engine, Mage ([mage.ai](https://www.mage.ai/)), BigQuery, Looker Studio

## Running the Local Version

```
pip install pandas duckdb
python etl_one.py
```

Reads `data/uber_data.csv`, builds the star schema, and writes everything into a local
`warehouse.duckdb` file you can query with the DuckDB CLI or Python.

## Running the Free Cloud Version

Uses [MotherDuck](https://motherduck.com/) as a hosted warehouse and GitHub Actions as the
free compute/orchestration layer — no Cloudflare/GCP storage needed since the CSV already
lives in this repo.

**Local test run:**
1. Sign up at motherduck.com and create an access token (Settings → Access Tokens).
2. Fill in `MOTHERDUCK_TOKEN` in the `.env` file next to `etl_cloud.py`.
3. `pip install pandas duckdb python-dotenv`
4. `python etl_cloud.py`

**Automated run (GitHub Actions):**
1. Add `MOTHERDUCK_TOKEN` as a repo secret: Settings → Secrets and variables → Actions.
2. Trigger `.github/workflows/etl.yml` manually from the Actions tab, or let it run on its
   weekly schedule.
3. Browse the resulting warehouse at [app.motherduck.com](https://app.motherduck.com).

## Running the Original GCP Version

The `mage-files/` folder (`extract.py`, `transform.py`, `load.py`) contains the original
Mage pipeline blocks: extract from Cloud Storage, transform with pandas, load into BigQuery
via `io_config.yaml`. Requires a GCP project with Cloud Storage + BigQuery enabled, and Mage
running locally or on a Compute Engine VM. Dashboards were built in Looker Studio connected
directly to the BigQuery dataset.

## Dataset Used

TLC Trip Record Data. Yellow and green taxi trip records include fields capturing pick-up and
drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate
types, payment types, and driver-reported passenger counts.

More info about the dataset can be found here:
1. Website — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. Data Dictionary — https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

