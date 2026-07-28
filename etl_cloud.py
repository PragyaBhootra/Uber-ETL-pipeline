"""
================================================================================
Uber / NYC-Taxi ETL pipeline  —  100% FREE CLOUD VERSION (Option B)
================================================================================
Same star-schema transform as etl_one.py, but the warehouse is hosted for
free, and it's designed to be triggered by GitHub Actions instead of your
laptop.

Paid tutorial              ->   What we use here (free, no card required)
--------------------------------------------------------------------------
Cloud Storage (raw file)   ->   the CSV already checked into this repo
Mage on a Compute VM       ->   this script, run by GitHub Actions
BigQuery (warehouse)       ->   MotherDuck (hosted DuckDB, free tier)
Looker Studio (charts)     ->   MotherDuck's built-in SQL UI

(Cloudflare R2 was dropped: it requires a card on file to even enable the
free tier. Since the CSV is small and already lives in the repo, GitHub
Actions can just read it straight off disk after checkout - no storage
service needed at all.)

REQUIRED ENVIRONMENT VARIABLES (set as GitHub Actions secrets, or in your
own shell for a local test run):
    MOTHERDUCK_TOKEN        MotherDuck service token
    MOTHERDUCK_DB           database name to create/use in MotherDuck
                            (default: uber_warehouse)
    CSV_PATH                path to the CSV (default: data/uber_data.csv)

LOCAL TEST RUN:
    1. Fill in the values in the .env file next to this script.
    2. pip install pandas duckdb python-dotenv
    3. python etl_cloud.py

(.env is only for local runs. GitHub Actions ignores it and uses the repo's
Actions secrets instead - see .github/workflows/etl.yml.)
================================================================================
"""

import os
import sys
import pandas as pd
import duckdb
from dotenv import load_dotenv

load_dotenv()

CSV_PATH = os.environ.get("CSV_PATH", "data/uber_data.csv")

MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
MOTHERDUCK_DB    = os.environ.get("MOTHERDUCK_DB", "uber_warehouse")

REQUIRED = ["MOTHERDUCK_TOKEN"]


def check_env():
    missing = [name for name in REQUIRED if not os.environ.get(name)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


# ============================================================================
# STEP 1 — EXTRACT   (read the CSV already checked out alongside this script)
# ============================================================================
def extract():
    print(f"STEP 1 — EXTRACT: reading raw CSV from {CSV_PATH} ...")
    df = pd.read_csv(CSV_PATH)

    df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df = df.drop_duplicates().reset_index(drop=True)
    df["trip_id"] = df.index
    print(f"   loaded {len(df):,} trips, {df.shape[1]} columns\n")
    return df


# ============================================================================
# STEP 2 — TRANSFORM   (identical star schema to the local version)
# ============================================================================
def transform(df):
    print("STEP 2 — TRANSFORM: building star schema ...")

    datetime_dim = df[["tpep_pickup_datetime", "tpep_dropoff_datetime"]].drop_duplicates().reset_index(drop=True)
    datetime_dim["pick_hour"]    = datetime_dim["tpep_pickup_datetime"].dt.hour
    datetime_dim["pick_day"]     = datetime_dim["tpep_pickup_datetime"].dt.day
    datetime_dim["pick_month"]   = datetime_dim["tpep_pickup_datetime"].dt.month
    datetime_dim["pick_year"]    = datetime_dim["tpep_pickup_datetime"].dt.year
    datetime_dim["pick_weekday"] = datetime_dim["tpep_pickup_datetime"].dt.weekday
    datetime_dim["drop_hour"]    = datetime_dim["tpep_dropoff_datetime"].dt.hour
    datetime_dim["datetime_id"]  = datetime_dim.index

    passenger_count_dim = df[["passenger_count"]].drop_duplicates().reset_index(drop=True)
    passenger_count_dim["passenger_count_id"] = passenger_count_dim.index

    trip_distance_dim = df[["trip_distance"]].drop_duplicates().reset_index(drop=True)
    trip_distance_dim["trip_distance_id"] = trip_distance_dim.index

    rate_code_type = {1:"Standard rate", 2:"JFK", 3:"Newark",
                      4:"Nassau or Westchester", 5:"Negotiated fare", 6:"Group ride"}
    rate_code_dim = df[["RatecodeID"]].drop_duplicates().reset_index(drop=True)
    rate_code_dim["rate_code_id"]   = rate_code_dim.index
    rate_code_dim["rate_code_name"] = rate_code_dim["RatecodeID"].map(rate_code_type)

    pickup_location_dim = df[["pickup_longitude", "pickup_latitude"]].drop_duplicates().reset_index(drop=True)
    pickup_location_dim["pickup_location_id"] = pickup_location_dim.index
    dropoff_location_dim = df[["dropoff_longitude", "dropoff_latitude"]].drop_duplicates().reset_index(drop=True)
    dropoff_location_dim["dropoff_location_id"] = dropoff_location_dim.index

    payment_type_name = {1:"Credit card", 2:"Cash", 3:"No charge",
                         4:"Dispute", 5:"Unknown", 6:"Voided trip"}
    payment_type_dim = df[["payment_type"]].drop_duplicates().reset_index(drop=True)
    payment_type_dim["payment_type_id"]   = payment_type_dim.index
    payment_type_dim["payment_type_name"] = payment_type_dim["payment_type"].map(payment_type_name)

    fact_table = (
        df.merge(passenger_count_dim, on="passenger_count")
          .merge(trip_distance_dim,   on="trip_distance")
          .merge(rate_code_dim,       on="RatecodeID")
          .merge(pickup_location_dim,  on=["pickup_longitude", "pickup_latitude"])
          .merge(dropoff_location_dim, on=["dropoff_longitude", "dropoff_latitude"])
          .merge(datetime_dim,        on=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
          .merge(payment_type_dim,    on="payment_type")
          [["trip_id", "VendorID", "datetime_id", "passenger_count_id", "trip_distance_id",
            "rate_code_id", "pickup_location_id", "dropoff_location_id", "payment_type_id",
            "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
            "improvement_surcharge", "total_amount"]]
    )

    assert len(fact_table) == len(df), "Row explosion! a dimension had duplicate keys"
    print(f"   fact_table rows = {len(fact_table):,}  (matches trip count - good)\n")

    return {
        "fact_table": fact_table,
        "datetime_dim": datetime_dim,
        "passenger_count_dim": passenger_count_dim,
        "trip_distance_dim": trip_distance_dim,
        "rate_code_dim": rate_code_dim,
        "pickup_location_dim": pickup_location_dim,
        "dropoff_location_dim": dropoff_location_dim,
        "payment_type_dim": payment_type_dim,
    }


# ============================================================================
# STEP 3 — LOAD   (write every table into your MotherDuck cloud warehouse)
# ============================================================================
def load(tables):
    print(f"STEP 3 — LOAD: writing tables into MotherDuck database '{MOTHERDUCK_DB}' ...")
    con = duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={MOTHERDUCK_TOKEN}")
    for name, frame in tables.items():
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.register("tmp", frame)
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM tmp")
        con.unregister("tmp")
        print(f"   loaded table: {name:22s} ({len(frame):,} rows)")
    print()
    return con


# ============================================================================
# STEP 4 — ANALYZE   (same SQL insights, now running against the cloud warehouse)
# ============================================================================
def analyze(con):
    print("STEP 4 — ANALYZE: running SQL against the MotherDuck warehouse\n")

    print(">>> Busiest pickup hour")
    print(con.execute("""
        SELECT d.pick_hour AS hour, COUNT(*) AS trips
        FROM fact_table f JOIN datetime_dim d USING (datetime_id)
        GROUP BY hour ORDER BY trips DESC LIMIT 3
    """).df().to_string(index=False)); print()

    print(">>> Revenue & trips by payment type")
    print(con.execute("""
        SELECT p.payment_type_name,
               COUNT(*) AS trips,
               ROUND(SUM(f.total_amount), 0) AS revenue
        FROM fact_table f JOIN payment_type_dim p USING (payment_type_id)
        GROUP BY 1 ORDER BY revenue DESC
    """).df().to_string(index=False)); print()

    print(">>> Average fare by passenger count")
    print(con.execute("""
        SELECT pc.passenger_count,
               ROUND(AVG(f.total_amount), 2) AS avg_total,
               COUNT(*) AS trips
        FROM fact_table f JOIN passenger_count_dim pc USING (passenger_count_id)
        GROUP BY 1 ORDER BY 1
    """).df().to_string(index=False)); print()

    print(">>> Data-quality flag: TRIPS with invalid ZERO coordinates (not NULL!)")
    print(con.execute("""
        SELECT COUNT(*) AS trips_with_bad_pickup
        FROM fact_table f
        JOIN pickup_location_dim p USING (pickup_location_id)
        WHERE p.pickup_latitude = 0 OR p.pickup_longitude = 0
    """).df().to_string(index=False)); print()

    print(">>> Building one wide reporting table (like tbl_analytics)")
    con.execute("""
        CREATE OR REPLACE TABLE tbl_analytics AS
        SELECT f.trip_id, f.VendorID, d.tpep_pickup_datetime,
               pc.passenger_count, t.trip_distance, r.rate_code_name,
               pay.payment_type_name, f.fare_amount, f.tip_amount, f.total_amount
        FROM fact_table f
        JOIN datetime_dim d        USING (datetime_id)
        JOIN passenger_count_dim pc USING (passenger_count_id)
        JOIN trip_distance_dim t    USING (trip_distance_id)
        JOIN rate_code_dim r        USING (rate_code_id)
        JOIN payment_type_dim pay   USING (payment_type_id)
    """)
    print("   tbl_analytics built. Peek:")
    print(con.execute("SELECT * FROM tbl_analytics LIMIT 3").df().to_string(index=False))


if __name__ == "__main__":
    check_env()
    df     = extract()
    tables = transform(df)
    con    = load(tables)
    analyze(con)
    con.close()
    print(f"\nDone. Your warehouse '{MOTHERDUCK_DB}' now lives in MotherDuck. "
          f"Browse it at https://app.motherduck.com")
