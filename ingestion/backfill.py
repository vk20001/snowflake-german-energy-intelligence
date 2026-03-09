import os
import requests
import time
from datetime import datetime, timezone
import snowflake.connector

SF_ACCOUNT   = os.environ["SF_ACCOUNT"]
SF_USER      = os.environ["SF_USER"]
SF_PASSWORD  = os.environ["SF_PASSWORD"]
SF_DATABASE  = "ENERGY_MARKET"
SF_SCHEMA    = "BRONZE"
SF_WAREHOUSE = "ENERGY_WH"

SMARD_BASE = "https://www.smard.de/app/chart_data"
REGION     = "DE"
RESOLUTION = "hour"

# January 2024 cutoff in milliseconds
BACKFILL_FROM_MS = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

GENERATION_FILTERS = {
    4067: "Wind Onshore",
    1225: "Wind Offshore",
    4068: "Photovoltaik",
    4066: "Biomasse",
    1223: "Braunkohle",
    4069: "Steinkohle",
    4071: "Erdgas",
    1224: "Kernenergie",
    1226: "Wasserkraft",
    1228: "Sonstige Erneuerbare",
    1227: "Sonstige Konventionelle",
    4070: "Pumpspeicher",
}

LOAD_FILTERS = {
    410:  "Stromverbrauch Gesamt",
    4359: "Residuallast",
    4387: "Pumpspeicher Verbrauch",
}

def get_all_timestamps(filter_id):
    url = f"{SMARD_BASE}/{filter_id}/{REGION}/index_{RESOLUTION}.json"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    timestamps = r.json().get("timestamps", [])
    return [ts for ts in timestamps if ts >= BACKFILL_FROM_MS]

def get_series(filter_id, timestamp):
    url = f"{SMARD_BASE}/{filter_id}/{REGION}/{filter_id}_{REGION}_{RESOLUTION}_{timestamp}.json"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    return r.json().get("series", [])

def fetch_smard_historical(filter_map):
    rows = []
    for filter_id, filter_name in filter_map.items():
        timestamps = get_all_timestamps(filter_id)
        print(f"  {filter_name}: {len(timestamps)} chunks to fetch")
        for ts in timestamps:
            try:
                series = get_series(filter_id, ts)
                for point in series:
                    if point[1] is None:
                        continue
                    timestamp_utc = datetime.fromtimestamp(point[0]/1000, tz=timezone.utc)
                    if timestamp_utc.timestamp() * 1000 < BACKFILL_FROM_MS:
                        continue
                    rows.append((
                        filter_id,
                        filter_name,
                        REGION,
                        RESOLUTION,
                        timestamp_utc.strftime("%Y-%m-%d %H:%M:%S"),
                        float(point[1])
                    ))
                time.sleep(0.3)
            except Exception as e:
                print(f"    Skipped chunk {ts}: {e}")
                continue
        print(f"  {filter_name}: done, {len(rows)} total rows so far")
    return rows

def merge_grid_data(conn, table, rows):
    if not rows:
        return
    cs = conn.cursor()
    # Insert in batches of 5000 to avoid memory issues
    batch_size = 5000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        cs.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE ENERGY_MARKET.BRONZE.{table}_STAGE (
                FILTER_ID     NUMBER,
                FILTER_NAME   VARCHAR(100),
                REGION        VARCHAR(20),
                RESOLUTION    VARCHAR(20),
                TIMESTAMP_UTC TIMESTAMP_NTZ,
                VALUE_MWH     FLOAT
            )
        """)
        cs.executemany(f"""
            INSERT INTO ENERGY_MARKET.BRONZE.{table}_STAGE
            (FILTER_ID, FILTER_NAME, REGION, RESOLUTION, TIMESTAMP_UTC, VALUE_MWH)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, batch)
        cs.execute(f"""
            MERGE INTO ENERGY_MARKET.BRONZE.{table} AS target
            USING ENERGY_MARKET.BRONZE.{table}_STAGE AS source
            ON target.TIMESTAMP_UTC = source.TIMESTAMP_UTC
            AND target.FILTER_ID = source.FILTER_ID
            WHEN MATCHED THEN UPDATE SET
                target.VALUE_MWH   = source.VALUE_MWH,
                target.INGESTED_AT = SYSDATE()
            WHEN NOT MATCHED THEN INSERT
                (FILTER_ID, FILTER_NAME, REGION, RESOLUTION, TIMESTAMP_UTC, VALUE_MWH)
            VALUES
                (source.FILTER_ID, source.FILTER_NAME, source.REGION,
                 source.RESOLUTION, source.TIMESTAMP_UTC, source.VALUE_MWH)
        """)
        print(f"    Merged batch {i//batch_size + 1} ({len(batch)} rows) into {table}")
    cs.close()

def main():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        database=SF_DATABASE, schema=SF_SCHEMA, warehouse=SF_WAREHOUSE
    )
    print("Connected.")

    print("\nFetching SMARD generation history (Jan 2024 onwards)...")
    gen_rows = fetch_smard_historical(GENERATION_FILTERS)
    print(f"Total generation rows: {len(gen_rows)}")

    print("\nFetching SMARD load history (Jan 2024 onwards)...")
    load_rows = fetch_smard_historical(LOAD_FILTERS)
    print(f"Total load rows: {len(load_rows)}")

    print("\nMerging into Snowflake...")
    merge_grid_data(conn, "GRID_GENERATION_RAW", gen_rows)
    merge_grid_data(conn, "GRID_LOAD_RAW", load_rows)

    conn.close()
    print("\nBackfill complete.")

if __name__ == "__main__":
    main()
