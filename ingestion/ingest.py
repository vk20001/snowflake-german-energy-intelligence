import requests
import os
import time
from datetime import datetime, timezone
import snowflake.connector

# ── Snowflake connection ──────────────────────────────────────────
SF_ACCOUNT  = os.environ["SF_ACCOUNT"]
SF_USER     = os.environ["SF_USER"]
SF_PASSWORD = os.environ["SF_PASSWORD"]
SF_DATABASE = "ENERGY_MARKET"
SF_SCHEMA   = "BRONZE"
SF_WAREHOUSE= "ENERGY_WH"

# ── SMARD config ─────────────────────────────────────────────────
SMARD_BASE = "https://www.smard.de/app/chart_data"
REGION     = "DE"
RESOLUTION = "hour"

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

# ── SMARD fetch ───────────────────────────────────────────────────
def get_latest_timestamp(filter_id):
    url = f"{SMARD_BASE}/{filter_id}/{REGION}/index_{RESOLUTION}.json"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    timestamps = r.json().get("timestamps", [])
    return timestamps[-1] if timestamps else None

def get_series(filter_id, timestamp):
    url = f"{SMARD_BASE}/{filter_id}/{REGION}/{filter_id}_{REGION}_{RESOLUTION}_{timestamp}.json"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.json().get("series", [])

def fetch_smard(filter_map):
    rows = []
    for filter_id, filter_name in filter_map.items():
        ts = get_latest_timestamp(filter_id)
        if not ts:
            continue
        series = get_series(filter_id, ts)
        for point in series:
            if point[1] is None:
                continue
            timestamp_utc = datetime.fromtimestamp(point[0]/1000, tz=timezone.utc)
            rows.append((
                filter_id,
                filter_name,
                REGION,
                RESOLUTION,
                timestamp_utc.strftime("%Y-%m-%d %H:%M:%S"),
                float(point[1])
            ))
        time.sleep(0.5)
    return rows

# ── Open-Meteo fetch ──────────────────────────────────────────────
def fetch_open_meteo():
    locations = [
        {"name": "Berlin",  "lat": 52.52, "lon": 13.41},
        {"name": "Hamburg", "lat": 53.55, "lon": 10.00},
        {"name": "München", "lat": 48.14, "lon": 11.58},
    ]
    rows = []
    for loc in locations:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={loc['lat']}&longitude={loc['lon']}"
            f"&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,"
            f"shortwave_radiation,cloud_cover,precipitation"
            f"&timezone=UTC&past_days=2&forecast_days=1"
        )
        for attempt in range(3):
            try:
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                break
            except Exception as e:
                if attempt == 2:
                    print(f"  Skipping {loc['name']} after 3 attempts: {e}")
                    r = None
                    break
                time.sleep(5)
        if r is None:
            continue
        data = r.json()
        hourly = data["hourly"]
        for i, ts in enumerate(hourly["time"]):
            rows.append((
                loc["name"],
                loc["lat"],
                loc["lon"],
                ts.replace("T", " "),
                hourly["temperature_2m"][i],
                hourly["wind_speed_10m"][i],
                hourly["wind_direction_10m"][i],
                hourly["shortwave_radiation"][i],
                hourly["cloud_cover"][i],
                hourly["precipitation"][i],
            ))
    return rows

# ── Snowflake merge ───────────────────────────────────────────────
def merge_grid_data(conn, table, rows):
    cs = conn.cursor()
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
    """, rows)
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
    cs.close()
    print(f"  Merged {len(rows)} rows into {table}")

def merge_weather_data(conn, rows):
    cs = conn.cursor()
    cs.execute("""
        CREATE OR REPLACE TEMPORARY TABLE ENERGY_MARKET.BRONZE.WEATHER_STAGE (
            LOCATION_NAME       VARCHAR(50),
            LATITUDE            FLOAT,
            LONGITUDE           FLOAT,
            TIMESTAMP_UTC       TIMESTAMP_NTZ,
            TEMPERATURE_2M      FLOAT,
            WIND_SPEED_10M      FLOAT,
            WIND_DIRECTION_10M  FLOAT,
            SHORTWAVE_RADIATION FLOAT,
            CLOUD_COVER         FLOAT,
            PRECIPITATION       FLOAT
        )
    """)
    cs.executemany("""
        INSERT INTO ENERGY_MARKET.BRONZE.WEATHER_STAGE
        (LOCATION_NAME, LATITUDE, LONGITUDE, TIMESTAMP_UTC,
         TEMPERATURE_2M, WIND_SPEED_10M, WIND_DIRECTION_10M,
         SHORTWAVE_RADIATION, CLOUD_COVER, PRECIPITATION)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, rows)
    cs.execute("""
        MERGE INTO ENERGY_MARKET.BRONZE.WEATHER_RAW AS target
        USING ENERGY_MARKET.BRONZE.WEATHER_STAGE AS source
        ON target.TIMESTAMP_UTC = source.TIMESTAMP_UTC
        AND target.LOCATION_NAME = source.LOCATION_NAME
        WHEN MATCHED THEN UPDATE SET
            target.TEMPERATURE_2M      = source.TEMPERATURE_2M,
            target.WIND_SPEED_10M      = source.WIND_SPEED_10M,
            target.WIND_DIRECTION_10M  = source.WIND_DIRECTION_10M,
            target.SHORTWAVE_RADIATION = source.SHORTWAVE_RADIATION,
            target.CLOUD_COVER         = source.CLOUD_COVER,
            target.PRECIPITATION       = source.PRECIPITATION,
            target.INGESTED_AT         = SYSDATE()
        WHEN NOT MATCHED THEN INSERT
            (LOCATION_NAME, LATITUDE, LONGITUDE, TIMESTAMP_UTC,
             TEMPERATURE_2M, WIND_SPEED_10M, WIND_DIRECTION_10M,
             SHORTWAVE_RADIATION, CLOUD_COVER, PRECIPITATION)
        VALUES
            (source.LOCATION_NAME, source.LATITUDE, source.LONGITUDE,
             source.TIMESTAMP_UTC, source.TEMPERATURE_2M, source.WIND_SPEED_10M,
             source.WIND_DIRECTION_10M, source.SHORTWAVE_RADIATION,
             source.CLOUD_COVER, source.PRECIPITATION)
    """)
    cs.close()
    print(f"  Merged {len(rows)} rows into WEATHER_RAW")

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account   = SF_ACCOUNT,
        user      = SF_USER,
        password  = SF_PASSWORD,
        database  = SF_DATABASE,
        schema    = SF_SCHEMA,
        warehouse = SF_WAREHOUSE
    )
    print("  Connected.")

    print("Fetching SMARD generation data...")
    gen_rows = fetch_smard(GENERATION_FILTERS)
    print(f"  Got {len(gen_rows)} rows")

    print("Fetching SMARD load data...")
    load_rows = fetch_smard(LOAD_FILTERS)
    print(f"  Got {len(load_rows)} rows")

    print("Fetching Open-Meteo weather data...")
    weather_rows = fetch_open_meteo()
    print(f"  Got {len(weather_rows)} rows")

    print("Loading into Snowflake...")
    merge_grid_data(conn, "GRID_GENERATION_RAW", gen_rows)
    merge_grid_data(conn, "GRID_LOAD_RAW", load_rows)
    merge_weather_data(conn, weather_rows)

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
