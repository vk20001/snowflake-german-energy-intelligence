-- =============================================================================
-- silver.sql
-- Silver layer: cleaning + aggregation Dynamic Tables
-- Reads from Bronze, applies deduplication, type validation, null filtering
-- Aggregation tables pre-compute hourly summaries for Gold INCREMENTAL support
--
-- Tables created:
--   GRID_GENERATION_CLEAN   — cleaned generation rows with ENERGY_CLASS label
--   GRID_LOAD_CLEAN         — cleaned load rows
--   WEATHER_CLEAN           — cleaned weather rows
--   GENERATION_HOURLY       — hourly aggregated generation per energy type
--   LOAD_HOURLY             — hourly total load
--   WEATHER_HOURLY_AVG      — hourly averaged weather across 3 locations
-- =============================================================================

-- ── Cleaned tables ────────────────────────────────────────────────────────────

CREATE OR REPLACE DYNAMIC TABLE ENERGY_MARKET.SILVER.GRID_GENERATION_CLEAN
    TARGET_LAG = '1 hour'
    WAREHOUSE  = ENERGY_WH
AS
SELECT
    RECORD_ID,
    FILTER_ID,
    FILTER_NAME,
    REGION,
    RESOLUTION,
    TIMESTAMP_UTC,
    VALUE_MWH,
    INGESTED_AT,
    CASE
        WHEN FILTER_NAME IN (
            'Wind Onshore', 'Wind Offshore', 'Photovoltaik',
            'Biomasse', 'Wasserkraft', 'Sonstige Erneuerbare'
        ) THEN 'Renewable'
        ELSE 'Conventional'
    END AS ENERGY_CLASS
FROM ENERGY_MARKET.BRONZE.GRID_GENERATION_RAW
WHERE VALUE_MWH    IS NOT NULL
  AND VALUE_MWH    >= 0
  AND TIMESTAMP_UTC IS NOT NULL;


CREATE OR REPLACE DYNAMIC TABLE ENERGY_MARKET.SILVER.GRID_LOAD_CLEAN
    TARGET_LAG = '1 hour'
    WAREHOUSE  = ENERGY_WH
AS
SELECT
    RECORD_ID,
    FILTER_ID,
    FILTER_NAME,
    REGION,
    RESOLUTION,
    TIMESTAMP_UTC,
    VALUE_MWH,
    INGESTED_AT
FROM ENERGY_MARKET.BRONZE.GRID_LOAD_RAW
WHERE VALUE_MWH    IS NOT NULL
  AND VALUE_MWH    > 0
  AND TIMESTAMP_UTC IS NOT NULL;


CREATE OR REPLACE DYNAMIC TABLE ENERGY_MARKET.SILVER.WEATHER_CLEAN
    TARGET_LAG = '1 hour'
    WAREHOUSE  = ENERGY_WH
AS
SELECT
    RECORD_ID,
    LOCATION_NAME,
    LATITUDE,
    LONGITUDE,
    TIMESTAMP_UTC,
    TEMPERATURE_2M,
    WIND_SPEED_10M,
    WIND_DIRECTION_10M,
    SHORTWAVE_RADIATION,
    CLOUD_COVER,
    PRECIPITATION,
    INGESTED_AT
FROM ENERGY_MARKET.BRONZE.WEATHER_RAW
WHERE TIMESTAMP_UTC  IS NOT NULL
  AND TEMPERATURE_2M IS NOT NULL
  AND WIND_SPEED_10M >= 0;


-- ── Aggregation tables (feed Gold with pre-computed hourly summaries) ──────────
-- Note: Aggregations on NUMBER(18,4) columns placed here at Silver layer
-- so Gold can be a pure join, supporting REFRESH_MODE = AUTO decision.

CREATE OR REPLACE DYNAMIC TABLE ENERGY_MARKET.SILVER.GENERATION_HOURLY
    TARGET_LAG = '1 hour'
    WAREHOUSE  = ENERGY_WH
AS
SELECT
    TIMESTAMP_UTC,
    SUM(VALUE_MWH)                                                          AS TOTAL_GENERATION_MWH,
    SUM(CASE WHEN ENERGY_CLASS = 'Renewable'    THEN VALUE_MWH ELSE 0 END)  AS RENEWABLE_MWH,
    SUM(CASE WHEN ENERGY_CLASS = 'Conventional' THEN VALUE_MWH ELSE 0 END)  AS CONVENTIONAL_MWH,
    SUM(CASE WHEN FILTER_NAME = 'Wind Onshore'  THEN VALUE_MWH ELSE 0 END)  AS WIND_ONSHORE_MWH,
    SUM(CASE WHEN FILTER_NAME = 'Wind Offshore' THEN VALUE_MWH ELSE 0 END)  AS WIND_OFFSHORE_MWH,
    SUM(CASE WHEN FILTER_NAME = 'Photovoltaik'  THEN VALUE_MWH ELSE 0 END)  AS SOLAR_MWH,
    SUM(CASE WHEN FILTER_NAME = 'Braunkohle'    THEN VALUE_MWH ELSE 0 END)  AS LIGNITE_MWH,
    SUM(CASE WHEN FILTER_NAME = 'Erdgas'        THEN VALUE_MWH ELSE 0 END)  AS GAS_MWH
FROM ENERGY_MARKET.SILVER.GRID_GENERATION_CLEAN
GROUP BY TIMESTAMP_UTC;


CREATE OR REPLACE DYNAMIC TABLE ENERGY_MARKET.SILVER.LOAD_HOURLY
    TARGET_LAG = '1 hour'
    WAREHOUSE  = ENERGY_WH
AS
SELECT
    TIMESTAMP_UTC,
    MAX(CASE WHEN FILTER_NAME = 'Stromverbrauch Gesamt' THEN VALUE_MWH END) AS TOTAL_LOAD_MWH
FROM ENERGY_MARKET.SILVER.GRID_LOAD_CLEAN
GROUP BY TIMESTAMP_UTC;


CREATE OR REPLACE DYNAMIC TABLE ENERGY_MARKET.SILVER.WEATHER_HOURLY_AVG
    TARGET_LAG = '1 hour'
    WAREHOUSE  = ENERGY_WH
AS
SELECT
    TIMESTAMP_UTC,
    AVG(TEMPERATURE_2M)      AS AVG_TEMPERATURE,
    AVG(WIND_SPEED_10M)      AS AVG_WIND_SPEED,
    AVG(SHORTWAVE_RADIATION) AS AVG_SOLAR_RADIATION,
    AVG(CLOUD_COVER)         AS AVG_CLOUD_COVER,
    AVG(PRECIPITATION)       AS AVG_PRECIPITATION
FROM ENERGY_MARKET.SILVER.WEATHER_CLEAN
GROUP BY TIMESTAMP_UTC;
