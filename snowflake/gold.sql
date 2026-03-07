-- =============================================================================
-- gold.sql
-- Gold layer: unified hourly energy + weather table
--
-- Design decision: REFRESH_MODE = FULL explicitly set.
-- Gold joins three Silver aggregation tables using LEFT JOIN on TIMESTAMP_UTC.
-- Snowflake AUTO mode selects FULL due to LEFT JOIN + float arithmetic
-- (NULLIF, division, CASE) in the same query block.
-- At current data volumes (~4,400 rows/year) FULL refresh completes in <2s
-- and costs fractions of a credit per hour. INCREMENTAL provides no
-- meaningful benefit at this scale.
-- Production fix for larger datasets: cast VALUE_MWH to NUMBER(18,4) at
-- Bronze (already done) and replace LEFT JOINs with INNER JOINs after
-- verifying referential integrity across sources.
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE ENERGY_MARKET.GOLD.ENERGY_WEATHER_HOURLY
    TARGET_LAG   = '1 hour'
    REFRESH_MODE = FULL
    INITIALIZE   = ON_CREATE
    WAREHOUSE    = ENERGY_WH
AS
SELECT
    g.TIMESTAMP_UTC,
    g.TOTAL_GENERATION_MWH,
    g.RENEWABLE_MWH,
    g.CONVENTIONAL_MWH,
    g.WIND_ONSHORE_MWH,
    g.WIND_OFFSHORE_MWH,
    g.SOLAR_MWH,
    g.LIGNITE_MWH,
    g.GAS_MWH,
    l.TOTAL_LOAD_MWH,
    g.RENEWABLE_MWH / NULLIF(g.TOTAL_GENERATION_MWH, 0) * 100         AS RENEWABLE_SHARE_PCT,
    g.TOTAL_GENERATION_MWH - NULLIF(l.TOTAL_LOAD_MWH, 0)              AS GENERATION_LOAD_DELTA_MWH,
    w.AVG_TEMPERATURE,
    w.AVG_WIND_SPEED,
    w.AVG_SOLAR_RADIATION,
    w.AVG_CLOUD_COVER,
    w.AVG_PRECIPITATION,
    CASE
        WHEN g.TOTAL_GENERATION_MWH - NULLIF(l.TOTAL_LOAD_MWH, 0) < -2000 THEN 'Critical'
        WHEN g.TOTAL_GENERATION_MWH - NULLIF(l.TOTAL_LOAD_MWH, 0) < 0     THEN 'Tight'
        WHEN g.TOTAL_GENERATION_MWH - NULLIF(l.TOTAL_LOAD_MWH, 0) < 2000  THEN 'Balanced'
        ELSE 'Surplus'
    END AS GRID_STATUS
FROM ENERGY_MARKET.SILVER.GENERATION_HOURLY       g
LEFT JOIN ENERGY_MARKET.SILVER.LOAD_HOURLY        l ON g.TIMESTAMP_UTC = l.TIMESTAMP_UTC
LEFT JOIN ENERGY_MARKET.SILVER.WEATHER_HOURLY_AVG w ON g.TIMESTAMP_UTC = w.TIMESTAMP_UTC;
