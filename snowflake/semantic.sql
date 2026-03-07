-- ============================================================
-- semantic.sql: Semantic View for Cortex Analyst
-- ============================================================

CREATE OR REPLACE SEMANTIC VIEW ENERGY_MARKET.GOLD.ENERGY_SEMANTIC
TABLES (
    energy_hourly AS ENERGY_MARKET.GOLD.ENERGY_WEATHER_HOURLY
        PRIMARY KEY (TIMESTAMP_UTC)
)
FACTS (
    energy_hourly.TOTAL_GENERATION_MWH AS TOTAL_GENERATION_MWH,
    energy_hourly.RENEWABLE_MWH AS RENEWABLE_MWH,
    energy_hourly.CONVENTIONAL_MWH AS CONVENTIONAL_MWH,
    energy_hourly.WIND_ONSHORE_MWH AS WIND_ONSHORE_MWH,
    energy_hourly.WIND_OFFSHORE_MWH AS WIND_OFFSHORE_MWH,
    energy_hourly.SOLAR_MWH AS SOLAR_MWH,
    energy_hourly.LIGNITE_MWH AS LIGNITE_MWH,
    energy_hourly.GAS_MWH AS GAS_MWH,
    energy_hourly.TOTAL_LOAD_MWH AS TOTAL_LOAD_MWH,
    energy_hourly.AVG_TEMPERATURE AS AVG_TEMPERATURE,
    energy_hourly.AVG_WIND_SPEED AS AVG_WIND_SPEED,
    energy_hourly.AVG_SOLAR_RADIATION AS AVG_SOLAR_RADIATION
)
DIMENSIONS (
    energy_hourly.TIMESTAMP_UTC AS TIMESTAMP_UTC,
    energy_hourly.GRID_STATUS AS GRID_STATUS
)
METRICS (
    energy_hourly.RENEWABLE_SHARE_PCT AS SUM(energy_hourly.RENEWABLE_MWH) / NULLIF(SUM(energy_hourly.TOTAL_GENERATION_MWH), 0) * 100,
    energy_hourly.GENERATION_LOAD_DELTA_MWH AS SUM(energy_hourly.TOTAL_GENERATION_MWH) - SUM(energy_hourly.TOTAL_LOAD_MWH)
)
COMMENT = 'Semantic layer for German Energy Market Intelligence. Covers hourly grid generation by source, total load, renewable share, grid balance status, and correlated weather conditions.';
