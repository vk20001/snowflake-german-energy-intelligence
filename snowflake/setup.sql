
CREATE DATABASE ENERGY_MARKET;
CREATE SCHEMA ENERGY_MARKET.BRONZE;
CREATE SCHEMA ENERGY_MARKET.SILVER;
CREATE SCHEMA ENERGY_MARKET.GOLD;

CREATE WAREHOUSE ENERGY_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'German Energy Market Intelligence';

CREATE TABLE ENERGY_MARKET.BRONZE.GRID_GENERATION_RAW (
    RECORD_ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    FILTER_ID           NUMBER,
    FILTER_NAME         VARCHAR(100),
    REGION              VARCHAR(20),
    RESOLUTION          VARCHAR(20),
    TIMESTAMP_UTC       TIMESTAMP_NTZ,
    VALUE_MWH           FLOAT,
    INGESTED_AT         TIMESTAMP_NTZ DEFAULT SYSDATE()
);

CREATE TABLE ENERGY_MARKET.BRONZE.GRID_LOAD_RAW (
    RECORD_ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    FILTER_ID           NUMBER,
    FILTER_NAME         VARCHAR(100),
    REGION              VARCHAR(20),
    RESOLUTION          VARCHAR(20),
    TIMESTAMP_UTC       TIMESTAMP_NTZ,
    VALUE_MWH           FLOAT,
    INGESTED_AT         TIMESTAMP_NTZ DEFAULT SYSDATE()
);

CREATE TABLE ENERGY_MARKET.BRONZE.WEATHER_RAW (
    RECORD_ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    LOCATION_NAME       VARCHAR(50),
    LATITUDE            FLOAT,
    LONGITUDE           FLOAT,
    TIMESTAMP_UTC       TIMESTAMP_NTZ,
    TEMPERATURE_2M      FLOAT,
    WIND_SPEED_10M      FLOAT,
    WIND_DIRECTION_10M  FLOAT,
    SHORTWAVE_RADIATION FLOAT,
    CLOUD_COVER         FLOAT,
    PRECIPITATION       FLOAT,
    INGESTED_AT         TIMESTAMP_NTZ DEFAULT SYSDATE()
);

CREATE TABLE ENERGY_MARKET.BRONZE.SMARD_FILTER_REF (
    FILTER_ID           NUMBER PRIMARY KEY,
    FILTER_NAME         VARCHAR(100),
    CATEGORY            VARCHAR(50),
    UNIT                VARCHAR(20)
);

INSERT INTO ENERGY_MARKET.BRONZE.SMARD_FILTER_REF VALUES
  (4067, 'Wind Onshore',           'Generation', 'MWh'),
  (1225, 'Wind Offshore',          'Generation', 'MWh'),
  (4068, 'Photovoltaik',           'Generation', 'MWh'),
  (4066, 'Biomasse',               'Generation', 'MWh'),
  (1223, 'Braunkohle',             'Generation', 'MWh'),
  (4069, 'Steinkohle',             'Generation', 'MWh'),
  (4071, 'Erdgas',                 'Generation', 'MWh'),
  (1224, 'Kernenergie',            'Generation', 'MWh'),
  (1226, 'Wasserkraft',            'Generation', 'MWh'),
  (1228, 'Sonstige Erneuerbare',   'Generation', 'MWh'),
  (1227, 'Sonstige Konventionelle','Generation', 'MWh'),
  (4070, 'Pumpspeicher',           'Generation', 'MWh'),
  (410,  'Stromverbrauch Gesamt',  'Load',       'MWh'),
  (4359, 'Residuallast',           'Load',       'MWh'),
  (4387, 'Pumpspeicher Verbrauch', 'Load',       'MWh');
