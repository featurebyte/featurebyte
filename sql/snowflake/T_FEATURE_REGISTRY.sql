CREATE OR REPLACE TABLE FEATURE_REGISTRY (
  ID INT AUTOINCREMENT,

  NAME VARCHAR,
  VERSION VARCHAR,
  IS_DEFAULT BOOLEAN DEFAULT True,
  STATUS VARCHAR DEFAULT 'DRAFT',

  FREQUENCY_MINUTES INT,
  TIME_MODULO_FREQUENCY_SECOND INT,
  BLIND_SPOT_SECOND INT,
  TILE_SQL VARCHAR,
  COLUMN_NAMES VARCHAR,
  TILE_ID VARCHAR,
  ONLINE_ENABLED BOOLEAN DEFAULT False,

  CREATED_AT TIMESTAMP DEFAULT SYSDATE()
);
