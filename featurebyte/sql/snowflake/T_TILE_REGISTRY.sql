CREATE TABLE TILE_REGISTRY (
  TILE_ID VARCHAR,
  TILE_SQL VARCHAR,
  COLUMN_NAMES VARCHAR,
  ENTITY_COLUMN_NAMES VARCHAR,
  VALUE_COLUMN_NAMES VARCHAR,
  FREQUENCY_MINUTE INT,
  TIME_MODULO_FREQUENCY_SECOND INT,
  BLIND_SPOT_SECOND INT,
  IS_ENABLED BOOLEAN DEFAULT True,

  LAST_TILE_START_DATE_ONLINE TIMESTAMP_TZ,
  LAST_TILE_INDEX_ONLINE INT DEFAULT -1,

  LAST_TILE_START_DATE_OFFLINE TIMESTAMP_TZ,
  LAST_TILE_INDEX_OFFLINE INT DEFAULT -1,

  CREATED_AT TIMESTAMP DEFAULT SYSDATE()
);
