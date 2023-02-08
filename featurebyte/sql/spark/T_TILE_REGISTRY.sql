CREATE TABLE TILE_REGISTRY (
  TILE_ID VARCHAR(16777216),
  TILE_SQL VARCHAR(16777216),
  ENTITY_COLUMN_NAMES VARCHAR(16777216),
  VALUE_COLUMN_NAMES VARCHAR(16777216),
  VALUE_COLUMN_TYPES VARCHAR(16777216),
  FREQUENCY_MINUTE INT,
  TIME_MODULO_FREQUENCY_SECOND INT,
  BLIND_SPOT_SECOND INT,
  IS_ENABLED BOOLEAN,

  LAST_TILE_START_DATE_ONLINE TIMESTAMP,
  LAST_TILE_INDEX_ONLINE INT,

  LAST_TILE_START_DATE_OFFLINE TIMESTAMP,
  LAST_TILE_INDEX_OFFLINE INT,

  CREATED_AT TIMESTAMP
);
