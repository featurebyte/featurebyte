CREATE TABLE IF NOT EXISTS TILE_MONITOR_SUMMARY (
  TILE_ID STRING,
  TILE_START_DATE TIMESTAMP,
  TILE_TYPE STRING,
  CREATED_AT TIMESTAMP
) USING DELTA PARTITIONED BY (TILE_ID, TILE_TYPE);
