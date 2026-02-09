CREATE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT DISTINCT
  CAST("cust_id" AS BIGINT) AS "cust_id",
  "col_text" AS "another_key"
FROM (
  SELECT
    "col_text" AS "col_text",
    "cust_id" AS "cust_id"
  FROM "sf_database"."sf_schema"."sf_table"
  WHERE
    "event_timestamp" >= CAST(FLOOR(
      (
        EXTRACT(epoch_second FROM CAST(CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP)) - 300
      ) / 1800
    ) * 1800 + 300 - 600 - 86400 AS TIMESTAMP)
    AND "event_timestamp" < CAST(FLOOR(
      (
        EXTRACT(epoch_second FROM CAST(CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP)) - 300
      ) / 1800
    ) * 1800 + 300 - 600 AS TIMESTAMP)
)
WHERE
  NOT "cust_id" IS NULL AND NOT "col_text" IS NULL;

CREATE OR REPLACE TABLE "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS
SELECT
  ROW_NUMBER() OVER (ORDER BY 1) AS "__FB_TABLE_ROW_INDEX",
  *
FROM "TEMP_REQUEST_TABLE_000000000000000000000000";

CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH ONLINE_REQUEST_TABLE AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    REQ."another_key",
    CAST('2022-01-01 00:00:00' AS TIMESTAMP) AS POINT_IN_TIME
  FROM "sf_db"."sf_schema"."TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
), "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id_another_key" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    "another_key",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) - 48 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id",
      "another_key"
    FROM ONLINE_REQUEST_TABLE
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."cust_id",
    REQ."another_key",
    REQ."POINT_IN_TIME",
    "T0"."_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c" AS "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c"
  FROM ONLINE_REQUEST_TABLE AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      "another_key",
      SUM(value_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c) AS "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        REQ."another_key",
        TILE.INDEX,
        TILE.value_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id_another_key" AS REQ
      INNER JOIN __FB_DEPLOYED_TILE_TABLE_000000000000000000000000 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
        AND REQ."another_key" = TILE."col_text"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        REQ."another_key",
        TILE.INDEX,
        TILE.value_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id_another_key" AS REQ
      INNER JOIN __FB_DEPLOYED_TILE_TABLE_000000000000000000000000 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
        AND REQ."another_key" = TILE."col_text"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "cust_id",
      "another_key"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME"
    AND REQ."cust_id" = T0."cust_id"
    AND REQ."another_key" = T0."another_key"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."cust_id",
  AGG."another_key",
  CAST("_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c" AS DOUBLE) AS "composite_entity_feature_1d_V220101",
  CAST((
    "_fb_internal_cust_id_another_key_window_w86400_sum_3d9184a92eb53a42a18b2fa8015e8dd8de52854c" + 123
  ) AS DOUBLE) AS "composite_entity_feature_1d_plus_123_V220101"
FROM _FB_AGGREGATED AS AGG;

CREATE TABLE "sf_db"."sf_schema"."TEMP_FEATURE_TABLE_000000000000000000000000" AS
SELECT
  REQ."cust_id",
  REQ."another_key",
  T0."composite_entity_feature_1d_V220101",
  T0."composite_entity_feature_1d_plus_123_V220101",
  COALESCE(
    CONCAT(CAST(REQ."cust_id" AS VARCHAR), '::', CAST(REQ."another_key" AS VARCHAR)),
    ''
  ) AS "cust_id x another_key"
FROM "TEMP_REQUEST_TABLE_000000000000000000000000" AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX";
