CREATE TABLE "__TEMP_000000000000000000000000_0" AS
WITH "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS (
  SELECT
    "POINT_IN_TIME",
    "cust_id",
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) AS __FB_LAST_TILE_INDEX,
    CAST(FLOOR((
      DATE_PART(EPOCH_SECOND, "POINT_IN_TIME") - 300
    ) / 1800) AS BIGINT) - 48 AS __FB_FIRST_TILE_INDEX
  FROM (
    SELECT DISTINCT
      "POINT_IN_TIME",
      "cust_id"
    FROM my_request_table
  )
), _FB_AGGREGATED AS (
  SELECT
    REQ."__FB_TABLE_ROW_INDEX",
    REQ."POINT_IN_TIME",
    REQ."cust_id",
    "T0"."_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
  FROM my_request_table AS REQ
  LEFT JOIN (
    SELECT
      "POINT_IN_TIME",
      "cust_id",
      SUM(value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295) AS "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295"
    FROM (
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
      UNION ALL
      SELECT
        REQ."POINT_IN_TIME",
        REQ."cust_id",
        TILE.INDEX,
        TILE.value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295
      FROM "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS REQ
      INNER JOIN TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295 AS TILE
        ON FLOOR(REQ.__FB_LAST_TILE_INDEX / 48) - 1 = FLOOR(TILE.INDEX / 48)
        AND REQ."cust_id" = TILE."cust_id"
      WHERE
        TILE.INDEX >= REQ.__FB_FIRST_TILE_INDEX AND TILE.INDEX < REQ.__FB_LAST_TILE_INDEX
    )
    GROUP BY
      "POINT_IN_TIME",
      "cust_id"
  ) AS T0
    ON REQ."POINT_IN_TIME" = T0."POINT_IN_TIME" AND REQ."cust_id" = T0."cust_id"
)
SELECT
  AGG."__FB_TABLE_ROW_INDEX",
  AGG."POINT_IN_TIME",
  AGG."cust_id",
  CAST("_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "sum_1d",
  CAST((
    "_fb_internal_cust_id_window_w86400_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" + 123
  ) AS DOUBLE) AS "another_feature"
FROM _FB_AGGREGATED AS AGG;

SELECT
  COUNT(DISTINCT "__FB_TABLE_ROW_INDEX") = COUNT(*) AS "is_row_index_valid"
FROM "__TEMP_000000000000000000000000_0";

CREATE TABLE "output_table" AS
SELECT
  REQ."__FB_TABLE_ROW_INDEX",
  REQ."a",
  REQ."b",
  REQ."c",
  T0."sum_1d",
  T0."another_feature"
FROM "request_table" AS REQ
LEFT JOIN "__TEMP_000000000000000000000000_0" AS T0
  ON REQ."__FB_TABLE_ROW_INDEX" = T0."__FB_TABLE_ROW_INDEX";

SELECT
  COUNT(DISTINCT "__FB_TABLE_ROW_INDEX") = COUNT(*) AS "is_row_index_valid"
FROM "output_table";
