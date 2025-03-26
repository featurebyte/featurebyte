WITH __FB_ENTITY_TABLE_NAME AS (
  __FB_ENTITY_TABLE_SQL_PLACEHOLDER
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      "event_timestamp" AS "event_timestamp",
      "cust_id" AS "cust_id",
      "col_float" AS "input_col_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
      "col_int" AS "input_col_avg_7878f6dd82c857a14e65c8c50286995e4ca267ec"
    FROM "sf_database"."sf_schema"."sf_table"
  ) AS R
    ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
    AND R."event_timestamp" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."event_timestamp" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "cust_id",
  SUM("input_col_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295") AS value_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295,
  SUM("input_col_avg_7878f6dd82c857a14e65c8c50286995e4ca267ec") AS sum_value_avg_7878f6dd82c857a14e65c8c50286995e4ca267ec,
  COUNT("input_col_avg_7878f6dd82c857a14e65c8c50286995e4ca267ec") AS count_value_avg_7878f6dd82c857a14e65c8c50286995e4ca267ec
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP), 300, 600, 30) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "cust_id"
