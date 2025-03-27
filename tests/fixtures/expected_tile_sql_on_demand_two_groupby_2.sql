WITH __FB_ENTITY_TABLE_NAME AS (
  __FB_ENTITY_TABLE_SQL_PLACEHOLDER
), __FB_TILE_COMPUTE_INPUT_TABLE_NAME AS (
  SELECT
    R.*
  FROM __FB_ENTITY_TABLE_NAME
  INNER JOIN (
    SELECT
      "ts" AS "ts",
      "biz_id" AS "biz_id",
      "a" AS "input_col_sum_8c11e770ad5121aec588693662ac607b4fba0528"
    FROM "db"."public"."event_table"
  ) AS R
    ON R."biz_id" = __FB_ENTITY_TABLE_NAME."biz_id"
    AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
    AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
)
SELECT
  index,
  "biz_id",
  SUM("input_col_sum_8c11e770ad5121aec588693662ac607b4fba0528") AS value_sum_8c11e770ad5121aec588693662ac607b4fba0528
FROM (
  SELECT
    *,
    F_TIMESTAMP_TO_INDEX(CAST(CONVERT_TIMEZONE('UTC', "ts") AS TIMESTAMP), 1800, 900, 60) AS index
  FROM __FB_TILE_COMPUTE_INPUT_TABLE_NAME
)
GROUP BY
  index,
  "biz_id"
